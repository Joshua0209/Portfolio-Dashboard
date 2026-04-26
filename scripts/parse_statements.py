#!/usr/bin/env python3
"""Parse Sinopac PDF statements into a transaction-level portfolio.json.

Boundary (per user instruction):
  - "Cash" side  = TWD bank account only.
  - "Investment" = TWD broker + foreign broker + foreign holdings + USD bank cash.

So every TWD bank line tagged 股票款 / 折讓款 / 手機換匯 is an investment cashflow.
Salary, peer transfers, TWD interest stay on the cash side.

KPIs:
  Real_now           = bank_twd_now + brokerage_tw_mv + brokerage_foreign_mv_twd + bank_usd_in_twd
  Counterfactual_twd = bank_twd_now + Σ(investment outflow from TWD bank)
                                    − Σ(investment inflow to TWD bank)
  Profit             = Real_now − Counterfactual_twd

Outputs: data/portfolio.json
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

import pdfplumber

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "sinopac_pdfs" / "decrypted"
OUT = ROOT / "data" / "portfolio.json"
TW_TICKER_MAP_FILE = ROOT / "data" / "tw_ticker_map.json"

NUM = r"-?[\d,]+(?:\.\d+)?"


def num(s: str) -> float:
    return float(s.replace(",", ""))


def normalize_tw_name(s: str | None) -> str:
    """Fullwidth → halfwidth fold so trade names ('台灣５０', '國巨＊') match
    holdings names ('台灣50', '國巨*') for ticker-code lookup."""
    if not s:
        return ""
    out = []
    for c in s:
        cp = ord(c)
        if 0xFF01 <= cp <= 0xFF5E:
            out.append(chr(cp - 0xFEE0))
        elif c == "＊":
            out.append("*")
        else:
            out.append(c)
    return "".join(out).strip()


def build_tw_name_to_code(months: list[dict]) -> dict[str, str]:
    """Map every TW trade name we can resolve to its ticker code.

    Two layered sources:
    1. Holdings tables across all parsed months (authoritative — the PDF
       holdings section prints both name and code).
    2. data/tw_ticker_map.json — manual overrides for names that never appear
       in any month-end holdings (intra-month round-trips, pre-window exits).

    Disambiguation: exact normalized-name match preferred, then a guarded
    prefix match (holding name starts with trade name, length floor 3, ratio
    cap 2.5×) so '致茂' doesn't inherit '致茂富邦57購'/042900 and '台灣50'
    doesn't fuse with '元大台灣50正'/00631L.
    """
    holdings_by_name: dict[str, str] = {}
    for m in months:
        for h in (m.get("tw") or {}).get("holdings", []) or []:
            n = normalize_tw_name(h.get("name"))
            code = h.get("code")
            if n and code:
                holdings_by_name.setdefault(n, code)

    overrides: dict[str, str] = {}
    if TW_TICKER_MAP_FILE.exists():
        raw = json.loads(TW_TICKER_MAP_FILE.read_text())
        for k, v in raw.items():
            if k.startswith("_") or not v:
                continue
            overrides[normalize_tw_name(k)] = str(v)

    return holdings_by_name | overrides   # overrides win


def resolve_tw_code(trade_name: str, name_to_code: dict[str, str]) -> str:
    """Look up a ticker code for a TW trade row name. Returns '' on no match."""
    n = normalize_tw_name(trade_name)
    if not n:
        return ""
    if n in name_to_code:
        return name_to_code[n]
    if len(n) < 3:
        return ""
    for hn, code in name_to_code.items():
        if hn.startswith(n) and len(hn) / len(n) < 2.5:
            return code
    return ""


# ---------------------------------------------------------------------------
# TW securities parser
# ---------------------------------------------------------------------------
# 證券交易明細 column headers (two-row, type-dependent):
#   普買/櫃買 (cash buy)    : date side name qty price gross fee 客戶應付
#   普賣/櫃賣 (cash sell)   : date side name qty price gross fee tax 客戶應收
#   資買 (margin buy)       : date side name qty price gross fee 起息日 融資金額 資自備款 客戶應付
#   資賣 (margin sell)      : date side name qty price gross fee tax 起息日 融資金額 擔保價款 客戶應收
TRADE_BUY_RE = re.compile(
    rf"^(\d{{4}}/\d{{2}}/\d{{2}})\s+(普買|櫃買)\s+(.+?)\s+({NUM})\s+({NUM})\s+({NUM})\s+({NUM})\s+({NUM})\s*$"
)
TRADE_SELL_RE = re.compile(
    rf"^(\d{{4}}/\d{{2}}/\d{{2}})\s+(普賣|櫃賣)\s+(.+?)\s+({NUM})\s+({NUM})\s+({NUM})\s+({NUM})\s+({NUM})\s+({NUM})\s*$"
)
TRADE_MARGIN_BUY_RE = re.compile(
    rf"^(\d{{4}}/\d{{2}}/\d{{2}})\s+(資買)\s+(.+?)\s+({NUM})\s+({NUM})\s+({NUM})\s+({NUM})\s+(\d{{4}}/\d{{2}}/\d{{2}})\s+({NUM})\s+({NUM})\s+({NUM})\s*$"
)
TRADE_MARGIN_SELL_RE = re.compile(
    rf"^(\d{{4}}/\d{{2}}/\d{{2}})\s+(資賣)\s+(.+?)\s+({NUM})\s+({NUM})\s+({NUM})\s+({NUM})\s+({NUM})\s+(\d{{4}}/\d{{2}}/\d{{2}})\s+({NUM})\s+({NUM})\s+({NUM})\s*$"
)


def parse_securities(pdf_path: Path) -> dict:
    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join((p.extract_text() or "") for p in pdf.pages)

    period = re.search(r"成交年月：(\d{6})", text).group(1)
    ym = f"{period[:4]}-{period[4:]}"

    # rebates (top of statement)
    rebates: list[dict] = []
    for label in ("電子折讓金", "一般折讓金"):
        m = re.search(rf"{label}：\s*({NUM})", text)
        if m and num(m.group(1)) != 0:
            rebates.append({"type": label, "amount_twd": num(m.group(1))})

    # holdings
    holdings: list[dict] = []
    subtotal: dict = {}
    in_holdings = False
    in_trades = False
    trades: list[dict] = []

    for raw in text.splitlines():
        line = raw.strip()
        if "證券庫存" in line and "證券交易明細" not in line:
            in_holdings, in_trades = True, False
            continue
        if "證券交易明細" in line:
            in_holdings, in_trades = False, True
            continue
        if "客戶淨收付" in line or "電子折讓金額明細" in line:
            in_holdings = in_trades = False

        if in_holdings:
            m = re.match(
                rf"^(現股|融資|融券)\s+(\S+)\s+(.+?)\s+({NUM})\s+({NUM})\s+({NUM})\s+({NUM})\s+({NUM})\s+({NUM})\s+(-?[\d.]+)%\s+({NUM})\s+({NUM})\s+(-?[\d.]+)%\s*$",
                line,
            )
            if m:
                holdings.append({
                    "type": m.group(1),
                    "code": m.group(2),
                    "name": m.group(3).strip(),
                    "qty": num(m.group(4)),
                    "avg_cost": num(m.group(5)),
                    "cost": num(m.group(6)),
                    "ref_price": num(m.group(7)),
                    "mkt_value": num(m.group(8)),
                    "unrealized_pnl": num(m.group(9)),
                    "unrealized_pct": float(m.group(10)) / 100.0,
                    # Sinopac prints accumulated cash dividend per holding plus a
                    # with-dividend P&L pair. Total-return analytics use these.
                    "cum_dividend": num(m.group(11)),
                    "unrealized_pnl_with_div": num(m.group(12)),
                    "unrealized_pct_with_div": float(m.group(13)) / 100.0,
                })
                continue
            sm = re.match(
                rf"^小計\s+({NUM})\s+({NUM})\s+({NUM})\s+({NUM})\s+(-?[\d.]+)%",
                line,
            )
            if sm:
                subtotal = {
                    "qty": num(sm.group(1)),
                    "cost": num(sm.group(2)),
                    "mkt_value": num(sm.group(3)),
                    "unrealized_pnl": num(sm.group(4)),
                }

        if in_trades:
            tr = _parse_tw_trade(line)
            if tr:
                trades.append(tr)

    # 客戶淨收付：幣別：臺幣 X (broker side: negative = client paid broker)
    cf = re.search(rf"客戶淨收付：\s*幣別：臺幣\s*({NUM})", text)
    net_cashflow = num(cf.group(1)) if cf else 0.0

    return {
        "month": ym,
        "holdings": holdings,
        "subtotal": subtotal,
        "trades": trades,
        "rebates": rebates,
        "net_cashflow_twd": net_cashflow,
    }


def _parse_tw_trade(line: str) -> dict | None:
    m = TRADE_BUY_RE.match(line)
    if m:
        return {
            "date": m.group(1), "side": m.group(2), "name": m.group(3).strip(),
            "qty": num(m.group(4)), "price": num(m.group(5)),
            "gross": num(m.group(6)), "fee": num(m.group(7)), "tax": 0.0,
            "self_funded": num(m.group(8)),  # 客戶應付
            "margin_loan": 0.0,
            "net_twd": -num(m.group(8)),     # cash out from client
        }
    m = TRADE_SELL_RE.match(line)
    if m:
        return {
            "date": m.group(1), "side": m.group(2), "name": m.group(3).strip(),
            "qty": num(m.group(4)), "price": num(m.group(5)),
            "gross": num(m.group(6)), "fee": num(m.group(7)), "tax": num(m.group(8)),
            "self_funded": 0.0, "margin_loan": 0.0,
            "net_twd": num(m.group(9)),      # cash in to client (客戶應收)
        }
    m = TRADE_MARGIN_BUY_RE.match(line)
    if m:
        return {
            "date": m.group(1), "side": m.group(2), "name": m.group(3).strip(),
            "qty": num(m.group(4)), "price": num(m.group(5)),
            "gross": num(m.group(6)), "fee": num(m.group(7)), "tax": 0.0,
            "interest_start": m.group(8),
            "margin_loan": num(m.group(9)),     # 融資金額
            "self_funded": num(m.group(10)),    # 資自備款
            "net_twd": -num(m.group(11)),       # 客戶應付 = self_funded + fee
        }
    m = TRADE_MARGIN_SELL_RE.match(line)
    if m:
        return {
            "date": m.group(1), "side": m.group(2), "name": m.group(3).strip(),
            "qty": num(m.group(4)), "price": num(m.group(5)),
            "gross": num(m.group(6)), "fee": num(m.group(7)), "tax": num(m.group(8)),
            "interest_start": m.group(9),
            "margin_loan": num(m.group(10)),
            "collateral": num(m.group(11)),
            "net_twd": num(m.group(12)),
        }
    return None


# ---------------------------------------------------------------------------
# Foreign (複委託) parser
# ---------------------------------------------------------------------------
def parse_foreign(pdf_path: Path) -> dict:
    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join((p.extract_text() or "") for p in pdf.pages)

    m = re.search(r"對帳單日期：(\d{4})/(\d{2})", text)
    ym = f"{m.group(1)}-{m.group(2)}"

    # Holdings
    sec = re.search(
        r"海外股票庫存及投資損益(.*?)債券商品庫存及投資損益",
        text, re.S,
    )
    holdings: list[dict] = []
    if sec and "無庫存明細" not in sec.group(1):
        for line in sec.group(1).splitlines():
            m = re.match(
                rf"^(\S+)\s+(.+?)\s+(\S+)\s+(\S+)\s+(USD|HKD|JPY)\s+({NUM})\s+(USD|HKD|JPY)\s+({NUM})\s+(\d{{4}}/\d{{2}}/\d{{2}})\s+({NUM})\s+({NUM})\s+(-?[\d,.]+)\s+(-?[\d.]+)%",
                line.strip(),
            )
            if m:
                holdings.append({
                    "code": m.group(1),
                    "name": m.group(2).strip(),
                    "market": m.group(3),
                    "exchange": m.group(4),
                    "ccy": m.group(5),
                    "qty": num(m.group(6)),
                    "cost": num(m.group(8)),
                    "ref_date": m.group(9),
                    "close": num(m.group(10)),
                    "mkt_value": num(m.group(11)),
                    "unrealized_pnl": num(m.group(12)),
                })

    # Trades — every trade row appears as TWO lines (calc-currency line + settlement line).
    trade_sec = re.search(
        r"海外股票交易明細(.*?)(?:債券商品交易明細|複委託基金交易明細|【截至)",
        text, re.S,
    )
    trades: list[dict] = []
    cashflow_by_ccy: dict[str, float] = defaultdict(float)
    if trade_sec and "無交易明細" not in trade_sec.group(1):
        for line in trade_sec.group(1).splitlines():
            tm = re.match(
                rf"^(\d{{4}}/\d{{2}}/\d{{2}})\s+(\S+)\s+(\S+)\s+(\S+)\s+(買進|賣出)\s+(USD|HKD|JPY)\s+({NUM})\s+({NUM})\s+({NUM})\s+({NUM})\s+(?:({NUM})\s+)?(-?[\d,]+\.\d+)\s*$",
                line.strip(),
            )
            if tm:
                ccy = tm.group(6)
                amt = num(tm.group(12))
                trades.append({
                    "date": tm.group(1),
                    "code": tm.group(2),
                    "market": tm.group(3),
                    "exchange": tm.group(4),
                    "side": tm.group(5),
                    "ccy": ccy,
                    "qty": num(tm.group(7)),
                    "price": num(tm.group(8)),
                    "gross": num(tm.group(9)),
                    "fee": num(tm.group(10)),
                    "other_fee": num(tm.group(11)) if tm.group(11) else 0.0,
                    "net_ccy": amt,  # signed: negative = paid (buy), positive = received (sell)
                })
                cashflow_by_ccy[ccy] += amt

    # Dividends — broker section columns:
    #   市場 撥扣日 商品代號 基準日 基準日股數 幣別 分配金額 上手費用 交所費用 券商費用 上手其他 股利淨額 匯率 交割幣別 應收/付(-)金額
    # The middle fee columns are inconsistently filled, but the row reliably
    # ends with: <股利淨額> <匯率> [tail]. We anchor on (NUM) (NUM (with decimal))
    # at the end and walk in: dividend net is the second-to-last number; the
    # last is the FX rate. Earlier code matched '匯率' as the amount.
    div_sec = re.search(r"海外股票現金股利明細(.*?)(?:<|【|$)", text, re.S)
    dividends: list[dict] = []
    if div_sec and "無交易明細" not in div_sec.group(1):
        for line in div_sec.group(1).splitlines():
            line = line.strip()
            dm = re.match(
                rf"^\S+\s+(\d{{4}}/\d{{2}}/\d{{2}})\s+(\S+)\s+\d{{4}}/\d{{2}}/\d{{2}}\s+({NUM})\s+(USD|HKD|JPY)\s+(.+)$",
                line,
            )
            if not dm:
                continue
            tail_nums = re.findall(NUM, dm.group(5))
            if len(tail_nums) < 2:
                continue
            # Last number is FX rate; the one before it is 股利淨額 (net dividend).
            net_amount = num(tail_nums[-2])
            dividends.append({
                "date": dm.group(1),
                "code": dm.group(2),
                "qty": num(dm.group(3)),
                "ccy": dm.group(4),
                "net_amount": net_amount,
            })
            cashflow_by_ccy[dm.group(4)] += net_amount

    return {
        "month": ym,
        "holdings": holdings,
        "trades": trades,
        "dividends": dividends,
        "cashflow_by_ccy": dict(cashflow_by_ccy),
    }


# ---------------------------------------------------------------------------
# Bank parser — FX rates, balances, AND every transaction line.
# ---------------------------------------------------------------------------
ACCOUNT_HEADER_RE = re.compile(r"帳號:\s*(\S+)\s*\((新臺幣|美元|.+?)\)")
TX_LINE_RE = re.compile(
    rf"^(\d{{4}}/\d{{2}}/\d{{2}})\s+(\S+?)\s+({NUM})\s+({NUM})\s*(.*)$"
)
# Some lines have only one amount (debit-only or credit-only). Bank uses 3 numeric cols:
# 支出 / 存入 / 餘額. We'll use a more permissive parser by walking from the right.


# Money tokens: comma-formatted (e.g. 1,234) or decimal (e.g. 1234.56) or small int (<8 digits).
# This deliberately rejects long digit strings (account numbers, stock codes have ≥10 digits).
MONEY_TOKEN_RE = re.compile(
    r"(?<![\w.])(?:\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{1,7}(?:\.\d+)?)(?![\w])"
)


def _parse_bank_tx_line(line: str) -> dict | None:
    """Bank tx line: `YYYY/MM/DD <summary> <amount> <balance> [memo]`.
    Memo can contain long digit strings (account numbers, stock codes); those
    are excluded by MONEY_TOKEN_RE which requires comma-grouped or decimal form
    for >7-digit numbers.
    """
    m = re.match(r"^(\d{4}/\d{2}/\d{2})\s+(.+)$", line.strip())
    if not m:
        return None
    date, rest = m.group(1), m.group(2)

    nums = list(MONEY_TOKEN_RE.finditer(rest))
    if len(nums) < 2:
        return None
    # Take the FIRST two money tokens after the summary word — the bank's
    # column order is amount, balance, then optional memo.
    amount = num(nums[0].group())
    balance = num(nums[1].group())
    summary = rest[:nums[0].start()].strip()
    memo = rest[nums[1].end():].strip()

    return {
        "date": date,
        "summary": summary,
        "amount": amount,
        "balance": balance,
        "memo": memo,
    }


# Categorization: which TWD bank txs are "investment" and which way the cash moves.
INVESTMENT_KEYWORDS = ("股票款", "折讓款", "股款交割", "手機換匯")
PERSONAL_KEYWORDS = ("薪資入帳", "利息存入", "跨行轉帳", "手機轉帳", "ATM", "現金")


def _categorize(summary: str) -> str:
    if "ACH" in summary.upper():     # ACH股息 — TW listed-stock dividend via 集保
        return "tw_dividend"
    if "國外股息" in summary or "海外股息" in summary:
        return "foreign_dividend"    # USD-bank credit from foreign-broker dividend
    if "股票款" in summary or "預扣股款" in summary:
        return "stock_settle_tw"     # TW broker stock settlement (bank-side aggregate)
    if "折讓款" in summary:
        return "rebate"              # broker rebate
    if "股款交割" in summary:
        return "stock_settle_fx"     # foreign broker settlement (USD account)
    if "手機換匯" in summary:
        return "fx_convert"          # TWD <-> USD
    if "薪資" in summary:
        return "salary"
    if "利息存入" in summary:
        return "interest"
    if "手機轉帳" in summary or "跨行轉帳" in summary:
        return "transfer"            # regular peer transfers
    if summary.strip() == "轉帳":
        return "stock_settle_tw"     # bare 轉帳 = refund of 預扣股款 (edge case)
    return "other"


def parse_bank(pdf_path: Path) -> dict:
    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join((p.extract_text() or "") for p in pdf.pages)

    m = re.search(r"對帳單期間：(\d{4})/(\d{2})/", text)
    ym = f"{m.group(1)}-{m.group(2)}"

    fx: dict[str, float] = {}
    for line in text.splitlines():
        fm = re.match(
            rf"^\S*\(([A-Z]{{3}})\)\s+({NUM})\s+([\d.]+)\s+({NUM})\s*$",
            line.strip(),
        )
        if fm:
            fx[fm.group(1)] = float(fm.group(3))

    # Balances
    total_cash = twd_cash = foreign_cash = 0.0
    tm = re.search(rf"存款\s+({NUM})\s*\n\s*臺幣\s+({NUM})\s*\n\s*外幣\s+({NUM})", text)
    if tm:
        total_cash = num(tm.group(1))
        twd_cash = num(tm.group(2))
        foreign_cash = num(tm.group(3))

    # Transactions: walk line-by-line, tracking which account section we're in.
    tx_twd: list[dict] = []
    tx_foreign: list[dict] = []
    current_acct: str | None = None     # "TWD" or "FOREIGN"
    current_ccy: str | None = None

    # Per-account balance tracking to infer debit vs credit:
    # 永豐's 餘額 column tells us direction — if balance went UP, it was a credit (存入);
    # if DOWN, it was a debit (支出). We track previous balance per account.
    prev_balance: dict[str, float | None] = {"TWD": None, "FOREIGN": None}

    for raw in text.splitlines():
        line = raw.strip()
        ah = ACCOUNT_HEADER_RE.search(line)
        if ah:
            ccy_label = ah.group(2)
            if ccy_label == "新臺幣":
                current_acct, current_ccy = "TWD", "TWD"
            elif ccy_label == "美元":
                current_acct, current_ccy = "FOREIGN", "USD"
            else:
                # other foreign ccy, treat as FOREIGN
                current_acct = "FOREIGN"
                # try to map label → 3-letter
                current_ccy = {"港幣": "HKD", "日圓": "JPY", "歐元": "EUR"}.get(
                    ccy_label, ccy_label
                )
            continue
        if current_acct is None:
            continue
        if "交易日" in line and "摘要" in line:
            continue
        if "頁" in line and "綜合對帳單" in line:
            continue  # page header
        if "保險" in line or "信託" in line or "貸款" in line:
            current_acct = None
            continue

        tx = _parse_bank_tx_line(line)
        if not tx:
            continue

        # Direction inference from balance delta
        prev = prev_balance[current_acct]
        amt = tx["amount"]
        bal = tx["balance"]
        if prev is None:
            # First tx in this account: we cannot infer; assume debit if summary
            # implies outflow. This is rare — most months have several txs.
            signed = -amt if any(k in tx["summary"] for k in ("股票款", "手機換匯", "股款交割")) else amt
        else:
            delta = bal - prev
            if abs(delta - amt) < abs(delta + amt):
                signed = amt        # balance went up → credit (in)
            else:
                signed = -amt       # balance went down → debit (out)
        prev_balance[current_acct] = bal

        tx["signed_amount"] = signed
        tx["category"] = _categorize(tx["summary"])
        tx["ccy"] = current_ccy

        if current_acct == "TWD":
            tx_twd.append(tx)
        else:
            tx_foreign.append(tx)

    return {
        "month": ym,
        "fx": fx,
        "cash_total_twd": total_cash,
        "cash_twd": twd_cash,
        "cash_foreign_twd": foreign_cash,
        "tx_twd": tx_twd,
        "tx_foreign": tx_foreign,
    }


# ---------------------------------------------------------------------------
# Aggregate
# ---------------------------------------------------------------------------
def _rederive_bank_signs(parsed: list[tuple[str, dict]]) -> None:
    """Re-sign bank txs in chronological order across months.

    `parse_bank` runs per-PDF and falls back to a keyword heuristic for the
    first tx of each month (no prior balance to compare against). That heuristic
    flips signs whenever the first tx is the opposite of what its keyword
    suggests — e.g. a sell settlement on month 1 day 1 looks like an outflow
    because 股票款 also covers buys. We carry the prior month's last balance
    into the next month and recompute every tx's sign as `bal_after − bal_before`.
    """
    prev_bal: dict[str, float | None] = {"TWD": None, "FOREIGN": None}
    for _, p in parsed:
        bank = p["bank"]
        if not bank:
            continue
        for acct, key in (("TWD", "tx_twd"), ("FOREIGN", "tx_foreign")):
            for tx in bank.get(key, []):
                bal = tx["balance"]
                amt = tx["amount"]
                prev = prev_bal[acct]
                if prev is None:
                    # Very first tx ever for this account: compare bal vs amt —
                    # if bal == amt, opening was 0 and tx is a credit; otherwise
                    # fall back to summary heuristic.
                    if abs(bal - amt) < 0.01:
                        signed = amt
                    else:
                        signed = -amt if any(
                            k in tx["summary"] for k in ("股票款", "手機換匯", "股款交割")
                        ) else amt
                else:
                    delta = bal - prev
                    signed = amt if abs(delta - amt) < abs(delta + amt) else -amt
                tx["signed_amount"] = signed
                prev_bal[acct] = bal


def main() -> int:
    files_by_month: dict[str, dict] = defaultdict(dict)
    for p in sorted(SRC.glob("*.pdf")):
        ym_m = re.match(r"(\d{4})-(\d{2})_", p.name)
        if not ym_m:
            continue
        ym = f"{ym_m.group(1)}-{ym_m.group(2)}"
        if "證券月對帳單" in p.name:
            files_by_month[ym]["securities"] = p
        elif "複委託" in p.name:
            files_by_month[ym]["foreign"] = p
        elif "銀行綜合" in p.name:
            files_by_month[ym]["bank"] = p

    # Pass 1: parse every PDF.
    parsed: list[tuple[str, dict]] = []
    for ym in sorted(files_by_month):
        f = files_by_month[ym]
        sec = parse_securities(f["securities"]) if "securities" in f else None
        fgn = parse_foreign(f["foreign"]) if "foreign" in f else None
        bank = parse_bank(f["bank"]) if "bank" in f else None
        parsed.append((ym, {"sec": sec, "fgn": fgn, "bank": bank}))

    # Pass 2: re-derive signed_amount on bank txs using running balance chained
    # across months. This fixes the per-month heuristic which mis-signs the first
    # tx of each month when it can't infer direction from a prior balance.
    _rederive_bank_signs(parsed)

    months_out = []
    for ym, p in parsed:
        sec, fgn, bank = p["sec"], p["fgn"], p["bank"]

        usd_twd = (bank["fx"].get("USD") if bank else None) or 0.0

        foreign_mv_twd = 0.0
        if fgn:
            for h in fgn["holdings"]:
                if h["ccy"] == "USD":
                    foreign_mv_twd += h["mkt_value"] * usd_twd

        tw_mv = (sec["subtotal"].get("mkt_value") if sec and sec["subtotal"] else 0.0) or 0.0
        equity_twd = tw_mv + foreign_mv_twd

        # Investment cashflow tagged from BANK side (ground truth for counterfactual).
        inv_flow = _bank_investment_flows(bank) if bank else _empty_flows()

        # Legacy broker-side flow (kept for TWR computation continuity).
        tw_flow_twd = -(sec["net_cashflow_twd"]) if sec else 0.0
        foreign_flow_twd = 0.0
        if fgn:
            for ccy, amt in fgn["cashflow_by_ccy"].items():
                rate = bank["fx"].get(ccy, 0.0) if bank else 0.0
                foreign_flow_twd += -amt * rate
        external_flow_twd = tw_flow_twd + foreign_flow_twd

        # Per-event dividends from bank statements (TW + foreign). The broker
        # PDFs leave 海外股票現金股利明細 empty almost every month — the bank
        # statement is the ground truth for actual dividend cash received,
        # with the ticker symbol or stock name in the memo column.
        div_events = _extract_dividend_events(bank, usd_twd) if bank else []

        months_out.append({
            "month": ym,
            "fx_usd_twd": usd_twd,
            "tw": sec or {},
            "foreign": fgn or {},
            "bank": bank or {},
            "tw_market_value_twd": tw_mv,
            "foreign_market_value_twd": foreign_mv_twd,
            "bank_usd_in_twd": (bank["cash_foreign_twd"] if bank else 0.0),
            "bank_twd": (bank["cash_twd"] if bank else 0.0),
            "equity_twd": equity_twd,
            "external_flow_twd": external_flow_twd,
            "investment_flows_twd": inv_flow,
            "dividend_events": div_events,
        })

    months_out = compute_performance(months_out)
    summary = build_summary(months_out)

    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(
        {"months": months_out, "summary": summary},
        ensure_ascii=False, indent=2,
    ))
    print(f"Wrote {OUT} with {len(months_out)} months")
    print(f"  Real_now           = NT$ {summary['kpis']['real_now_twd']:>14,.0f}")
    print(f"  Counterfactual_twd = NT$ {summary['kpis']['counterfactual_twd']:>14,.0f}")
    print(f"  Profit             = NT$ {summary['kpis']['profit_twd']:>14,.0f}")
    return 0


def _extract_dividend_events(bank: dict, fx_usd_twd: float) -> list[dict]:
    """Per-event dividends mined from bank ledger.

    TWD bank ACH股息: ticker name appears in memo column (e.g. '台積電').
    Foreign 國外股息: ticker symbol in memo (e.g. 'GOOGL', 'NVDA').

    Both account-currency columns are credits (positive signed_amount). The
    `amount` is local currency; we also add a TWD-equivalent for aggregation.
    """
    out: list[dict] = []
    for tx in bank.get("tx_twd", []) or []:
        if tx.get("category") != "tw_dividend":
            continue
        memo = (tx.get("memo") or "").strip()
        amt = abs(tx.get("signed_amount") or tx.get("amount") or 0)
        out.append({
            "date": tx["date"],
            "venue": "TW",
            "ccy": "TWD",
            "name": memo or "TW dividend",
            "code": "",  # back-filled later via name→code map
            "amount_local": amt,
            "amount_twd": amt,
        })
    rate = fx_usd_twd or 1.0
    for tx in bank.get("tx_foreign", []) or []:
        if tx.get("category") != "foreign_dividend":
            continue
        memo = (tx.get("memo") or "").strip()
        amt = abs(tx.get("signed_amount") or tx.get("amount") or 0)
        ccy = tx.get("ccy") or "USD"
        twd = amt * rate if ccy == "USD" else 0.0
        out.append({
            "date": tx["date"],
            "venue": "Foreign",
            "ccy": ccy,
            "name": memo or "Foreign dividend",
            "code": memo,  # for foreign, memo is the symbol itself
            "amount_local": amt,
            "amount_twd": twd,
        })
    return out


def _empty_flows() -> dict:
    return {
        "stock_buy_twd": 0.0, "stock_sell_twd": 0.0,
        "rebate_in_twd": 0.0,
        "tw_dividend_in_twd": 0.0,
        "fx_to_usd_twd": 0.0, "fx_to_twd_twd": 0.0,
        "salary_in_twd": 0.0, "transfer_net_twd": 0.0, "interest_in_twd": 0.0,
    }


def _bank_investment_flows(bank: dict) -> dict:
    """Sum bank TWD account by category. Sign: + = inflow to TWD bank, − = outflow."""
    flows = _empty_flows()
    for tx in bank.get("tx_twd", []):
        s = tx["signed_amount"]
        cat = tx["category"]
        if cat == "stock_settle_tw":
            if s < 0: flows["stock_buy_twd"] += -s   # outflow magnitude
            else:     flows["stock_sell_twd"] += s
        elif cat == "rebate":
            flows["rebate_in_twd"] += s
        elif cat == "tw_dividend":
            flows["tw_dividend_in_twd"] += s
        elif cat == "fx_convert":
            if s < 0: flows["fx_to_usd_twd"] += -s   # TWD → USD (outflow from TWD)
            else:     flows["fx_to_twd_twd"] += s
        elif cat == "salary":
            flows["salary_in_twd"] += s
        elif cat == "transfer":
            flows["transfer_net_twd"] += s
        elif cat == "interest":
            flows["interest_in_twd"] += s
    return flows


def build_summary(months: list[dict]) -> dict:
    if not months:
        return {
            "kpis": {}, "totals": {}, "all_trades": [], "by_ticker": {},
            "dividends": [], "venue_flows_twd": [],
        }

    last = months[-1]

    # Cumulative investment-related TWD flows
    cum = _empty_flows()
    for m in months:
        f = m["investment_flows_twd"]
        for k in cum:
            cum[k] += f[k]

    bank_twd_now = last["bank_twd"]
    bank_usd_in_twd_now = last["bank_usd_in_twd"]
    brokerage_equity = last["equity_twd"]

    real_now = bank_twd_now + brokerage_equity + bank_usd_in_twd_now

    # Counterfactual: take TWD bank now, undo every investment-tagged tx.
    # Outflow (stock buy / fx→usd) ⇒ would have stayed in TWD bank ⇒ ADD back.
    # Inflow (stock sell / rebate / fx→twd / dividend) ⇒ wouldn't have arrived ⇒ SUBTRACT.
    counterfactual = (
        bank_twd_now
        + cum["stock_buy_twd"]
        - cum["stock_sell_twd"]
        - cum["rebate_in_twd"]
        - cum["tw_dividend_in_twd"]
        + cum["fx_to_usd_twd"]
        - cum["fx_to_twd_twd"]
    )

    profit = real_now - counterfactual

    tw_name_to_code = build_tw_name_to_code(months)

    # Flat trade list, sorted, with TWD-equivalent net (use month's USD/TWD for foreign).
    all_trades: list[dict] = []
    by_ticker: dict[str, dict] = defaultdict(lambda: {
        "code": "", "name": "", "venue": "",
        "buy_qty": 0.0, "buy_cost_twd": 0.0,
        "sell_qty": 0.0, "sell_proceeds_twd": 0.0,
        "fees_twd": 0.0, "tax_twd": 0.0,
        "dividends_twd": 0.0, "dividend_count": 0,
        "trades": [],
        "first_trade_date": None, "last_trade_date": None,
    })

    # Venue-level TWD flow per month (used by attribution + cashflow waterfall).
    # These come straight from the broker trade tables — ground truth for what
    # was bought/sold each month per venue. Bank-level flows can't separate
    # TW vs foreign without inferring from memos.
    venue_flows: list[dict] = []

    # All dividend events (bank-derived for actual cash; broker section as backup).
    div_events: list[dict] = []

    for m in months:
        ym = m["month"]
        fx_rate = m["fx_usd_twd"] or 0.0

        tw_buy_twd = tw_sell_twd = tw_fee_twd = tw_tax_twd = tw_margin_used_twd = 0.0
        fr_buy_twd = fr_sell_twd = fr_fee_twd = 0.0

        for t in (m["tw"] or {}).get("trades", []) or []:
            row = {
                "month": ym, "date": t["date"], "venue": "TW",
                "side": t["side"],
                "code": resolve_tw_code(t["name"], tw_name_to_code),
                "name": t["name"],
                "qty": t["qty"], "price": t["price"], "ccy": "TWD",
                "gross_twd": t["gross"], "fee_twd": t["fee"], "tax_twd": t.get("tax", 0.0),
                "net_twd": t["net_twd"],
                "margin_loan_twd": t.get("margin_loan", 0.0),
                "self_funded_twd": t.get("self_funded", 0.0),
            }
            all_trades.append(row)
            tw_fee_twd += row["fee_twd"]
            tw_tax_twd += row["tax_twd"]
            if "買" in row["side"]:
                tw_buy_twd += row["gross_twd"]
                tw_margin_used_twd += row["margin_loan_twd"]
            else:
                tw_sell_twd += row["gross_twd"]
            key = row["code"] or row["name"]
            bt = by_ticker[key]
            bt["code"] = bt["code"] or row["code"]
            bt["name"] = row["name"]
            bt["venue"] = "TW"
            bt["fees_twd"] += row["fee_twd"]
            bt["tax_twd"] += row["tax_twd"]
            if "買" in row["side"]:
                bt["buy_qty"] += row["qty"]
                bt["buy_cost_twd"] += row["gross_twd"] + row["fee_twd"]
            else:
                bt["sell_qty"] += row["qty"]
                bt["sell_proceeds_twd"] += row["gross_twd"] - row["fee_twd"] - row["tax_twd"]
            bt["trades"].append(row)
            bt["first_trade_date"] = bt["first_trade_date"] or row["date"]
            bt["last_trade_date"] = row["date"]

        for t in (m["foreign"] or {}).get("trades", []) or []:
            rate = fx_rate if t["ccy"] == "USD" else 0.0
            net_twd = t["net_ccy"] * rate
            row = {
                "month": ym, "date": t["date"], "venue": "Foreign",
                "side": t["side"], "code": t["code"], "name": t["code"],
                "exchange": t.get("exchange"),
                "qty": t["qty"], "price": t["price"], "ccy": t["ccy"],
                "gross_local": t["gross"], "fee_local": t["fee"],
                "other_fee_local": t.get("other_fee", 0.0),
                "gross_twd": t["gross"] * rate,
                "fee_twd": (t["fee"] + t.get("other_fee", 0.0)) * rate,
                "tax_twd": 0.0,
                "net_twd": net_twd,
            }
            all_trades.append(row)
            fr_fee_twd += row["fee_twd"]
            if t["side"] == "買進":
                fr_buy_twd += row["gross_twd"]
            else:
                fr_sell_twd += row["gross_twd"]
            key = row["code"]
            bt = by_ticker[key]
            bt["code"], bt["name"] = row["code"], row["name"]
            bt["venue"] = "Foreign"
            bt["fees_twd"] += row["fee_twd"]
            if t["side"] == "買進":
                bt["buy_qty"] += row["qty"]
                bt["buy_cost_twd"] += -row["net_twd"]
            else:
                bt["sell_qty"] += row["qty"]
                bt["sell_proceeds_twd"] += row["net_twd"]
            bt["trades"].append(row)
            bt["first_trade_date"] = bt["first_trade_date"] or row["date"]
            bt["last_trade_date"] = row["date"]

        # Bank-derived dividend events are the source of truth (every foreign
        # dividend hits the USD account; every TW listed-stock dividend hits
        # TWD via 集保 ACH). The broker section is sparse and cross-checks
        # bank amounts when present — we use it only to backfill events that
        # the bank ledger missed (e.g. fee-only entries, payment-in-kind).
        bank_div_keys: set[tuple] = set()
        for ev in m.get("dividend_events", []) or []:
            code = ev.get("code") or ""
            if ev["venue"] == "TW" and not code:
                code = resolve_tw_code(ev.get("name") or "", tw_name_to_code)
            div_events.append({
                "month": ym, "date": ev["date"], "venue": ev["venue"],
                "code": code, "name": ev.get("name") or code,
                "ccy": ev["ccy"],
                "amount_local": ev["amount_local"],
                "amount_twd": ev["amount_twd"],
                "source": "bank",
            })
            bank_div_keys.add((ev["venue"], (code or ev.get("name") or "").upper(), ev["date"][:7]))
            key = code or (ev.get("name") or "DIV")
            bt = by_ticker[key]
            if ev["venue"] == "TW":
                bt["code"] = bt["code"] or code
                bt["name"] = bt["name"] or (ev.get("name") or "")
                bt["venue"] = bt["venue"] or "TW"
            else:
                bt["code"] = bt["code"] or code
                bt["name"] = bt["name"] or (ev.get("name") or code)
                bt["venue"] = bt["venue"] or "Foreign"
            bt["dividends_twd"] += ev["amount_twd"]
            bt["dividend_count"] += 1

        # Broker-section dividends — only add ones the bank didn't already have.
        for d in (m["foreign"] or {}).get("dividends", []) or []:
            key = ("Foreign", (d["code"] or "").upper(), d["date"][:7])
            if key in bank_div_keys:
                continue
            rate = fx_rate if d["ccy"] == "USD" else 0.0
            twd = d["net_amount"] * rate
            div_events.append({
                "month": ym, "date": d["date"], "venue": "Foreign",
                "code": d["code"], "name": d["code"],
                "ccy": d["ccy"], "amount_local": d["net_amount"],
                "amount_twd": twd, "source": "broker",
            })
            bt = by_ticker[d["code"]]
            bt["code"] = bt["code"] or d["code"]
            bt["venue"] = bt["venue"] or "Foreign"
            bt["dividends_twd"] += twd
            bt["dividend_count"] += 1

        venue_flows.append({
            "month": ym,
            "tw_buy_twd": tw_buy_twd, "tw_sell_twd": tw_sell_twd,
            "tw_fee_twd": tw_fee_twd, "tw_tax_twd": tw_tax_twd,
            "tw_margin_used_twd": tw_margin_used_twd,
            "foreign_buy_twd": fr_buy_twd, "foreign_sell_twd": fr_sell_twd,
            "foreign_fee_twd": fr_fee_twd,
        })

    all_trades.sort(key=lambda r: (r["date"], r["venue"], r["side"]))
    div_events.sort(key=lambda r: (r["date"], r["venue"]))

    # Add total-return on closed positions (price proceeds + dividends) per ticker.
    for code, bt in by_ticker.items():
        bt["total_return_proxy_twd"] = (
            bt["sell_proceeds_twd"] + bt["dividends_twd"] - bt["buy_cost_twd"]
        )

    # Latest-snapshot per-holding data for total-return analytics: cum_dividend
    # is what Sinopac reports for the *open* position (lifetime).
    holdings_total_return = []
    for h in (last["tw"] or {}).get("holdings", []) or []:
        cum_div = h.get("cum_dividend", 0) or 0
        cost = h.get("cost", 0) or 0
        upl = h.get("unrealized_pnl", 0) or 0
        upl_div = h.get("unrealized_pnl_with_div", upl) or upl
        holdings_total_return.append({
            "venue": "TW", "code": h.get("code"), "name": h.get("name"),
            "cost_twd": cost, "mkt_value_twd": h.get("mkt_value", 0),
            "unrealized_pnl_twd": upl,
            "cum_dividend_twd": cum_div,
            "unrealized_pnl_with_div_twd": upl_div,
            "total_return_pct": (upl_div / cost) if cost else None,
        })
    for h in (last["foreign"] or {}).get("holdings", []) or []:
        rate = last["fx_usd_twd"] if h.get("ccy") == "USD" else 0.0
        cost_twd = (h.get("cost", 0) or 0) * rate
        mv_twd = (h.get("mkt_value", 0) or 0) * rate
        upl_twd = (h.get("unrealized_pnl", 0) or 0) * rate
        # foreign broker doesn't currently expose cum_dividend; use bank-derived
        # per-ticker totals (lifetime, not just current holding).
        bt = by_ticker.get(h.get("code") or "", {})
        cum_div_twd = bt.get("dividends_twd", 0)
        holdings_total_return.append({
            "venue": "Foreign", "code": h.get("code"), "name": h.get("name"),
            "cost_twd": cost_twd, "mkt_value_twd": mv_twd,
            "unrealized_pnl_twd": upl_twd,
            "cum_dividend_twd": cum_div_twd,
            "unrealized_pnl_with_div_twd": upl_twd + cum_div_twd,
            "total_return_pct": ((upl_twd + cum_div_twd) / cost_twd) if cost_twd else None,
        })

    return {
        "kpis": {
            "as_of": last["month"],
            "real_now_twd": real_now,
            "counterfactual_twd": counterfactual,
            "profit_twd": profit,
            "bank_twd_now": bank_twd_now,
            "bank_usd_in_twd_now": bank_usd_in_twd_now,
            "brokerage_tw_mv_twd": last["tw_market_value_twd"],
            "brokerage_foreign_mv_twd": last["foreign_market_value_twd"],
            "fx_usd_twd": last["fx_usd_twd"],
            "total_dividends_twd": sum(d["amount_twd"] for d in div_events),
        },
        "cumulative_flows": cum,
        "all_trades": all_trades,
        "by_ticker": dict(by_ticker),
        "dividends": div_events,
        "venue_flows_twd": venue_flows,
        "holdings_total_return": holdings_total_return,
    }


# ---------------------------------------------------------------------------
# Performance metrics (kept from previous version)
# ---------------------------------------------------------------------------
def compute_performance(months: list[dict]) -> list[dict]:
    if not months:
        return months
    cum_twr = 1.0
    for i, m in enumerate(months):
        V_end = m["equity_twd"]
        F = m["external_flow_twd"]
        V_start = (V_end - F) if i == 0 else months[i - 1]["equity_twd"]
        denom = V_start + 0.5 * F
        r = (V_end - V_start - F) / denom if denom > 1e-6 else 0.0
        cum_twr *= (1 + r)
        m["period_return"] = r
        m["cum_twr"] = cum_twr - 1
        m["v_start"] = V_start

    cashflows = [(f"{m['month']}-15", -m["external_flow_twd"]) for m in months]
    cashflows.append((f"{months[-1]['month']}-28", months[-1]["equity_twd"]))
    months[-1]["xirr"] = xirr(cashflows)
    return months


def xirr(cashflows: list[tuple[str, float]], guess: float = 0.1) -> float | None:
    from datetime import date

    def parse(d: str) -> date:
        y, mo, dd = d.split("-")
        return date(int(y), int(mo), int(dd))

    dates = [parse(d) for d, _ in cashflows]
    amounts = [a for _, a in cashflows]
    d0 = dates[0]
    years = [(d - d0).days / 365.0 for d in dates]

    def npv(rate: float) -> float:
        return sum(a / (1 + rate) ** t for a, t in zip(amounts, years))

    def dnpv(rate: float) -> float:
        return sum(-t * a / (1 + rate) ** (t + 1) for a, t in zip(amounts, years))

    rate = guess
    for _ in range(100):
        f = npv(rate)
        df = dnpv(rate)
        if abs(df) < 1e-12:
            break
        new_rate = rate - f / df
        if new_rate <= -0.999:
            new_rate = -0.5
        if abs(new_rate - rate) < 1e-9:
            return new_rate
        rate = new_rate
    return rate if abs(npv(rate)) < 1e-3 else None


if __name__ == "__main__":
    raise SystemExit(main())
