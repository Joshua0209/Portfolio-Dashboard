"""Bank (永豐銀行 綜合對帳單) monthly statement parser.

Three layers, all importable as module-level:

  categorize(summary)           -> str
      9-bucket keyword classifier mapping a bank tx summary line
      to a category string used downstream by the cashflow boundary
      (tw_dividend / foreign_dividend / stock_settle_tw /
      stock_settle_fx / rebate / fx_convert / salary / interest /
      transfer / other).

  parse_bank_tx_line(line)      -> Optional[ParsedBankTx]
      One transaction line. Date, summary, two money tokens
      (amount, balance), trailing memo. The MONEY_TOKEN regex
      rejects long bare digit strings so account numbers and stock
      codes in the memo column don't accidentally consume the
      amount/balance walk.

  parse_bank_text(text)         -> ParsedBankStatement
      Full statement: period from 對帳單期間, FX rate table,
      balance triple (total/TWD/foreign), per-account tx routing,
      and balance-delta sign inference for the unsigned amount
      column.
"""
from __future__ import annotations

import dataclasses
import re
from dataclasses import dataclass
from datetime import date as _date_t
from decimal import Decimal
from typing import Optional

_NUM = r"-?[\d,]+(?:\.\d+)?"

_PERIOD_RE = re.compile(r"對帳單期間：(\d{4})/(\d{2})/")
_ACCOUNT_HEADER_RE = re.compile(r"帳號:\s*(\S+)\s*\((新臺幣|美元|.+?)\)")
# FX line: <whatever>(USD) <buy> <mid> <sell> — middle is the TWD-per-1-foreign mid rate.
_FX_RE = re.compile(rf"^\S*\(([A-Z]{{3}})\)\s+({_NUM})\s+([\d.]+)\s+({_NUM})\s*$")
_BALANCE_RE = re.compile(
    rf"存款\s+({_NUM})\s*\n\s*臺幣\s+({_NUM})\s*\n\s*外幣\s+({_NUM})"
)

# Money tokens: comma-formatted (e.g. 1,234[.56]) or short decimal/int (<8 digits).
# Rejects long digit strings so account numbers / stock codes in memo don't match.
_MONEY_TOKEN_RE = re.compile(
    r"(?<![\w.])(?:\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{1,7}(?:\.\d+)?)(?![\w])"
)

# Categorize is order-sensitive: most-specific keywords first. The bare
# '轉帳' rule sits LAST because it would otherwise swallow 跨行轉帳 etc.
_OUTFLOW_HEURISTIC_KEYWORDS = ("股票款", "手機換匯", "股款交割")


@dataclass(frozen=True)
class ParsedBankTx:
    date: _date_t
    summary: str
    amount: Decimal
    balance: Decimal
    memo: str
    signed_amount: Decimal
    category: str
    ccy: str


@dataclass(frozen=True)
class ParsedBankStatement:
    month: str
    fx_rates: dict[str, Decimal]
    cash_total_twd: Decimal
    cash_twd: Decimal
    cash_foreign_twd: Decimal
    tx_twd: tuple[ParsedBankTx, ...]
    tx_foreign: tuple[ParsedBankTx, ...]


def _dec(s: str) -> Decimal:
    return Decimal(s.replace(",", ""))


def _parse_date(s: str) -> _date_t:
    y, m, d = s.split("/")
    return _date_t(int(y), int(m), int(d))


def categorize(summary: str) -> str:
    """Keyword-based classifier. Order matters — see module docstring."""
    s = summary.upper()
    if "ACH" in s:
        return "tw_dividend"
    if "國外股息" in summary or "海外股息" in summary:
        return "foreign_dividend"
    if "股票款" in summary or "預扣股款" in summary:
        return "stock_settle_tw"
    if "折讓款" in summary:
        return "rebate"
    if "股款交割" in summary:
        return "stock_settle_fx"
    if "手機換匯" in summary:
        return "fx_convert"
    if "薪資" in summary:
        return "salary"
    if "利息存入" in summary:
        return "interest"
    if "手機轉帳" in summary or "跨行轉帳" in summary:
        return "transfer"
    if summary.strip() == "轉帳":
        return "stock_settle_tw"  # bare 轉帳 = refund of 預扣股款 (edge)
    return "other"


def parse_bank_tx_line(line: str) -> Optional[ParsedBankTx]:
    """Parse a tx line into ParsedBankTx with placeholder fields for
    signed_amount / category / ccy.

    parse_bank_text overwrites those placeholders with the real values
    inferred from balance delta + categorize() + current account ccy.
    Standalone callers (rare) get the raw row with summary still
    available for their own categorization.
    """
    if not line or not line.strip():
        return None
    m = re.match(r"^(\d{4}/\d{2}/\d{2})\s+(.+)$", line.strip())
    if not m:
        return None
    rest = m.group(2)

    nums = list(_MONEY_TOKEN_RE.finditer(rest))
    if len(nums) < 2:
        return None

    amount = _dec(nums[0].group())
    balance = _dec(nums[1].group())
    summary = rest[: nums[0].start()].strip()
    memo = rest[nums[1].end():].strip()

    return ParsedBankTx(
        date=_parse_date(m.group(1)),
        summary=summary,
        amount=amount,
        balance=balance,
        memo=memo,
        signed_amount=Decimal("0"),  # placeholder; parse_bank_text overwrites
        category="",                 # placeholder
        ccy="",                      # placeholder
    )


_CCY_LABEL_MAP = {"港幣": "HKD", "日圓": "JPY", "歐元": "EUR"}


def parse_bank_text(text: str) -> ParsedBankStatement:
    pm = _PERIOD_RE.search(text)
    if not pm:
        raise ValueError("Missing 對帳單期間 (period) in bank statement")
    month = f"{pm.group(1)}-{pm.group(2)}"

    fx_rates: dict[str, Decimal] = {}
    for line in text.splitlines():
        fm = _FX_RE.match(line.strip())
        if fm:
            fx_rates[fm.group(1)] = Decimal(fm.group(3))

    cash_total_twd = cash_twd = cash_foreign_twd = Decimal("0")
    bm = _BALANCE_RE.search(text)
    if bm:
        cash_total_twd = _dec(bm.group(1))
        cash_twd = _dec(bm.group(2))
        cash_foreign_twd = _dec(bm.group(3))

    tx_twd: list[ParsedBankTx] = []
    tx_foreign: list[ParsedBankTx] = []
    current_acct: Optional[str] = None  # "TWD" | "FOREIGN"
    current_ccy: Optional[str] = None
    prev_balance: dict[str, Optional[Decimal]] = {"TWD": None, "FOREIGN": None}

    for raw in text.splitlines():
        line = raw.strip()
        ah = _ACCOUNT_HEADER_RE.search(line)
        if ah:
            label = ah.group(2)
            if label == "新臺幣":
                current_acct, current_ccy = "TWD", "TWD"
            elif label == "美元":
                current_acct, current_ccy = "FOREIGN", "USD"
            else:
                current_acct = "FOREIGN"
                current_ccy = _CCY_LABEL_MAP.get(label, label)
            continue
        if current_acct is None:
            continue
        if "交易日" in line and "摘要" in line:
            continue
        if "頁" in line and "綜合對帳單" in line:
            continue
        if "保險" in line or "信託" in line or "貸款" in line:
            current_acct = None
            continue

        raw_tx = parse_bank_tx_line(line)
        if not raw_tx:
            continue

        prev = prev_balance[current_acct]
        amt = raw_tx.amount
        bal = raw_tx.balance
        if prev is None:
            # First tx in this account — no prior balance to compare.
            # Fall back to keyword heuristic.
            if any(k in raw_tx.summary for k in _OUTFLOW_HEURISTIC_KEYWORDS):
                signed = -amt
            else:
                signed = amt
        else:
            delta = bal - prev
            if abs(delta - amt) < abs(delta + amt):
                signed = amt  # balance went up → credit
            else:
                signed = -amt  # balance went down → debit
        prev_balance[current_acct] = bal

        tx = dataclasses.replace(
            raw_tx,
            signed_amount=signed,
            category=categorize(raw_tx.summary),
            ccy=current_ccy or "",
        )
        if current_acct == "TWD":
            tx_twd.append(tx)
        else:
            tx_foreign.append(tx)

    return ParsedBankStatement(
        month=month,
        fx_rates=fx_rates,
        cash_total_twd=cash_total_twd,
        cash_twd=cash_twd,
        cash_foreign_twd=cash_foreign_twd,
        tx_twd=tuple(tx_twd),
        tx_foreign=tuple(tx_foreign),
    )
