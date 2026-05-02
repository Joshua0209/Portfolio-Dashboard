"""Foreign (複委託) monthly statement parser.

Three section types per statement, plus per-currency cashflow
aggregation across trades and dividends:

  海外股票庫存及投資損益   →   ParsedForeignHolding rows
  海外股票交易明細          →   ParsedForeignTrade rows (買進/賣出)
  海外股票現金股利明細      →   ParsedForeignDividend rows

Foreign trades are always cash. 買進 → Side.CASH_BUY,
賣出 → Side.CASH_SELL.
"""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import date as _date_t
from decimal import Decimal
from typing import Optional

from invest.domain.trade import Side

_NUM = r"-?[\d,]+(?:\.\d+)?"
_DATE_RE = r"\d{4}/\d{2}/\d{2}"
_CCY_RE = r"USD|HKD|JPY"


@dataclass(frozen=True)
class ParsedForeignHolding:
    code: str
    name: str
    market: str
    exchange: str
    ccy: str
    qty: int
    cost: Decimal
    ref_date: _date_t
    close: Decimal
    mkt_value: Decimal
    unrealized_pnl: Decimal


@dataclass(frozen=True)
class ParsedForeignTrade:
    date: _date_t
    code: str
    market: str
    exchange: str
    side: Side
    ccy: str
    qty: int
    price: Decimal
    gross: Decimal
    fee: Decimal
    other_fee: Decimal
    net_ccy: Decimal


@dataclass(frozen=True)
class ParsedForeignDividend:
    date: _date_t
    code: str
    qty: int
    ccy: str
    net_amount: Decimal


@dataclass(frozen=True)
class ParsedForeignStatement:
    month: str
    holdings: tuple[ParsedForeignHolding, ...]
    trades: tuple[ParsedForeignTrade, ...]
    dividends: tuple[ParsedForeignDividend, ...]
    cashflow_by_ccy: dict[str, Decimal]


_PERIOD_RE = re.compile(r"對帳單日期：(\d{4})/(\d{2})")

_HOLDING_RE = re.compile(
    rf"^(\S+)\s+(.+?)\s+(\S+)\s+(\S+)\s+({_CCY_RE})\s+({_NUM})\s+({_CCY_RE})\s+({_NUM})\s+({_DATE_RE})\s+({_NUM})\s+({_NUM})\s+(-?[\d,.]+)\s+(-?[\d.]+)%"
)

_TRADE_RE = re.compile(
    rf"^({_DATE_RE})\s+(\S+)\s+(\S+)\s+(\S+)\s+(買進|賣出)\s+({_CCY_RE})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+(?:({_NUM})\s+)?(-?[\d,]+\.\d+)\s*$"
)

_HOLDINGS_SECTION_RE = re.compile(
    r"海外股票庫存及投資損益(.*?)債券商品庫存及投資損益", re.S
)
_TRADES_SECTION_RE = re.compile(
    r"海外股票交易明細(.*?)(?:債券商品交易明細|複委託基金交易明細|【截至)", re.S
)
_DIVIDENDS_SECTION_RE = re.compile(r"海外股票現金股利明細(.*?)(?:<|【|$)", re.S)

_DIV_HEAD_RE = re.compile(
    rf"^\S+\s+({_DATE_RE})\s+(\S+)\s+{_DATE_RE}\s+({_NUM})\s+({_CCY_RE})\s+(.+)$"
)

_FOREIGN_SIDE_MAP = {"買進": Side.CASH_BUY, "賣出": Side.CASH_SELL}


def _dec(s: str) -> Decimal:
    return Decimal(s.replace(",", ""))


def _parse_date(s: str) -> _date_t:
    y, m, d = s.split("/")
    return _date_t(int(y), int(m), int(d))


def _parse_holdings(section: str) -> list[ParsedForeignHolding]:
    if "無庫存明細" in section:
        return []
    out: list[ParsedForeignHolding] = []
    for line in section.splitlines():
        m = _HOLDING_RE.match(line.strip())
        if not m:
            continue
        out.append(
            ParsedForeignHolding(
                code=m.group(1),
                name=m.group(2).strip(),
                market=m.group(3),
                exchange=m.group(4),
                ccy=m.group(5),
                qty=int(_dec(m.group(6))),
                cost=_dec(m.group(8)),
                ref_date=_parse_date(m.group(9)),
                close=_dec(m.group(10)),
                mkt_value=_dec(m.group(11)),
                unrealized_pnl=_dec(m.group(12)),
            )
        )
    return out


def _parse_trades(section: str) -> tuple[list[ParsedForeignTrade], dict[str, Decimal]]:
    if "無交易明細" in section:
        return [], {}
    out: list[ParsedForeignTrade] = []
    cashflow: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for line in section.splitlines():
        m = _TRADE_RE.match(line.strip())
        if not m:
            continue
        ccy = m.group(6)
        net = _dec(m.group(12))
        out.append(
            ParsedForeignTrade(
                date=_parse_date(m.group(1)),
                code=m.group(2),
                market=m.group(3),
                exchange=m.group(4),
                side=_FOREIGN_SIDE_MAP[m.group(5)],
                ccy=ccy,
                qty=int(_dec(m.group(7))),
                price=_dec(m.group(8)),
                gross=_dec(m.group(9)),
                fee=_dec(m.group(10)),
                other_fee=_dec(m.group(11)) if m.group(11) else Decimal("0"),
                net_ccy=net,
            )
        )
        cashflow[ccy] += net
    return out, dict(cashflow)


def _parse_dividends(
    section: str,
) -> tuple[list[ParsedForeignDividend], dict[str, Decimal]]:
    if "無交易明細" in section:
        return [], {}
    out: list[ParsedForeignDividend] = []
    cashflow: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for line in section.splitlines():
        line = line.strip()
        m = _DIV_HEAD_RE.match(line)
        if not m:
            continue
        tail_nums = re.findall(_NUM, m.group(5))
        if len(tail_nums) < 2:
            continue
        # Last token is the FX rate; the one before is 股利淨額.
        net = _dec(tail_nums[-2])
        ccy = m.group(4)
        out.append(
            ParsedForeignDividend(
                date=_parse_date(m.group(1)),
                code=m.group(2),
                qty=int(_dec(m.group(3))),
                ccy=ccy,
                net_amount=net,
            )
        )
        cashflow[ccy] += net
    return out, dict(cashflow)


def parse_foreign_text(text: str) -> ParsedForeignStatement:
    pm = _PERIOD_RE.search(text)
    if not pm:
        raise ValueError("Missing 對帳單日期 (period) in foreign statement")
    month = f"{pm.group(1)}-{pm.group(2)}"

    h_sec = _HOLDINGS_SECTION_RE.search(text)
    holdings = _parse_holdings(h_sec.group(1)) if h_sec else []

    t_sec = _TRADES_SECTION_RE.search(text)
    trades, trade_cashflow = (
        _parse_trades(t_sec.group(1)) if t_sec else ([], {})
    )

    d_sec = _DIVIDENDS_SECTION_RE.search(text)
    dividends, div_cashflow = (
        _parse_dividends(d_sec.group(1)) if d_sec else ([], {})
    )

    cashflow_by_ccy: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for ccy, amt in trade_cashflow.items():
        cashflow_by_ccy[ccy] += amt
    for ccy, amt in div_cashflow.items():
        cashflow_by_ccy[ccy] += amt

    return ParsedForeignStatement(
        month=month,
        holdings=tuple(holdings),
        trades=tuple(trades),
        dividends=tuple(dividends),
        cashflow_by_ccy=dict(cashflow_by_ccy),
    )
