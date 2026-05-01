"""TW 證券交易明細 trade-row parser.

Single-line regex parser for the four TW trade families:

  普買, 櫃買        →  cash buy   (Side.CASH_BUY,    8 columns)
  普賣, 櫃賣        →  cash sell  (Side.CASH_SELL,   9 columns; +tax)
  資買              →  margin buy (Side.MARGIN_BUY, 11 columns; +loan)
  資賣              →  margin sell(Side.MARGIN_SELL,12 columns; +loan,+collateral)

Each line in the trade table maps to at most one ParsedTwTrade. Lines
that do not match any of the four shapes return None — they are
column headers, page footers, holdings-table rows, or other non-trade
content that the caller skips.

Code resolution (trade name → ticker code) happens later in
statement_parser.py, after every month's holdings have been parsed
and the name_to_code map is built. Storing the raw name on
ParsedTwTrade defers that resolution to the orchestrator layer.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional

from invest.domain.trade import Side

_NUM = r"-?[\d,]+(?:\.\d+)?"
_DATE = r"\d{4}/\d{2}/\d{2}"

_TRADE_BUY_RE = re.compile(
    rf"^({_DATE})\s+(普買|櫃買)\s+(.+?)\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s*$"
)
_TRADE_SELL_RE = re.compile(
    rf"^({_DATE})\s+(普賣|櫃賣)\s+(.+?)\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s*$"
)
_TRADE_MARGIN_BUY_RE = re.compile(
    rf"^({_DATE})\s+(資買)\s+(.+?)\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_DATE})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s*$"
)
_TRADE_MARGIN_SELL_RE = re.compile(
    rf"^({_DATE})\s+(資賣)\s+(.+?)\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_DATE})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s*$"
)

_SIDE_MAP: dict[str, Side] = {
    "普買": Side.CASH_BUY,
    "櫃買": Side.CASH_BUY,
    "普賣": Side.CASH_SELL,
    "櫃賣": Side.CASH_SELL,
    "資買": Side.MARGIN_BUY,
    "資賣": Side.MARGIN_SELL,
}


@dataclass(frozen=True)
class ParsedTwTrade:
    """Trade-row intermediate. Carries the basics plus settlement-only
    fields the seeder/verifier may use for cashflow reconciliation.

    Fields default to zero/None on the families where they don't apply
    (e.g. tax=0 on margin buys, self_funded=0 on margin sells) so
    every parsed trade has a uniform shape regardless of family.
    """

    date: date
    name: str
    side: Side
    qty: int
    price: Decimal
    gross: Decimal
    fee: Decimal
    net_twd: Decimal
    tax: Decimal = field(default_factory=lambda: Decimal("0"))
    margin_loan: Decimal = field(default_factory=lambda: Decimal("0"))
    self_funded: Decimal = field(default_factory=lambda: Decimal("0"))
    collateral: Decimal = field(default_factory=lambda: Decimal("0"))
    interest_start: Optional[date] = None


def _dec(s: str) -> Decimal:
    return Decimal(s.replace(",", ""))


def _date(s: str) -> date:
    y, m, d = s.split("/")
    return date(int(y), int(m), int(d))


def parse_tw_trade_line(line: str) -> Optional[ParsedTwTrade]:
    """Parse a single trade-table row. Returns None for non-trade lines."""
    if not line or not line.strip():
        return None

    m = _TRADE_BUY_RE.match(line)
    if m:
        gross = _dec(m.group(6))
        fee = _dec(m.group(7))
        owed = _dec(m.group(8))  # 客戶應付
        return ParsedTwTrade(
            date=_date(m.group(1)),
            name=m.group(3).strip(),
            side=_SIDE_MAP[m.group(2)],
            qty=int(_dec(m.group(4))),
            price=_dec(m.group(5)),
            gross=gross,
            fee=fee,
            net_twd=-owed,  # client pays out
        )

    m = _TRADE_SELL_RE.match(line)
    if m:
        return ParsedTwTrade(
            date=_date(m.group(1)),
            name=m.group(3).strip(),
            side=_SIDE_MAP[m.group(2)],
            qty=int(_dec(m.group(4))),
            price=_dec(m.group(5)),
            gross=_dec(m.group(6)),
            fee=_dec(m.group(7)),
            tax=_dec(m.group(8)),
            net_twd=_dec(m.group(9)),  # client receives 客戶應收
        )

    m = _TRADE_MARGIN_BUY_RE.match(line)
    if m:
        owed = _dec(m.group(11))  # 客戶應付
        return ParsedTwTrade(
            date=_date(m.group(1)),
            name=m.group(3).strip(),
            side=_SIDE_MAP[m.group(2)],
            qty=int(_dec(m.group(4))),
            price=_dec(m.group(5)),
            gross=_dec(m.group(6)),
            fee=_dec(m.group(7)),
            interest_start=_date(m.group(8)),
            margin_loan=_dec(m.group(9)),
            self_funded=_dec(m.group(10)),
            net_twd=-owed,
        )

    m = _TRADE_MARGIN_SELL_RE.match(line)
    if m:
        return ParsedTwTrade(
            date=_date(m.group(1)),
            name=m.group(3).strip(),
            side=_SIDE_MAP[m.group(2)],
            qty=int(_dec(m.group(4))),
            price=_dec(m.group(5)),
            gross=_dec(m.group(6)),
            fee=_dec(m.group(7)),
            tax=_dec(m.group(8)),
            interest_start=_date(m.group(9)),
            margin_loan=_dec(m.group(10)),
            collateral=_dec(m.group(11)),
            net_twd=_dec(m.group(12)),  # 客戶應收
        )

    return None
