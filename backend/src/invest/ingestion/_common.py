"""Shared helpers for trade_seeder and trade_verifier.

Extracted from the two modules to avoid duplication of:
  - _FOREIGN_CCY_TO_VENUE   currency → venue mapping
  - _flat_holdings()        flatten ParsedSecuritiesStatement list
  - _tw_to_trade()          ParsedTwTrade → Trade
  - _foreign_to_trade()     ParsedForeignTrade → Trade
"""
from __future__ import annotations

from decimal import Decimal

from invest.ingestion.foreign_parser import ParsedForeignTrade
from invest.ingestion.tw_parser import ParsedSecuritiesStatement, ParsedTwTrade
from invest.persistence.models.trade import Trade

_FOREIGN_CCY_TO_VENUE: dict[str, str] = {"USD": "US", "HKD": "HK", "JPY": "JP"}


def _flat_holdings(statements: list[ParsedSecuritiesStatement]) -> list[dict]:
    """Flatten holdings across statements into [{name, code}, ...] shape
    that build_name_to_code expects."""
    out: list[dict] = []
    for s in statements:
        for h in s.holdings:
            out.append({"name": h.name, "code": h.code})
    return out


def _tw_to_trade(t: ParsedTwTrade, code: str) -> Trade:
    return Trade(
        date=t.date,
        code=code,
        side=int(t.side),
        qty=t.qty,
        price=t.price,
        currency="TWD",
        fee=t.fee,
        tax=t.tax,
        rebate=Decimal("0"),
        source="pdf",
        venue="TW",
    )


def _foreign_to_trade(t: ParsedForeignTrade) -> Trade:
    venue = _FOREIGN_CCY_TO_VENUE.get(t.ccy, t.ccy)
    return Trade(
        date=t.date,
        code=t.code,
        side=int(t.side),
        qty=t.qty,
        price=t.price,
        currency=t.ccy,
        fee=t.fee,
        tax=Decimal("0"),
        rebate=Decimal("0"),
        source="pdf-foreign",
        venue=venue,
    )
