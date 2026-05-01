"""Trade seeder — write parsed-statement trades into the Trade table.

Idempotent one-shot for pre-Shioaji historical seeding. Maps:

  ParsedTwTrade       (TW)        → Trade(source='pdf',         venue='TW',  currency='TWD')
  ParsedForeignTrade  (foreign)   → Trade(source='pdf-foreign', venue=USD/HK/JP, currency=USD/HKD/JPY)

Idempotency comes from TradeRepo.replace_for_period — bounded by
(source, date range) so re-running with the same input replaces
the same window without touching shioaji/manual sources.

This module does NOT decide which months to seed. The orchestrator
(eventually Phase 9 cutover) handles cutoff logic for pre- vs
post-Shioaji-min dates. Here we just map and write.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date as _date_t
from decimal import Decimal
from pathlib import Path
from typing import Optional

from invest.ingestion.foreign_parser import (
    ParsedForeignStatement,
    ParsedForeignTrade,
)
from invest.ingestion.tw_naming import (
    build_name_to_code,
    load_overrides,
    resolve_tw_code,
)
from invest.ingestion.tw_parser import (
    ParsedSecuritiesStatement,
    ParsedTwTrade,
)
from invest.persistence.models.trade import Trade
from invest.persistence.repositories.trade_repo import TradeRepo


_FOREIGN_CCY_TO_VENUE = {"USD": "US", "HKD": "HK", "JPY": "JP"}


@dataclass(frozen=True)
class SeedResult:
    tw_inserted: int
    foreign_inserted: int
    tw_unresolved_codes: tuple[str, ...]


def _flat_holdings(statements: list[ParsedSecuritiesStatement]) -> list[dict]:
    """Flatten holdings across statements into the [{name, code}, ...]
    shape build_name_to_code expects."""
    out: list[dict] = []
    for s in statements:
        for h in s.holdings:
            out.append({"name": h.name, "code": h.code})
    return out


def _date_range(rows: list[Trade]) -> tuple[_date_t, _date_t]:
    dates = [r.date for r in rows]
    return min(dates), max(dates)


def _tw_to_trade_row(t: ParsedTwTrade, code: str) -> Trade:
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


def _foreign_to_trade_row(t: ParsedForeignTrade) -> Trade:
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


def seed_trades_from_statements(
    *,
    securities: list[ParsedSecuritiesStatement],
    foreign: list[ParsedForeignStatement],
    trade_repo: TradeRepo,
    overrides_path: Optional[Path] = None,
) -> SeedResult:
    """Seed Trade rows from parsed statements. Idempotent."""
    overrides = load_overrides(overrides_path) if overrides_path else {}
    name_to_code = build_name_to_code(_flat_holdings(securities), overrides)

    tw_rows: list[Trade] = []
    unresolved: list[str] = []
    for s in securities:
        for t in s.trades:
            code = resolve_tw_code(t.name, name_to_code)
            if not code:
                unresolved.append(t.name)
                continue
            tw_rows.append(_tw_to_trade_row(t, code))

    if tw_rows:
        start, end = _date_range(tw_rows)
        trade_repo.replace_for_period(
            source="pdf", start=start, end=end, rows=tw_rows
        )

    foreign_rows: list[Trade] = []
    for s in foreign:
        for t in s.trades:
            foreign_rows.append(_foreign_to_trade_row(t))

    if foreign_rows:
        start, end = _date_range(foreign_rows)
        trade_repo.replace_for_period(
            source="pdf-foreign", start=start, end=end, rows=foreign_rows
        )

    # Dedupe unresolved names while preserving first-seen order.
    seen: set[str] = set()
    unresolved_unique: list[str] = []
    for n in unresolved:
        if n not in seen:
            seen.add(n)
            unresolved_unique.append(n)

    return SeedResult(
        tw_inserted=len(tw_rows),
        foreign_inserted=len(foreign_rows),
        tw_unresolved_codes=tuple(unresolved_unique),
    )
