"""Trade verifier — monthly audit of Trade table against parsed PDFs.

Diffs PDF-parsed trades against existing rows in the Trade table.
Match key: (date, code, side, qty). Mismatches emit reconcile_events
the operator reviews via /today reconcile banner.

Two run modes:
  apply=False (default)   diff-only; events emitted, no Trade writes
  apply=True              additionally insert pdf_only rows as source='pdf'

apply mode NEVER deletes shioaji rows. The operator decides what to
do with shioaji_only events via /today (auto-deletion would risk
data loss on legit Shioaji trades the parser missed for unrelated
reasons — e.g. PDF format change, missing column).
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
from invest.persistence.models.reconcile_event import ReconcileEvent
from invest.persistence.models.trade import Trade
from invest.persistence.repositories.reconcile_repo import ReconcileRepo
from invest.persistence.repositories.trade_repo import TradeRepo


_FOREIGN_CCY_TO_VENUE = {"USD": "US", "HKD": "HK", "JPY": "JP"}

# Match key for diffing parsed-trade vs existing Trade row. Price
# intentionally excluded — Shioaji and PDF can disagree by a few
# decimal places on micro-FX-rounded prices.
_MatchKey = tuple[_date_t, str, int, int]


@dataclass(frozen=True)
class TradeDiff:
    matched: int
    pdf_only: tuple[Trade, ...]
    shioaji_only: tuple[Trade, ...]


@dataclass(frozen=True)
class VerifyResult:
    diff: TradeDiff
    events_inserted: int


def _key(t: Trade) -> _MatchKey:
    return (t.date, t.code, t.side, t.qty)


def _tw_to_trade(t: ParsedTwTrade, code: str) -> Trade:
    return Trade(
        date=t.date, code=code, side=int(t.side), qty=t.qty,
        price=t.price, currency="TWD",
        fee=t.fee, tax=t.tax, rebate=Decimal("0"),
        source="pdf", venue="TW",
    )


def _foreign_to_trade(t: ParsedForeignTrade) -> Trade:
    venue = _FOREIGN_CCY_TO_VENUE.get(t.ccy, t.ccy)
    return Trade(
        date=t.date, code=t.code, side=int(t.side), qty=t.qty,
        price=t.price, currency=t.ccy,
        fee=t.fee, tax=Decimal("0"), rebate=Decimal("0"),
        source="pdf-foreign", venue=venue,
    )


def _flat_holdings(statements: list[ParsedSecuritiesStatement]) -> list[dict]:
    out: list[dict] = []
    for s in statements:
        for h in s.holdings:
            out.append({"name": h.name, "code": h.code})
    return out


def _build_pdf_trades(
    securities: list[ParsedSecuritiesStatement],
    foreign: list[ParsedForeignStatement],
    overrides_path: Optional[Path],
) -> list[Trade]:
    overrides = load_overrides(overrides_path) if overrides_path else {}
    name_to_code = build_name_to_code(_flat_holdings(securities), overrides)

    out: list[Trade] = []
    for s in securities:
        for t in s.trades:
            code = resolve_tw_code(t.name, name_to_code)
            if code:
                out.append(_tw_to_trade(t, code))
    for s in foreign:
        for t in s.trades:
            out.append(_foreign_to_trade(t))
    return out


def _months_in_input(
    securities: list[ParsedSecuritiesStatement],
    foreign: list[ParsedForeignStatement],
) -> set[str]:
    return {s.month for s in securities} | {s.month for s in foreign}


def _month_of(d: _date_t) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _trade_to_event_detail(t: Trade) -> dict:
    return {
        "date": t.date.isoformat(),
        "code": t.code,
        "side": int(t.side),
        "qty": t.qty,
        "price": str(t.price),
        "source": t.source,
        "venue": t.venue,
    }


def verify_trades_against_statements(
    *,
    securities: list[ParsedSecuritiesStatement],
    foreign: list[ParsedForeignStatement],
    trade_repo: TradeRepo,
    reconcile_repo: ReconcileRepo,
    overrides_path: Optional[Path] = None,
    apply: bool = False,
) -> VerifyResult:
    """Diff parsed trades vs Trade table; emit reconcile events."""
    pdf_trades = _build_pdf_trades(securities, foreign, overrides_path)
    pdf_index: dict[_MatchKey, Trade] = {_key(t): t for t in pdf_trades}

    months = _months_in_input(securities, foreign)
    db_index: dict[_MatchKey, Trade] = {}
    for month in months:
        for row in trade_repo.find_by_month(month):
            db_index[_key(row)] = row

    pdf_only: list[Trade] = []
    shioaji_only: list[Trade] = []
    matched = 0

    for k, ptrade in pdf_index.items():
        if k in db_index:
            matched += 1
        else:
            pdf_only.append(ptrade)

    for k, dbtrade in db_index.items():
        if k not in pdf_index:
            shioaji_only.append(dbtrade)

    events_inserted = 0
    for t in pdf_only:
        reconcile_repo.insert(
            ReconcileEvent(
                pdf_month=_month_of(t.date),
                event_type="pdf_trade_missing_from_shioaji",
                detail=_trade_to_event_detail(t),
            )
        )
        events_inserted += 1
    for t in shioaji_only:
        reconcile_repo.insert(
            ReconcileEvent(
                pdf_month=_month_of(t.date),
                event_type="shioaji_trade_missing_from_pdf",
                detail=_trade_to_event_detail(t),
            )
        )
        events_inserted += 1

    if apply and pdf_only:
        for t in pdf_only:
            trade_repo.insert(t)

    return VerifyResult(
        diff=TradeDiff(
            matched=matched,
            pdf_only=tuple(pdf_only),
            shioaji_only=tuple(shioaji_only),
        ),
        events_inserted=events_inserted,
    )
