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
from pathlib import Path

from invest.ingestion._common import (
    _flat_holdings,
    _foreign_to_trade,
    _tw_to_trade,
)
from invest.ingestion.foreign_parser import ParsedForeignStatement
from invest.ingestion.tw_naming import (
    build_name_to_code,
    load_overrides,
    resolve_tw_code,
)
from invest.ingestion.tw_parser import ParsedSecuritiesStatement
from invest.persistence.models.reconcile_event import ReconcileEvent
from invest.persistence.models.trade import Trade
from invest.persistence.repositories.reconcile_repo import ReconcileRepo
from invest.persistence.repositories.trade_repo import TradeRepo

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


def _build_pdf_trades(
    securities: list[ParsedSecuritiesStatement],
    foreign: list[ParsedForeignStatement],
    overrides_path: Path | None,
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
    overrides_path: Path | None = None,
    apply: bool = False,
) -> VerifyResult:
    """Diff parsed trades vs Trade table; emit reconcile events."""
    pdf_trades = _build_pdf_trades(securities, foreign, overrides_path)
    # pdf_index is also a list-per-key so two PDF fills with the same
    # (date, code, side, qty) are both tracked.
    pdf_index: dict[_MatchKey, list[Trade]] = {}
    for t in pdf_trades:
        pdf_index.setdefault(_key(t), []).append(t)

    months = _months_in_input(securities, foreign)
    # db_index maps each match key to a list of DB rows.  Using a list
    # (instead of the old single-entry dict) means two scaling-in fills
    # that share (date, code, side, qty) are both visible — the old
    # dict silently dropped the second entry.
    db_index: dict[_MatchKey, list[Trade]] = {}
    for month in months:
        for row in trade_repo.find_by_month(month):
            db_index.setdefault(_key(row), []).append(row)

    pdf_only: list[Trade] = []
    shioaji_only: list[Trade] = []
    matched = 0

    # Work on copies so we can pop entries as we consume them.
    remaining_db: dict[_MatchKey, list[Trade]] = {
        k: list(v) for k, v in db_index.items()
    }

    for k, ptrades in pdf_index.items():
        db_pool = remaining_db.get(k, [])
        for ptrade in ptrades:
            if db_pool:
                db_pool.pop(0)
                matched += 1
            else:
                pdf_only.append(ptrade)

    # Anything still left in remaining_db was unmatched (shioaji-only).
    for leftover_list in remaining_db.values():
        shioaji_only.extend(leftover_list)

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
