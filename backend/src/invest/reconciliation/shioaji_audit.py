"""Phase 5 Cycle 38 — live audit hook (broker buy-leg vs PDF coverage).

This hook is the read-side companion to shioaji_sync (Cycle 37). After
sync writes Trade rows from broker surfaces, the audit re-examines the
realized pairs and fires a 'broker_pdf_buy_leg_mismatch' reconcile event
for any pair whose SDK buy legs are not all covered by PDF buys.

Composes Cycle 37 (shioaji_sync) + Cycle 35 (trade_verifier — same
match-key philosophy) + the ReconcileRepo. The audit is invoked
post-sync, not from inside sync, to keep the write side and the audit
side as separable concerns: a future change to the audit policy (e.g.
disabling for a specific event_type) should not require touching the
write path.

Policy — Option B (PDF coverage gap):
    For each realized pair (sell_summary + N buy legs grouped by
    pair_id), every SDK buy leg's (date, qty) MUST appear in the PDF
    buys for the same code, dated <= sell_date. Any leg without a
    (date, qty) match contributes to the pair's missing_legs list.
    A pair with a non-empty missing_legs fires one event.

Read-only contract: this module never inserts/updates/deletes Trade
rows. It only reads them. The whole point of the hook is to *surface*
divergence; the operator decides resolution via /today's reconcile
banner. Auto-resolution would risk silently dropping legit Shioaji
writes the parser missed for unrelated reasons (e.g. PDF format change).

Idempotency: a pair_id with an existing OPEN event is skipped on
subsequent runs to keep the banner count from doubling. Dismissed
events DO allow refire — dismissal means "reviewed", and if the
divergence persists the operator wants to see it again.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date as _date
from typing import TYPE_CHECKING, Any, Iterable

from sqlmodel import Session, SQLModel, create_engine, select

from invest.domain.trade import Side
from invest.persistence.models.reconcile_event import ReconcileEvent
from invest.persistence.models.trade import Trade
from invest.persistence.repositories.reconcile_repo import ReconcileRepo
from invest.persistence.repositories.trade_repo import TradeRepo

if TYPE_CHECKING:
    from invest.persistence.daily_store import DailyStore


log = logging.getLogger(__name__)

_EVENT_TYPE = "broker_pdf_buy_leg_mismatch"


@dataclass(frozen=True)
class AuditResult:
    """Outcome of one audit run.

    pairs_examined
        Count of distinct pair_ids walked. Includes pairs that had
        legs and pairs that didn't (degenerate). Useful for ops
        triage: pairs_examined=0 with realized_pairs non-empty means
        the SDK contract has changed.
    events_fired
        Count of NEW reconcile events written this run. Excludes
        dedup-skipped pairs (open event already exists).
    """

    pairs_examined: int
    events_fired: int


def audit_realized_pairs(
    *,
    realized_pairs: Iterable[dict[str, Any]],
    trade_repo: TradeRepo,
    reconcile_repo: ReconcileRepo,
) -> AuditResult:
    """Fire broker_pdf_buy_leg_mismatch events for pairs with missing
    PDF coverage. See module docstring for policy rationale."""
    pair_groups = _group_by_pair_id(realized_pairs)
    open_pair_ids = _open_audit_pair_ids(reconcile_repo)

    fired = 0
    for pair_id, group in pair_groups.items():
        sell_date = group["sell_date"]
        code = group["code"]
        legs = group["legs"]

        # Degenerate pair (no buy legs — sync.list_profit_loss_detail
        # rate-limited or empty). Nothing to compare; stay silent.
        if not legs or not sell_date or not code:
            continue

        if _norm_pair_id(pair_id) in open_pair_ids:
            continue

        missing = _missing_legs(
            code=code, sell_date=sell_date, legs=legs, trade_repo=trade_repo,
        )
        if not missing:
            continue

        reconcile_repo.insert(
            ReconcileEvent(
                pdf_month=sell_date[:7],  # bucket banner by sell month
                event_type=_EVENT_TYPE,
                detail={
                    "pair_id": pair_id,
                    "code": code,
                    "sell_date": sell_date,
                    "missing_legs": missing,
                },
            )
        )
        fired += 1

    return AuditResult(pairs_examined=len(pair_groups), events_fired=fired)


# --- Internals ------------------------------------------------------------


def _group_by_pair_id(
    realized: Iterable[dict[str, Any]],
) -> dict[Any, dict[str, Any]]:
    """{pair_id: {sell_date, code, legs: [...]}}

    Buy legs and the sell summary share a pair_id. Sell sets sell_date
    + code; buys append to legs. Records without a pair_id are dropped
    (defensive — every record from list_realized_pairs carries one).
    """
    groups: dict[Any, dict[str, Any]] = {}
    for rec in realized:
        pid = rec.get("pair_id")
        if pid is None:
            continue
        g = groups.setdefault(
            pid, {"sell_date": None, "code": None, "legs": []},
        )
        side = rec.get("side")
        if side == "普買":
            g["legs"].append(rec)
            if not g["code"]:
                g["code"] = rec.get("code")
        elif side == "普賣":
            g["sell_date"] = rec.get("date")
            g["code"] = rec.get("code")
    return groups


def _missing_legs(
    *,
    code: str,
    sell_date: str,
    legs: list[dict[str, Any]],
    trade_repo: TradeRepo,
) -> list[dict[str, Any]]:
    """Return legs whose (date, qty) is not covered by any PDF buy
    for `code` dated <= sell_date.

    Match key is (date, qty) — price is intentionally excluded
    (mirrors shioaji_sync._dedup_key and trade_verifier).
    """
    sell_d = _parse_iso(sell_date)
    pdf_keys = {
        (t.date.isoformat(), t.qty)
        for t in trade_repo.find_by_code(code)
        if t.source == "pdf"
        and t.side == int(Side.CASH_BUY)
        and t.date <= sell_d
    }

    missing: list[dict[str, Any]] = []
    for leg in legs:
        leg_qty = int(round(float(leg.get("qty") or 0)))
        leg_date = str(leg.get("date") or "")
        if (leg_date, leg_qty) not in pdf_keys:
            missing.append({"date": leg_date, "qty": leg_qty})
    return missing


def _open_audit_pair_ids(repo: ReconcileRepo) -> set[str]:
    """Normalized pair_ids of currently-open broker_pdf_buy_leg_mismatch
    events. Dismissed events are excluded so a persistent divergence
    refires after the operator dismisses it."""
    seen: set[str] = set()
    for e in repo.find_open():
        if e.event_type != _EVENT_TYPE:
            continue
        pid = (e.detail or {}).get("pair_id")
        if pid is not None:
            seen.add(_norm_pair_id(pid))
    return seen


def _norm_pair_id(pid: Any) -> str:
    """Normalize pair_id to str for dedup comparison.

    Shioaji's pl.id type varies across SDK versions; ReconcileEvent.detail
    is a JSON column that round-trips by type. Without normalization,
    int 12345 from the SDK wouldn't compare equal to JSON-deserialized
    12345 (or vice versa). str() collapses both to the same key.
    """
    return str(pid)


def _parse_iso(s: str) -> _date:
    y, m, d = s.split("-")
    return _date(int(y), int(m), int(d))


# --- Orchestrator (Phase 14.5) ------------------------------------------


def _trade_table_has_rows(session: Session) -> bool:
    """True iff at least one Trade row exists. Defensive guard for run()."""
    return session.scalar(select(Trade).limit(1)) is not None


def run(
    realized_pairs: Iterable[dict[str, Any]],
    *,
    daily_store: "DailyStore",
) -> AuditResult:
    """Bootstrap a SQLModel session against ``daily_store`` and audit.

    The post-PDF caller (``jobs.snapshot.run``) holds a
    ``DailyStore`` but no SQLModel session — the read-side of the audit
    needs the SQLModel ``trades`` table (populated by
    ``invest.jobs.trade_backfill``). This wrapper opens an engine
    against the same SQLite file, runs ``create_all`` (idempotent), and
    invokes ``audit_realized_pairs``.

    Defensive skip: when the ``trades`` table is empty (operator hasn't
    run ``scripts/backfill_trades.py`` yet) every SDK leg would look
    "uncovered" and an event would fire per pair. We skip silently
    instead — the operator's first encounter with the audit hook
    should be after they've populated PDF trades.
    """
    pairs = list(realized_pairs or [])
    if not pairs:
        return AuditResult(pairs_examined=0, events_fired=0)

    engine = create_engine(
        f"sqlite:///{daily_store.path}",
        connect_args={"timeout": 5},
    )
    SQLModel.metadata.create_all(engine)

    with Session(engine) as session:
        if not _trade_table_has_rows(session):
            log.info(
                "shioaji_audit.run: trades table empty — skipping audit "
                "(run scripts/backfill_trades.py to enable)"
            )
            return AuditResult(pairs_examined=0, events_fired=0)

        return audit_realized_pairs(
            realized_pairs=pairs,
            trade_repo=TradeRepo(session),
            reconcile_repo=ReconcileRepo(session),
        )
