"""Reproducer for invest.reconciliation.shioaji_audit — Phase 5 Cycle 38.

RED: invest.reconciliation.shioaji_audit does not exist.

The live audit hook composes the WRITE side (Cycle 37 — shioaji_sync) with
the persistence layer (TradeRepo + ReconcileRepo) to surface broker-vs-PDF
buy-leg divergence in real time, rather than waiting for the monthly
trade_verifier cron.

Contract per PLAN section 5 / Cycle 37 commit message:
  "Composes shioaji_sync (Cycle 37) + ingestion.trade_verifier (Cycle 35)
   + the ReconcileEventRepo to fire 'broker_pdf_buy_leg_mismatch' events
   when buy-leg counts disagree per pair_id. Read-only - never mutates
   Trade rows."

Policy chosen 2026-05-02 (Option B - PDF coverage gap):
  For each realized pair (sell + N buy legs grouped by pair_id), every
  SDK buy leg's (date, qty) MUST be findable in the PDF buys for the
  same code, dated <= sell_date. Any leg without a (date, qty) match
  fires a single 'broker_pdf_buy_leg_mismatch' event for the pair, with
  the missing-leg list in detail.

Why (date, qty) and not (date, qty, price):
  Mirrors shioaji_sync._dedup_key and trade_verifier - Shioaji and PDF
  prices disagree by a few decimal places on micro-FX-rounded fills.
  Keying on price would make every pair "missing".

Why one event per pair, not per leg:
  Banner readability. Multiple missing legs on one pair are still one
  reconciliation conversation; per-leg events would multiply the count.
  detail['missing_legs'] carries the full list.

Why <= sell_date (not exact match):
  Buy legs may pre-date the realized pair's sell by months - locked
  decision #1 option C from PLAN section 3. The window filters PDF buys
  that CAN'T have funded this pair (post-sell) but accepts everything
  earlier.

Test invariants pinned here:
  * Empty input -> 0 events
  * Perfect match (every SDK leg present in PDF) -> 0 events
  * Single missing leg -> 1 event with missing_legs containing that leg
  * Multiple pairs, mixed match/miss -> only the missing ones fire
  * Idempotent - open event for the same pair_id blocks refire
  * Dismissed event for the same pair_id ALLOWS refire (operator wants
    to re-see the divergence if it persists)
  * PDF buys dated AFTER sell_date are NOT candidates
  * Trade table is never mutated
  * Degenerate pair (sell summary qty=0, no legs - pre-filtered by
    sync but the audit must not crash on it) -> 0 events
  * Event detail has the documented schema
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from invest.domain.trade import Side
from invest.persistence.models.trade import Trade
from invest.persistence.repositories.reconcile_repo import ReconcileRepo
from invest.persistence.repositories.trade_repo import TradeRepo


# --- Fixtures -------------------------------------------------------------


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


@pytest.fixture
def trade_repo(session):
    return TradeRepo(session)


@pytest.fixture
def reconcile_repo(session):
    return ReconcileRepo(session)


# --- Test helpers ---------------------------------------------------------


def _pdf_buy(code: str, d: date, qty: int) -> Trade:
    """Build a PDF buy trade row (source='pdf', side=CASH_BUY)."""
    return Trade(
        date=d, code=code, side=int(Side.CASH_BUY), qty=qty,
        price=Decimal("100.0"), currency="TWD",
        fee=Decimal("0"), tax=Decimal("0"), rebate=Decimal("0"),
        source="pdf", venue="TW",
    )


def _sdk_leg(*, pair_id, code: str, d: str, qty: int) -> dict:
    """SDK buy leg shaped like list_realized_pairs output (side='普買')."""
    return {
        "date": d, "code": code, "side": "普買", "qty": float(qty),
        "price": 100.0, "cost_twd": 100.0 * qty, "ccy": "TWD",
        "venue": "TW", "type": "現股", "pair_id": pair_id,
    }


def _sdk_sell(*, pair_id, code: str, d: str, qty: int) -> dict:
    """SDK sell summary record for a pair (side='普賣')."""
    return {
        "date": d, "code": code, "side": "普賣", "qty": float(qty),
        "price": 110.0, "ccy": "TWD",
        "venue": "TW", "type": "現股", "pair_id": pair_id, "pnl": 1000.0,
    }


# --- Test cases -----------------------------------------------------------


class TestEmpty:
    def test_no_pairs_no_events(self, trade_repo, reconcile_repo):
        from invest.reconciliation.shioaji_audit import audit_realized_pairs

        result = audit_realized_pairs(
            realized_pairs=[],
            trade_repo=trade_repo,
            reconcile_repo=reconcile_repo,
        )
        assert result.pairs_examined == 0
        assert result.events_fired == 0
        assert reconcile_repo.find_open() == []


class TestPerfectMatch:
    def test_every_sdk_leg_present_in_pdf_no_event(
        self, trade_repo, reconcile_repo,
    ):
        """SDK pair with 2 legs both present as PDF buys -> silent."""
        from invest.reconciliation.shioaji_audit import audit_realized_pairs

        trade_repo.insert(_pdf_buy("2330", date(2026, 1, 10), 1000))
        trade_repo.insert(_pdf_buy("2330", date(2026, 2, 5), 500))

        pairs = [
            _sdk_leg(pair_id="P1", code="2330", d="2026-01-10", qty=1000),
            _sdk_leg(pair_id="P1", code="2330", d="2026-02-05", qty=500),
            _sdk_sell(pair_id="P1", code="2330", d="2026-03-15", qty=1500),
        ]
        result = audit_realized_pairs(
            realized_pairs=pairs,
            trade_repo=trade_repo,
            reconcile_repo=reconcile_repo,
        )
        assert result.pairs_examined == 1
        assert result.events_fired == 0
        assert reconcile_repo.find_open() == []


class TestMissingLeg:
    def test_one_leg_missing_one_event_fires(
        self, trade_repo, reconcile_repo,
    ):
        """SDK pair with 2 legs; PDF has only 1 -> one event fires
        with missing_legs containing the orphan."""
        from invest.reconciliation.shioaji_audit import audit_realized_pairs

        trade_repo.insert(_pdf_buy("2330", date(2026, 1, 10), 1000))
        # Missing: (2026-02-05, 500)

        pairs = [
            _sdk_leg(pair_id="P1", code="2330", d="2026-01-10", qty=1000),
            _sdk_leg(pair_id="P1", code="2330", d="2026-02-05", qty=500),
            _sdk_sell(pair_id="P1", code="2330", d="2026-03-15", qty=1500),
        ]
        result = audit_realized_pairs(
            realized_pairs=pairs,
            trade_repo=trade_repo,
            reconcile_repo=reconcile_repo,
        )
        assert result.events_fired == 1

        events = reconcile_repo.find_open()
        assert len(events) == 1
        e = events[0]
        assert e.event_type == "broker_pdf_buy_leg_mismatch"
        assert e.pdf_month == "2026-03"  # bucketed by sell_date
        assert e.detail["pair_id"] == "P1"
        assert e.detail["code"] == "2330"
        assert e.detail["sell_date"] == "2026-03-15"
        assert len(e.detail["missing_legs"]) == 1
        assert e.detail["missing_legs"][0]["date"] == "2026-02-05"
        assert e.detail["missing_legs"][0]["qty"] == 500


class TestMultiplePairs:
    def test_only_broken_pairs_fire(self, trade_repo, reconcile_repo):
        """Two pairs: one perfect, one broken. Only the broken one fires."""
        from invest.reconciliation.shioaji_audit import audit_realized_pairs

        # P1 perfect: leg present in PDF
        trade_repo.insert(_pdf_buy("2330", date(2026, 1, 10), 1000))
        # P2 broken: leg NOT in PDF (no row for 2454 / 2026-01-15 / 200)

        pairs = [
            _sdk_leg(pair_id="P1", code="2330", d="2026-01-10", qty=1000),
            _sdk_sell(pair_id="P1", code="2330", d="2026-03-15", qty=1000),
            _sdk_leg(pair_id="P2", code="2454", d="2026-01-15", qty=200),
            _sdk_sell(pair_id="P2", code="2454", d="2026-04-01", qty=200),
        ]
        result = audit_realized_pairs(
            realized_pairs=pairs,
            trade_repo=trade_repo,
            reconcile_repo=reconcile_repo,
        )
        assert result.pairs_examined == 2
        assert result.events_fired == 1
        events = reconcile_repo.find_open()
        assert len(events) == 1
        assert events[0].detail["pair_id"] == "P2"


class TestIdempotency:
    def test_open_event_for_same_pair_id_blocks_refire(
        self, trade_repo, reconcile_repo,
    ):
        """Re-running the audit with the same broker state must NOT
        double the banner. Open event for pair_id='P1' blocks refire."""
        from invest.reconciliation.shioaji_audit import audit_realized_pairs

        pairs = [
            _sdk_leg(pair_id="P1", code="2454", d="2026-01-15", qty=200),
            _sdk_sell(pair_id="P1", code="2454", d="2026-04-01", qty=200),
        ]
        audit_realized_pairs(
            realized_pairs=pairs,
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )
        result2 = audit_realized_pairs(
            realized_pairs=pairs,
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )
        assert result2.events_fired == 0
        events = reconcile_repo.find_open()
        assert len(events) == 1


class TestDismissedAllowsRefire:
    def test_dismissed_event_does_not_block(
        self, trade_repo, reconcile_repo,
    ):
        """Dismissal means 'reviewed'. If the divergence persists on the
        next run the operator wants to see it again."""
        from invest.reconciliation.shioaji_audit import audit_realized_pairs

        pairs = [
            _sdk_leg(pair_id="P1", code="2454", d="2026-01-15", qty=200),
            _sdk_sell(pair_id="P1", code="2454", d="2026-04-01", qty=200),
        ]
        audit_realized_pairs(
            realized_pairs=pairs,
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )
        first_event = reconcile_repo.find_open()[0]
        reconcile_repo.dismiss(first_event.id)
        assert reconcile_repo.find_open() == []

        result2 = audit_realized_pairs(
            realized_pairs=pairs,
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )
        assert result2.events_fired == 1
        assert len(reconcile_repo.find_open()) == 1


class TestSellDateWindow:
    def test_pdf_buys_after_sell_date_are_not_candidates(
        self, trade_repo, reconcile_repo,
    ):
        """A PDF buy dated AFTER sell_date can't have funded this pair.
        Even if (code, qty) would otherwise match, it must NOT count
        as covering the SDK leg."""
        from invest.reconciliation.shioaji_audit import audit_realized_pairs

        # PDF buy is AFTER the sell - ineligible to cover this pair's leg.
        trade_repo.insert(_pdf_buy("2330", date(2026, 5, 1), 1000))

        pairs = [
            _sdk_leg(pair_id="P1", code="2330", d="2026-05-01", qty=1000),
            _sdk_sell(pair_id="P1", code="2330", d="2026-04-15", qty=1000),
        ]
        result = audit_realized_pairs(
            realized_pairs=pairs,
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )
        assert result.events_fired == 1


class TestReadOnly:
    def test_trade_table_is_not_mutated(
        self, session, trade_repo, reconcile_repo,
    ):
        """Audit reads trades; never writes/updates/deletes them."""
        from invest.reconciliation.shioaji_audit import audit_realized_pairs

        trade_repo.insert(_pdf_buy("2330", date(2026, 1, 10), 1000))
        pairs = [
            _sdk_leg(pair_id="P1", code="2454", d="2026-01-15", qty=200),
            _sdk_sell(pair_id="P1", code="2454", d="2026-04-01", qty=200),
        ]
        before = list(session.exec(select(Trade)).all())
        before_snap = [(t.code, t.date, t.qty, t.source) for t in before]

        audit_realized_pairs(
            realized_pairs=pairs,
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )

        after = list(session.exec(select(Trade)).all())
        after_snap = [(t.code, t.date, t.qty, t.source) for t in after]
        assert before_snap == after_snap


class TestDegeneratePair:
    def test_sell_summary_with_no_legs_does_not_crash(
        self, trade_repo, reconcile_repo,
    ):
        """sync._merge_records pre-filters qty=0 sell summaries, but
        the audit hook may receive raw realized_pairs from contexts
        other than sync. A pair with just the sell summary (legs
        unrecoverable due to rate-limited list_profit_loss_detail)
        must not crash and must not fire - there's nothing to compare."""
        from invest.reconciliation.shioaji_audit import audit_realized_pairs

        pairs = [
            _sdk_sell(pair_id="P1", code="2330", d="2026-04-15", qty=0),
        ]
        result = audit_realized_pairs(
            realized_pairs=pairs,
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )
        assert result.events_fired == 0


class TestEventDetailSchema:
    def test_detail_carries_full_context(
        self, trade_repo, reconcile_repo,
    ):
        """detail must include pair_id, code, sell_date, missing_legs.
        Each missing_leg dict must include date and qty so the operator
        can act without re-querying the SDK."""
        from invest.reconciliation.shioaji_audit import audit_realized_pairs

        pairs = [
            _sdk_leg(pair_id="P1", code="2454", d="2026-01-15", qty=200),
            _sdk_leg(pair_id="P1", code="2454", d="2026-02-20", qty=300),
            _sdk_sell(pair_id="P1", code="2454", d="2026-04-01", qty=500),
        ]
        audit_realized_pairs(
            realized_pairs=pairs,
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )
        e = reconcile_repo.find_open()[0]
        d = e.detail
        assert set(d.keys()) >= {"pair_id", "code", "sell_date", "missing_legs"}
        assert d["pair_id"] == "P1"
        assert d["code"] == "2454"
        assert d["sell_date"] == "2026-04-01"
        keys = {(leg["date"], leg["qty"]) for leg in d["missing_legs"]}
        assert keys == {("2026-01-15", 200), ("2026-02-20", 300)}


class TestPairIdNormalization:
    def test_int_pair_id_round_trips_via_json(
        self, trade_repo, reconcile_repo,
    ):
        """Shioaji's pl.id can be int or str. ReconcileEvent.detail is a
        JSON column - values round-trip by type. The dedup logic must
        compare normalized values (str(pair_id)) so a follow-up run
        with the same int pair_id still matches the open event."""
        from invest.reconciliation.shioaji_audit import audit_realized_pairs

        pairs = [
            _sdk_leg(pair_id=12345, code="2454", d="2026-01-15", qty=200),
            _sdk_sell(pair_id=12345, code="2454", d="2026-04-01", qty=200),
        ]
        audit_realized_pairs(
            realized_pairs=pairs,
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )
        result2 = audit_realized_pairs(
            realized_pairs=pairs,
            trade_repo=trade_repo, reconcile_repo=reconcile_repo,
        )
        assert result2.events_fired == 0
        assert len(reconcile_repo.find_open()) == 1


# --- Phase 14.5 — `run()` orchestrator -----------------------------------
#
# The post-PDF caller (jobs.snapshot_workflow.run) holds a DailyStore but
# no SQLModel session. `run()` is the seam: bootstrap engine + session
# against the same SQLite file, defensively skip when the trades table
# is empty (operator hasn't run scripts/backfill_trades.py), then
# delegate to audit_realized_pairs.


class TestRunOrchestrator:
    def test_empty_pairs_returns_zero_without_db_touch(self, tmp_path):
        """Empty input is a no-op: no engine bootstrap, no audit call."""
        from invest.persistence.daily_store import DailyStore
        from invest.reconciliation import shioaji_audit

        store = DailyStore(tmp_path / "audit_run.db")
        # NOTE: deliberately not calling init_schema — the empty-pair
        # short-circuit must run before any DB I/O.
        result = shioaji_audit.run([], daily_store=store)
        assert result.pairs_examined == 0
        assert result.events_fired == 0

    def test_skips_audit_when_trade_table_empty(self, tmp_path, caplog):
        """Defensive: a fresh dashboard.db (no PDF trades yet) would
        flag every SDK leg as `missing` — skip silently and log.

        Mirrors the FastAPI startup order: SQLModel.create_all owns the
        `trades`, `reconcile_events`, `failed_tasks` tables; legacy
        DailyStore.init_schema only creates the regenerable cache
        tables (prices, fx_daily, positions_daily, ...). We don't call
        init_schema here — run() bootstraps the SQLModel shape itself.
        """
        import logging

        from invest.persistence.daily_store import DailyStore
        from invest.reconciliation import shioaji_audit

        store = DailyStore(tmp_path / "audit_empty.db")
        pairs = [
            _sdk_leg(pair_id="P1", code="2330", d="2026-01-10", qty=1000),
            _sdk_sell(pair_id="P1", code="2330", d="2026-03-15", qty=1000),
        ]
        with caplog.at_level(logging.INFO, logger=shioaji_audit.__name__):
            result = shioaji_audit.run(pairs, daily_store=store)
        assert result.pairs_examined == 0
        assert result.events_fired == 0
        assert any(
            "trades table empty" in r.message for r in caplog.records
        )

    def test_fires_event_on_real_dailystore_with_populated_trades(
        self, tmp_path,
    ):
        """End-to-end: a populated trades table + a missing leg fires
        a real ReconcileEvent row.

        Same startup-order assumption as the empty-table test —
        SQLModel.metadata.create_all owns the `trades` and
        `reconcile_events` schemas in the FastAPI process, and run()
        invokes it during bootstrap. Calling DailyStore.init_schema()
        first would create incompatible legacy DDL for those two
        tables; that schema fork is tracked separately.
        """
        from sqlmodel import Session, SQLModel, create_engine

        from invest.persistence.daily_store import DailyStore
        from invest.persistence.repositories.reconcile_repo import (
            ReconcileRepo,
        )
        from invest.persistence.repositories.trade_repo import TradeRepo
        from invest.reconciliation import shioaji_audit

        db = tmp_path / "audit_e2e.db"
        store = DailyStore(db)
        engine = create_engine(f"sqlite:///{db}")
        SQLModel.metadata.create_all(engine)

        # Seed one PDF buy, deliberately leaving the second leg uncovered.
        with Session(engine) as s:
            TradeRepo(s).insert(_pdf_buy("2330", date(2026, 1, 10), 1000))

        pairs = [
            _sdk_leg(pair_id="P1", code="2330", d="2026-01-10", qty=1000),
            _sdk_leg(pair_id="P1", code="2330", d="2026-02-05", qty=500),
            _sdk_sell(pair_id="P1", code="2330", d="2026-03-15", qty=1500),
        ]
        result = shioaji_audit.run(pairs, daily_store=store)
        assert result.pairs_examined == 1
        assert result.events_fired == 1

        with Session(engine) as s:
            events = ReconcileRepo(s).find_open()
        assert len(events) == 1
        assert events[0].event_type == "broker_pdf_buy_leg_mismatch"
        missing = events[0].detail["missing_legs"]
        assert missing == [{"date": "2026-02-05", "qty": 500}]
