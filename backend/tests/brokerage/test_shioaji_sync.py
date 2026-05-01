"""Phase 5 Cycle 37 — invest.brokerage.shioaji_sync.

Pins the WRITE-side contract for the brokerage authority flip.

Two-source design (per Phase 5 design discussion):
  * list_realized_pairs and list_open_lots are mutually exclusive
    by SDK contract — never overlap.
  * list_trades is intentionally NOT consumed (overlaps with
    both above; sync runs post-close so intraday lag is moot).
  * Defense-in-depth: same dedup key in both surfaces → ValueError.

Idempotency via TradeRepo.replace_for_period(source='shioaji', ...).
PDF rows in the same date range have source='pdf' / 'pdf-foreign'
and survive every Shioaji sync.

T+1 finalization rule:
  SyncResult.finalized_through = end - 1. The orchestrator's high-
  water mark stops one day short of the run's end so the next sync
  re-includes today (when today's data finalizes).

Price derivation for open_lots uses Decimal arithmetic so
qty × price reconstructs cost_total exactly under 18-digit
Decimal precision — no analytics drift.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.persistence.models.trade import Trade
from invest.persistence.repositories.trade_repo import TradeRepo


# --- Fixtures -------------------------------------------------------------


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


class _FakeClient:
    """Stub ShioajiClient for sync tests.

    No network, no SDK. Returns canned per-surface output. Sync
    code only calls the three public methods we care about.
    """

    def __init__(
        self,
        *,
        realized: list[dict] | None = None,
        lots: list[dict] | None = None,
    ) -> None:
        self._realized = realized or []
        self._lots = lots or []
        self.realized_calls: list[tuple[str, str]] = []
        self.lots_calls = 0

    def list_realized_pairs(self, begin_date, end_date):
        self.realized_calls.append((begin_date, end_date))
        # Filter SELL date by the window — mirrors real client behavior.
        return [
            r for r in self._realized
            if r.get("side") != "普賣" or begin_date <= r["date"] <= end_date
        ]

    def list_open_lots(self, close_resolver=None):
        self.lots_calls += 1
        return list(self._lots)

    def list_trades(self, start_date, end_date):  # pragma: no cover
        # Sync MUST NOT call this — fail loudly if it does so the
        # 2-source design contract is enforced by tests, not just docs.
        raise AssertionError(
            "shioaji_sync should not consume list_trades — "
            "two-source design"
        )


# --- Test cases -----------------------------------------------------------


def test_empty_state_writes_nothing_no_errors(session):
    """No realized, no lots → 0 Trade rows, no exception, finalized
    through end-1 still set correctly."""
    from invest.brokerage.shioaji_sync import sync_shioaji_trades

    client = _FakeClient()
    start, end = date(2026, 4, 17), date(2026, 4, 20)
    result = sync_shioaji_trades(client, session, start, end)

    assert result.written == 0
    assert result.sources_seen == {"realized_pair": 0, "open_lot": 0}
    assert result.finalized_through == date(2026, 4, 19)


def test_realized_pair_writes_buy_legs_and_sell_summary(session):
    """One closed pair: 2 buy legs (2025-12-03 + 2026-04-10), sold
    2026-04-15. Writes 3 Trade rows with source='shioaji'.

    INVARIANT: buy legs may pre-date the [start, end] window — the
    realized-pairs surface filters SELLs only (decision #1 option C),
    and the buy legs ride along regardless of when they happened.
    The window for replace_for_period must be expanded to cover the
    earliest leg, otherwise the buy rows would land outside the
    delete-and-replace bound and orphan in the table.
    """
    from invest.brokerage.shioaji_sync import sync_shioaji_trades

    realized = [
        {"date": "2025-12-03", "code": "7769", "side": "普買",
         "qty": 1000.0, "price": 205.0, "cost_twd": 205_000.0,
         "ccy": "TWD", "venue": "TW", "type": "現股", "pair_id": 101},
        {"date": "2026-04-10", "code": "7769", "side": "普買",
         "qty": 500.0, "price": 210.0, "cost_twd": 105_000.0,
         "ccy": "TWD", "venue": "TW", "type": "現股", "pair_id": 101},
        {"date": "2026-04-15", "code": "7769", "side": "普賣",
         "qty": 1500.0, "price": 220.0, "ccy": "TWD",
         "venue": "TW", "type": "現股", "pair_id": 101, "pnl": 22_500.0},
    ]
    client = _FakeClient(realized=realized)

    # Window must include the earliest buy leg date for replace_for_period
    # to bound the write set correctly.
    start, end = date(2025, 12, 1), date(2026, 4, 20)
    result = sync_shioaji_trades(client, session, start, end)

    assert result.written == 3

    repo = TradeRepo(session)
    rows = repo.find_by_source("shioaji")
    assert len(rows) == 3

    by_date = {r.date: r for r in rows}
    assert by_date[date(2025, 12, 3)].side == 1   # CASH_BUY
    assert by_date[date(2026, 4, 10)].side == 1
    assert by_date[date(2026, 4, 15)].side == 2   # CASH_SELL
    assert by_date[date(2025, 12, 3)].qty == 1000
    assert by_date[date(2026, 4, 15)].qty == 1500
    assert by_date[date(2026, 4, 15)].price == Decimal("220")
    assert all(r.source == "shioaji" for r in rows)
    assert all(r.venue == "TW" for r in rows)
    assert all(r.currency == "TWD" for r in rows)


def test_open_lot_synthetic_buy_uses_decimal_price_exact_to_cost(session):
    """SDK quirk: open lots carry cost_total + derived qty, no
    per-share price. Sync derives price = Decimal(cost) / Decimal(qty).

    INVARIANT (production case): when cost is divisible by qty
    — which is the realistic broker-side state because they compute
    cost = qty × actual_fill_price + fees with cost stored to
    settlement precision — qty × derived_price reconstructs cost
    EXACTLY. Pins the FIFO P&L cost basis roundtrip for the case
    that actually appears in real data.
    """
    from invest.brokerage.shioaji_sync import sync_shioaji_trades

    lots = [
        {"date": "2026-04-10", "code": "2330",
         "qty": 1000.0, "cost_twd": 920_000.0, "mv_twd": 1_050_000.0,
         "ccy": "TWD", "venue": "TW", "type": "現股"},
    ]
    client = _FakeClient(lots=lots)
    sync_shioaji_trades(client, session, date(2026, 4, 1), date(2026, 4, 30))

    rows = TradeRepo(session).find_by_source("shioaji")
    assert len(rows) == 1
    row = rows[0]
    assert row.qty == 1000
    assert row.price == Decimal("920")
    # Roundtrip exact for divisible case
    assert row.qty * row.price == Decimal("920000")


def test_open_lot_price_is_bounded_for_non_divisible_cost(session):
    """Non-divisible cost/qty (e.g. broker-allocated odd-lot fees)
    cannot store exactly in any fixed-precision Decimal column —
    100_000 / 333 = 300.3003003003... is a repeating decimal.

    INVARIANT: Trade.price is stored at decimal_places=10, bounding
    the per-share error to 1e-10 NTD. For typical TW lot sizes
    (qty ≤ 10_000), the cost reconstruction error is bounded by
    1e-6 NTD — well below any analytic threshold. This test pins
    the precision contract so a future schema narrowing would
    surface immediately.
    """
    from invest.brokerage.shioaji_sync import sync_shioaji_trades

    lots = [
        {"date": "2026-04-10", "code": "2330",
         "qty": 333.0, "cost_twd": 100_000.0, "mv_twd": 105_000.0,
         "ccy": "TWD", "venue": "TW", "type": "現股"},
    ]
    client = _FakeClient(lots=lots)
    sync_shioaji_trades(client, session, date(2026, 4, 1), date(2026, 4, 30))

    rows = TradeRepo(session).find_by_source("shioaji")
    row = rows[0]
    error = abs(row.qty * row.price - Decimal("100000"))
    assert error < Decimal("0.000001")  # < 1 micro-NTD


def test_realized_and_lots_combine_when_disjoint(session):
    """Both surfaces populated, no overlap — sum of rows written."""
    from invest.brokerage.shioaji_sync import sync_shioaji_trades

    realized = [
        {"date": "2026-04-10", "code": "7769", "side": "普買",
         "qty": 1000.0, "price": 200.0, "cost_twd": 200_000.0,
         "ccy": "TWD", "venue": "TW", "type": "現股", "pair_id": 1},
        {"date": "2026-04-15", "code": "7769", "side": "普賣",
         "qty": 1000.0, "price": 220.0, "ccy": "TWD",
         "venue": "TW", "type": "現股", "pair_id": 1, "pnl": 20_000.0},
    ]
    lots = [
        {"date": "2026-04-12", "code": "2330",
         "qty": 1000.0, "cost_twd": 920_000.0, "mv_twd": 1_050_000.0,
         "ccy": "TWD", "venue": "TW", "type": "現股"},
    ]
    client = _FakeClient(realized=realized, lots=lots)
    start, end = date(2026, 4, 1), date(2026, 4, 30)
    result = sync_shioaji_trades(client, session, start, end)

    assert result.written == 3
    assert result.sources_seen == {"realized_pair": 2, "open_lot": 1}

    rows = TradeRepo(session).find_by_source("shioaji")
    codes = {r.code for r in rows}
    assert codes == {"7769", "2330"}


def test_sdk_contract_violation_raises_value_error(session):
    """Defense in depth: realized_pair AND open_lot share a dedup
    key → ValueError. Silent dedup would mask a real SDK regression
    where a sold position is also reported as held."""
    from invest.brokerage.shioaji_sync import sync_shioaji_trades

    # Same (date, code, side, qty) in both surfaces:
    realized = [
        {"date": "2026-04-10", "code": "2330", "side": "普買",
         "qty": 1000.0, "price": 920.0, "cost_twd": 920_000.0,
         "ccy": "TWD", "venue": "TW", "type": "現股", "pair_id": 1},
        {"date": "2026-04-15", "code": "2330", "side": "普賣",
         "qty": 1000.0, "price": 950.0, "ccy": "TWD",
         "venue": "TW", "type": "現股", "pair_id": 1, "pnl": 30_000.0},
    ]
    lots = [
        # Bug: SDK reports the sold position as still held
        {"date": "2026-04-10", "code": "2330",
         "qty": 1000.0, "cost_twd": 920_000.0, "mv_twd": 950_000.0,
         "ccy": "TWD", "venue": "TW", "type": "現股"},
    ]
    client = _FakeClient(realized=realized, lots=lots)

    with pytest.raises(ValueError, match="SDK contract violation"):
        sync_shioaji_trades(
            client, session, date(2026, 4, 1), date(2026, 4, 30),
        )


def test_idempotency_replaces_in_place(session):
    """Re-sync with same data → same row count, no duplicates."""
    from invest.brokerage.shioaji_sync import sync_shioaji_trades

    realized = [
        {"date": "2026-04-10", "code": "7769", "side": "普買",
         "qty": 1000.0, "price": 200.0, "cost_twd": 200_000.0,
         "ccy": "TWD", "venue": "TW", "type": "現股", "pair_id": 1},
        {"date": "2026-04-15", "code": "7769", "side": "普賣",
         "qty": 1000.0, "price": 220.0, "ccy": "TWD",
         "venue": "TW", "type": "現股", "pair_id": 1, "pnl": 20_000.0},
    ]
    client = _FakeClient(realized=realized)

    sync_shioaji_trades(client, session, date(2026, 4, 1), date(2026, 4, 30))
    sync_shioaji_trades(client, session, date(2026, 4, 1), date(2026, 4, 30))

    rows = TradeRepo(session).find_by_source("shioaji")
    assert len(rows) == 2  # not 4


def test_pdf_rows_in_same_window_survive_shioaji_sync(session):
    """INVARIANT: source='pdf' rows in the date window are NOT
    touched. replace_for_period filters by source AND date — Shioaji
    sync only deletes Shioaji rows. Phase 5 explicitly drops the
    PDF-canonical UPSERT guard for Shioaji writes, but the symmetric
    guarantee — PDFs not touched by Shioaji writes — comes from
    TradeRepo's per-source replacement, and we pin it here so a
    future repo refactor can't accidentally regress it."""
    from invest.brokerage.shioaji_sync import sync_shioaji_trades

    # Pre-existing PDF row in the window
    pdf_row = Trade(
        date=date(2026, 4, 12), code="6442",
        side=1, qty=500, price=Decimal("145"),
        currency="TWD", source="pdf", venue="TW",
    )
    session.add(pdf_row)
    session.commit()

    # Shioaji has its own (different code) trades in the same window
    realized = [
        {"date": "2026-04-10", "code": "2330", "side": "普買",
         "qty": 1000.0, "price": 920.0, "cost_twd": 920_000.0,
         "ccy": "TWD", "venue": "TW", "type": "現股", "pair_id": 1},
        {"date": "2026-04-15", "code": "2330", "side": "普賣",
         "qty": 1000.0, "price": 950.0, "ccy": "TWD",
         "venue": "TW", "type": "現股", "pair_id": 1, "pnl": 30_000.0},
    ]
    client = _FakeClient(realized=realized)
    sync_shioaji_trades(client, session, date(2026, 4, 1), date(2026, 4, 30))

    pdf_rows = TradeRepo(session).find_by_source("pdf")
    assert len(pdf_rows) == 1
    assert pdf_rows[0].code == "6442"


def test_finalized_through_is_end_minus_one(session):
    """T+1 rule: SyncResult.finalized_through == end - 1.
    The orchestrator persists this and computes the next sync's
    start as finalized_through + 1, ensuring today gets re-synced
    tomorrow when its data is finalized."""
    from invest.brokerage.shioaji_sync import sync_shioaji_trades

    client = _FakeClient()
    today = date(2026, 4, 20)
    result = sync_shioaji_trades(client, session, date(2026, 4, 17), today)

    assert result.finalized_through == date(2026, 4, 19)


def test_open_lots_outside_window_filtered(session):
    """Open lots dated before the [start, end] window are filtered
    out before merge. They're still real positions, but they don't
    belong to *this* sync run's idempotency scope and would otherwise
    expand replace_for_period beyond its intended bounds."""
    from invest.brokerage.shioaji_sync import sync_shioaji_trades

    lots = [
        {"date": "2025-01-15", "code": "OLD",
         "qty": 1000.0, "cost_twd": 100_000.0, "mv_twd": 110_000.0,
         "ccy": "TWD", "venue": "TW", "type": "現股"},
        {"date": "2026-04-10", "code": "NEW",
         "qty": 500.0, "cost_twd": 50_000.0, "mv_twd": 55_000.0,
         "ccy": "TWD", "venue": "TW", "type": "現股"},
    ]
    client = _FakeClient(lots=lots)
    start, end = date(2026, 4, 1), date(2026, 4, 30)
    result = sync_shioaji_trades(client, session, start, end)

    assert result.written == 1
    rows = TradeRepo(session).find_by_source("shioaji")
    assert len(rows) == 1
    assert rows[0].code == "NEW"


def test_degenerate_pair_qty_zero_skipped(session):
    """When list_profit_loss_detail returned no legs (rate-limit /
    partial response — handled in shioaji_client), the sell summary
    still emits with qty=0 from the client. shioaji_sync MUST NOT
    write a qty=0 Trade row — that's nonsense as a transaction.
    The reconcile-event channel surfaces this case (Cycle 38 task)."""
    from invest.brokerage.shioaji_sync import sync_shioaji_trades

    realized = [
        {"date": "2026-04-15", "code": "2317", "side": "普賣",
         "qty": 0.0, "price": 120.0, "ccy": "TWD",
         "venue": "TW", "type": "現股", "pair_id": 300, "pnl": 1_000.0},
    ]
    client = _FakeClient(realized=realized)
    result = sync_shioaji_trades(
        client, session, date(2026, 4, 1), date(2026, 4, 30),
    )

    assert result.written == 0
    assert TradeRepo(session).find_by_source("shioaji") == []


def test_sync_does_not_consume_list_trades(session):
    """Two-source design contract: list_trades is NOT called by sync.
    The fake client's list_trades raises AssertionError; sync passing
    confirms the surface is untouched. Pinning this prevents a
    future commit from reintroducing list_trades and silently
    creating overlap that depends on dedup logic."""
    from invest.brokerage.shioaji_sync import sync_shioaji_trades

    client = _FakeClient()
    sync_shioaji_trades(client, session, date(2026, 4, 1), date(2026, 4, 30))
    # If sync called list_trades, the fake would have raised.
