"""Tests for the two-pass orchestration: discovery → price-fetch → projection.

Background (Phase 11 Path A bug, found 2026-05-01):
  merge() never wrote a single overlay row in production despite the
  audit hook firing 16 times. Root cause: prices for overlay-discovered
  symbols (6531, 7769, etc.) were never fetched — the price-fetcher in
  snapshot_daily.run() iterated only PDF-known symbols. merge() then
  silently skipped every write at `if close is None: continue`.

The fix is a two-pass orchestration in snapshot_daily:
  1. Pull SDK sources once → discover symbol universe.
  2. Fetch prices for any symbols not already in the PDF set.
  3. Run merge with the pre-pulled SDK data so the broker isn't called twice.

This file pins:
  - pull_sdk_sources() returns (session, lots, pairs).
  - discover_overlay_symbols() flattens to {code, ...}.
  - merge() accepts pre-pulled data and skips internal SDK calls.
  - audit-hook dedups against existing open events for the same pair_id.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app import reconcile, trade_overlay
from app.daily_store import DailyStore


# --- Fixtures -------------------------------------------------------------


@pytest.fixture()
def store(tmp_path: Path) -> DailyStore:
    s = DailyStore(tmp_path / "orchestration.db")
    s.init_schema()
    return s


@pytest.fixture()
def empty_portfolio() -> dict:
    return {
        "months": [{"month": "2026-03",
                    "tw": {"holdings": []},
                    "foreign": {"holdings": []}}],
        "summary": {"all_trades": []},
    }


class _RecordingFakeClient:
    """Counts calls per surface so tests can prove single-pass behavior."""

    def __init__(self, lots=None, pairs=None, trades=None):
        self._lots = lots or []
        self._pairs = pairs or []
        self._trades = trades or []
        self.lots_calls = 0
        self.pairs_calls = 0
        self.trades_calls = 0

    def lazy_login(self):
        return True

    def list_trades(self, start, end):
        self.trades_calls += 1
        return [t for t in self._trades if start <= t["date"] <= end]

    def list_open_lots(self, close_resolver=None):
        self.lots_calls += 1
        return list(self._lots)

    def list_realized_pairs(self, start, end):
        self.pairs_calls += 1
        return list(self._pairs)


# --- pull_sdk_sources ----------------------------------------------------


def test_pull_sdk_sources_returns_tuple_of_three_lists(store):
    """The shared 3-source pull is the seam between discovery and merge.
    Both call sites consume the same tuple — pinning the shape here."""
    fake = _RecordingFakeClient(
        trades=[{"date": "2026-04-22", "code": "2330", "side": "普買",
                 "qty": 1000, "price": 920.0, "ccy": "TWD", "venue": "TW"}],
        lots=[{"date": "2026-03-10", "code": "00981A", "qty": 2000.0,
               "cost_twd": 60_000.0, "mv_twd": 64_000.0,
               "type": "融資", "ccy": "TWD", "venue": "TW"}],
        pairs=[{"date": "2026-04-15", "code": "7769", "side": "普賣",
                "qty": 3000, "price": 210.0, "pair_id": 1,
                "ccy": "TWD", "venue": "TW", "type": "現股"}],
    )

    session, lots, pairs = trade_overlay.pull_sdk_sources(
        fake, store, "2026-04-01", "2026-04-26"
    )

    assert len(session) == 1 and session[0]["code"] == "2330"
    assert len(lots) == 1 and lots[0]["code"] == "00981A"
    assert len(pairs) == 1 and pairs[0]["code"] == "7769"
    # Each surface called exactly once — the helper is the single seam.
    assert fake.trades_calls == 1
    assert fake.lots_calls == 1
    assert fake.pairs_calls == 1


def test_pull_sdk_sources_empty_when_unconfigured(store):
    """No creds → pull returns three empty lists, no exceptions."""

    class _Unconfigured:
        def lazy_login(self):
            return False

        def list_trades(self, *a, **k):
            raise AssertionError("must not be called")

        def list_open_lots(self, *a, **k):
            raise AssertionError("must not be called")

        def list_realized_pairs(self, *a, **k):
            raise AssertionError("must not be called")

    session, lots, pairs = trade_overlay.pull_sdk_sources(
        _Unconfigured(), store, "2026-04-01", "2026-04-26"
    )
    assert session == [] and lots == [] and pairs == []


# --- discover_overlay_symbols --------------------------------------------


def test_discover_overlay_symbols_unions_codes_from_all_three_sources(store):
    """The discovery set must be a SUPERSET of every TW code reachable
    via the SDK. Missing one means snapshot_daily skips its price fetch
    and merge() silently drops that symbol's positions_daily writes —
    exactly the bug we're fixing."""
    fake = _RecordingFakeClient(
        trades=[{"date": "2026-04-22", "code": "2330", "side": "普買",
                 "qty": 1000, "price": 920.0, "ccy": "TWD", "venue": "TW"}],
        lots=[{"date": "2026-03-10", "code": "00981A", "qty": 2000.0,
               "cost_twd": 60_000.0, "mv_twd": 64_000.0,
               "type": "融資", "ccy": "TWD", "venue": "TW"}],
        pairs=[
            {"date": "2026-02-08", "code": "7769", "side": "普買",
             "qty": 1000, "price": 210.0, "cost_twd": 210_000.0,
             "pair_id": 5, "ccy": "TWD", "venue": "TW", "type": "現股"},
            {"date": "2026-04-15", "code": "7769", "side": "普賣",
             "qty": 1000, "price": 215.0, "pair_id": 5,
             "ccy": "TWD", "venue": "TW", "type": "現股"},
            {"date": "2026-04-27", "code": "6531", "side": "普賣",
             "qty": 50, "price": 681.0, "pair_id": 6,
             "ccy": "TWD", "venue": "TW", "type": "現股"},
        ],
    )

    symbols = trade_overlay.discover_overlay_symbols(
        fake, store, "2026-04-01", "2026-04-26"
    )

    assert symbols == {"2330", "00981A", "7769", "6531"}


def test_discover_overlay_symbols_empty_when_unconfigured(store):
    class _Unconfigured:
        def lazy_login(self):
            return False

        def list_trades(self, *a, **k):
            return []

        def list_open_lots(self, *a, **k):
            return []

        def list_realized_pairs(self, *a, **k):
            return []

    assert trade_overlay.discover_overlay_symbols(
        _Unconfigured(), store, "2026-04-01", "2026-04-26"
    ) == set()


# --- merge() with pre-pulled data ----------------------------------------


def test_merge_with_sdk_data_skips_internal_sdk_calls(store, empty_portfolio):
    """When orchestrator already pulled the 3 sources, merge() must not
    call them again — that's the whole point of the two-pass design."""
    # Seed a price so merge can write the overlay row
    with store.connect_rw() as conn:
        conn.execute(
            "INSERT INTO prices(date, symbol, close, currency, source, fetched_at)"
            " VALUES ('2026-04-22', '6531', 681.0, 'TWD', 'yfinance',"
            " '2026-04-22T00:00:00Z')"
        )

    fake = _RecordingFakeClient()  # all surfaces will raise if hit
    pre_pulled = (
        [],  # session
        [{"date": "2026-03-15", "code": "6531", "qty": 50.0,
          "cost_twd": 34_000.0, "mv_twd": 34_050.0,
          "type": "現股", "ccy": "TWD", "venue": "TW"}],  # lots
        [],  # pairs
    )

    summary = trade_overlay.merge(
        store, empty_portfolio, fake,
        gap_start="2026-04-01", gap_end="2026-04-26",
        sdk_data=pre_pulled,
    )

    # Critical: merge() did NOT call the SDK because data was provided
    assert fake.trades_calls == 0
    assert fake.lots_calls == 0
    assert fake.pairs_calls == 0
    # And it actually wrote the overlay row
    assert summary["overlay_trades"] >= 1
    with store.connect_ro() as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM positions_daily WHERE source='overlay' "
            "AND symbol='6531'"
        ).fetchone()[0]
    assert n >= 1


def test_merge_without_sdk_data_pulls_internally_for_backwards_compat(
    store, empty_portfolio
):
    """Existing callers that don't pre-pull (CLI ad-hoc, tests) must
    keep working. merge() pulls internally when sdk_data is None."""
    fake = _RecordingFakeClient(
        lots=[{"date": "2026-03-15", "code": "6531", "qty": 50.0,
               "cost_twd": 34_000.0, "mv_twd": 34_050.0,
               "type": "現股", "ccy": "TWD", "venue": "TW"}],
    )

    trade_overlay.merge(
        store, empty_portfolio, fake,
        gap_start="2026-04-01", gap_end="2026-04-26",
    )

    assert fake.trades_calls == 1
    assert fake.lots_calls == 1
    assert fake.pairs_calls == 1


# --- Audit event dedup ---------------------------------------------------


def _portfolio_with_pdf(*trades) -> dict:
    return {
        "months": [{"month": "2026-03", "tw": {"holdings": []},
                    "foreign": {"holdings": []}}],
        "summary": {
            "all_trades": [
                {"month": d[:7], "date": d.replace("-", "/"), "venue": "TW",
                 "side": s, "code": c, "qty": q, "price": p, "ccy": "TWD"}
                for (d, c, s, q, p) in trades
            ],
        },
    }


@pytest.mark.skip(reason="trade_overlay._audit_policy is silent (2026-05-01); un-skip when implementing option A/B/C")
def test_audit_hook_skips_when_open_event_for_pair_id_already_exists(store):
    """Re-running merge() must not insert a duplicate audit event for
    the same pair_id. Production hit this on 2026-05-01: 8 unique
    mismatches → 16 events after two refresh runs."""
    portfolio = _portfolio_with_pdf(
        ("2025-11-15", "7769", "普買", 1000, 200.0),
    )
    pairs = [
        {"date": d, "code": "7769", "side": "普買", "qty": 1000,
         "price": p, "cost_twd": 1000 * p, "ccy": "TWD", "venue": "TW",
         "type": "現股", "pair_id": 999}
        for (d, p) in [("2025-11-15", 200.0), ("2025-12-03", 205.0)]
    ]
    pairs.append({
        "date": "2026-04-15", "code": "7769", "side": "普賣", "qty": 2000,
        "price": 210.0, "ccy": "TWD", "venue": "TW",
        "type": "現股", "pair_id": 999, "pnl": 20_000.0,
    })

    class _Fake(_RecordingFakeClient):
        def __init__(self):
            super().__init__(pairs=pairs)

    # First run inserts 1 audit event
    trade_overlay.merge(
        store, portfolio, _Fake(),
        gap_start="2026-04-01", gap_end="2026-04-26"
    )
    n1 = len([
        e for e in reconcile.get_open_events(store)
        if json.loads(e["diff_summary"]).get("event_type")
           == "broker_pdf_buy_leg_mismatch"
    ])
    assert n1 == 1

    # Second run with identical SDK data must NOT insert a duplicate
    trade_overlay.merge(
        store, portfolio, _Fake(),
        gap_start="2026-04-01", gap_end="2026-04-26"
    )
    n2 = len([
        e for e in reconcile.get_open_events(store)
        if json.loads(e["diff_summary"]).get("event_type")
           == "broker_pdf_buy_leg_mismatch"
    ])
    assert n2 == 1, "audit hook duplicated event for same pair_id"


@pytest.mark.skip(reason="trade_overlay._audit_policy is silent (2026-05-01); un-skip when implementing option A/B/C")
def test_audit_hook_refires_after_dismissal(store):
    """Dismissed events should NOT block re-firing. Rationale: dismissal
    means 'I've reviewed this divergence' — if the same divergence is
    still observable on the next run, the operator wants to know."""
    portfolio = _portfolio_with_pdf(
        ("2025-11-15", "7769", "普買", 1000, 200.0),
    )
    pairs = [
        {"date": "2025-11-15", "code": "7769", "side": "普買",
         "qty": 1000, "price": 200.0, "cost_twd": 200_000,
         "ccy": "TWD", "venue": "TW", "type": "現股", "pair_id": 1000},
        {"date": "2025-12-03", "code": "7769", "side": "普買",
         "qty": 1000, "price": 205.0, "cost_twd": 205_000,
         "ccy": "TWD", "venue": "TW", "type": "現股", "pair_id": 1000},
        {"date": "2026-04-15", "code": "7769", "side": "普賣",
         "qty": 2000, "price": 210.0,
         "ccy": "TWD", "venue": "TW", "type": "現股", "pair_id": 1000,
         "pnl": 20_000.0},
    ]

    class _Fake(_RecordingFakeClient):
        def __init__(self):
            super().__init__(pairs=pairs)

    trade_overlay.merge(
        store, portfolio, _Fake(),
        gap_start="2026-04-01", gap_end="2026-04-26"
    )
    open_events = reconcile.get_open_events(store)
    assert len(open_events) == 1
    reconcile.dismiss_event(store, open_events[0]["id"])
    assert reconcile.get_open_events(store) == []

    # Re-run after dismissal: same divergence is still in the SDK data,
    # so the audit hook fires again.
    trade_overlay.merge(
        store, portfolio, _Fake(),
        gap_start="2026-04-01", gap_end="2026-04-26"
    )
    open_again = reconcile.get_open_events(store)
    assert len(open_again) == 1


# --- Banner endpoint surfacing audit fields ------------------------------


def test_today_reconcile_endpoint_surfaces_audit_event_fields(
    store, monkeypatch, tmp_path
):
    """Banner JS branches on event_type. The endpoint must expose enough
    payload from the audit JSON for the banner to render correct copy
    instead of '0 differing trades'."""
    # Insert one audit event by hand
    reconcile.record_event(
        store,
        event_type="broker_pdf_buy_leg_mismatch",
        detail={
            "pair_id": 11,
            "code": "6531",
            "sell_date": "2026-04-27",
            "sdk_leg_count": 5,
            "pdf_trade_count": 0,
            "sdk_legs": [],
            "pdf_trades": [],
        },
        pdf_month="2026-04",
    )

    # Spin up the Flask app pointed at this store
    monkeypatch.setenv("DAILY_DB_PATH", str(store.path))
    monkeypatch.delenv("BACKFILL_ON_STARTUP", raising=False)

    # Minimal portfolio.json
    pj = tmp_path / "portfolio.json"
    pj.write_text(json.dumps({
        "months": [], "summary": {"all_trades": []}
    }))

    from app import create_app
    app = create_app(pj)
    client = app.test_client()

    r = client.get("/api/today/reconcile")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    events = body["data"]["events"]
    assert len(events) == 1

    e = events[0]
    # New fields the banner needs
    assert e["event_type"] == "broker_pdf_buy_leg_mismatch"
    assert e["code"] == "6531"
    assert e["sdk_leg_count"] == 5
    assert e["pdf_trade_count"] == 0
    # Existing fields still present
    assert e["pdf_month"] == "2026-04"
