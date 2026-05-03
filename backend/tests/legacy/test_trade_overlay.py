"""Phase 11 — trade_overlay tests.

The overlay's job: fill the gap between the last PDF month-end and today
with trades pulled from Shioaji. After this runs, positions_daily for
gap dates carries `source='overlay'` rows and portfolio_daily.has_overlay
flips to 1.

Source-of-truth contract: the PDF parser is authoritative for everything
up to and including the last month-end. The overlay never overwrites
PDF-sourced rows — those are immutable.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from invest.persistence.daily_store import DailyStore
from invest.brokerage import trade_overlay


# --- compute_gap_window ---------------------------------------------------

def test_compute_gap_window_starts_day_after_latest_pdf_month_end():
    """Latest portfolio month is '2026-03'. Gap starts 2026-04-01."""
    portfolio = {
        "months": [
            {"month": "2026-02"},
            {"month": "2026-03"},
        ],
    }
    out = trade_overlay.compute_gap_window(portfolio, today="2026-04-26")
    assert out == ("2026-04-01", "2026-04-26")


def test_compute_gap_window_returns_none_when_no_months():
    """No PDF data → no gap to fill (overlay is meaningless)."""
    portfolio = {"months": []}
    out = trade_overlay.compute_gap_window(portfolio, today="2026-04-26")
    assert out is None


def test_compute_gap_window_returns_none_when_today_inside_pdf_month():
    """If the latest PDF month covers today (e.g., user re-parsed mid-month
    after the partial statement landed), the gap is a no-op."""
    portfolio = {"months": [{"month": "2026-04"}]}
    # Today is 2026-04-15, latest_month is 2026-04 → gap_start would be
    # 2026-05-01 which is after today → nothing to fill.
    out = trade_overlay.compute_gap_window(portfolio, today="2026-04-15")
    assert out is None


# --- merge() persistence --------------------------------------------------


@pytest.fixture()
def store(tmp_path: Path) -> DailyStore:
    s = DailyStore(tmp_path / "overlay.db")
    s.init_schema()
    # Seed a price for the symbol we'll overlay so positions_daily can be
    # computed.
    with s.connect_rw() as conn:
        conn.execute(
            "INSERT INTO prices(date, symbol, close, currency, source, ingested_at)"
            " VALUES ('2026-04-22', '2330', 920.0, 'TWD', 'yfinance', '2026-04-22T00:00:00Z')"
        )
        conn.execute(
            "INSERT INTO prices(date, symbol, close, currency, source, ingested_at)"
            " VALUES ('2026-04-23', '2330', 925.0, 'TWD', 'yfinance', '2026-04-23T00:00:00Z')"
        )
    return s


@pytest.fixture()
def portfolio_with_held_position(tmp_path: Path) -> Path:
    """Portfolio.json with one held TW position (2330) at month-end 2026-03."""
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps({
        "months": [
            {
                "month": "2026-03",
                "tw": {
                    "holdings": [
                        {"code": "2330", "name": "TSMC", "qty": 1000,
                         "avg_cost": 880.0, "type": "現股"},
                    ],
                },
                "foreign": {"holdings": []},
            }
        ],
        "summary": {
            "all_trades": [
                {"month": "2026-03", "date": "2026/03/15", "venue": "TW",
                 "side": "普買", "code": "2330", "qty": 1000, "price": 880.0, "ccy": "TWD"},
            ],
        },
    }))
    return p


class _FakeShioajiClient:
    """Doubles for ShioajiClient — returns canned data per source.

    Phase 11 Path A added two new methods: list_open_lots() and
    list_realized_pairs(). Existing tests pass empty default stubs so
    the 3-source merge falls back cleanly to single-source behavior.
    """

    def __init__(self, trades=None, lots=None, pairs=None, configured=True):
        self._trades = trades or []
        self._lots = lots or []
        self._pairs = pairs or []
        self.configured = configured
        self.calls: list[tuple[str, str]] = []
        self.lots_calls = 0
        self.pairs_calls: list[tuple[str, str]] = []

    def lazy_login(self):
        return self.configured

    def list_trades(self, start, end):
        self.calls.append((start, end))
        return [t for t in self._trades if start <= t["date"] <= end]

    def list_open_lots(self, close_resolver=None):
        self.lots_calls += 1
        # When tests provide raw lots WITH qty pre-derived (skipping the
        # resolver path), pass them through. The merge-layer test fixtures
        # don't exercise the resolver path — that's covered in
        # test_shioaji_client.py.
        return list(self._lots)

    def list_realized_pairs(self, start, end):
        self.pairs_calls.append((start, end))
        return [
            p for p in self._pairs
            if (p.get("side") == "普賣" and start <= p["date"] <= end)
            or p.get("side") == "普買"  # buy legs may pre-date start (Decision C)
        ]


def test_merge_writes_overlay_positions_for_gap_dates(store, portfolio_with_held_position):
    """A buy on 2026-04-22 should produce a positions_daily row with
    source='overlay' on 2026-04-22 and 2026-04-23 (the held qty carries
    forward across all priced days in the gap)."""
    fake = _FakeShioajiClient(trades=[
        {"date": "2026-04-22", "code": "2330", "side": "普買", "qty": 500,
         "price": 920.0, "ccy": "TWD", "venue": "TW"},
    ])
    portfolio = json.loads(Path(portfolio_with_held_position).read_text())

    summary = trade_overlay.merge(
        store, portfolio, fake, gap_start="2026-04-01", gap_end="2026-04-26"
    )

    assert summary["overlay_trades"] == 1
    assert fake.calls == [("2026-04-01", "2026-04-26")]

    with store.connect_ro() as conn:
        rows = list(conn.execute(
            "SELECT date, symbol, qty, source FROM positions_daily ORDER BY date"
        ).fetchall())

    # Both priced gap dates produce a row; opening qty 1000 + 500 buy = 1500
    assert len(rows) == 2
    assert all(r["source"] == "overlay" for r in rows)
    assert all(r["symbol"] == "2330" for r in rows)
    assert rows[0]["qty"] == 1500.0
    assert rows[1]["qty"] == 1500.0


def test_merge_flips_has_overlay_flag(store, portfolio_with_held_position):
    """has_overlay=1 is set by derive when it sees source='overlay' rows
    in positions_daily — under the single-writer architecture (Option B,
    2026-05-01), merge() writes only positions_daily; derive owns
    portfolio_daily.has_overlay. The contract being tested here is that
    the integrated flow (merge → derive) flips the flag; merge alone
    no longer writes portfolio_daily."""
    from invest.jobs import backfill_runner

    fake = _FakeShioajiClient(trades=[
        {"date": "2026-04-22", "code": "2330", "side": "普買", "qty": 500,
         "price": 920.0, "ccy": "TWD", "venue": "TW"},
    ])
    portfolio = json.loads(Path(portfolio_with_held_position).read_text())

    trade_overlay.merge(
        store, portfolio, fake, gap_start="2026-04-01", gap_end="2026-04-26"
    )
    backfill_runner._derive_positions_and_portfolio(store, portfolio)

    with store.connect_ro() as conn:
        flags = [r[0] for r in conn.execute(
            "SELECT has_overlay FROM portfolio_daily ORDER BY date"
        ).fetchall()]
    assert flags == [1, 1]


def test_merge_noop_when_client_unconfigured(store, portfolio_with_held_position):
    """No creds → merge() must be a clean no-op. Tests run as part of the
    full suite without hitting any real API."""
    fake = _FakeShioajiClient(trades=[], configured=False)
    portfolio = json.loads(Path(portfolio_with_held_position).read_text())

    summary = trade_overlay.merge(
        store, portfolio, fake, gap_start="2026-04-01", gap_end="2026-04-26"
    )

    assert summary["overlay_trades"] == 0
    assert summary["skipped_reason"] == "shioaji_unconfigured"

    with store.connect_ro() as conn:
        n = conn.execute("SELECT COUNT(*) FROM positions_daily").fetchone()[0]
    assert n == 0


def test_merge_noop_when_no_gap(store, portfolio_with_held_position):
    """gap_start > gap_end (or None window) → no API call, no rows."""
    fake = _FakeShioajiClient(trades=[
        {"date": "2026-04-22", "code": "2330", "side": "普買", "qty": 500,
         "price": 920.0, "ccy": "TWD", "venue": "TW"},
    ])
    portfolio = json.loads(Path(portfolio_with_held_position).read_text())

    summary = trade_overlay.merge(
        store, portfolio, fake, gap_start=None, gap_end=None
    )

    assert summary["overlay_trades"] == 0
    assert summary["skipped_reason"] == "no_gap"
    assert fake.calls == []


def test_merge_does_not_overwrite_pdf_sourced_rows(store, portfolio_with_held_position):
    """If positions_daily already has a 'pdf' row for a gap date (shouldn't
    happen but defense in depth), the overlay must NOT replace it. PDF is
    canonical."""
    with store.connect_rw() as conn:
        conn.execute(
            "INSERT INTO positions_daily(date, symbol, qty, cost_local, "
            "mv_local, mv_twd, type, source) "
            "VALUES ('2026-04-22', '2330', 1000, 880.0, 920000, 920000, '現股', 'pdf')"
        )

    fake = _FakeShioajiClient(trades=[
        {"date": "2026-04-22", "code": "2330", "side": "普買", "qty": 500,
         "price": 920.0, "ccy": "TWD", "venue": "TW"},
    ])
    portfolio = json.loads(Path(portfolio_with_held_position).read_text())

    trade_overlay.merge(
        store, portfolio, fake, gap_start="2026-04-01", gap_end="2026-04-26"
    )

    with store.connect_ro() as conn:
        row = conn.execute(
            "SELECT qty, source FROM positions_daily "
            "WHERE date='2026-04-22' AND symbol='2330'"
        ).fetchone()
    assert row["source"] == "pdf"  # untouched
    assert row["qty"] == 1000.0    # NOT 1500


# --- Phase 11 Path A: 3-source merge integration -------------------------


def test_merge_calls_all_three_shioaji_surfaces(store, portfolio_with_held_position):
    """Plan §"3-source merge": merge() must invoke list_open_lots,
    list_realized_pairs, and list_trades — not just list_trades.
    """
    fake = _FakeShioajiClient()
    portfolio = json.loads(Path(portfolio_with_held_position).read_text())

    trade_overlay.merge(
        store, portfolio, fake, gap_start="2026-04-01", gap_end="2026-04-26"
    )

    assert fake.lots_calls == 1
    assert fake.pairs_calls == [("2026-04-01", "2026-04-26")]
    assert fake.calls == [("2026-04-01", "2026-04-26")]


def test_merge_writes_open_lot_position_for_currently_held_only_lot(store, tmp_path):
    """A currently-held lot from list_open_lots becomes a synthetic
    '普買' opening trade dated to lot.date, threaded through qty_history.
    For a code that's NOT in PDF history, the overlay still recognizes
    it as held and writes positions_daily rows for the gap."""
    # Portfolio has NO holdings; the open-lot is the only signal that
    # 00981A is held.
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps({
        "months": [{"month": "2026-03",
                    "tw": {"holdings": []},
                    "foreign": {"holdings": []}}],
        "summary": {"all_trades": []},
    }))
    portfolio = json.loads(p.read_text())

    # Seed a price for 00981A so positions_daily can compute MV.
    with store.connect_rw() as conn:
        conn.execute(
            "INSERT INTO prices(date, symbol, close, currency, source, ingested_at)"
            " VALUES ('2026-04-22', '00981A', 32.0, 'TWD', 'yfinance', '2026-04-22T00:00:00Z')"
        )

    fake = _FakeShioajiClient(lots=[
        {"date": "2026-03-10", "code": "00981A", "qty": 2000.0,
         "cost_twd": 60_000.0, "mv_twd": 64_000.0,
         "type": "融資", "ccy": "TWD", "venue": "TW"},
    ])

    summary = trade_overlay.merge(
        store, portfolio, fake, gap_start="2026-04-01", gap_end="2026-04-26"
    )

    with store.connect_ro() as conn:
        rows = list(conn.execute(
            "SELECT date, symbol, qty, type, source FROM positions_daily "
            "ORDER BY date"
        ).fetchall())

    assert len(rows) == 1
    assert rows[0]["symbol"] == "00981A"
    assert rows[0]["qty"] == 2000.0
    assert rows[0]["source"] == "overlay"
    assert summary["overlay_trades"] >= 1


def test_merge_writes_realized_pair_buy_legs_into_qty_history(store, tmp_path):
    """A closed pair's buy leg (pre-gap date) plus its sell summary in the
    gap window must net to zero qty for that code. positions_daily should
    NOT show the closed-out code on gap dates after the sell."""
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps({
        "months": [{"month": "2026-03",
                    "tw": {"holdings": []},
                    "foreign": {"holdings": []}}],
        "summary": {"all_trades": []},
    }))
    portfolio = json.loads(p.read_text())

    with store.connect_rw() as conn:
        conn.execute(
            "INSERT INTO prices(date, symbol, close, currency, source, ingested_at)"
            " VALUES ('2026-04-22', '6442', 145.0, 'TWD', 'yfinance', '2026-04-22T00:00:00Z')"
        )
        conn.execute(
            "INSERT INTO prices(date, symbol, close, currency, source, ingested_at)"
            " VALUES ('2026-04-23', '6442', 144.0, 'TWD', 'yfinance', '2026-04-23T00:00:00Z')"
        )

    # 1-leg same-month round-trip (6442 pattern from probe).
    fake = _FakeShioajiClient(pairs=[
        {"date": "2026-04-15", "code": "6442", "side": "普買", "qty": 1000,
         "price": 144.0, "cost_twd": 144_000, "ccy": "TWD", "venue": "TW",
         "type": "現股", "pair_id": 200},
        {"date": "2026-04-20", "code": "6442", "side": "普賣", "qty": 1000,
         "price": 145.0, "ccy": "TWD", "venue": "TW",
         "type": "現股", "pair_id": 200, "pnl": 1000.0},
    ])

    trade_overlay.merge(
        store, portfolio, fake, gap_start="2026-04-01", gap_end="2026-04-26"
    )

    with store.connect_ro() as conn:
        rows = list(conn.execute(
            "SELECT date, qty FROM positions_daily WHERE symbol='6442' "
            "ORDER BY date"
        ).fetchall())

    # Buy 2026-04-15, sell 2026-04-20 → qty=0 on 2026-04-22 and 2026-04-23
    # ⇒ no rows written for those dates (qty <= 0 short-circuit).
    assert rows == []


def test_merge_dedups_records_appearing_in_both_pairs_and_session_trades(
    store, tmp_path
):
    """Same trade (date, code, side, qty, price) coming back from multiple
    sources collapses to one entry in the qty_history. Otherwise the qty
    would double-count and the resulting positions_daily mv would be
    inflated."""
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps({
        "months": [{"month": "2026-03",
                    "tw": {"holdings": []},
                    "foreign": {"holdings": []}}],
        "summary": {"all_trades": []},
    }))
    portfolio = json.loads(p.read_text())

    # The `store` fixture already seeds 2330@920 for 2026-04-22 — reuse it.

    duplicate = {
        "date": "2026-04-15", "code": "2330", "side": "普買", "qty": 1000.0,
        "price": 880.0, "ccy": "TWD", "venue": "TW",
    }
    fake = _FakeShioajiClient(
        trades=[duplicate],
        pairs=[
            {**duplicate, "cost_twd": 880_000, "type": "現股", "pair_id": 99},
        ],
    )

    trade_overlay.merge(
        store, portfolio, fake, gap_start="2026-04-01", gap_end="2026-04-26"
    )

    with store.connect_ro() as conn:
        row = conn.execute(
            "SELECT qty FROM positions_daily WHERE symbol='2330' "
            "AND date='2026-04-22'"
        ).fetchone()
    # If dedup failed, qty would be 2000 (1000 from list_trades +
    # 1000 from list_realized_pairs leg). Correct dedup → 1000.
    assert row is not None and row["qty"] == 1000.0


# --- Bug 2 fix (2026-05-01) — overlay net_twd math + persistence -----------


def test_compute_overlay_net_twd_tw_buy_includes_only_broker_fee():
    """TW cash buy: net_twd = -(qty * price * 1.001425). Tax not levied."""
    money = trade_overlay._compute_overlay_net_twd({
        "side": "普買", "qty": 1000, "price": 100.0, "venue": "TW",
    })
    assert money["fee_twd"] == 100_000 * 0.001425
    assert money["tax_twd"] == 0.0
    assert money["gross_twd"] == 100_000
    assert money["net_twd"] == -(100_000 + 100_000 * 0.001425)


def test_compute_overlay_net_twd_tw_sell_subtracts_fee_and_tax():
    """TW sell: net_twd = +(gross - fee - tax) where tax_pct = 0.003 (stocks)."""
    money = trade_overlay._compute_overlay_net_twd({
        "side": "普賣", "qty": 1000, "price": 100.0, "venue": "TW",
    })
    expected_net = 100_000 - 100_000 * 0.001425 - 100_000 * 0.003
    assert money["net_twd"] == expected_net
    assert money["fee_twd"] > 0
    assert money["tax_twd"] > 0


def test_compute_overlay_net_twd_foreign_no_fee():
    """Foreign trades use option (a): no fee, fx-converted."""
    buy = trade_overlay._compute_overlay_net_twd(
        {"side": "買進", "qty": 10, "price": 200.0, "venue": "Foreign"},
        fx_to_twd=31.5,
    )
    sell = trade_overlay._compute_overlay_net_twd(
        {"side": "賣出", "qty": 10, "price": 200.0, "venue": "Foreign"},
        fx_to_twd=31.5,
    )
    assert buy["fee_twd"] == 0.0
    assert buy["net_twd"] == -(10 * 200.0 * 31.5)
    assert sell["net_twd"] == +(10 * 200.0 * 31.5)


def test_merge_persists_overlay_trades_to_table(store, tmp_path):
    """After merge() runs, trades_overlay should hold one row per deduped
    overlay trade, with computed net_twd/fee_twd/tax_twd. This is what the
    derive() cash walk reads to extend running_cash_twd past PDF dates."""
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps({
        "months": [{"month": "2026-03",
                    "tw": {"holdings": []},
                    "foreign": {"holdings": []}}],
        "summary": {"all_trades": []},
    }))
    portfolio = json.loads(p.read_text())

    fake = _FakeShioajiClient(trades=[
        {"date": "2026-04-15", "code": "2330", "side": "普買", "qty": 1000,
         "price": 880.0, "ccy": "TWD", "venue": "TW", "type": "現股"},
        {"date": "2026-04-22", "code": "2330", "side": "普賣", "qty": 1000,
         "price": 920.0, "ccy": "TWD", "venue": "TW", "type": "現股"},
    ])

    trade_overlay.merge(
        store, portfolio, fake, gap_start="2026-04-01", gap_end="2026-04-26",
    )

    with store.connect_ro() as conn:
        rows = conn.execute(
            "SELECT date, code, side, qty, net_twd FROM trades_overlay "
            "ORDER BY date"
        ).fetchall()
    assert len(rows) == 2
    buy, sell = rows
    assert buy["side"] == "普買" and buy["qty"] == 1000
    assert buy["net_twd"] == -(1000 * 880.0 * 1.001425)
    assert sell["side"] == "普賣"
    assert sell["net_twd"] == 1000 * 920.0 - 1000 * 920.0 * 0.001425 - 1000 * 920.0 * 0.003


def test_merge_filters_pre_gap_buy_legs_out_of_trades_overlay(store, tmp_path):
    """list_realized_pairs returns buy legs that may pre-date gap_start
    (decision #1 option C — for audit hook visibility). Those legs are
    already in PDF summary.all_trades, so persisting them to trades_overlay
    would double-count cash impact. _persist_overlay_trades MUST filter
    to [gap_start, gap_end].
    """
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps({
        "months": [{"month": "2026-03",
                    "tw": {"holdings": []},
                    "foreign": {"holdings": []}}],
        "summary": {"all_trades": [
            # PDF already has the March buy.
            {"month": "2026-03", "date": "2026/03/24", "venue": "TW",
             "side": "普買", "code": "00981A", "qty": 1232, "price": 20.31,
             "ccy": "TWD", "net_twd": -25_016},
        ]},
    }))
    portfolio = json.loads(p.read_text())

    # Pair: buy leg dated March (pre-gap), sell dated April (in-gap).
    fake = _FakeShioajiClient(pairs=[
        {"date": "2026-03-24", "code": "00981A", "side": "普買", "qty": 1232,
         "price": 20.31, "ccy": "TWD", "venue": "TW", "type": "現股",
         "pair_id": 7, "cost_twd": 25_016},
        {"date": "2026-04-20", "code": "00981A", "side": "普賣", "qty": 1232,
         "price": 22.0, "ccy": "TWD", "venue": "TW", "type": "現股",
         "pair_id": 7, "pnl": 2_000},
    ])

    trade_overlay.merge(
        store, portfolio, fake,
        gap_start="2026-04-01", gap_end="2026-04-26",
    )

    with store.connect_ro() as conn:
        rows = conn.execute(
            "SELECT date, side FROM trades_overlay ORDER BY date"
        ).fetchall()
    # Only the April sell should be persisted. The March buy is already
    # in PDF and would double-count the cash debit.
    assert len(rows) == 1
    assert rows[0]["date"] == "2026-04-20"
    assert rows[0]["side"] == "普賣"


def test_merge_replaces_overlay_trades_in_window(store, tmp_path):
    """Re-running merge() with a smaller trade list should remove the
    stale rows from trades_overlay (DELETE-then-INSERT contract). Without
    this, a broker correction (trade canceled / qty adjusted) would leave
    a phantom net_twd contributing to the cash walk forever."""
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps({
        "months": [{"month": "2026-03",
                    "tw": {"holdings": []},
                    "foreign": {"holdings": []}}],
        "summary": {"all_trades": []},
    }))
    portfolio = json.loads(p.read_text())

    # First run: two trades.
    fake_run_1 = _FakeShioajiClient(trades=[
        {"date": "2026-04-15", "code": "2330", "side": "普買", "qty": 1000,
         "price": 880.0, "ccy": "TWD", "venue": "TW", "type": "現股"},
        {"date": "2026-04-22", "code": "2330", "side": "普賣", "qty": 1000,
         "price": 920.0, "ccy": "TWD", "venue": "TW", "type": "現股"},
    ])
    trade_overlay.merge(
        store, portfolio, fake_run_1,
        gap_start="2026-04-01", gap_end="2026-04-26",
    )

    # Second run: only one trade (broker corrected — sell never happened).
    fake_run_2 = _FakeShioajiClient(trades=[
        {"date": "2026-04-15", "code": "2330", "side": "普買", "qty": 1000,
         "price": 880.0, "ccy": "TWD", "venue": "TW", "type": "現股"},
    ])
    trade_overlay.merge(
        store, portfolio, fake_run_2,
        gap_start="2026-04-01", gap_end="2026-04-26",
    )

    with store.connect_ro() as conn:
        rows = conn.execute("SELECT side FROM trades_overlay").fetchall()
    assert len(rows) == 1
    assert rows[0]["side"] == "普買"
