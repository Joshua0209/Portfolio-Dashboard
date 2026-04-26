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

from app.daily_store import DailyStore
from app import trade_overlay


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
            "INSERT INTO prices(date, symbol, close, currency, source, fetched_at)"
            " VALUES ('2026-04-22', '2330', 920.0, 'TWD', 'twse', '2026-04-22T00:00:00Z')"
        )
        conn.execute(
            "INSERT INTO prices(date, symbol, close, currency, source, fetched_at)"
            " VALUES ('2026-04-23', '2330', 925.0, 'TWD', 'twse', '2026-04-23T00:00:00Z')"
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
    """Doubles for ShioajiClient — returns a canned trade list."""

    def __init__(self, trades, configured=True):
        self._trades = trades
        self.configured = configured
        self.calls: list[tuple[str, str]] = []

    def lazy_login(self):
        return self.configured

    def list_trades(self, start, end):
        self.calls.append((start, end))
        return [t for t in self._trades if start <= t["date"] <= end]


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
    fake = _FakeShioajiClient(trades=[
        {"date": "2026-04-22", "code": "2330", "side": "普買", "qty": 500,
         "price": 920.0, "ccy": "TWD", "venue": "TW"},
    ])
    portfolio = json.loads(Path(portfolio_with_held_position).read_text())

    trade_overlay.merge(
        store, portfolio, fake, gap_start="2026-04-01", gap_end="2026-04-26"
    )

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
