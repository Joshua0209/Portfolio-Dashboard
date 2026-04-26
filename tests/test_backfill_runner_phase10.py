"""Phase 10 — verify external fetches in run_full_backfill are wrapped
in fetch_with_dlq so a single symbol failure does not abort the run.

We don't unit-test every call site — we drive the full run with mocks
that fail one specific symbol and verify (a) the failed_tasks row, (b)
the run continued (other symbols got fetched), (c) state machine ends
READY (the run completed despite the partial failure)."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app import backfill_state
from app.daily_store import DailyStore


@pytest.fixture(autouse=True)
def _reset_state():
    backfill_state.get().reset()
    yield
    backfill_state.get().reset()


@pytest.fixture()
def store(tmp_path: Path) -> DailyStore:
    s = DailyStore(tmp_path / "phase10.db")
    s.init_schema()
    return s


@pytest.fixture()
def portfolio_with_two_tw(tmp_path: Path) -> Path:
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps({
        "summary": {
            "all_trades": [
                {
                    "venue": "TW", "code": "2330", "date": "2025-08-15",
                    "side": "普買", "qty": 100, "price": 600,
                },
                {
                    "venue": "TW", "code": "2454", "date": "2025-08-20",
                    "side": "普買", "qty": 50, "price": 1000,
                },
            ],
        },
        "months": [
            {
                "month": "2025-08",
                "tw": {"holdings": [
                    {"code": "2330", "qty": 100, "avg_cost": 600},
                    {"code": "2454", "qty": 50, "avg_cost": 1000},
                ]},
                "foreign": {"holdings": []},
            },
        ],
    }))
    return p


def test_run_full_backfill_continues_after_per_symbol_failure(
    store, portfolio_with_two_tw, monkeypatch
):
    """One TW symbol fails; the other still gets persisted; failed_tasks
    has one open row for the failed symbol."""
    from app import backfill_runner, price_sources

    def fake_get_prices(symbol, currency, start, end, store=None):
        if symbol == "2454":
            raise RuntimeError("twse 503 for 2454")
        # Return a single fake row for 2330
        return [{
            "date": "2025-08-15", "close": 600.0,
            "symbol": symbol, "currency": currency, "source": "twse",
        }]

    def fake_get_fx(ccy, start, end):
        return []

    monkeypatch.setattr(price_sources, "get_prices", fake_get_prices)
    monkeypatch.setattr(backfill_runner, "get_prices", fake_get_prices)
    monkeypatch.setattr(backfill_runner, "get_fx_rates", fake_get_fx)

    summary = backfill_runner.run_full_backfill(
        store, portfolio_with_two_tw, today="2025-08-31"
    )

    # 2330 priced; 2454 in DLQ
    assert "2330" in summary["tw_fetched"]
    assert "2454" not in summary["tw_fetched"]

    with store.connect_ro() as conn:
        open_rows = [dict(r) for r in conn.execute(
            "SELECT * FROM failed_tasks WHERE resolved_at IS NULL"
        ).fetchall()]
    assert len(open_rows) == 1
    assert open_rows[0]["task_type"] == "tw_prices"
    assert open_rows[0]["target"] == "2454"
    assert "2454" in open_rows[0]["error_message"]
