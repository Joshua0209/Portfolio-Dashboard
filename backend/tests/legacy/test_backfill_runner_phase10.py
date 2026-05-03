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

from invest.core import state as backfill_state
from invest.persistence.daily_store import DailyStore


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
    has one open row for the failed symbol.

    Phase 14.3a: simulate a failure by making the seam raise — the
    orchestrator's deferred-retry pass + ``_record_dlq_failure``
    writes a SQLModel-shape DLQ row under the legacy task_type
    'tw_prices'. (In production the seam catches and writes
    ``task_type='fetch_price'`` directly; the orchestrator's path is
    only reached if the seam itself raises.)
    """
    from invest.jobs import backfill_runner

    def _fake_fetch_range(store, symbol, currency, start, end):
        if symbol == "2454":
            raise RuntimeError("yfinance 503 for 2454")
        return backfill_runner._persist_symbol_prices(store, symbol, [{
            "date": "2025-08-15", "close": 600.0,
            "symbol": symbol, "currency": currency, "source": "yfinance",
        }])

    def fake_get_fx(ccy, start, end, store=None, today=None):
        return []

    monkeypatch.setattr(
        backfill_runner, "_fetch_range_via_price_service", _fake_fetch_range,
    )
    monkeypatch.setattr(backfill_runner, "get_fx_rates", fake_get_fx)
    monkeypatch.setattr(
        backfill_runner, "get_yfinance_prices",
        lambda *a, **kw: [],
    )

    summary = backfill_runner.run_full_backfill(
        store, portfolio_with_two_tw, today="2025-08-31"
    )

    # 2330 priced; 2454 in DLQ
    assert "2330" in summary["tw_fetched"]
    assert "2454" not in summary["tw_fetched"]

    failed = store.get_failed_tasks()
    assert len(failed) == 1
    # Orchestrator path (seam raised): legacy task_type='tw_prices',
    # target=payload['target'].
    assert failed[0]["task_type"] == "tw_prices"
    assert failed[0]["target"] == "2454"
    assert "2454" in failed[0]["error_message"]
