"""Phase 10 — admin endpoints in app/api/today.py.

  GET  /api/admin/failed-tasks  → { tasks: [{id, task_type, target,
                                            error_message, attempts,
                                            first_seen_at, last_attempt_at}], count }
  POST /api/admin/retry-failed  → { resolved: N, still_failing: M }

The retry path delegates to backfill_runner.retry_open_tasks with a
resolver that maps task_type → live fetch_fn. Per spec §10, the
resolver lives inside the request handler so each task_type stays
self-describing.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app import backfill_state, create_app
from app.daily_store import DailyStore


@pytest.fixture(autouse=True)
def _reset_state():
    backfill_state.get().reset()
    yield
    backfill_state.get().reset()


@pytest.fixture()
def app(tmp_path: Path, monkeypatch, empty_portfolio_json: Path):
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "phase10_admin.db"))
    monkeypatch.delenv("BACKFILL_ON_STARTUP", raising=False)
    return create_app(empty_portfolio_json)


@pytest.fixture()
def client(app):
    return app.test_client()


def _seed_failure(app, task_type="tw_prices", target="2330", error="boom"):
    ds: DailyStore = app.extensions["daily_store"]
    with ds.connect_rw() as conn:
        conn.execute(
            """
            INSERT INTO failed_tasks(
                task_type, target, error_message,
                attempts, first_seen_at, last_attempt_at
            ) VALUES (?, ?, ?, 1, '2026-04-25T00:00:00Z', '2026-04-25T00:00:00Z')
            """,
            (task_type, target, error),
        )


def test_failed_tasks_returns_empty_list_on_clean_db(client):
    r = client.get("/api/admin/failed-tasks")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["data"]["tasks"] == []
    assert body["data"]["count"] == 0


def test_failed_tasks_returns_open_rows(client, app):
    _seed_failure(app, target="2330", error="twse 503")
    _seed_failure(app, target="2454", error="twse timeout")

    r = client.get("/api/admin/failed-tasks")
    assert r.status_code == 200
    tasks = r.get_json()["data"]["tasks"]
    assert len(tasks) == 2
    targets = {t["target"] for t in tasks}
    assert targets == {"2330", "2454"}
    assert all("error_message" in t for t in tasks)


def test_failed_tasks_excludes_resolved_rows(client, app):
    _seed_failure(app, target="2330")
    ds: DailyStore = app.extensions["daily_store"]
    with ds.connect_rw() as conn:
        conn.execute(
            "UPDATE failed_tasks SET resolved_at = '2026-04-26T00:00:00Z'"
        )

    r = client.get("/api/admin/failed-tasks")
    assert r.get_json()["data"]["count"] == 0


def test_retry_failed_marks_rows_resolved(client, app, monkeypatch):
    """POST /api/admin/retry-failed dispatches each open row through
    the live fetch path. Successful retries get resolved_at."""
    _seed_failure(app, task_type="tw_prices", target="2330")

    from app import price_sources

    def fake_get_prices(symbol, currency, start, end, store=None):
        return [{
            "date": "2026-04-25", "close": 600.0,
            "symbol": symbol, "currency": currency, "source": "twse",
        }]

    monkeypatch.setattr(price_sources, "get_prices", fake_get_prices)

    r = client.post("/api/admin/retry-failed")
    assert r.status_code == 200
    body = r.get_json()
    assert body["data"]["resolved"] == 1
    assert body["data"]["still_failing"] == 0


def test_retry_failed_increments_attempts_when_still_failing(
    client, app, monkeypatch
):
    _seed_failure(app, task_type="tw_prices", target="2330")

    from app import price_sources

    def boom(*a, **kw):
        raise RuntimeError("twse still 503")

    monkeypatch.setattr(price_sources, "get_prices", boom)

    r = client.post("/api/admin/retry-failed")
    body = r.get_json()
    assert body["data"]["resolved"] == 0
    assert body["data"]["still_failing"] == 1

    ds: DailyStore = app.extensions["daily_store"]
    with ds.connect_ro() as conn:
        row = conn.execute(
            "SELECT attempts, resolved_at FROM failed_tasks"
        ).fetchone()
    assert row[0] == 2
    assert row[1] is None
