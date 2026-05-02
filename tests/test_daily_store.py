"""Phase 1 acceptance tests for app/daily_store.py.

These pin down the schema, PRAGMAs, and seeded meta rows that every later
phase depends on. The store is read-only on the request path; writes flow
through backfill_runner (Phase 3+), so this test file only exercises
init_schema() + read-side helpers returning empty results on empty DB.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.daily_store import (
    BACKFILL_FLOOR_DEFAULT,
    EXPECTED_TABLES,
    DailyStore,
)


@pytest.fixture()
def store(tmp_path: Path) -> DailyStore:
    """A fresh DailyStore initialized at a tmp path."""
    db_path = tmp_path / "nested" / "dashboard.db"
    s = DailyStore(db_path)
    s.init_schema()
    return s


def test_init_schema_creates_all_eight_tables(store: DailyStore) -> None:
    with sqlite3.connect(store.path) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
    actual = {r[0] for r in rows}
    assert EXPECTED_TABLES.issubset(actual), f"missing tables: {EXPECTED_TABLES - actual}"


def test_init_schema_sets_wal_journal_mode(store: DailyStore) -> None:
    with sqlite3.connect(store.path) as conn:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode.lower() == "wal", f"expected WAL, got {mode!r}"


def test_init_schema_sets_busy_timeout(store: DailyStore) -> None:
    """busy_timeout is per-connection, not persisted in the file. Verify the
    store hands out connections with the right timeout."""
    with store.connect_ro() as conn:
        timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]
    assert timeout == 5000


def test_init_schema_seeds_backfill_floor(store: DailyStore) -> None:
    with sqlite3.connect(store.path) as conn:
        row = conn.execute(
            "SELECT value FROM meta WHERE key='backfill_floor'"
        ).fetchone()
    assert row is not None, "meta.backfill_floor row missing"
    assert row[0] == BACKFILL_FLOOR_DEFAULT == "2025-08-01"


def test_init_schema_seeds_schema_version(store: DailyStore) -> None:
    with sqlite3.connect(store.path) as conn:
        row = conn.execute(
            "SELECT value FROM meta WHERE key='schema_version'"
        ).fetchone()
    assert row is not None
    assert row[0] == "1"


def test_init_schema_creates_parent_dir_if_missing(tmp_path: Path) -> None:
    nested = tmp_path / "does" / "not" / "exist" / "dashboard.db"
    assert not nested.parent.exists()
    DailyStore(nested).init_schema()
    assert nested.exists()


def test_init_schema_is_idempotent(tmp_path: Path) -> None:
    """Re-running init_schema must not fail or duplicate seed rows."""
    p = tmp_path / "dashboard.db"
    s = DailyStore(p)
    s.init_schema()
    s.init_schema()
    s.init_schema()
    with sqlite3.connect(p) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM meta WHERE key='backfill_floor'"
        ).fetchone()[0]
    assert n == 1, "backfill_floor row should exist exactly once"


def test_prices_table_has_expected_columns(store: DailyStore) -> None:
    with sqlite3.connect(store.path) as conn:
        cols = {r[1]: r[2] for r in conn.execute("PRAGMA table_info(prices)")}
    assert set(cols) == {"date", "symbol", "close", "currency", "source", "fetched_at"}
    assert cols["close"] == "REAL"


def test_failed_tasks_has_partial_index_on_open_rows(store: DailyStore) -> None:
    """Phase 10 relies on idx_failed_open being a partial index over open rows."""
    with sqlite3.connect(store.path) as conn:
        idx = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_failed_open'"
        ).fetchone()
    assert idx is not None
    assert "WHERE resolved_at IS NULL" in idx[0]


def test_read_helpers_return_empty_on_fresh_db(store: DailyStore) -> None:
    assert store.get_equity_curve() == []
    assert store.get_ticker_history("2330") == []
    assert store.get_failed_tasks() == []
    assert store.get_today_snapshot() is None


def test_get_meta_round_trips(store: DailyStore) -> None:
    assert store.get_meta("backfill_floor") == BACKFILL_FLOOR_DEFAULT
    store.set_meta("last_known_date", "2026-04-25")
    assert store.get_meta("last_known_date") == "2026-04-25"
    store.set_meta("last_known_date", "2026-04-26")
    assert store.get_meta("last_known_date") == "2026-04-26"


def test_create_app_initializes_daily_store(tmp_path: Path, monkeypatch) -> None:
    """create_app() should construct a DailyStore so subsequent phases
    can do `app.extensions['daily_store']` without a None check."""
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "dashboard.db"))
    from app import create_app

    app = create_app(data_path=tmp_path / "portfolio.json")
    assert "daily_store" in app.extensions
    ds = app.extensions["daily_store"]
    assert isinstance(ds, DailyStore)
    # Schema initialized on construction
    with sqlite3.connect(ds.path) as conn:
        n = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='prices'"
        ).fetchone()[0]
    assert n == 1
