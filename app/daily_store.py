"""Daily-resolution SQLite cache.

This is a regenerable denormalized layer on top of `data/portfolio.json`.
The PDF parser remains the source of truth for cost basis, dividends, and
month-end equity; this store holds daily prices, FX rates, and per-day
position state derived from them.

The store is read-only on the request path. Writes flow through
`backfill_runner.py` (Phase 3+) which holds a single writer connection
guarded by `_write_lock`. Reads use per-thread connections in WAL mode so
they never block each other or the writer.

WAL is mandatory because two processes may write concurrently: the Flask
backfill thread and the standalone `scripts/snapshot_daily.py` CLI.

Schema is documented in:
docs/superpowers/specs/2026-04-26-daily-prices-and-today-page-design.md
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

BACKFILL_FLOOR_DEFAULT = "2025-08-01"
SCHEMA_VERSION = "1"
BUSY_TIMEOUT_MS = 5000

EXPECTED_TABLES: frozenset[str] = frozenset(
    {
        "prices",
        "fx_daily",
        "symbol_market",
        "positions_daily",
        "portfolio_daily",
        "failed_tasks",
        "reconcile_events",
        "meta",
    }
)

_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS prices (
    date         TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    close        REAL NOT NULL,
    currency     TEXT NOT NULL,
    source       TEXT NOT NULL,
    fetched_at   TEXT NOT NULL,
    PRIMARY KEY (date, symbol)
);
CREATE INDEX IF NOT EXISTS idx_prices_symbol_date ON prices(symbol, date);

CREATE TABLE IF NOT EXISTS fx_daily (
    date         TEXT NOT NULL,
    ccy          TEXT NOT NULL,
    rate_to_twd  REAL NOT NULL,
    source       TEXT NOT NULL,
    fetched_at   TEXT NOT NULL,
    PRIMARY KEY (date, ccy)
);

CREATE TABLE IF NOT EXISTS symbol_market (
    symbol            TEXT PRIMARY KEY,
    market            TEXT NOT NULL,
    resolved_at       TEXT NOT NULL,
    last_verified_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS positions_daily (
    date         TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    qty          REAL NOT NULL,
    cost_local   REAL NOT NULL,
    mv_local     REAL NOT NULL,
    mv_twd       REAL NOT NULL,
    type         TEXT NOT NULL,
    source       TEXT NOT NULL,
    PRIMARY KEY (date, symbol)
);

CREATE TABLE IF NOT EXISTS portfolio_daily (
    date         TEXT PRIMARY KEY,
    equity_twd   REAL NOT NULL,
    cash_twd     REAL,
    fx_usd_twd   REAL NOT NULL,
    n_positions  INTEGER NOT NULL,
    has_overlay  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS failed_tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_type       TEXT NOT NULL,
    target          TEXT NOT NULL,
    error_message   TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 1,
    first_seen_at   TEXT NOT NULL,
    last_attempt_at TEXT NOT NULL,
    resolved_at     TEXT
);
CREATE INDEX IF NOT EXISTS idx_failed_open
    ON failed_tasks(resolved_at) WHERE resolved_at IS NULL;

CREATE TABLE IF NOT EXISTS reconcile_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pdf_month       TEXT NOT NULL,
    diff_summary    TEXT NOT NULL,
    detected_at     TEXT NOT NULL,
    dismissed_at    TEXT
);

CREATE TABLE IF NOT EXISTS meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);
"""


class DailyStore:
    """Owns the SQLite schema and connection lifecycle.

    Instances are safe to share across Flask request threads. Reads open
    short-lived per-thread connections; writes go through `connect_rw()`
    serialized by `_write_lock`.
    """

    def __init__(self, db_path: Path | str):
        self._path = Path(db_path)
        self._write_lock = threading.RLock()
        self._initialized = False

    # --- connection management -------------------------------------------------

    @property
    def path(self) -> Path:
        return self._path

    def _new_connection(self) -> sqlite3.Connection:
        """Open a new connection with the project's standard PRAGMAs."""
        conn = sqlite3.connect(
            self._path,
            check_same_thread=False,
            timeout=BUSY_TIMEOUT_MS / 1000,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @contextmanager
    def connect_ro(self) -> Iterator[sqlite3.Connection]:
        """Context-managed read-only connection. Always closes on exit."""
        conn = self._new_connection()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def connect_rw(self) -> Iterator[sqlite3.Connection]:
        """Serialized writer connection — single writer at a time."""
        with self._write_lock:
            conn = self._new_connection()
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    # --- schema bootstrap ------------------------------------------------------

    def init_schema(self) -> None:
        """Idempotent: create the parent dir, set WAL+busy_timeout, run DDL,
        seed required `meta` rows. Safe to call multiple times."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._write_lock:
            conn = sqlite3.connect(self._path, timeout=BUSY_TIMEOUT_MS / 1000)
            try:
                # WAL must be enabled before significant writes on a fresh
                # DB, otherwise the journal_mode setting may not stick.
                mode = conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
                if mode.lower() != "wal":  # pragma: no cover — platform-specific
                    raise RuntimeError(f"Failed to enable WAL mode (got {mode!r})")
                conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
                conn.executescript(_SCHEMA_DDL)
                conn.execute(
                    "INSERT OR IGNORE INTO meta(key, value) VALUES (?, ?)",
                    ("backfill_floor", BACKFILL_FLOOR_DEFAULT),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO meta(key, value) VALUES (?, ?)",
                    ("schema_version", SCHEMA_VERSION),
                )
                conn.commit()
            finally:
                conn.close()
        self._initialized = True

    # --- meta -----------------------------------------------------------------

    def get_meta(self, key: str) -> str | None:
        with self.connect_ro() as conn:
            row = conn.execute(
                "SELECT value FROM meta WHERE key = ?", (key,)
            ).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self.connect_rw() as conn:
            conn.execute(
                """
                INSERT INTO meta(key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    # --- read helpers (Phase 1: stubs returning empty results) ----------------

    def get_equity_curve(
        self, start: str | None = None, end: str | None = None
    ) -> list[dict[str, Any]]:
        sql = "SELECT date, equity_twd, fx_usd_twd, n_positions, has_overlay FROM portfolio_daily"
        params: list[Any] = []
        clauses = []
        if start:
            clauses.append("date >= ?")
            params.append(start)
        if end:
            clauses.append("date <= ?")
            params.append(end)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY date ASC"
        with self.connect_ro() as conn:
            return [dict(r) for r in conn.execute(sql, params).fetchall()]

    def get_ticker_history(
        self, symbol: str, start: str | None = None, end: str | None = None
    ) -> list[dict[str, Any]]:
        sql = "SELECT date, close, currency, source FROM prices WHERE symbol = ?"
        params: list[Any] = [symbol]
        if start:
            sql += " AND date >= ?"
            params.append(start)
        if end:
            sql += " AND date <= ?"
            params.append(end)
        sql += " ORDER BY date ASC"
        with self.connect_ro() as conn:
            return [dict(r) for r in conn.execute(sql, params).fetchall()]

    def get_today_snapshot(self) -> dict[str, Any] | None:
        with self.connect_ro() as conn:
            row = conn.execute(
                "SELECT date, equity_twd, fx_usd_twd, n_positions, has_overlay "
                "FROM portfolio_daily ORDER BY date DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None

    def get_failed_tasks(self) -> list[dict[str, Any]]:
        with self.connect_ro() as conn:
            return [
                dict(r)
                for r in conn.execute(
                    "SELECT id, task_type, target, error_message, attempts, "
                    "first_seen_at, last_attempt_at "
                    "FROM failed_tasks WHERE resolved_at IS NULL "
                    "ORDER BY last_attempt_at DESC"
                ).fetchall()
            ]
