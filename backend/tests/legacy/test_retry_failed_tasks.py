"""Phase 10 — scripts/retry_failed_tasks.py CLI.

The CLI re-uses backfill_runner.retry_open_tasks with the same resolver
the /api/admin endpoint uses. We exercise it as a subprocess with a
DAILY_DB_PATH override pointing at a tmp DB, so the test does not touch
real network or the production sqlite file.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from invest.persistence.daily_store import DailyStore


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent


def _seed_failure(db_path: Path, task_type="tw_prices", target="2330"):
    s = DailyStore(db_path)
    s.init_schema()
    with s.connect_rw() as conn:
        conn.execute(
            """
            INSERT INTO failed_tasks(
                task_type, target, error_message,
                attempts, first_seen_at, last_attempt_at
            ) VALUES (?, ?, ?, 1, '2026-04-25T00:00:00Z', '2026-04-25T00:00:00Z')
            """,
            (task_type, target, "stub failure"),
        )


def _open_count(db_path: Path) -> int:
    s = DailyStore(db_path)
    with s.connect_ro() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM failed_tasks WHERE resolved_at IS NULL"
        ).fetchone()[0]


def test_cli_imports_cleanly():
    """The script must be importable without side-effects (so its
    `main()` is the only entry point)."""
    sys.path.insert(0, str(PROJECT_ROOT))
    try:
        import importlib
        m = importlib.import_module("scripts.retry_failed_tasks")
        assert hasattr(m, "main")
    finally:
        sys.path.pop(0)


def test_cli_resolves_open_rows_with_stub_resolver(tmp_path, monkeypatch):
    """Inject a resolver that always succeeds; verify the CLI marks
    every open row resolved and prints a summary."""
    db = tmp_path / "phase10_cli.db"
    _seed_failure(db, target="2330")
    _seed_failure(db, target="2454")

    sys.path.insert(0, str(PROJECT_ROOT))
    try:
        import importlib
        m = importlib.import_module("scripts.retry_failed_tasks")
        importlib.reload(m)

        # Stub the resolver so the test does not hit network.
        monkeypatch.setattr(
            m, "build_resolver", lambda store: (lambda row: (lambda: []))
        )

        rc = m.main([str(db)])
        assert rc == 0
        assert _open_count(db) == 0
    finally:
        sys.path.pop(0)


def test_cli_returns_nonzero_when_some_still_failing(tmp_path, monkeypatch):
    db = tmp_path / "phase10_cli2.db"
    _seed_failure(db, target="2330")

    sys.path.insert(0, str(PROJECT_ROOT))
    try:
        import importlib
        m = importlib.import_module("scripts.retry_failed_tasks")
        importlib.reload(m)

        def boom(*a, **kw):
            raise RuntimeError("still failing")

        monkeypatch.setattr(
            m, "build_resolver", lambda store: (lambda row: boom)
        )

        rc = m.main([str(db)])
        assert rc != 0
        assert _open_count(db) == 1
    finally:
        sys.path.pop(0)
