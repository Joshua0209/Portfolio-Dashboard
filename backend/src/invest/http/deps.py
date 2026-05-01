"""FastAPI dependency-injection seams.

Two deps live here because every router in Phase 6 will use them:

  get_session    Per-request SQLModel session. Tests override via
                 app.dependency_overrides[get_session] to swap an
                 in-memory engine.

  require_admin  ADMIN_TOKEN gate. ADMIN_TOKEN unset → no gate (legacy
                 default — localhost-friendly). Set → endpoint must
                 receive matching X-Admin-Token header or 401.

The default get_session opens an engine against settings.daily_db_path
with WAL + busy_timeout=5000 to mirror the legacy DailyStore. In tests
the override side-steps this entirely.
"""
from __future__ import annotations

import os
from typing import Iterator

from fastapi import Header, HTTPException, status
from sqlalchemy.engine import Engine
from sqlmodel import Session, create_engine

from invest.core.config import get_settings


_engine: Engine | None = None


def _get_engine() -> Engine:
    """Lazy module-level engine — first call configures WAL on the
    file-backed DB; subsequent calls reuse it. SQLite engines are
    thread-safe under SQLModel's default settings."""
    global _engine
    if _engine is None:
        s = get_settings()
        _engine = create_engine(
            f"sqlite:///{s.daily_db_path}",
            connect_args={"timeout": 5},
        )
        with _engine.begin() as conn:
            conn.exec_driver_sql("PRAGMA journal_mode=WAL")
            conn.exec_driver_sql("PRAGMA busy_timeout=5000")
    return _engine


def get_session() -> Iterator[Session]:
    """Yield one Session per request. Closes on exit."""
    with Session(_get_engine()) as s:
        yield s


def require_admin(x_admin_token: str | None = Header(default=None)) -> None:
    """Header-gated admin auth. ADMIN_TOKEN unset → allow (no gate).

    Read fresh from os.environ each call so tests using monkeypatch
    see the current value. Process-wide env caching would prevent
    this and force a setUp/tearDown dance.
    """
    expected = os.environ.get("ADMIN_TOKEN")
    if not expected:
        return
    if x_admin_token != expected:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
