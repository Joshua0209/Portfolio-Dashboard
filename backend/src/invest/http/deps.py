"""FastAPI dependency-injection seams.

Four deps live here for Phase 6.5 router wiring:

  get_session         Per-request SQLModel session (Phase 6 baseline).
  get_portfolio_store Singleton PortfolioStore reading data/portfolio.json
                      via mtime-driven reload. Singleton so the mtime
                      cache is shared across requests.
  get_daily_store     Singleton DailyStore wrapping the SQLite cache.
                      Module-level to share connection management.
  require_admin       ADMIN_TOKEN gate.

Tests override any of these via app.dependency_overrides[<dep>] = <fake>.
"""
from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Iterator

from fastapi import Header, HTTPException, status
from sqlalchemy.engine import Engine
from sqlmodel import Session, create_engine

from invest.core.config import get_settings
from invest.persistence.daily_store import DailyStore
from invest.persistence.portfolio_store import PortfolioStore


# Default JSON path. Mirrors the legacy app/__init__.py:115 fallback —
# `data/portfolio.json` relative to the repo root. Override in tests
# via app.dependency_overrides[get_portfolio_store].
_REPO_ROOT = Path(__file__).resolve().parents[4]
_DEFAULT_PORTFOLIO_PATH = _REPO_ROOT / "data" / "portfolio.json"


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


_portfolio_store: PortfolioStore | None = None
_portfolio_lock = threading.Lock()


def get_portfolio_store() -> PortfolioStore:
    """Singleton PortfolioStore. mtime-driven reload means one instance
    safely serves the whole process — every property access checks the
    file's mtime and re-reads if it changed."""
    global _portfolio_store
    if _portfolio_store is None:
        with _portfolio_lock:
            if _portfolio_store is None:
                _portfolio_store = PortfolioStore(_DEFAULT_PORTFOLIO_PATH)
    return _portfolio_store


_daily_store: DailyStore | None = None
_daily_lock = threading.Lock()


def get_daily_store() -> DailyStore:
    """Singleton DailyStore — shares SQLite connection management
    across requests. Tests override the dep entirely (in-memory DB)
    rather than try to monkeypatch the singleton."""
    global _daily_store
    if _daily_store is None:
        with _daily_lock:
            if _daily_store is None:
                s = get_settings()
                _daily_store = DailyStore(Path(s.daily_db_path))
    return _daily_store


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
