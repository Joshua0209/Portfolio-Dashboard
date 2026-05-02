"""Pydantic Settings — env-driven config for the new backend.

Mirrors the env-var contract documented in CLAUDE.md so an operator
moving from legacy → new doesn't have to relearn the surface.

DAILY_DB_PATH is the only one that affects the HTTP layer directly;
the others are read by individual modules (sync, jobs) when needed.
The Settings object is lightweight enough to instantiate per-request
where helpful, but a module-level singleton is fine for read-mostly
config.
"""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    daily_db_path: str = "data/dashboard.db"
    admin_token: str | None = None
    backfill_on_startup: bool = False


def get_settings() -> Settings:
    """Per-call factory — lets tests monkeypatch env vars and observe
    fresh values without process-wide caching."""
    return Settings()
