"""Shared pytest fixtures for the investment dashboard test suite.

The daily layer is a regenerable SQLite cache, so every test gets a fresh
on-disk DB inside `tmp_path`. We deliberately avoid `:memory:` because some
phases (Phase 9's background thread, Phase 15's snapshot script) exercise
the WAL multi-process path, which `:memory:` does not model faithfully.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _scrub_shioaji_creds_for_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tests must not hit the real broker. Without this, an operator's
    `.env` (with SINOPAC_API_KEY set) silently turns every
    `run_full_backfill()` call into a live broker connection that pulls
    real trades into the test DB and fails downstream assertions on
    expected equity_twd values.

    Tests that need Shioaji behavior must set creds explicitly via
    monkeypatch.setenv(...) inside the test, which overrides this scrub.
    """
    for key in ("SINOPAC_API_KEY", "SINOPAC_SECRET_KEY",
                "SINOPAC_CA_CERT_PATH", "SINOPAC_CA_PASSWORD"):
        monkeypatch.delenv(key, raising=False)


@pytest.fixture()
def project_root() -> Path:
    """Repo root, resolved from this file."""
    return Path(__file__).resolve().parent.parent


@pytest.fixture()
def empty_portfolio_json(tmp_path: Path) -> Path:
    """A valid-but-empty portfolio.json — exercises empty-data paths."""
    p = tmp_path / "portfolio.json"
    p.write_text(json.dumps({"months": [], "summary": {}}))
    return p


@pytest.fixture()
def fake_data_dir(tmp_path: Path, empty_portfolio_json: Path) -> Path:
    """A temp data/ that mirrors the real layout but is throwaway."""
    return tmp_path
