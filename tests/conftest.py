"""Shared pytest fixtures for the investment dashboard test suite.

The daily layer is a regenerable SQLite cache, so every test gets a fresh
on-disk DB inside `tmp_path`. We deliberately avoid `:memory:` because some
phases (Phase 9's background thread, Phase 15's snapshot script) exercise
the WAL multi-process path, which `:memory:` does not model faithfully.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


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
