"""Phase 14.4 — scripts/retry_failed_tasks.py CLI.

The CLI routes through ``invest.jobs.retry_failed.run`` (SQLModel-
backed). We exercise it as an in-process call with a tmp DB so the
test does not touch real network or the production sqlite file.

Pre-14.4 the script called ``backfill_runner.retry_open_tasks``
against the legacy ``failed_tasks`` schema (target/error_message/
first_seen_at). Those tests were retired with that function.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.persistence.models.failed_task import FailedTask
from invest.persistence.repositories.failed_task_repo import FailedTaskRepo


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent


def _bootstrap(db_path: Path) -> tuple:
    """Create the SQLModel schema in `db_path` and return (engine, Session)."""
    engine = create_engine(f"sqlite:///{db_path}")
    SQLModel.metadata.create_all(engine)
    return engine


def _seed_failure(db_path: Path, task_type: str = "tw_prices",
                  target: str = "2330") -> FailedTask:
    engine = _bootstrap(db_path)
    with Session(engine) as s:
        return FailedTaskRepo(s).insert(
            FailedTask(
                task_type=task_type,
                payload={"target": target},
                error="stub failure",
            )
        )


def _open_count(db_path: Path) -> int:
    engine = create_engine(f"sqlite:///{db_path}")
    with Session(engine) as s:
        return FailedTaskRepo(s).count_open()


@pytest.fixture
def cli_module():
    """Reload the CLI fresh — the script is import-side-effect-free
    but pre-loading via prior tests can leave a stale ``main`` ref."""
    sys.path.insert(0, str(PROJECT_ROOT))
    try:
        m = importlib.import_module("scripts.retry_failed_tasks")
        importlib.reload(m)
        yield m
    finally:
        sys.path.pop(0)


def test_cli_imports_cleanly(cli_module):
    """Importable without side-effects (so its `main()` is the only
    entry point)."""
    assert hasattr(cli_module, "main")
    assert hasattr(cli_module, "build_resolver")


def test_cli_resolves_open_rows_with_stub_resolver(
    tmp_path, monkeypatch, cli_module,
):
    """Inject a resolver that always succeeds; verify the CLI marks
    every open row resolved."""
    db = tmp_path / "phase14_4_cli.db"
    _seed_failure(db, target="2330")
    _seed_failure(db, target="2454")

    monkeypatch.setattr(
        cli_module,
        "build_resolver",
        lambda store: (lambda task: (lambda: None)),
    )

    rc = cli_module.main([str(db)])
    assert rc == 0
    assert _open_count(db) == 0


def test_cli_returns_nonzero_when_some_still_failing(
    tmp_path, monkeypatch, cli_module,
):
    """A resolver that raises bumps `attempts` and the CLI exits 1."""
    db = tmp_path / "phase14_4_cli2.db"
    _seed_failure(db, target="2330")

    def boom_resolver(store):
        def _build(task):
            def _do() -> None:
                raise RuntimeError("still failing")
            return _do
        return _build

    monkeypatch.setattr(cli_module, "build_resolver", boom_resolver)

    rc = cli_module.main([str(db)])
    assert rc != 0
    assert _open_count(db) == 1
