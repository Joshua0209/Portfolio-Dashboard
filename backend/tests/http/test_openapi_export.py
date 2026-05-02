"""Reproducer for Phase 6 Cycle 44 — OpenAPI export.

RED: invest.http.openapi.export_schema does not exist.

Per PLAN section 6: "Generate OpenAPI; export schema to frontend/
openapi.json for client codegen." Phase 8 picks the file up from a
known path; tests pin the function contract so the script + Phase 8
codegen never disagree on shape.

The exporter is a pure function over a FastAPI app — no I/O coupling.
The CLI shim (scripts/export_openapi.py) is a thin adapter that picks
the path and calls write_schema(). Keeping the function pure lets
tests run without filesystem mocking.
"""
from __future__ import annotations

import json
from pathlib import Path

from invest.app import create_app


def test_export_schema_returns_dict() -> None:
    """export_schema(app) returns the FastAPI-generated OpenAPI dict."""
    from invest.http.openapi import export_schema

    schema = export_schema(create_app())
    assert isinstance(schema, dict)
    assert schema["openapi"].startswith("3.")
    assert "paths" in schema
    # Spot-check one canonical path so we know the schema actually came
    # from the real app, not a stub.
    assert "/api/health" in schema["paths"]


def test_write_schema_writes_pretty_json(tmp_path: Path) -> None:
    """write_schema(app, path) writes prettified JSON to disk."""
    from invest.http.openapi import write_schema

    out_path = tmp_path / "openapi.json"
    written = write_schema(create_app(), out_path)
    assert written == out_path
    assert out_path.exists()

    parsed = json.loads(out_path.read_text())
    assert parsed["openapi"].startswith("3.")
    assert "/api/summary" in parsed["paths"]

    # Pretty-printed: contains newlines + indentation. Phase 8 codegen
    # tools handle minified fine, but pretty diffs read better in PRs
    # if the schema is committed.
    raw = out_path.read_text()
    assert "\n" in raw
    assert "  " in raw  # indent=2


def test_write_schema_creates_parent_dir(tmp_path: Path) -> None:
    """write_schema creates missing parent directories.

    Phase 8 hasn't built frontend/ yet — when Cycle 44's CLI shim
    runs from a fresh checkout, it must create frontend/ on demand
    rather than crashing with FileNotFoundError.
    """
    from invest.http.openapi import write_schema

    out_path = tmp_path / "frontend" / "deeply" / "nested" / "openapi.json"
    write_schema(create_app(), out_path)
    assert out_path.exists()


def test_write_schema_overwrites_existing(tmp_path: Path) -> None:
    """Idempotent: a second run overwrites the first cleanly."""
    from invest.http.openapi import write_schema

    out_path = tmp_path / "openapi.json"
    out_path.write_text("garbage")  # poison the file
    write_schema(create_app(), out_path)
    parsed = json.loads(out_path.read_text())
    assert parsed["openapi"].startswith("3.")
