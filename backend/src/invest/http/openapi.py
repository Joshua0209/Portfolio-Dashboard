"""OpenAPI export — Phase 6 Cycle 44.

Pure functions over a FastAPI app. The CLI shim
`scripts/export_openapi.py` is the only caller that decides the output
path; everything else stays I/O-free for testability.

Phase 8 codegens a typed TS client from the JSON file. Pretty-printed
output keeps PR diffs readable when the schema lives in source control.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI


def export_schema(app: FastAPI) -> dict[str, Any]:
    """Return the OpenAPI schema dict for `app`.

    Wraps `app.openapi()` so the exporter has its own seam for future
    transformations (e.g. tag rewrites, response-shape annotation)
    without test-fixture coupling to FastAPI internals.
    """
    return app.openapi()


def write_schema(app: FastAPI, path: Path) -> Path:
    """Write the schema to `path` (creates parent dirs); returns the path."""
    schema = export_schema(app)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(schema, indent=2, sort_keys=True) + "\n")
    return path
