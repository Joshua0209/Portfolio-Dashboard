#!/usr/bin/env python3
"""Export the FastAPI OpenAPI schema to disk.

Default output path: frontend/openapi.json (Phase 8 picks it up here
for typed TS-client codegen). Override with --out for ad-hoc dumps.

Idempotent — re-running overwrites cleanly. Pretty-printed JSON keeps
PR diffs readable when the schema is committed.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BACKEND_SRC = ROOT / "backend" / "src"
sys.path.insert(0, str(BACKEND_SRC))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out",
        type=Path,
        default=ROOT / "frontend" / "openapi.json",
        help="Output path (default: frontend/openapi.json)",
    )
    args = parser.parse_args()

    from invest.app import create_app
    from invest.http.openapi import write_schema

    written = write_schema(create_app(), args.out)
    print(f"openapi schema written to {written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
