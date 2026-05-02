"""Phase 10 — port of legacy `tests/` directory.

These tests cover the legacy app/* modules that have been ported
verbatim to invest.* namespaces. Imports were rewritten in bulk
during the Phase 10 cutover; the test bodies are untouched.

Adds the repo root (one level up from ``backend/``) to ``sys.path``
so the few tests that exercise CLI scripts (``from scripts import
validate_data`` etc.) keep working — pytest's pythonpath is
``backend/src`` only.
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
