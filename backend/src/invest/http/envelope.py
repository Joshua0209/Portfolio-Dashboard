"""Response envelope — uniform {ok, data} shape for every HTTP route.

Same convention as the legacy Flask blueprints (see CLAUDE.md
'API surface'): success bodies are always {"ok": true, "data": ...}.
Errors are non-200 HTTP statuses with FastAPI's default error body
(handled by exception handlers in core/errors.py once they land).

Why a tiny helper instead of a Pydantic response_model:
  Each router returns heterogeneously-shaped data dicts. Forcing them
  through a typed envelope model would require Generic[T] gymnastics
  for trivial benefit. The helper is one line and OpenAPI still
  documents the response via the route's return annotation.
"""
from __future__ import annotations

from typing import Any


def success(data: Any) -> dict[str, Any]:
    """Wrap a payload in the canonical success envelope."""
    return {"ok": True, "data": data}


def error(message: str) -> dict[str, Any]:
    """Wrap an error message in the canonical failure envelope.

    Diverges from the legacy Flask convention (which returned
    ok=True even on 404 with meta.error). Phase 8 regenerates the
    frontend client from OpenAPI so this is a safe, cleaner break.
    """
    return {"ok": False, "error": message}
