"""Daily-resolution endpoints (Phase 4+).

Phase 4 ships /api/daily/equity. Phase 5 adds /api/daily/positions/<date>.
Phase 8 adds /api/daily/prices/<symbol>. The /api/admin/* endpoints
referenced by phases 10–12 are added by their own commits but mounted on
this same blueprint for namespace cohesion.

The daily store is read-only on the request path. If portfolio_daily has
no rows yet (cold start before Phase 9's background thread completes),
endpoints return an empty envelope with `empty=true` rather than 500.
Phase 9 will replace the empty envelope with a 202 + progress for the
INITIALIZING/FAILED states.
"""
from __future__ import annotations

from flask import Blueprint, current_app, request

from ._helpers import envelope

bp = Blueprint("daily", __name__, url_prefix="/api/daily")


def _store():
    return current_app.extensions["daily_store"]


@bp.get("/equity")
def equity_curve():
    start = request.args.get("start") or None
    end = request.args.get("end") or None
    points = _store().get_equity_curve(start=start, end=end)
    return envelope({
        "points": points,
        "empty": len(points) == 0,
        "start": start,
        "end": end,
    })
