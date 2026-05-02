#!/usr/bin/env python3
"""Manually reconcile PDF trades vs Shioaji-overlay trades for one month.

Usage:
    python scripts/reconcile.py --month 2026-03

Exit codes:
    0   clean diff (no event written) OR overlay unconfigured (skipped)
    1   diff detected (one reconcile_events row inserted)
    2   bad arguments

Per spec §12 this is the *only* path that can trigger reconciliation
besides the /api/admin/reconcile endpoint. The backfill runner and
snapshot_daily CLI are forbidden from invoking it.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Mirror app/__init__.py:_load_env so the reconcile CLI sees Shioaji
# creds without the operator having to source .env first. override=False
# keeps real shell env wins.
try:
    from dotenv import load_dotenv  # noqa: E402
    _ENV_PATH = ROOT / ".env"
    if _ENV_PATH.exists():
        load_dotenv(_ENV_PATH, override=False)
except ImportError:
    pass

from app.daily_store import DailyStore  # noqa: E402
from app.shioaji_client import ShioajiClient  # noqa: E402
from app import reconcile  # noqa: E402

_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--month", required=True, help="PDF month (YYYY-MM)")
    p.add_argument("--portfolio", default=str(ROOT / "data" / "portfolio.json"))
    p.add_argument("--db", default=str(ROOT / "data" / "dashboard.db"))
    args = p.parse_args(argv)

    if not _MONTH_RE.match(args.month):
        print("error: --month must be YYYY-MM", file=sys.stderr)
        return 2

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    portfolio_path = Path(args.portfolio)
    if not portfolio_path.exists():
        print(f"error: portfolio.json not found at {portfolio_path}", file=sys.stderr)
        return 2

    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))

    store = DailyStore(Path(args.db))
    store.init_schema()

    client = ShioajiClient()
    overlay_fn = client.list_trades if client.configured else None

    summary = reconcile.run_for_month(store, portfolio, args.month, overlay_client=overlay_fn)

    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))

    if summary.get("event_id") is not None:
        # Diff detected — non-zero exit so cron / scripted callers can react.
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
