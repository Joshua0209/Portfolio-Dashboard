#!/usr/bin/env python3
"""Phase 10 — incremental daily refresh (thin shim).

Fills the gap between ``meta.last_known_date`` and "today" without doing a
full cold-start backfill. Two callsites:
  - CLI:        ``python scripts/snapshot_daily.py``
  - Endpoint:   ``POST /api/admin/refresh``  (calls
                ``invest.jobs.snapshot_workflow.run`` directly)

Idempotent: re-running back-to-back is a no-op (``already_current``).
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "backend" / "src"))

# Load .env so CLI usage sees Shioaji creds without manual sourcing.
try:
    from dotenv import load_dotenv  # noqa: E402

    _ENV_PATH = ROOT / ".env"
    if _ENV_PATH.exists():
        load_dotenv(_ENV_PATH, override=False)
except ImportError:
    pass

from invest.jobs import snapshot_workflow  # noqa: E402
from invest.persistence.daily_store import DailyStore  # noqa: E402


# Re-exports for backwards compatibility with any external callers that
# import these names directly from the script.
compute_increment_window = snapshot_workflow.compute_increment_window
run = snapshot_workflow.run


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--portfolio", default=str(ROOT / "data" / "portfolio.json"))
    p.add_argument("--db", default=str(ROOT / "data" / "dashboard.db"))
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    portfolio_path = Path(args.portfolio)
    if not portfolio_path.exists():
        print(
            f"error: portfolio.json not found at {portfolio_path}", file=sys.stderr
        )
        return 2

    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))
    store = DailyStore(Path(args.db))
    store.init_schema()

    summary = snapshot_workflow.run(store, portfolio)
    print(json.dumps(summary, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
