"""Phase 11 - backfill the trades SQLModel table from data/portfolio.json.
Populates `trades` (source='pdf') from summary.all_trades. Idempotent:
re-running clears prior source='pdf' rows and reinserts. source='overlay'
rows are never touched.
This is scaffolding for the Trade-table aggregator (PLAN section 4).
Analytics still read PortfolioStore today; Phase 11.2 ports analytics
one metric at a time with byte-equality verification before flipping
the read path to the trades table.
Usage:
    python scripts/backfill_trades.py [--portfolio PATH] [--db PATH]
"""
from __future__ import annotations
import argparse
import json
import logging
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "backend" / "src"))
try:
    from dotenv import load_dotenv
    _ENV_PATH = ROOT / ".env"
    if _ENV_PATH.exists():
        load_dotenv(_ENV_PATH, override=False)
except ImportError:
    pass
from sqlmodel import SQLModel, Session, create_engine  # noqa: E402
from invest.jobs import trade_backfill  # noqa: E402
from invest.persistence.models import (  # noqa: F401  - register tables
    failed_task,
    fx_rate,
    portfolio_daily,
    position_daily,
    price,
    reconcile_event,
    symbol_market,
    trade,
)
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
        print(f"error: portfolio.json not found at {portfolio_path}", file=sys.stderr)
        return 2
    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))
    engine = create_engine(f"sqlite:///{args.db}", connect_args={"timeout": 5})
    with engine.begin() as conn:
        conn.exec_driver_sql("PRAGMA journal_mode=WAL")
        conn.exec_driver_sql("PRAGMA busy_timeout=5000")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        summary = trade_backfill.run(session, portfolio)
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0
if __name__ == "__main__":
    sys.exit(main())
