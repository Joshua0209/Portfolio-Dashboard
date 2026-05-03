#!/usr/bin/env python3
"""Phase 7 NON-NEGOTIABLE GATE — verify daily-prices data integrity.

Runs four checks against `data/dashboard.db` (populated by
`scripts/backfill_daily.py`):

  (a) per-symbol price gaps — every held symbol has a price row for every
      trading day between its first-trade-date and yesterday
  (b) symbol_market resolution — every held TW symbol has a non-'unknown'
      market verdict (twse-listed or tpex-listed). Foreign symbols are
      exempted: yfinance fetches them with their bare ticker, no router
      decision to make.
  (c) fx_rates gaps — for every currency held, fx_rates covers every
      held-position trading day with no gaps (forward-fill is acceptable
      *during derivation*, but the source rows must be dense)
  (d) most-recent month-end equity reconciliation — the derived
      portfolio_daily row for the latest PDF month-end must be within
      1% of portfolio.json's equity_twd for that month.

(Earlier revisions had a fifth cross-source agreement check that compared
cached TWSE/TPEX prices against a fresh `.TW` yfinance probe. Now that
yfinance is the only backend for TW prices, both sides would always
agree by construction — the check was dropped.)

Exit code 0 on clean run, 1 if any check finds an issue.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "backend" / "src"))

from invest.jobs.backfill_runner import (  # noqa: E402
    iter_foreign_symbols_with_metadata,
    iter_tw_symbols_with_metadata,
    month_end_iso,
)
from invest.persistence.daily_store import (  # noqa: E402
    BACKFILL_FLOOR_DEFAULT,
    DailyStore,
)

log = logging.getLogger(__name__)


# --- Helpers --------------------------------------------------------------


def _trading_days(start: str, end: str) -> list[str]:
    """All weekdays between start..end inclusive (proxy for trading days).

    Real Taiwan/US holidays are not modeled here — the gap check only flags
    weekday rows missing from `prices`. False positives on local holidays
    are accepted (the check is advisory; structural gaps are what matter).
    """
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    out: list[str] = []
    cur = s
    while cur <= e:
        if cur.weekday() < 5:  # Mon=0 .. Fri=4
            out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _yesterday(today: str) -> str:
    return (date.fromisoformat(today) - timedelta(days=1)).isoformat()


# --- Check (a): per-symbol price gaps -------------------------------------


def check_price_gaps(
    store: DailyStore, expected_by_symbol: dict[str, list[str]]
) -> list[dict]:
    """`expected_by_symbol`: {symbol → list of ISO dates that must have prices}.
    Returns a list of {symbol, missing: [...]} dicts for any symbol with gaps.
    """
    issues: list[dict] = []
    with store.connect_ro() as conn:
        for symbol, expected in expected_by_symbol.items():
            present = {
                r[0] for r in conn.execute(
                    "SELECT date FROM prices WHERE symbol = ?", (symbol,)
                ).fetchall()
            }
            missing = sorted(set(expected) - present)
            if missing:
                issues.append({"symbol": symbol, "missing": missing})
    return issues


# --- Check (b): symbol_market coverage ------------------------------------


def check_symbol_market_coverage(
    store: DailyStore, held_symbols: Iterable[str]
) -> list[dict]:
    """Every held TW symbol must have a non-'unknown' market verdict."""
    issues: list[dict] = []
    with store.connect_ro() as conn:
        rows = {
            r["symbol"]: r["market"]
            for r in conn.execute(
                "SELECT symbol, market FROM symbol_market"
            ).fetchall()
        }
    for sym in held_symbols:
        market = rows.get(sym)
        if market is None:
            issues.append({"symbol": sym, "market": None,
                           "reason": "no_symbol_market_row"})
        elif market == "unknown":
            issues.append({"symbol": sym, "market": "unknown",
                           "reason": "yfinance_recognized_neither_TW_nor_TWO_suffix"})
    return issues


# --- Check (c): fx_rates gaps ---------------------------------------------


def check_fx_gaps(
    store: DailyStore, ccy: str, expected_dates: list[str]
) -> list[dict]:
    """`expected_dates`: weekday dates within the held-position window for
    which `fx_rates` rows must exist.

    Phase 14.3b: schema is SQLModel-canonical (``fx_rates`` keyed on
    base/quote/rate); this app always queries with quote='TWD'.
    """
    with store.connect_ro() as conn:
        present = {
            r[0] for r in conn.execute(
                "SELECT date FROM fx_rates WHERE base = ? AND quote = 'TWD'",
                (ccy,),
            ).fetchall()
        }
    missing = sorted(set(expected_dates) - present)
    if missing:
        return [{"ccy": ccy, "missing": missing}]
    return []


# --- Check (d): month-end equity reconciliation --------------------------


def check_month_end_equity(
    store: DailyStore, portfolio: dict, tolerance_pct: float = 1.0
) -> list[dict]:
    """Compare portfolio_daily.equity_twd at each PDF month-end against
    portfolio.json[month].equity_twd.
    """
    issues: list[dict] = []
    months = portfolio.get("months", [])
    if not months:
        return issues

    # Phase 7 hard requirement only covers the most-recent month-end. Loop
    # all months for richer signal but only flag the latest if all-clean.
    latest = months[-1]
    pj_equity = float(latest.get("equity_twd", 0) or 0)
    target_date = month_end_iso(latest["month"])

    with store.connect_ro() as conn:
        # Use the most-recent portfolio_daily row at-or-before the target —
        # if the target itself is a non-trading day, we still want a
        # comparable equity to assert against. Subtract cash_twd so we
        # compare positions-only against the monthly contract (monthly
        # equity_twd is positions-only; daily equity_twd folds in the
        # synthesized broker-cash schedule).
        row = conn.execute(
            "SELECT date, equity_twd, COALESCE(cash_twd, 0) AS cash_twd "
            "FROM portfolio_daily WHERE date <= ? "
            "ORDER BY date DESC LIMIT 1",
            (target_date,),
        ).fetchone()
    if row is None:
        return [{
            "month": latest["month"],
            "reason": "no_portfolio_daily_row",
            "target_date": target_date,
        }]
    derived = float(row["equity_twd"]) - float(row["cash_twd"])
    if pj_equity == 0:
        return issues  # no comparison possible
    diff_pct = abs(derived - pj_equity) / pj_equity * 100
    if diff_pct > tolerance_pct:
        issues.append({
            "month": latest["month"],
            "derived_equity": derived,
            "pdf_equity": pj_equity,
            "diff_pct": diff_pct,
            "compared_date": row["date"],
        })
    return issues


# --- Orchestrator -------------------------------------------------------


def _expected_dates_for_symbol(
    trade_dates: list[str], today: str, floor: str
) -> list[str]:
    """Weekdays between max(first_trade, floor) and yesterday."""
    if not trade_dates:
        return []
    first = max(min(trade_dates), floor)
    last = _yesterday(today)
    if last < first:
        return []
    return _trading_days(first, last)


def run_validation(
    store: DailyStore,
    portfolio_path: Path | str,
    today: str,
    floor: str = BACKFILL_FLOOR_DEFAULT,
) -> int:
    """Run all 4 checks; return 0 on clean, 1 on any issue."""
    portfolio_path = Path(portfolio_path)
    portfolio = json.loads(portfolio_path.read_text(encoding="utf-8"))

    tw = list(iter_tw_symbols_with_metadata(portfolio))
    foreign = list(iter_foreign_symbols_with_metadata(portfolio))

    held_tw: dict[str, list[str]] = {
        e["code"]: _expected_dates_for_symbol(e["trade_dates"], today, floor)
        for e in tw
        if _expected_dates_for_symbol(e["trade_dates"], today, floor)
    }
    held_foreign: dict[str, list[str]] = {
        e["code"]: _expected_dates_for_symbol(e["trade_dates"], today, floor)
        for e in foreign
        if _expected_dates_for_symbol(e["trade_dates"], today, floor)
    }
    expected_by_symbol = {**held_tw, **held_foreign}

    all_issues: dict[str, list] = {}

    a = check_price_gaps(store, expected_by_symbol)
    if a:
        all_issues["price_gaps"] = a

    b = check_symbol_market_coverage(store, held_tw.keys())
    if b:
        all_issues["symbol_market"] = b

    # Check (c): FX gaps for each currency in scope
    foreign_ccys = {e["currency"] for e in foreign}
    foreign_ccys.add("USD")  # always validate USD even with no current positions
    fx_issues: list[dict] = []
    if held_foreign:
        # Build the union of dates we'd need FX for
        needed = sorted({d for ds in held_foreign.values() for d in ds})
        for ccy in foreign_ccys - {"TWD"}:
            fx_issues.extend(check_fx_gaps(store, ccy, needed))
    if fx_issues:
        all_issues["fx_gaps"] = fx_issues

    d = check_month_end_equity(store, portfolio)
    if d:
        all_issues["month_end_equity"] = d

    if all_issues:
        log.error("validate_data found issues: %s", json.dumps(all_issues, indent=2))
        return 1

    log.info("validate_data: OK across %d held TW + %d held foreign symbols",
             len(held_tw), len(held_foreign))
    return 0


# --- CLI ----------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--portfolio", type=Path,
                        default=ROOT / "data" / "portfolio.json")
    parser.add_argument("--db", type=Path,
                        default=ROOT / "data" / "dashboard.db")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if not args.portfolio.exists():
        log.error("portfolio not found at %s", args.portfolio)
        return 1
    if not args.db.exists():
        log.error("dashboard.db not found at %s — run scripts/backfill_daily.py first",
                  args.db)
        return 1

    store = DailyStore(args.db)
    today = date.today().isoformat()
    return run_validation(store, args.portfolio, today=today)


if __name__ == "__main__":
    raise SystemExit(main())
