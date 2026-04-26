"""Benchmark price fetching + strategy definitions.

Fetches monthly closing prices via yfinance, caches them in
``data/benchmarks.json`` keyed by (ticker, month). The dashboard then
chains monthly returns the same way it computes the portfolio's TWR.

Two markets:
  * TW market — comparator portfolios priced in TWD
  * US market — comparator portfolios priced in USD

Strategies are weighted blends. Each tier reflects a different
"analyst archetype" — broad-index passive vs concentrated mega-cap
picking vs balanced 60/40 — so the user can A/B their performance
against the strategy that most resembles their style.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

CACHE_PATH = Path(__file__).resolve().parent.parent / "data" / "benchmarks.json"
CACHE_TTL_DAYS = 7


@dataclass(frozen=True)
class Strategy:
    key: str
    name: str
    market: str  # "TW" or "US"
    weights: dict[str, float]  # ticker → weight (sum to 1)
    description: str


# ---------------------------------------------------------------------------
# Strategy catalogue
# ---------------------------------------------------------------------------
# Tickers use Yahoo Finance symbology:
#   *.TW = TWSE primary listing (price in TWD)
#   *.TWO = OTC listing
#   plain symbol = US listing (price in USD)

STRATEGIES: list[Strategy] = [
    # ── TW market ────────────────────────────────────────────────────────
    Strategy(
        key="tw_passive",
        name="TW Passive (0050)",
        market="TW",
        weights={"0050.TW": 1.0},
        description="100% Yuanta Taiwan 50 ETF. The 'do nothing' baseline.",
    ),
    Strategy(
        key="tw_dividend",
        name="TW Dividend (0056)",
        market="TW",
        weights={"0056.TW": 1.0},
        description="100% Yuanta High-Dividend ETF. Income tilt.",
    ),
    Strategy(
        key="tw_megacap",
        name="TW Mega-cap (TSMC heavy)",
        market="TW",
        weights={"2330.TW": 0.7, "0050.TW": 0.3},
        description="Concentrated bet on TSMC with index ballast — naive picker.",
    ),
    Strategy(
        key="tw_balanced",
        name="TW Balanced 60/40",
        market="TW",
        weights={"0050.TW": 0.6, "00679B.TW": 0.4},
        description="60% Taiwan 50 + 40% Yuanta US Treasury 20Y bond ETF.",
    ),
    # ── US market ────────────────────────────────────────────────────────
    Strategy(
        key="us_passive",
        name="US Passive (S&P 500)",
        market="US",
        weights={"SPY": 1.0},
        description="100% S&P 500. The reference benchmark for active US managers.",
    ),
    Strategy(
        key="us_growth",
        name="US Growth (QQQ)",
        market="US",
        weights={"QQQ": 1.0},
        description="100% Nasdaq 100. Tech-heavy growth tilt.",
    ),
    Strategy(
        key="us_megacap",
        name="US Mega-cap (NVDA + GOOGL)",
        market="US",
        weights={"NVDA": 0.5, "GOOGL": 0.5},
        description="Equal-weight in two AI/cloud bellwethers — naive picker.",
    ),
    Strategy(
        key="us_balanced",
        name="US Balanced 60/40",
        market="US",
        weights={"SPY": 0.6, "TLT": 0.4},
        description="60% S&P 500 + 40% 20Y Treasury — the textbook balanced portfolio.",
    ),
]


def get_strategy(key: str) -> Strategy | None:
    return next((s for s in STRATEGIES if s.key == key), None)


# ---------------------------------------------------------------------------
# Cache I/O
# ---------------------------------------------------------------------------


def _load_cache() -> dict:
    if not CACHE_PATH.exists():
        return {"prices": {}, "updated_at": None}
    try:
        return json.loads(CACHE_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return {"prices": {}, "updated_at": None}


def _save_cache(data: dict) -> None:
    CACHE_PATH.parent.mkdir(exist_ok=True)
    CACHE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def _cache_fresh(updated_at: str | None) -> bool:
    if not updated_at:
        return False
    try:
        ts = datetime.fromisoformat(updated_at)
    except ValueError:
        return False
    return datetime.utcnow() - ts < timedelta(days=CACHE_TTL_DAYS)


# ---------------------------------------------------------------------------
# Price fetching
# ---------------------------------------------------------------------------


def fetch_monthly_prices(
    tickers: Iterable[str],
    start_month: str,
    end_month: str,
    *,
    force: bool = False,
) -> dict[str, dict[str, float]]:
    """Return ``{ticker: {YYYY-MM: close}}`` for the given window.

    Uses cache if fresh; otherwise fetches via yfinance and updates cache.
    Months map to the *first trading day of that month* in yfinance.
    """
    cache = _load_cache()
    prices = cache.get("prices") or {}

    needed = [t for t in tickers if force or t not in prices or not _cache_fresh(cache.get("updated_at"))]
    if needed:
        try:
            import yfinance as yf
        except ImportError as e:
            raise RuntimeError(
                "yfinance not installed. Run: pip install yfinance"
            ) from e

        # Pull a generous window so we always have prior-month-end as the
        # starting basis, even for the first month in scope.
        start_dt = datetime.strptime(start_month, "%Y-%m") - timedelta(days=45)
        end_dt = datetime.strptime(end_month, "%Y-%m") + timedelta(days=35)

        for ticker in needed:
            df = yf.download(
                ticker,
                start=start_dt.strftime("%Y-%m-%d"),
                end=end_dt.strftime("%Y-%m-%d"),
                interval="1mo",
                progress=False,
                auto_adjust=True,
            )
            if df.empty:
                prices[ticker] = {}
                continue
            close = df["Close"]
            # When yfinance returns a DataFrame, the Close column may itself
            # be a one-column DataFrame (with the ticker as column name).
            # Squeeze to a Series for uniform iteration.
            if hasattr(close, "columns"):
                close = close.iloc[:, 0]
            month_prices: dict[str, float] = {}
            for ts, val in close.items():
                if val is None or (val != val):  # NaN check
                    continue
                month_prices[ts.strftime("%Y-%m")] = float(val)
            prices[ticker] = month_prices

        cache["prices"] = prices
        cache["updated_at"] = datetime.utcnow().isoformat(timespec="seconds")
        _save_cache(cache)

    return {t: prices.get(t, {}) for t in tickers}


# ---------------------------------------------------------------------------
# Strategy evaluation
# ---------------------------------------------------------------------------


def strategy_monthly_returns(
    strategy: Strategy, months: list[str]
) -> list[dict]:
    """Compute weighted monthly TWR for a strategy across the given months.

    ``months`` is the list of "YYYY-MM" strings to evaluate. We need
    prices for ``months[0]-1`` (prior month-end) up through ``months[-1]``.

    Returns a list of dicts with ``month``, ``period_return``, ``cum_return``.
    Months with missing prices are skipped (period_return = None).
    """
    if not months:
        return []
    tickers = list(strategy.weights.keys())
    prices = fetch_monthly_prices(tickers, months[0], months[-1])

    out = []
    cum = 1.0
    for i, ym in enumerate(months):
        if i == 0:
            # Match portfolio convention: first month has no prior basis,
            # so return is forced to 0.
            out.append({"month": ym, "period_return": 0.0, "cum_return": 0.0})
            continue

        prev_ym = months[i - 1]
        weighted_r = 0.0
        any_priced = False
        for t, w in strategy.weights.items():
            p_now = prices.get(t, {}).get(ym)
            p_prev = prices.get(t, {}).get(prev_ym)
            if p_now is None or p_prev is None or p_prev == 0:
                continue
            r = (p_now - p_prev) / p_prev
            weighted_r += w * r
            any_priced = True

        if any_priced:
            cum = cum * (1.0 + weighted_r)
            out.append({
                "month": ym,
                "period_return": weighted_r,
                "cum_return": cum - 1.0,
            })
        else:
            out.append({"month": ym, "period_return": None, "cum_return": cum - 1.0})

    return out
