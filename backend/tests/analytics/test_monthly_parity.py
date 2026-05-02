"""Golden-vector parity tests: legacy app.analytics ≡ invest.analytics.monthly.

PLAN §3 explicitly mandates this approach:
  > Tests: golden-vector tests against the *current implementation's
  > outputs* for the real portfolio.json. Lock these BEFORE rewriting
  > any math — they catch silent drift between old and new.

This test file is transitional. It lives only until Phase 9 deletes
app/. At that point the test goes too — the router tests (which
exercise these functions through real HTTP endpoints) provide the
ongoing coverage.

Why parity over hand-derived unit tests:
  The legacy is the production-correct source of truth. Hand-deriving
  expected values for 22 functions is error-prone and produces a
  test suite that codifies what the test author thought the math
  should be — not what it actually is. Parity tests lock the actual
  observed numerical output, byte-for-byte where possible and float-
  close where floating-point is unavoidable.

Why it's ok to import app.analytics from backend tests:
  The legacy app/ package sits at the repo root. We append the repo
  root to sys.path here so the import resolves. When Phase 9 deletes
  app/, this whole file gets deleted — no lingering coupling.

Why we skip when data/portfolio.json is missing:
  CI clones don't have the real portfolio (gitignored). The test
  needs real data because tiny synthetic fixtures don't exercise
  enough of the code paths (e.g., FIFO with 200 trades, drawdown
  episodes across multi-year curves). The skip lets pytest -q stay
  green on a fresh clone; the test author runs it locally where
  data/portfolio.json exists.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[3]
_PORTFOLIO_JSON = _REPO_ROOT / "data" / "portfolio.json"


# Make `app.analytics` importable from backend's pytest run.
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


pytestmark = pytest.mark.skipif(
    not _PORTFOLIO_JSON.exists(),
    reason="data/portfolio.json missing — parity tests need real data",
)


@pytest.fixture(scope="module")
def real_data() -> dict:
    return json.loads(_PORTFOLIO_JSON.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def months(real_data: dict) -> list[dict]:
    return real_data.get("months", [])


@pytest.fixture(scope="module")
def by_ticker(real_data: dict) -> dict:
    return real_data.get("summary", {}).get("by_ticker", {})


@pytest.fixture(scope="module")
def venue_flows(real_data: dict) -> list[dict]:
    return real_data.get("summary", {}).get("venue_flows_twd", [])


@pytest.fixture(scope="module")
def all_trades(real_data: dict) -> list[dict]:
    return real_data.get("summary", {}).get("all_trades", [])


# Imports that fail until invest.analytics.monthly exists — defines RED.
@pytest.fixture(scope="module")
def legacy():
    from app import analytics as legacy_analytics
    return legacy_analytics


@pytest.fixture(scope="module")
def new():
    from invest.analytics import monthly as new_monthly
    return new_monthly


# Helpers -----------------------------------------------------------


def _close_lists(a, b, *, rel_tol: float = 1e-9, abs_tol: float = 1e-9) -> bool:
    """Compare two lists element-by-element with float tolerance."""
    if len(a) != len(b):
        return False
    for x, y in zip(a, b):
        if isinstance(x, dict) and isinstance(y, dict):
            if not _close_dicts(x, y, rel_tol=rel_tol, abs_tol=abs_tol):
                return False
        elif isinstance(x, float) or isinstance(y, float):
            if x is None and y is None:
                continue
            if x is None or y is None:
                return False
            if not math.isclose(x, y, rel_tol=rel_tol, abs_tol=abs_tol):
                return False
        elif x != y:
            return False
    return True


def _close_dicts(a, b, *, rel_tol: float = 1e-9, abs_tol: float = 1e-9) -> bool:
    if set(a.keys()) != set(b.keys()):
        return False
    for k in a:
        x, y = a[k], b[k]
        if isinstance(x, list) and isinstance(y, list):
            if not _close_lists(x, y, rel_tol=rel_tol, abs_tol=abs_tol):
                return False
        elif isinstance(x, dict) and isinstance(y, dict):
            if not _close_dicts(x, y, rel_tol=rel_tol, abs_tol=abs_tol):
                return False
        elif isinstance(x, float) or isinstance(y, float):
            if x is None and y is None:
                continue
            if x is None or y is None:
                return False
            if not math.isclose(x, y, rel_tol=rel_tol, abs_tol=abs_tol):
                return False
        elif x != y:
            return False
    return True


# Period returns / cumulative curve / drawdown -----------------------


class TestPeriodReturns:
    @pytest.mark.parametrize("method", ["day_weighted", "mid_month", "eom"])
    def test_three_methods_match(self, legacy, new, months, method) -> None:
        out_l = legacy.period_returns(months, method=method)
        out_n = new.period_returns(months, method=method)
        assert _close_lists(out_l, out_n)


class TestCumulativeCurve:
    def test_matches(self, legacy, new, months) -> None:
        prs = [r["period_return"] for r in legacy.period_returns(months)]
        assert _close_lists(legacy.cumulative_curve(prs), new.cumulative_curve(prs))


class TestDrawdownCurve:
    def test_matches(self, legacy, new, months) -> None:
        prs = [r["period_return"] for r in legacy.period_returns(months)]
        cum = legacy.cumulative_curve(prs)
        assert _close_lists(legacy.drawdown_curve(cum), new.drawdown_curve(cum))


class TestMaxDrawdown:
    def test_matches(self, legacy, new, months) -> None:
        prs = [r["period_return"] for r in legacy.period_returns(months)]
        cum = legacy.cumulative_curve(prs)
        assert math.isclose(legacy.max_drawdown(cum), new.max_drawdown(cum))


class TestDrawdownEpisodes:
    def test_matches(self, legacy, new, months) -> None:
        prs = [r["period_return"] for r in legacy.period_returns(months)]
        cum = legacy.cumulative_curve(prs)
        labels = [m["month"] for m in months]
        assert _close_lists(
            legacy.drawdown_episodes(cum, labels),
            new.drawdown_episodes(cum, labels),
        )


# Stats --------------------------------------------------------------


class TestStdev:
    def test_matches(self, legacy, new, months) -> None:
        prs = [m.get("period_return", 0) or 0 for m in months]
        assert math.isclose(legacy.stdev(prs), new.stdev(prs))

    def test_short_input_zero(self, legacy, new) -> None:
        assert legacy.stdev([1.0]) == new.stdev([1.0]) == 0.0


class TestDownsideStdev:
    def test_matches(self, legacy, new, months) -> None:
        prs = [m.get("period_return", 0) or 0 for m in months]
        assert math.isclose(legacy.downside_stdev(prs), new.downside_stdev(prs))


class TestAnnualizeReturn:
    @pytest.mark.parametrize("r,p", [(0.005, 12), (0.01, 4), (-0.02, 12)])
    def test_matches(self, legacy, new, r, p) -> None:
        assert math.isclose(legacy.annualize_return(r, p), new.annualize_return(r, p))


class TestCagrFromCum:
    @pytest.mark.parametrize("cum,n", [(0.5, 36), (0.0, 12), (-0.3, 24)])
    def test_matches(self, legacy, new, cum, n) -> None:
        assert math.isclose(legacy.cagr_from_cum(cum, n), new.cagr_from_cum(cum, n))


class TestSharpe:
    def test_matches(self, legacy, new, months) -> None:
        prs = [m.get("period_return", 0) or 0 for m in months]
        assert math.isclose(legacy.sharpe(prs), new.sharpe(prs))


class TestSortino:
    def test_matches(self, legacy, new, months) -> None:
        prs = [m.get("period_return", 0) or 0 for m in months]
        assert math.isclose(legacy.sortino(prs), new.sortino(prs))


class TestCalmar:
    def test_matches(self, legacy, new, months) -> None:
        prs = [m.get("period_return", 0) or 0 for m in months]
        assert math.isclose(legacy.calmar(prs), new.calmar(prs))


class TestRollingReturns:
    @pytest.mark.parametrize("window", [3, 6, 12])
    def test_matches(self, legacy, new, months, window) -> None:
        prs = [m.get("period_return", 0) or 0 for m in months]
        out_l = legacy.rolling_returns(prs, window)
        out_n = new.rolling_returns(prs, window)
        assert len(out_l) == len(out_n)
        for x, y in zip(out_l, out_n):
            if x is None and y is None:
                continue
            assert math.isclose(x, y)


class TestRollingSharpe:
    @pytest.mark.parametrize("window", [3, 12])
    def test_matches(self, legacy, new, months, window) -> None:
        prs = [m.get("period_return", 0) or 0 for m in months]
        out_l = legacy.rolling_sharpe(prs, window)
        out_n = new.rolling_sharpe(prs, window)
        assert len(out_l) == len(out_n)
        for x, y in zip(out_l, out_n):
            if x is None and y is None:
                continue
            assert math.isclose(x, y, abs_tol=1e-12)


# Concentration ------------------------------------------------------


class TestHhi:
    def test_matches(self, legacy, new) -> None:
        weights = [0.4, 0.3, 0.2, 0.1]
        assert math.isclose(legacy.hhi(weights), new.hhi(weights))


class TestTopNShare:
    def test_matches(self, legacy, new) -> None:
        weights = [0.1, 0.4, 0.05, 0.3, 0.15]
        assert math.isclose(legacy.top_n_share(weights, 3), new.top_n_share(weights, 3))


class TestEffectiveN:
    def test_matches(self, legacy, new) -> None:
        weights = [0.4, 0.3, 0.2, 0.1]
        assert math.isclose(legacy.effective_n(weights), new.effective_n(weights))


# P&L ---------------------------------------------------------------


class TestRealizedPnlByTicker:
    def test_matches(self, legacy, new, by_ticker) -> None:
        assert _close_lists(
            legacy.realized_pnl_by_ticker(by_ticker),
            new.realized_pnl_by_ticker(by_ticker),
        )


class TestRealizedPnlByTickerFifo:
    def test_matches(self, legacy, new, by_ticker) -> None:
        assert _close_lists(
            legacy.realized_pnl_by_ticker_fifo(by_ticker),
            new.realized_pnl_by_ticker_fifo(by_ticker),
        )


# Cashflow ----------------------------------------------------------


class TestMonthlyFlows:
    def test_matches(self, legacy, new, months, venue_flows) -> None:
        assert _close_lists(
            legacy.monthly_flows(months, venue_flows),
            new.monthly_flows(months, venue_flows),
        )


class TestDailyInvestmentFlows:
    def test_matches(self, legacy, new, months) -> None:
        assert _close_lists(
            legacy.daily_investment_flows(months),
            new.daily_investment_flows(months),
        )


class TestDailyExternalFlows:
    def test_matches(self, legacy, new, months) -> None:
        assert _close_lists(
            legacy.daily_external_flows(months),
            new.daily_external_flows(months),
        )


# FX -----------------------------------------------------------------


class TestFxPnl:
    def test_matches(self, legacy, new, months) -> None:
        out_l = legacy.fx_pnl(months)
        out_n = new.fx_pnl(months)
        assert math.isclose(out_l["contribution_twd"], out_n["contribution_twd"])
        assert _close_lists(out_l["monthly"], out_n["monthly"])


class TestDailyFxPnl:
    def test_matches(self, legacy, new) -> None:
        # Synthetic input — daily_fx_pnl needs (usd_exposure, fx_series)
        # which are daily-store-shaped and not present in portfolio.json.
        usd = [
            {"date": "2024-01-02", "usd_mv_twd": 100_000.0},
            {"date": "2024-01-03", "usd_mv_twd": 102_000.0},
            {"date": "2024-01-04", "usd_mv_twd": 101_000.0},
        ]
        fx = [
            {"date": "2024-01-02", "rate_to_twd": 31.0},
            {"date": "2024-01-03", "rate_to_twd": 31.2},
            {"date": "2024-01-04", "rate_to_twd": 31.1},
        ]
        out_l = legacy.daily_fx_pnl(usd, fx)
        out_n = new.daily_fx_pnl(usd, fx)
        assert math.isclose(out_l["contribution_twd"], out_n["contribution_twd"])
        assert _close_lists(out_l["daily"], out_n["daily"])


# Activity ----------------------------------------------------------


class TestTopMovers:
    def test_matches(self, legacy, new, by_ticker, months) -> None:
        # latest_holdings comes from the last month's tw + foreign holdings.
        last = months[-1] if months else {}
        latest_holdings = (
            (last.get("tw") or {}).get("holdings", []) or []
        ) + (
            (last.get("foreign") or {}).get("holdings", []) or []
        )
        out_l = legacy.top_movers(by_ticker, latest_holdings, top_n=5)
        out_n = new.top_movers(by_ticker, latest_holdings, top_n=5)
        assert _close_dicts(out_l, out_n)


class TestRecentActivity:
    def test_matches(self, legacy, new, all_trades) -> None:
        assert _close_lists(
            legacy.recent_activity(all_trades, limit=25),
            new.recent_activity(all_trades, limit=25),
        )


# Sectors ----------------------------------------------------------


class TestSectorOf:
    @pytest.mark.parametrize(
        "code,venue",
        [
            ("2330", "TW"),
            ("0050", "TW"),
            ("00878", "TW"),
            ("9999", "TW"),  # unknown
            ("AAPL", "FOREIGN"),
            ("NVDA", "FOREIGN"),
            ("ZZZZ", "FOREIGN"),  # unknown
        ],
    )
    def test_matches(self, legacy, new, code, venue) -> None:
        assert legacy.sector_of(code, venue) == new.sector_of(code, venue)


class TestSectorBreakdown:
    def test_matches(self, legacy, new) -> None:
        holdings = [
            {"code": "2330", "venue": "TW", "mkt_value_twd": 1_000_000.0},
            {"code": "0050", "venue": "TW", "mkt_value_twd": 500_000.0},
            {"code": "AAPL", "venue": "FOREIGN", "mkt_value_twd": 800_000.0},
            {"code": "9999", "venue": "TW", "mkt_value_twd": 100_000.0},
        ]
        assert _close_lists(
            legacy.sector_breakdown(holdings),
            new.sector_breakdown(holdings),
        )


# Reprice ----------------------------------------------------------


class TestRepriceHoldings:
    def test_matches(self, legacy, new) -> None:
        holdings = [
            {"code": "2330", "venue": "TW", "qty": 1000,
             "cost_local": 600_000.0, "cost_twd": 600_000.0,
             "ref_price": 600.0, "mkt_value_twd": 600_000.0},
            {"code": "AAPL", "venue": "FOREIGN", "qty": 100,
             "cost_local": 15_000.0, "cost_twd": 472_500.0,
             "ref_price": 150.0, "ccy": "USD",
             "mkt_value_local": 15_000.0, "mkt_value_twd": 472_500.0},
        ]
        closes = {
            "2330": {"date": "2026-05-02", "close": 650.0, "currency": "TWD"},
            "AAPL": {"date": "2026-05-02", "close": 180.0, "currency": "USD"},
        }
        get = lambda code: closes.get(code)
        out_l = legacy.reprice_holdings_with_daily(holdings, get, current_fx_usd_twd=31.5)
        out_n = new.reprice_holdings_with_daily(holdings, get, current_fx_usd_twd=31.5)
        assert _close_lists(out_l, out_n)


# Daily TWR --------------------------------------------------------


class TestDailyTwr:
    def test_matches(self, legacy, new) -> None:
        equity = [
            {"date": "2024-01-02", "equity_twd": 1_000_000.0},
            {"date": "2024-01-03", "equity_twd": 1_010_000.0},
            {"date": "2024-01-04", "equity_twd": 1_005_000.0},
            {"date": "2024-01-05", "equity_twd": 1_020_000.0},
        ]
        flows = [
            {"date": "2024-01-04", "flow_twd": 100_000.0},
        ]
        out_l = legacy.daily_twr(equity, flows, weight=0.5, anchor_cum_return=0.05)
        out_n = new.daily_twr(equity, flows, weight=0.5, anchor_cum_return=0.05)
        assert _close_lists(out_l, out_n)
