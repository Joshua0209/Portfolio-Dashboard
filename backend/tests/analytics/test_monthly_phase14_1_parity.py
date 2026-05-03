"""Phase 14.1 characterization tests — pin the current legacy output of
monthly.py for the eight functions the plan wires through analytics
primitives (ratios, drawdown, concentration, attribution, sectors,
tax_pnl).

These goldens were captured BEFORE wiring against the unmodified
monthly.py at refactor/modularization HEAD. Once Phase 14.1 ships,
every assertion here must still hold within `_FLOAT_TOL` (1e-9 per
plan §4 — float→Decimal→float roundtrip noise). Any failure is a real
regression and blocks the phase.

The list-of-dicts goldens for fx_pnl / realized_pnl_by_ticker_fifo are
stored as data so individual fields can be diffed precisely; counts
and string fields must match exactly.
"""
from __future__ import annotations

import math

import pytest

from invest.analytics import monthly as m


_FLOAT_TOL = 1e-9


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------

# 24 months of synthetic but realistic returns: mix of positive and
# negative, two visible drawdowns, no all-zero stretches.
_RETURNS = [
    0.052, -0.031, 0.024, -0.018, 0.041, 0.013, -0.022, 0.007, 0.038,
    -0.045, 0.029, 0.011, -0.063, 0.054, 0.022, -0.014, 0.031, 0.018,
    -0.027, 0.046, 0.009, -0.012, 0.025, 0.033,
]

# Concentration-weight inputs: raw weights and the same weights normalized
# to sum=1 (matches what risk.py:69 passes to analytics.hhi/top_n_share).
_WEIGHTS_RAW = [500000.0, 320000.0, 180000.0, 95000.0, 60000.0, 40000.0]
_WEIGHTS_NORM = [w / sum(_WEIGHTS_RAW) for w in _WEIGHTS_RAW]

# 3-month sequence with USD bank + foreign equity exposure for fx_pnl.
_FX_MONTHS = [
    {"month": "2024-01", "fx_usd_twd": 30.5, "bank_usd_in_twd": 100000, "foreign_market_value_twd": 500000},
    {"month": "2024-02", "fx_usd_twd": 31.2, "bank_usd_in_twd": 110000, "foreign_market_value_twd": 520000},
    {"month": "2024-03", "fx_usd_twd": 30.8, "bank_usd_in_twd": 105000, "foreign_market_value_twd": 480000},
]

# One ticker with a partial-close and a profitable sell to exercise
# wins/losses, holding period, and avg_open_cost.
_BY_TICKER = {
    "2330": {
        "name": "TSMC", "venue": "TW", "dividends_twd": 5000.0,
        "trades": [
            {"date": "2024-01-15", "side": "現買", "qty": 100, "gross_twd": 60000.0, "fee_twd": 100.0, "tax_twd": 0.0},
            {"date": "2024-03-20", "side": "現買", "qty": 50,  "gross_twd": 35000.0, "fee_twd": 50.0,  "tax_twd": 0.0},
            {"date": "2024-06-10", "side": "現賣", "qty": 80,  "gross_twd": 56000.0, "fee_twd": 90.0,  "tax_twd": 168.0},
        ],
    },
}


# ---------------------------------------------------------------------------
# Ratios — to be wired through analytics.ratios
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_sharpe_legacy_pin() -> None:
    assert math.isclose(m.sharpe(_RETURNS), 1.0092060705602466, abs_tol=_FLOAT_TOL)


@pytest.mark.unit
def test_sortino_principled_pin() -> None:
    """Sample-stdev (n − 1, full series) convention — Sortino & Price 1994.

    Phase 14.1 deliberately switched from the legacy population-stdev
    convention. Old golden was 0.9600351147211151. The dashboard's
    sortino_annualized number on /risk and /benchmarks is now ~70%
    larger; this is the modern convention and matches what the
    principled `analytics.ratios.sortino` has always produced.
    """
    assert math.isclose(m.sortino(_RETURNS), 1.6278187358565313, abs_tol=_FLOAT_TOL)


@pytest.mark.unit
def test_calmar_legacy_pin() -> None:
    assert math.isclose(m.calmar(_RETURNS), 1.5909578951321215, abs_tol=_FLOAT_TOL)


@pytest.mark.unit
def test_stdev_legacy_pin() -> None:
    """Sample stdev (Bessel's correction). analytics.ratios._sample_stdev
    is the same formula but private — wiring requires exposing it."""
    assert math.isclose(m.stdev(_RETURNS), 0.03160762038918252, abs_tol=_FLOAT_TOL)


@pytest.mark.unit
def test_downside_stdev_principled_pin() -> None:
    """Sample stdev (divide by n − 1, FULL series count) of below-target
    observations. Phase 14.1 switch from legacy population convention.

    Old golden was 0.0332264954516723 (population, divide by count of
    negatives). The dashboard's downside_volatility on /risk is now
    smaller; risk.py:111 still surfaces it directly.
    """
    assert math.isclose(m.downside_stdev(_RETURNS), 0.019595917942265426, abs_tol=_FLOAT_TOL)


# ---------------------------------------------------------------------------
# Drawdown — to be wired through analytics.drawdown
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_max_drawdown_legacy_pin() -> None:
    cum = m.cumulative_curve(_RETURNS)
    assert math.isclose(m.max_drawdown(cum), -0.0690861476350001, abs_tol=_FLOAT_TOL)


@pytest.mark.unit
def test_drawdown_curve_shape_pin() -> None:
    """Legacy returns rich list[{wealth, drawdown}]; analytics.drawdown
    .underwater_curve returns list[Decimal]. Wiring needs an adapter
    that re-attaches the wealth values."""
    cum = m.cumulative_curve(_RETURNS)
    curve = m.drawdown_curve(cum)
    assert len(curve) == len(_RETURNS)
    assert set(curve[0].keys()) == {"wealth", "drawdown"}
    # Spot-check the first and last points for value drift.
    assert math.isclose(curve[0]["wealth"], 1.052, abs_tol=_FLOAT_TOL)
    assert math.isclose(curve[0]["drawdown"], 0.0, abs_tol=_FLOAT_TOL)
    last = curve[-1]
    assert math.isclose(last["wealth"], 1.2319072050362212, abs_tol=_FLOAT_TOL)
    assert math.isclose(last["drawdown"], 0.0, abs_tol=_FLOAT_TOL)


# ---------------------------------------------------------------------------
# Concentration — to be wired through analytics.concentration
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_hhi_normalized_pin() -> None:
    """risk.py:88 passes pre-normalized weights, so legacy and principled
    agree here. Verify the invariant explicitly so any future caller
    that forgets to normalize trips this test."""
    assert math.isclose(m.hhi(_WEIGHTS_NORM), 0.2794243798252831, abs_tol=_FLOAT_TOL)


@pytest.mark.unit
def test_top_n_share_normalized_pin() -> None:
    assert math.isclose(m.top_n_share(_WEIGHTS_NORM, 3), 0.8368200836820083, abs_tol=_FLOAT_TOL)


@pytest.mark.unit
def test_effective_n_normalized_pin() -> None:
    assert math.isclose(m.effective_n(_WEIGHTS_NORM), 3.578785790364013, abs_tol=_FLOAT_TOL)


# ---------------------------------------------------------------------------
# Sectors — to be wired through analytics.sectors
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_sector_of_pin() -> None:
    assert m.sector_of("2330", "TW") == "Semiconductors"
    assert m.sector_of("NVDA", "US") == "Semiconductors"
    assert m.sector_of("0050", "TW") == "ETF (TW broad)"
    assert m.sector_of("UNKNOWN", "TW") == "TW Equity (other)"
    assert m.sector_of("", "TW") == "Unknown"


# ---------------------------------------------------------------------------
# Realized P&L — to be wired through analytics.tax_pnl (NOTE: shape gap)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_realized_pnl_fifo_legacy_pin() -> None:
    """Legacy returns 16-key dict per ticker (win_rate, profit_factor,
    avg_holding_days, …). analytics.tax_pnl.realized_pnl_per_position
    returns only {code: Money} — it computes the realized number, not
    the rich shape. Wiring requires either extending tax_pnl or
    keeping the rich-shape post-processing inline in monthly.py."""
    out = m.realized_pnl_by_ticker_fifo(_BY_TICKER)
    assert len(out) == 1
    row = out[0]
    assert row["code"] == "2330"
    assert row["name"] == "TSMC"
    assert row["venue"] == "TW"
    assert math.isclose(row["realized_pnl_twd"], 7662.0, abs_tol=_FLOAT_TOL)
    assert math.isclose(row["sell_proceeds_twd"], 55742.0, abs_tol=_FLOAT_TOL)
    assert math.isclose(row["cost_of_sold_twd"], 48080.0, abs_tol=_FLOAT_TOL)
    assert math.isclose(row["sell_qty"], 80.0, abs_tol=_FLOAT_TOL)
    assert row["open_qty"] == 70
    assert math.isclose(row["open_cost_twd"], 47070.0, abs_tol=_FLOAT_TOL)
    assert math.isclose(row["avg_open_cost_twd"], 672.4285714285714, abs_tol=_FLOAT_TOL)
    assert row["wins"] == 1
    assert row["losses"] == 0
    assert row["win_rate"] == 1.0
    assert row["profit_factor"] is None
    assert math.isclose(row["avg_holding_days"], 147.0, abs_tol=_FLOAT_TOL)
    assert math.isclose(row["dividends_twd"], 5000.0, abs_tol=_FLOAT_TOL)
    assert row["fully_closed"] is False


# ---------------------------------------------------------------------------
# FX P&L — plan says wire through analytics.attribution (NOTE: concept gap)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_fx_pnl_legacy_pin() -> None:
    """Legacy: full USD exposure delta over a month sequence, returning
    {contribution_twd, monthly: [...]}. Principled
    analytics.attribution.fx_attribution is per-position three-way
    decomposition (price, fx, cross, total) — different concept.
    Cannot be drop-in wired; either keep the legacy month-walk inline
    or design a new attribution facade."""
    out = m.fx_pnl(_FX_MONTHS)
    assert math.isclose(out["contribution_twd"], 5693.568726355626, abs_tol=_FLOAT_TOL)
    assert len(out["monthly"]) == 2

    feb = out["monthly"][0]
    assert feb["month"] == "2024-02"
    assert math.isclose(feb["fx_usd_twd"], 31.2, abs_tol=_FLOAT_TOL)
    assert math.isclose(feb["usd_amount"], 19672.131147540982, abs_tol=_FLOAT_TOL)
    assert math.isclose(feb["fx_pnl_twd"], 13770.491803278674, abs_tol=_FLOAT_TOL)
    assert math.isclose(feb["cumulative_fx_pnl_twd"], 13770.491803278674, abs_tol=_FLOAT_TOL)

    mar = out["monthly"][1]
    assert mar["month"] == "2024-03"
    assert math.isclose(mar["fx_pnl_twd"], -8076.923076923048, abs_tol=_FLOAT_TOL)
