"""Tests for analytics.daily_twr — per-day Modified Dietz chain.

These tests are written against the function contract in analytics.py,
not the implementation. They will pass once the loop body is filled in.
"""
from __future__ import annotations

import math

import pytest

from app import analytics


def _eq(rows: list[dict]) -> list[dict]:
    """Helper: minimal equity_series shape."""
    return [{"date": d, "equity_twd": v} for d, v in rows]


def _flow(rows: list[dict]) -> list[dict]:
    return [{"date": d, "flow_twd": v} for d, v in rows]


def test_empty_input_returns_empty_list():
    assert analytics.daily_twr([], []) == []


def test_single_day_input_zero_return():
    """One priced day: r_1 must be 0 (no prior equity to compare)."""
    out = analytics.daily_twr(
        _eq([("2026-04-01", 1_000_000)]),
        flow_series=[],
    )
    assert len(out) == 1
    assert out[0]["period_return"] == 0.0
    assert out[0]["cum_twr"] == 0.0
    assert out[0]["wealth_index"] == 1.0
    assert out[0]["date"] == "2026-04-01"


def test_constant_equity_no_flows_zero_cumulative_return():
    """Equity flat, no flows: every day's return is 0, cum stays 0."""
    series = _eq([
        ("2026-04-01", 1_000_000),
        ("2026-04-02", 1_000_000),
        ("2026-04-03", 1_000_000),
    ])
    out = analytics.daily_twr(series, flow_series=[])
    assert len(out) == 3
    for r in out:
        assert r["period_return"] == 0.0
        assert r["cum_twr"] == 0.0
    assert out[-1]["wealth_index"] == pytest.approx(1.0)


def test_pure_growth_no_flows_compounds_correctly():
    """+1% then +1% should compound to +2.01% (geometric chain)."""
    series = _eq([
        ("2026-04-01", 1_000_000),
        ("2026-04-02", 1_010_000),  # +1%
        ("2026-04-03", 1_020_100),  # +1%
    ])
    out = analytics.daily_twr(series, flow_series=[])
    assert out[1]["period_return"] == pytest.approx(0.01, rel=1e-6)
    assert out[2]["period_return"] == pytest.approx(0.01, rel=1e-6)
    assert out[2]["cum_twr"] == pytest.approx(0.0201, rel=1e-6)
    assert out[2]["wealth_index"] == pytest.approx(1.0201, rel=1e-6)


def test_inflow_does_not_inflate_return():
    """Modified Dietz invariant: a deposit must not change TWR.

    Day 2: equity jumps from 1M to 1.5M, but 500k of that came from a
    deposit. True investment return is 0 — the equity grew because of
    new capital, not market gain.
    """
    series = _eq([
        ("2026-04-01", 1_000_000),
        ("2026-04-02", 1_500_000),
    ])
    flows = _flow([("2026-04-02", 500_000)])  # deposit on day 2
    out = analytics.daily_twr(series, flows)
    # period_return on day 2 should be ~0 (whole jump is the deposit)
    assert abs(out[1]["period_return"]) < 1e-6
    assert abs(out[1]["cum_twr"]) < 1e-6


def test_withdrawal_does_not_depress_return():
    """Symmetric to deposit: a withdrawal of 500k that takes equity from
    1M to 500k is a 0% return, not a -50% return."""
    series = _eq([
        ("2026-04-01", 1_000_000),
        ("2026-04-02", 500_000),
    ])
    flows = _flow([("2026-04-02", -500_000)])
    out = analytics.daily_twr(series, flows)
    assert abs(out[1]["period_return"]) < 1e-6


def test_market_gain_with_concurrent_deposit():
    """Day 2: market drove equity 1M→1.05M (+5%) AND user deposited 100k.
    End-of-day equity = 1.15M. TWR should reflect ~5%, not 15%.

    Modified Dietz with weight=0.5:
      r = (1.15M − 1.0M − 0.1M) / (1.0M + 0.5·0.1M) = 0.05M / 1.05M ≈ 4.76%

    NOTE: This is NOT exactly 5% because Dietz with mid-period weight
    slightly overweights the denominator when there's a positive flow.
    With weight=0 it would be exactly 5%. We test the weight=0.5 result
    here since that's the function's default.
    """
    series = _eq([
        ("2026-04-01", 1_000_000),
        ("2026-04-02", 1_150_000),
    ])
    flows = _flow([("2026-04-02", 100_000)])
    out = analytics.daily_twr(series, flows, weight=0.5)
    # (50_000) / (1_000_000 + 50_000) ≈ 0.04762
    assert out[1]["period_return"] == pytest.approx(50_000 / 1_050_000, rel=1e-6)


def test_flow_lookup_skips_days_without_flow():
    """Days with no entry in flow_series should be treated as F=0."""
    series = _eq([
        ("2026-04-01", 1_000_000),
        ("2026-04-02", 1_010_000),
        ("2026-04-03", 1_020_000),
    ])
    flows = _flow([("2026-04-03", 0)])  # explicit zero only on day 3
    out = analytics.daily_twr(series, flows)
    # Day 2 has no flow entry; should still compute (1.01M - 1M) / 1M = 1%
    assert out[1]["period_return"] == pytest.approx(0.01, rel=1e-6)


def test_chain_invariance_across_decomposition():
    """Whether you compute TWR over 3 days as one period or chain across,
    the cumulative answer must match.

    Series: 1M → 1.05M → 1.0395M (≈+5% then -1%)
    No flows. Cum should be 0.05 × (1 - 0.01) ≈ 0.0395
    """
    series = _eq([
        ("2026-04-01", 1_000_000),
        ("2026-04-02", 1_050_000),
        ("2026-04-03", 1_039_500),
    ])
    out = analytics.daily_twr(series, flow_series=[])
    assert out[-1]["cum_twr"] == pytest.approx(0.0395, rel=1e-6)


def test_output_row_contains_required_keys():
    """Every row must carry the keys frontend / drawdown analysis depend on."""
    series = _eq([
        ("2026-04-01", 1_000_000),
        ("2026-04-02", 1_010_000),
    ])
    out = analytics.daily_twr(series, flow_series=[])
    required = {"date", "equity_twd", "flow_twd", "period_return", "cum_twr", "wealth_index"}
    for row in out:
        assert required.issubset(row.keys()), f"missing keys: {required - row.keys()}"


def test_wealth_index_starts_at_one():
    """wealth_index is the running ∏(1+r_d), starting at 1.0 on day 1."""
    series = _eq([
        ("2026-04-01", 1_000_000),
        ("2026-04-02", 1_010_000),
    ])
    out = analytics.daily_twr(series, flow_series=[])
    assert out[0]["wealth_index"] == 1.0
    assert out[1]["wealth_index"] == pytest.approx(1.01, rel=1e-6)


def test_wealth_index_equals_one_plus_cum_twr():
    """Algebraic invariant: wealth_index ≡ 1 + cum_twr for every row."""
    series = _eq([
        ("2026-04-01", 1_000_000),
        ("2026-04-02", 1_005_000),
        ("2026-04-03", 998_000),
        ("2026-04-04", 1_020_000),
    ])
    out = analytics.daily_twr(series, flow_series=[])
    for row in out:
        assert row["wealth_index"] == pytest.approx(1.0 + row["cum_twr"], rel=1e-9)
