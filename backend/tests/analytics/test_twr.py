"""First-principles tests for Modified Dietz TWR.

The plan calls for golden-vector tests against the existing app/analytics.py
output. Those land later — these tests pin the MATH from first principles
so we don't bake in any bugs the legacy code may carry.
"""
from datetime import date
from decimal import Decimal

import pytest

from invest.analytics.twr import modified_dietz, twr_chain
from invest.domain.cashflow import Cashflow, CashflowKind
from invest.domain.money import Money


def _flow(day: int, amount_str: str, kind=CashflowKind.DEPOSIT) -> Cashflow:
    return Cashflow(
        date=date(2026, 5, day),
        amount=Money(Decimal(amount_str), "TWD"),
        kind=kind,
    )


class TestZeroReturn:
    def test_no_change_no_flows_returns_zero(self):
        r = modified_dietz(
            start_equity=Money(Decimal("1000000"), "TWD"),
            end_equity=Money(Decimal("1000000"), "TWD"),
            cashflows=[],
            period_start=date(2026, 5, 1),
            period_end=date(2026, 5, 31),
        )
        assert r == Decimal("0")


class TestPureAppreciation:
    """No external flows — TWR == raw return regardless of method."""

    def test_five_percent_gain(self):
        r = modified_dietz(
            start_equity=Money(Decimal("1000000"), "TWD"),
            end_equity=Money(Decimal("1050000"), "TWD"),
            cashflows=[],
            period_start=date(2026, 5, 1),
            period_end=date(2026, 5, 31),
        )
        # (1050000 - 1000000) / 1000000 = 0.05
        assert r == Decimal("0.05")

    def test_loss(self):
        r = modified_dietz(
            start_equity=Money(Decimal("1000000"), "TWD"),
            end_equity=Money(Decimal("950000"), "TWD"),
            cashflows=[],
            period_start=date(2026, 5, 1),
            period_end=date(2026, 5, 31),
        )
        # -50000 / 1000000 = -0.05
        assert r == Decimal("-0.05")

    def test_method_irrelevant_when_no_flows(self):
        kw = dict(
            start_equity=Money(Decimal("1000000"), "TWD"),
            end_equity=Money(Decimal("1100000"), "TWD"),
            cashflows=[],
            period_start=date(2026, 5, 1),
            period_end=date(2026, 5, 31),
        )
        a = modified_dietz(method="day_weighted", **kw)
        b = modified_dietz(method="mid_month", **kw)
        c = modified_dietz(method="eom", **kw)
        assert a == b == c == Decimal("0.10")


class TestMidMonthMethod:
    """Modified Dietz with weight=0.5 for all flows.

    r = (V_end - V_start - F) / (V_start + 0.5*F)
    """

    def test_one_mid_month_deposit(self):
        # V_start=1M, V_end=1.2M, deposit 100K mid-month
        # numerator = 1.2M - 1M - 100K = 100K
        # denominator = 1M + 0.5 * 100K = 1.05M
        # r = 100K / 1.05M = 0.0952380952...
        r = modified_dietz(
            start_equity=Money(Decimal("1000000"), "TWD"),
            end_equity=Money(Decimal("1200000"), "TWD"),
            cashflows=[_flow(15, "100000")],
            method="mid_month",
            period_start=date(2026, 5, 1),
            period_end=date(2026, 5, 31),
        )
        # Compare with controlled tolerance.
        expected = Decimal("100000") / Decimal("1050000")
        assert abs(r - expected) < Decimal("1E-10")

    def test_internal_flow_is_excluded(self):
        # A dividend (internal, kind=DIVIDEND) should NOT enter the
        # Dietz numerator/denominator — it is already in the equity curve.
        r = modified_dietz(
            start_equity=Money(Decimal("1000000"), "TWD"),
            end_equity=Money(Decimal("1100000"), "TWD"),
            cashflows=[_flow(15, "5000", kind=CashflowKind.DIVIDEND)],
            method="mid_month",
            period_start=date(2026, 5, 1),
            period_end=date(2026, 5, 31),
        )
        # Equivalent to no flows: (1.1M - 1M) / 1M = 0.10
        assert r == Decimal("0.10")


class TestEndOfMonthMethod:
    """Modified Dietz with weight=0 for all flows.

    r = (V_end - V_start - F) / V_start
    """

    def test_one_end_of_month_deposit(self):
        # V_start=1M, V_end=1.2M, deposit 100K (any time)
        # r = (1.2M - 1M - 100K) / 1M = 100K / 1M = 0.10
        r = modified_dietz(
            start_equity=Money(Decimal("1000000"), "TWD"),
            end_equity=Money(Decimal("1200000"), "TWD"),
            cashflows=[_flow(15, "100000")],
            method="eom",
            period_start=date(2026, 5, 1),
            period_end=date(2026, 5, 31),
        )
        assert r == Decimal("0.10")


class TestDayWeightedMethod:
    """Day-weighted Modified Dietz: each flow F_i carries
    W_i = (D - d_i) / D where D is total period days, d_i is days
    elapsed when flow happened.

    A late-month deposit barely shrinks the denominator.
    """

    def test_late_month_flow_barely_affects_denominator(self):
        # 31-day month, deposit on day 31: weight ~ 0/31 ~ 0
        # numerator = 1.2M - 1M - 100K = 100K
        # denominator ~= 1M + (0/31) * 100K = 1M
        # r ~= 100K / 1M = 0.10
        r = modified_dietz(
            start_equity=Money(Decimal("1000000"), "TWD"),
            end_equity=Money(Decimal("1200000"), "TWD"),
            cashflows=[_flow(31, "100000")],
            method="day_weighted",
            period_start=date(2026, 5, 1),
            period_end=date(2026, 5, 31),
        )
        # Period length = 30 days (5/1 to 5/31), flow on day 31
        # weight = (30 - 30) / 30 = 0 → matches 'eom' exactly
        assert r == Decimal("0.10")

    def test_early_month_flow_approximates_mid_month(self):
        # 31-day month, deposit on day 1: weight = (30-0)/30 = 1.0
        # numerator = 1.2M - 1M - 100K = 100K
        # denominator = 1M + 1.0 * 100K = 1.1M
        # r = 100K / 1.1M = 0.0909...
        r = modified_dietz(
            start_equity=Money(Decimal("1000000"), "TWD"),
            end_equity=Money(Decimal("1200000"), "TWD"),
            cashflows=[_flow(1, "100000")],
            method="day_weighted",
            period_start=date(2026, 5, 1),
            period_end=date(2026, 5, 31),
        )
        expected = Decimal("100000") / Decimal("1100000")
        assert abs(r - expected) < Decimal("1E-10")


class TestInvalidMethod:
    def test_unknown_method_raises(self):
        with pytest.raises(ValueError, match="unknown method"):
            modified_dietz(
                start_equity=Money(Decimal("1000000"), "TWD"),
                end_equity=Money(Decimal("1100000"), "TWD"),
                cashflows=[],
                method="bogus",
                period_start=date(2026, 5, 1),
                period_end=date(2026, 5, 31),
            )


class TestChain:
    """twr_chain composes multi-period returns:
    cumulative = (1 + r_1) * (1 + r_2) * ... - 1
    """

    def test_empty_chain_is_zero(self):
        assert twr_chain([]) == Decimal("0")

    def test_single_period(self):
        assert twr_chain([Decimal("0.05")]) == Decimal("0.05")

    def test_two_period_compound(self):
        # 5% then 10%: (1.05)(1.10) - 1 = 0.155
        assert twr_chain([Decimal("0.05"), Decimal("0.10")]) == Decimal("0.155")

    def test_alternating_gain_loss(self):
        # 10% gain then 10% loss: (1.10)(0.90) - 1 = -0.01
        result = twr_chain([Decimal("0.10"), Decimal("-0.10")])
        assert result == Decimal("-0.01")
