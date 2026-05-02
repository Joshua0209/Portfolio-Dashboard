from dataclasses import FrozenInstanceError
from datetime import date
from decimal import Decimal

import pytest

from invest.domain.cashflow import Cashflow, CashflowKind
from invest.domain.money import Money


class TestCashflowKind:
    def test_known_values(self):
        assert CashflowKind.DEPOSIT.value == "deposit"
        assert CashflowKind.WITHDRAWAL.value == "withdrawal"
        assert CashflowKind.DIVIDEND.value == "dividend"
        assert CashflowKind.INTEREST.value == "interest"
        assert CashflowKind.REBATE.value == "rebate"


class TestConstruction:
    def test_minimal(self):
        cf = Cashflow(
            date=date(2026, 5, 1),
            amount=Money(Decimal("100000"), "TWD"),
            kind=CashflowKind.DEPOSIT,
        )
        assert cf.date == date(2026, 5, 1)
        assert cf.amount == Money(Decimal("100000"), "TWD")
        assert cf.kind is CashflowKind.DEPOSIT
        assert cf.note == ""

    def test_with_note(self):
        cf = Cashflow(
            date=date(2026, 5, 1),
            amount=Money(Decimal("-50000"), "TWD"),
            kind=CashflowKind.WITHDRAWAL,
            note="ATM 5/1 早餐前",
        )
        assert cf.note == "ATM 5/1 早餐前"

    def test_frozen(self):
        cf = Cashflow(
            date=date(2026, 5, 1),
            amount=Money(Decimal("100"), "TWD"),
            kind=CashflowKind.DEPOSIT,
        )
        with pytest.raises(FrozenInstanceError):
            cf.note = "tamper"  # type: ignore

    def test_equality(self):
        a = Cashflow(date=date(2026, 5, 1), amount=Money(Decimal("100"), "TWD"), kind=CashflowKind.DEPOSIT)
        b = Cashflow(date=date(2026, 5, 1), amount=Money(Decimal("100"), "TWD"), kind=CashflowKind.DEPOSIT)
        assert a == b


class TestDirection:
    def test_positive_amount_is_inflow(self):
        cf = Cashflow(
            date=date(2026, 5, 1),
            amount=Money(Decimal("100"), "TWD"),
            kind=CashflowKind.DEPOSIT,
        )
        assert cf.is_inflow
        assert not cf.is_outflow

    def test_negative_amount_is_outflow(self):
        cf = Cashflow(
            date=date(2026, 5, 1),
            amount=Money(Decimal("-100"), "TWD"),
            kind=CashflowKind.WITHDRAWAL,
        )
        assert cf.is_outflow
        assert not cf.is_inflow

    def test_zero_is_neither(self):
        cf = Cashflow(
            date=date(2026, 5, 1),
            amount=Money(Decimal("0"), "TWD"),
            kind=CashflowKind.REBATE,
        )
        assert not cf.is_inflow
        assert not cf.is_outflow


class TestExternalClassification:
    """TWR (Modified Dietz) only counts EXTERNAL cashflows — those that
    move capital across the portfolio boundary. Internal income
    (dividends, interest, rebates) happens INSIDE the portfolio and
    is already reflected in the equity curve."""

    def test_deposit_is_external(self):
        cf = Cashflow(date=date(2026, 5, 1), amount=Money(Decimal("100"), "TWD"), kind=CashflowKind.DEPOSIT)
        assert cf.is_external

    def test_withdrawal_is_external(self):
        cf = Cashflow(date=date(2026, 5, 1), amount=Money(Decimal("-100"), "TWD"), kind=CashflowKind.WITHDRAWAL)
        assert cf.is_external

    def test_dividend_is_internal(self):
        cf = Cashflow(date=date(2026, 5, 1), amount=Money(Decimal("100"), "TWD"), kind=CashflowKind.DIVIDEND)
        assert not cf.is_external

    def test_interest_is_internal(self):
        cf = Cashflow(date=date(2026, 5, 1), amount=Money(Decimal("100"), "TWD"), kind=CashflowKind.INTEREST)
        assert not cf.is_external

    def test_rebate_is_internal(self):
        cf = Cashflow(date=date(2026, 5, 1), amount=Money(Decimal("100"), "TWD"), kind=CashflowKind.REBATE)
        assert not cf.is_external
