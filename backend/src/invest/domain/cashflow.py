from dataclasses import dataclass
from datetime import date as _date
from decimal import Decimal
from enum import StrEnum

from invest.domain.money import Money


class CashflowKind(StrEnum):
    DEPOSIT = "deposit"
    WITHDRAWAL = "withdrawal"
    DIVIDEND = "dividend"
    INTEREST = "interest"
    REBATE = "rebate"


_EXTERNAL = frozenset({CashflowKind.DEPOSIT, CashflowKind.WITHDRAWAL})


@dataclass(frozen=True)
class Cashflow:
    """Signed cashflow event.

    Sign convention: amount > 0 is inflow (deposit, dividend received,
    rebate credited), amount < 0 is outflow (withdrawal, fee paid).
    """

    date: _date
    amount: Money
    kind: CashflowKind
    note: str = ""

    @property
    def is_inflow(self) -> bool:
        return self.amount.amount > Decimal("0")

    @property
    def is_outflow(self) -> bool:
        return self.amount.amount < Decimal("0")

    @property
    def is_external(self) -> bool:
        # External flows cross the portfolio boundary (capital in/out)
        # and are the only flows Modified Dietz / TWR count. Internal
        # flows like dividends are already in the equity curve.
        return self.kind in _EXTERNAL
