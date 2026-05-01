from dataclasses import dataclass
from decimal import Decimal
from typing import Union


@dataclass(frozen=True)
class Money:
    """Immutable monetary value: amount + ISO 4217 currency code.

    Currency-mismatched arithmetic raises ValueError; cross-currency
    operations must go through .convert_to(target, rate) explicitly.
    """

    amount: Decimal
    currency: str

    def __post_init__(self) -> None:
        if not isinstance(self.amount, Decimal):
            raise TypeError(
                f"amount must be Decimal, got {type(self.amount).__name__}"
            )
        if not isinstance(self.currency, str) or len(self.currency) != 3:
            raise ValueError(
                f"currency must be a 3-letter ISO code, got {self.currency!r}"
            )
        object.__setattr__(self, "currency", self.currency.upper())

    def _same_currency_or_raise(self, other: "Money") -> None:
        if self.currency != other.currency:
            raise ValueError(
                f"currency mismatch: {self.currency} vs {other.currency}"
            )

    def __add__(self, other: "Money") -> "Money":
        self._same_currency_or_raise(other)
        return Money(self.amount + other.amount, self.currency)

    def __sub__(self, other: "Money") -> "Money":
        self._same_currency_or_raise(other)
        return Money(self.amount - other.amount, self.currency)

    def __mul__(self, scalar: Union[int, Decimal]) -> "Money":
        if not isinstance(scalar, (int, Decimal)) or isinstance(scalar, bool):
            raise TypeError(
                f"Money * scalar requires int or Decimal, got "
                f"{type(scalar).__name__}"
            )
        return Money(self.amount * scalar, self.currency)

    def __rmul__(self, scalar: Union[int, Decimal]) -> "Money":
        return self.__mul__(scalar)

    def __neg__(self) -> "Money":
        return Money(-self.amount, self.currency)

    def convert_to(self, target_currency: str, rate: Decimal) -> "Money":
        target = target_currency.upper()
        if target == self.currency:
            return self
        if not isinstance(rate, Decimal):
            raise TypeError(
                f"FX rate must be Decimal, got {type(rate).__name__}"
            )
        return Money(self.amount * rate, target)

    def __lt__(self, other: "Money") -> bool:
        self._same_currency_or_raise(other)
        return self.amount < other.amount

    def __le__(self, other: "Money") -> bool:
        self._same_currency_or_raise(other)
        return self.amount <= other.amount

    def __gt__(self, other: "Money") -> bool:
        self._same_currency_or_raise(other)
        return self.amount > other.amount

    def __ge__(self, other: "Money") -> bool:
        self._same_currency_or_raise(other)
        return self.amount >= other.amount
