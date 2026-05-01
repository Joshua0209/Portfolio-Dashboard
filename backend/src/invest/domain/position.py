from dataclasses import dataclass, field
from datetime import date as _date
from decimal import Decimal
from typing import List

from invest.domain.money import Money
from invest.domain.trade import Side, Trade


@dataclass(frozen=True)
class Lot:
    """One open buy lot, FIFO-tracked inside Position.open_lots."""

    date: _date
    qty: int
    cost_per_share: Money


@dataclass(frozen=True)
class RealizedPair:
    """A matched buy+sell with realized P&L."""

    open_lot: Lot
    close_date: _date
    close_qty: int
    close_price: Money

    @property
    def realized_pnl(self) -> Money:
        return (self.close_price - self.open_lot.cost_per_share) * self.close_qty


@dataclass
class Position:
    """FIFO position over a stream of cash-equity Trades.

    Mutable: trades stream in, position ages. Lot and RealizedPair
    underneath are frozen so the historical record stays immutable.

    v1 supports CASH_BUY / CASH_SELL only. Margin and short trades
    raise NotImplementedError to fail loud rather than silent-mis-
    account.
    """

    code: str
    currency: str
    open_lots: List[Lot] = field(default_factory=list)
    realized_pairs: List[RealizedPair] = field(default_factory=list)

    @property
    def total_qty(self) -> int:
        return sum(lot.qty for lot in self.open_lots)

    @property
    def cost_basis(self) -> Money:
        total = Money(Decimal("0"), self.currency)
        for lot in self.open_lots:
            total = total + (lot.cost_per_share * lot.qty)
        return total

    @property
    def realized_pnl(self) -> Money:
        total = Money(Decimal("0"), self.currency)
        for pair in self.realized_pairs:
            total = total + pair.realized_pnl
        return total

    def apply(self, trade: Trade) -> None:
        if trade.price.currency != self.currency:
            raise ValueError(
                f"currency mismatch: position={self.currency} "
                f"trade={trade.price.currency}"
            )
        if trade.side in (Side.SHORT_SELL, Side.SHORT_COVER):
            raise NotImplementedError(
                "short positions not yet supported in Position v1"
            )
        if trade.side in (Side.MARGIN_BUY, Side.MARGIN_SELL):
            raise NotImplementedError(
                "margin trades not yet supported in Position v1"
            )
        if trade.side is Side.CASH_BUY:
            self._buy(trade)
        else:  # CASH_SELL
            self._sell(trade)

    def _buy(self, trade: Trade) -> None:
        self.open_lots.append(
            Lot(
                date=trade.date,
                qty=trade.qty,
                cost_per_share=trade.price,
            )
        )

    def _sell(self, sell: Trade) -> None:
        remaining = sell.qty
        while remaining > 0:
            if not self.open_lots:
                raise ValueError(
                    f"sell exceeds open qty for {self.code}: "
                    f"{remaining} share(s) unmatched"
                )
            lot = self.open_lots[0]
            close_qty = min(remaining, lot.qty)
            self.realized_pairs.append(
                RealizedPair(
                    open_lot=lot,
                    close_date=sell.date,
                    close_qty=close_qty,
                    close_price=sell.price,
                )
            )
            remaining -= close_qty
            if close_qty == lot.qty:
                self.open_lots.pop(0)
            else:
                self.open_lots[0] = Lot(
                    date=lot.date,
                    qty=lot.qty - close_qty,
                    cost_per_share=lot.cost_per_share,
                )
