"""Per-ticker realized + unrealized P&L.

Composes the Position VO with a stream of Trades for realized P&L
(via FIFO matching) and a current-price snapshot for unrealized P&L
(via mark-to-market across open lots).

Three public surfaces:

  realized_pnl_per_position(trades)
      Returns {code: realized_pnl_money}. Lightweight.

  realized_stats_per_position(trades)
      Returns {code: RealizedStats} — FIFO P&L + per-position
      bookkeeping (sell proceeds/cost, open qty/cost, win/loss
      counters, gross win/loss, holding periods). Powers the rich
      tax page that needs win-rate / profit-factor / avg-holding-days.

  unrealized_pnl_per_position(positions, current_prices)
      Mark-to-market across open lots.

v1 scope: cash equity trades only. Margin and short trades are
silently skipped — Position v1 raises NotImplementedError on those,
so we filter upstream rather than crash the page.
"""
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, Iterable, List, Optional

from invest.domain.money import Money
from invest.domain.position import Position
from invest.domain.trade import Side, Trade

_CASH_SIDES = frozenset({Side.CASH_BUY, Side.CASH_SELL})
_ZERO = Decimal("0")


@dataclass(frozen=True)
class RealizedStats:
    """Per-position realized P&L bookkeeping for the rich tax page.

    Aggregated from `Position.realized_pairs` by grouping pairs that
    came from the same sell trade (same `(close_date, close_price)`).
    `holding_periods_days` has one entry per lot match (not per sell
    trade) — `avg_holding_days` is their unweighted mean.
    """

    code: str
    currency: str
    realized_pnl: Money
    sell_proceeds: Money
    cost_of_sold: Money
    sell_qty: int
    open_qty: int
    open_cost: Money
    avg_open_cost: Optional[Money]
    wins: int
    losses: int
    gross_win: Money
    gross_loss: Money
    holding_periods_days: List[int] = field(default_factory=list)

    @property
    def avg_holding_days(self) -> Optional[float]:
        if not self.holding_periods_days:
            return None
        return sum(self.holding_periods_days) / len(self.holding_periods_days)

    @property
    def win_rate(self) -> Optional[float]:
        total = self.wins + self.losses
        return (self.wins / total) if total else None

    @property
    def profit_factor(self) -> Optional[Decimal]:
        if self.gross_loss.amount <= _ZERO:
            return None
        return self.gross_win.amount / self.gross_loss.amount

    @property
    def fully_closed(self) -> bool:
        return self.open_qty == 0


def build_positions(trades: Iterable[Trade]) -> Dict[str, Position]:
    """Group cash-only trades by code, build a Position per code."""
    by_code: Dict[str, Position] = {}
    cash_trades = [t for t in trades if t.side in _CASH_SIDES]
    for t in sorted(cash_trades, key=lambda t: t.date):
        if t.code not in by_code:
            by_code[t.code] = Position(code=t.code, currency=t.price.currency)
        by_code[t.code].apply(t)
    return by_code


def realized_pnl_per_position(trades: Iterable[Trade]) -> Dict[str, Money]:
    """Per-ticker realized P&L from FIFO-matched buy/sell pairs."""
    positions = build_positions(trades)
    return {code: pos.realized_pnl for code, pos in positions.items()}


def realized_stats_per_position(trades: Iterable[Trade]) -> Dict[str, RealizedStats]:
    """Per-ticker realized P&L plus bookkeeping (wins/losses, holding
    period, etc.). One sell trade contributes one win or one loss
    based on the net realized P&L across the lots it consumed.

    Pairs are grouped by `(close_date, close_price)` to recover sell-
    trade boundaries — RealizedPair doesn't carry a sell-trade id, but
    a sell trade always produces consecutive pairs with identical
    close_date and close_price, so the key is sufficient unless two
    distinct sells happen on the same day at the exact same price
    (rare; would conflate as a single trade — same as legacy when the
    inputs match).
    """
    positions = build_positions(trades)
    out: Dict[str, RealizedStats] = {}
    for code, pos in positions.items():
        ccy = pos.currency

        # Per-sell-trade groups.
        groups: Dict[tuple, list] = {}
        order: List[tuple] = []
        for pair in pos.realized_pairs:
            key = (pair.close_date, pair.close_price.amount)
            if key not in groups:
                groups[key] = []
                order.append(key)
            groups[key].append(pair)

        sell_proceeds = _ZERO
        cost_of_sold = _ZERO
        sell_qty_total = 0
        wins = 0
        losses = 0
        gross_win = _ZERO
        gross_loss = _ZERO
        holding_periods: List[int] = []
        for key in order:
            group = groups[key]
            trade_realized = _ZERO
            for p in group:
                lot_proceeds = p.close_price.amount * p.close_qty
                lot_cost = p.open_lot.cost_per_share.amount * p.close_qty
                sell_proceeds += lot_proceeds
                cost_of_sold += lot_cost
                sell_qty_total += p.close_qty
                trade_realized += (lot_proceeds - lot_cost)
                holding_periods.append((p.close_date - p.open_lot.date).days)
            if trade_realized > _ZERO:
                wins += 1
                gross_win += trade_realized
            elif trade_realized < _ZERO:
                losses += 1
                gross_loss += -trade_realized

        open_qty = pos.total_qty
        open_cost = pos.cost_basis
        avg_open_cost: Optional[Money] = None
        if open_qty > 0:
            avg_open_cost = Money(open_cost.amount / Decimal(open_qty), ccy)

        out[code] = RealizedStats(
            code=code,
            currency=ccy,
            realized_pnl=pos.realized_pnl,
            sell_proceeds=Money(sell_proceeds, ccy),
            cost_of_sold=Money(cost_of_sold, ccy),
            sell_qty=sell_qty_total,
            open_qty=open_qty,
            open_cost=open_cost,
            avg_open_cost=avg_open_cost,
            wins=wins,
            losses=losses,
            gross_win=Money(gross_win, ccy),
            gross_loss=Money(gross_loss, ccy),
            holding_periods_days=holding_periods,
        )
    return out


def unrealized_pnl_per_position(
    positions: Dict[str, Position],
    current_prices: Dict[str, Money],
) -> Dict[str, Money]:
    """Per-ticker unrealized P&L = sum_lots((price - cost_per_share) * qty).
    Codes without a current price are skipped silently (e.g., a
    yfinance miss should not blank the entire page). Currency
    mismatch between the position's lots and the current price
    raises — that's a data-integrity bug worth surfacing.
    """
    out: Dict[str, Money] = {}
    for code, pos in positions.items():
        price = current_prices.get(code)
        if price is None:
            continue
        if price.currency != pos.currency:
            raise ValueError(
                f"currency mismatch for {code}: "
                f"position={pos.currency} price={price.currency}"
            )
        unrealized = Money(_ZERO, pos.currency)
        for lot in pos.open_lots:
            unrealized = unrealized + (price - lot.cost_per_share) * lot.qty
        out[code] = unrealized
    return out
