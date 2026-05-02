"""Per-ticker realized + unrealized P&L.
Composes the Position VO with a stream of Trades for realized P&L
(via FIFO matching) and a current-price snapshot for unrealized P&L
(via mark-to-market across open lots).
v1 scope: cash equity trades only. Margin and short trades are
silently skipped — Position v1 raises NotImplementedError on those,
so we filter upstream rather than crash the page.
"""
from decimal import Decimal
from typing import Dict, Iterable
from invest.domain.money import Money
from invest.domain.position import Position
from invest.domain.trade import Side, Trade
_CASH_SIDES = frozenset({Side.CASH_BUY, Side.CASH_SELL})
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
        unrealized = Money(Decimal("0"), pos.currency)
        for lot in pos.open_lots:
            unrealized = unrealized + (price - lot.cost_per_share) * lot.qty
        out[code] = unrealized
    return out
