from decimal import Decimal

from invest.domain.money import Money
from invest.domain.trade import Side, Trade as TradeVO, Venue
from invest.persistence.models.trade import Trade as TradeRow


_ZERO = Decimal("0")


class TradeMapper:
    """Boundary between persistence.models.trade and domain.trade.

    Lives on the persistence side per Clean Architecture: this module
    imports both layers, but the domain itself never imports persistence.
    """

    @staticmethod
    def to_domain(row: TradeRow) -> TradeVO:
        ccy = row.currency
        return TradeVO(
            date=row.date,
            code=row.code,
            side=Side(row.side),
            qty=row.qty,
            price=Money(row.price, ccy),
            venue=Venue(row.venue),
            fee=Money(row.fee, ccy),
            tax=Money(row.tax, ccy),
            rebate=Money(row.rebate, ccy),
        )

    @staticmethod
    def to_row(vo: TradeVO, source: str) -> TradeRow:
        ccy = vo.price.currency
        for label, m in (("fee", vo.fee), ("tax", vo.tax), ("rebate", vo.rebate)):
            if m is not None and m.currency != ccy:
                raise ValueError(
                    f"currency mismatch on {label}: price={ccy} {label}={m.currency}"
                )
        return TradeRow(
            date=vo.date,
            code=vo.code,
            side=int(vo.side),
            qty=vo.qty,
            price=vo.price.amount,
            currency=ccy,
            fee=vo.fee.amount if vo.fee is not None else _ZERO,
            tax=vo.tax.amount if vo.tax is not None else _ZERO,
            rebate=vo.rebate.amount if vo.rebate is not None else _ZERO,
            source=source,
            venue=str(vo.venue),
        )
