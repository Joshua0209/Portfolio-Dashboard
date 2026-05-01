from datetime import date as _date
from decimal import Decimal
from typing import List, Optional
from sqlmodel import Session, select
from invest.persistence.models.price import Price
class PriceRepo:
    def __init__(self, session: Session):
        self.session = session
    def upsert(self, price: Price) -> Price:
        existing = self.session.exec(
            select(Price).where(
                Price.date == price.date,
                Price.symbol == price.symbol,
            )
        ).first()
        if existing is not None:
            self.session.delete(existing)
            self.session.flush()
        self.session.add(price)
        self.session.commit()
        self.session.refresh(price)
        return price
    def find_price(self, on_date: _date, symbol: str) -> Optional[Decimal]:
        row = self.session.exec(
            select(Price).where(
                Price.date == on_date,
                Price.symbol == symbol,
            )
        ).first()
        return row.close if row is not None else None
    def find_prices(self, symbol: str) -> List[Price]:
        stmt = (
            select(Price)
            .where(Price.symbol == symbol)
            .order_by(Price.date)
        )
        return list(self.session.exec(stmt).all())
    def find_prices_in_range(
        self, symbol: str, start: _date, end: _date
    ) -> List[Price]:
        stmt = (
            select(Price)
            .where(
                Price.symbol == symbol,
                Price.date >= start,
                Price.date <= end,
            )
            .order_by(Price.date)
        )
        return list(self.session.exec(stmt).all())
