from datetime import date as _date
from decimal import Decimal
from typing import List, Optional

from sqlmodel import Session, select

from invest.persistence.models.fx_rate import FxRate


class FxRepo:
    def __init__(self, session: Session):
        self.session = session

    def upsert(self, rate: FxRate) -> FxRate:
        existing = self.session.exec(
            select(FxRate).where(
                FxRate.date == rate.date,
                FxRate.base == rate.base,
                FxRate.quote == rate.quote,
            )
        ).first()
        if existing is not None:
            self.session.delete(existing)
            self.session.flush()
        self.session.add(rate)
        self.session.commit()
        self.session.refresh(rate)
        return rate

    def find_rate(
        self, on_date: _date, base: str, quote: str
    ) -> Optional[Decimal]:
        row = self.session.exec(
            select(FxRate).where(
                FxRate.date == on_date,
                FxRate.base == base,
                FxRate.quote == quote,
            )
        ).first()
        return row.rate if row is not None else None

    def find_rates(self, base: str, quote: str) -> List[FxRate]:
        stmt = (
            select(FxRate)
            .where(FxRate.base == base, FxRate.quote == quote)
            .order_by(FxRate.date)
        )
        return list(self.session.exec(stmt).all())

    def find_rates_in_range(
        self, base: str, quote: str, start: _date, end: _date
    ) -> List[FxRate]:
        stmt = (
            select(FxRate)
            .where(
                FxRate.base == base,
                FxRate.quote == quote,
                FxRate.date >= start,
                FxRate.date <= end,
            )
            .order_by(FxRate.date)
        )
        return list(self.session.exec(stmt).all())
