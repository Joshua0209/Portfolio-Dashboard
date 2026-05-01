from datetime import date as _date
from typing import List, Optional
from sqlmodel import Session, desc, select
from invest.persistence.models.portfolio_daily import PortfolioDaily
class PortfolioRepo:
    def __init__(self, session: Session):
        self.session = session
    def upsert(self, row: PortfolioDaily) -> PortfolioDaily:
        existing = self.session.get(PortfolioDaily, row.date)
        if existing is not None:
            self.session.delete(existing)
            self.session.flush()
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row
    def find_by_date(self, on_date: _date) -> Optional[PortfolioDaily]:
        return self.session.get(PortfolioDaily, on_date)
    def find_in_range(
        self, start: _date, end: _date
    ) -> List[PortfolioDaily]:
        stmt = (
            select(PortfolioDaily)
            .where(
                PortfolioDaily.date >= start,
                PortfolioDaily.date <= end,
            )
            .order_by(PortfolioDaily.date)
        )
        return list(self.session.exec(stmt).all())
    def find_latest(self) -> Optional[PortfolioDaily]:
        stmt = (
            select(PortfolioDaily)
            .order_by(desc(PortfolioDaily.date))
            .limit(1)
        )
        return self.session.exec(stmt).first()
    def find_all_dates(self) -> List[_date]:
        stmt = select(PortfolioDaily.date).order_by(PortfolioDaily.date)
        return list(self.session.exec(stmt).all())
