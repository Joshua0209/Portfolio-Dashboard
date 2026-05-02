from datetime import date as _date
from typing import Iterable, List
from sqlmodel import Session, select
from invest.persistence.models.trade import Trade
class TradeRepo:
    def __init__(self, session: Session):
        self.session = session
    def insert(self, trade: Trade) -> Trade:
        self.session.add(trade)
        self.session.commit()
        self.session.refresh(trade)
        return trade
    def find_by_month(self, month: str) -> List[Trade]:
        """Return all trades in the given month. month must be 'YYYY-MM'."""
        year_s, mon_s = month.split("-")
        y, m = int(year_s), int(mon_s)
        start = _date(y, m, 1)
        end = _date(y + 1, 1, 1) if m == 12 else _date(y, m + 1, 1)
        stmt = select(Trade).where(Trade.date >= start, Trade.date < end)
        return list(self.session.exec(stmt).all())
    def find_by_code(self, code: str) -> List[Trade]:
        return list(self.session.exec(select(Trade).where(Trade.code == code)).all())
    def find_since(self, since: _date) -> List[Trade]:
        return list(self.session.exec(select(Trade).where(Trade.date >= since)).all())
    def find_by_source(self, source: str) -> List[Trade]:
        return list(self.session.exec(select(Trade).where(Trade.source == source)).all())
    def replace_for_period(
        self,
        source: str,
        start: _date,
        end: _date,
        rows: Iterable[Trade],
    ) -> None:
        """Idempotent truncate-and-replace by (source, [start, end] inclusive)."""
        existing = self.session.exec(
            select(Trade).where(
                Trade.source == source,
                Trade.date >= start,
                Trade.date <= end,
            )
        ).all()
        for row in existing:
            self.session.delete(row)
        for row in rows:
            self.session.add(row)
        self.session.commit()
