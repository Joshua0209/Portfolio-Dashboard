from datetime import date as _date
from typing import Iterable, List
from sqlmodel import Session, select
from invest.persistence.models.position_daily import PositionDaily
class PositionDailyRepo:
    def __init__(self, session: Session):
        self.session = session
    def upsert(self, row: PositionDaily) -> PositionDaily:
        existing = self.session.exec(
            select(PositionDaily).where(
                PositionDaily.date == row.date,
                PositionDaily.code == row.code,
                PositionDaily.source == row.source,
            )
        ).first()
        if existing is not None:
            self.session.delete(existing)
            self.session.flush()
        self.session.add(row)
        self.session.commit()
        self.session.refresh(row)
        return row
    def find_by_date(self, on_date: _date) -> List[PositionDaily]:
        stmt = select(PositionDaily).where(PositionDaily.date == on_date)
        return list(self.session.exec(stmt).all())
    def find_for_code(self, code: str) -> List[PositionDaily]:
        stmt = (
            select(PositionDaily)
            .where(PositionDaily.code == code)
            .order_by(PositionDaily.date)
        )
        return list(self.session.exec(stmt).all())
    def replace_for_period(
        self,
        source: str,
        start: _date,
        end: _date,
        rows: Iterable[PositionDaily],
    ) -> None:
        """Idempotent truncate-and-replace by (source, [start, end] inclusive).
        flush() between deletes and inserts is required: SQLite enforces
        UniqueConstraint(date, code, source) at INSERT time, not at commit,
        so pending deletes must reach the DB before the new rows land.
        """
        existing = self.session.exec(
            select(PositionDaily).where(
                PositionDaily.source == source,
                PositionDaily.date >= start,
                PositionDaily.date <= end,
            )
        ).all()
        for row in existing:
            self.session.delete(row)
        self.session.flush()
        for row in rows:
            self.session.add(row)
        self.session.commit()
