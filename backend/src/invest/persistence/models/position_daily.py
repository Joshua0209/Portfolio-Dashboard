from datetime import date as _date, datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlmodel import Field, SQLModel
from sqlalchemy import UniqueConstraint


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PositionDaily(SQLModel, table=True):
    __tablename__ = "positions_daily"
    __table_args__ = (
        UniqueConstraint(
            "date", "code", "source", name="uq_pos_daily_date_code_source"
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    date: _date = Field(index=True)
    code: str = Field(index=True)
    qty: int
    close: Decimal = Field(max_digits=18, decimal_places=6)
    currency: str = Field(max_length=3)
    market_value: Decimal = Field(max_digits=20, decimal_places=4)
    source: str = Field(index=True)
    ingested_at: datetime = Field(default_factory=_utcnow)
