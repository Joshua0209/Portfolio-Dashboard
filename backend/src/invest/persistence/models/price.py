from datetime import date as _date, datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlmodel import Field, SQLModel
from sqlalchemy import UniqueConstraint


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Price(SQLModel, table=True):
    __tablename__ = "prices"
    __table_args__ = (
        UniqueConstraint("date", "symbol", name="uq_price_date_symbol"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    date: _date = Field(index=True)
    symbol: str = Field(index=True)
    close: Decimal = Field(max_digits=18, decimal_places=6)
    currency: str = Field(max_length=3)
    source: str = Field(index=True)
    ingested_at: datetime = Field(default_factory=_utcnow)
