from datetime import date as _date, datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import UniqueConstraint
from sqlmodel import Field, SQLModel

from invest.persistence._utils import utcnow


class FxRate(SQLModel, table=True):
    __tablename__ = "fx_rates"
    __table_args__ = (
        UniqueConstraint("date", "base", "quote", name="uq_fx_date_pair"),
    )

    id: Optional[int] = Field(default=None, primary_key=True)
    date: _date = Field(index=True)
    base: str = Field(max_length=3, index=True)
    quote: str = Field(max_length=3, index=True)
    rate: Decimal = Field(max_digits=18, decimal_places=8)
    source: str = Field(index=True)
    ingested_at: datetime = Field(default_factory=utcnow)
