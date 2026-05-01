from datetime import date as _date, datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlmodel import Field, SQLModel


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Trade(SQLModel, table=True):
    __tablename__ = "trades"

    id: Optional[int] = Field(default=None, primary_key=True)
    date: _date = Field(index=True)
    code: str = Field(index=True)
    side: int
    qty: int
    price: Decimal = Field(max_digits=18, decimal_places=6)
    currency: str = Field(max_length=3)
    fee: Decimal = Field(default=Decimal("0"), max_digits=18, decimal_places=4)
    tax: Decimal = Field(default=Decimal("0"), max_digits=18, decimal_places=4)
    rebate: Decimal = Field(default=Decimal("0"), max_digits=18, decimal_places=4)
    source: str = Field(index=True)
    venue: str = Field(index=True)
    ingested_at: datetime = Field(default_factory=_utcnow)
