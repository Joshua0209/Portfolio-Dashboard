from datetime import date as _date, datetime, timezone
from decimal import Decimal

from sqlmodel import Field, SQLModel


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PortfolioDaily(SQLModel, table=True):
    __tablename__ = "portfolio_daily"

    date: _date = Field(primary_key=True)
    equity: Decimal = Field(max_digits=20, decimal_places=4)
    cost_basis: Decimal = Field(max_digits=20, decimal_places=4)
    currency: str = Field(max_length=3)
    source: str = Field(index=True)
    ingested_at: datetime = Field(default_factory=_utcnow)
