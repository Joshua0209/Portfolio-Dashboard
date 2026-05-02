from datetime import date as _date, datetime
from decimal import Decimal
from sqlmodel import Field, SQLModel
from invest.persistence._utils import utcnow
class PortfolioDaily(SQLModel, table=True):
    """One aggregated portfolio snapshot per day.
    PK is date alone (no source column in the key), unlike PositionDaily
    which keys on (date, code, source). PortfolioDaily intentionally stores
    a single rolled-up equity/cost_basis for the whole portfolio per day;
    source tracks provenance but is not part of the uniqueness constraint.
    If multi-source coexistence at the portfolio level is ever needed,
    the PK must be expanded to (date, source).
    """
    __tablename__ = "portfolio_daily"
    date: _date = Field(primary_key=True)
    equity: Decimal = Field(max_digits=20, decimal_places=4)
    cost_basis: Decimal = Field(max_digits=20, decimal_places=4)
    currency: str = Field(max_length=3)
    source: str = Field(index=True)
    ingested_at: datetime = Field(default_factory=utcnow)
