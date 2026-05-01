from datetime import datetime, timezone

from sqlmodel import Field, SQLModel


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class SymbolMarket(SQLModel, table=True):
    """Cache of the .TW / .TWO probe verdict per bare TW symbol.

    Verdict values:
      'twse'    -> use Yahoo .TW suffix
      'tpex'    -> use Yahoo .TWO suffix
      'unknown' -> negative cache; both suffixes probed empty

    Regenerable: drop the table, the next probe re-derives every
    verdict. Treat as a cache, not source-of-truth.
    """

    __tablename__ = "symbol_market"

    symbol: str = Field(primary_key=True)
    market: str
    resolved_at: datetime = Field(default_factory=_utcnow)
    last_verified_at: datetime = Field(default_factory=_utcnow)
