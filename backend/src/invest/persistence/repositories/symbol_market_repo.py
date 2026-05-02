from typing import Optional

from sqlmodel import Session

from invest.persistence._utils import utcnow
from invest.persistence.models.symbol_market import SymbolMarket


class SymbolMarketRepo:
    def __init__(self, session: Session):
        self.session = session

    def upsert(self, record: SymbolMarket) -> SymbolMarket:
        """Merge by `symbol` PK. Preserves resolved_at on re-upsert,
        advances last_verified_at to now."""
        existing = self.session.get(SymbolMarket, record.symbol)
        if existing is not None:
            existing.market = record.market
            existing.last_verified_at = utcnow()
            self.session.add(existing)
            self.session.commit()
            self.session.refresh(existing)
            return existing
        self.session.add(record)
        self.session.commit()
        self.session.refresh(record)
        return record

    def find(self, symbol: str) -> Optional[SymbolMarket]:
        return self.session.get(SymbolMarket, symbol)
