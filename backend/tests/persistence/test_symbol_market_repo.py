"""Reproducer for SymbolMarket model + SymbolMarketRepo.

SymbolMarket caches the .TW / .TWO probe verdict for each Taiwanese
ticker so subsequent calls skip the probe. Three verdict values:

  'twse'    -> use Yahoo .TW suffix
  'tpex'    -> use Yahoo .TWO suffix
  'unknown' -> NEGATIVE CACHE: we probed both and got nothing;
               do not re-probe (used by the price service to
               short-circuit calls for delisted/wrong tickers)

Unlike most models in this layer, the natural PK is `symbol` alone
— each symbol has exactly one current verdict, no time series.
"""
from datetime import datetime, timezone

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.persistence.models.symbol_market import SymbolMarket
from invest.persistence.repositories.symbol_market_repo import (
    SymbolMarketRepo,
)


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


@pytest.fixture
def repo(session):
    return SymbolMarketRepo(session)


def _record(symbol: str = "2330", market: str = "twse") -> SymbolMarket:
    return SymbolMarket(symbol=symbol, market=market)


# --- Upsert --------------------------------------------------------------


class TestUpsert:
    def test_inserts_new_symbol_with_default_timestamps(self, repo):
        saved = repo.upsert(_record("2330", "twse"))

        assert saved.symbol == "2330"
        assert saved.market == "twse"
        # Pattern matches FailedTask's test: SQLite strips tz on
        # round-trip, so naive vs aware comparison is fragile. Just
        # confirm the defaults populated.
        assert saved.resolved_at is not None
        assert saved.last_verified_at is not None

    def test_overwrites_market_verdict_for_existing_symbol(self, repo):
        """When a probe revisits a symbol previously marked 'unknown'
        and finds it on TPEX (rare but possible if Yahoo finally lists
        it), the verdict must update."""
        repo.upsert(_record("5483", "unknown"))
        repo.upsert(_record("5483", "tpex"))

        record = repo.find("5483")
        assert record is not None
        assert record.market == "tpex"

    def test_overwrite_advances_last_verified_keeps_resolved_at(
        self, repo
    ):
        """resolved_at is when we FIRST learned the verdict;
        last_verified_at is when we most recently confirmed it.
        Re-upserting must NOT clobber resolved_at — that's the audit
        breadcrumb for 'when did we first ID this symbol'."""
        first = repo.upsert(_record("2330", "twse"))
        original_resolved_at = first.resolved_at

        # Re-upsert the same verdict (e.g. nightly verification).
        second = repo.upsert(_record("2330", "twse"))

        assert second.resolved_at == original_resolved_at
        assert second.last_verified_at >= original_resolved_at


# --- Find ----------------------------------------------------------------


class TestFind:
    def test_returns_none_for_unknown_symbol(self, repo):
        assert repo.find("9999") is None

    def test_returns_record_for_known_symbol(self, repo):
        repo.upsert(_record("2330", "twse"))
        record = repo.find("2330")
        assert record is not None
        assert record.market == "twse"
