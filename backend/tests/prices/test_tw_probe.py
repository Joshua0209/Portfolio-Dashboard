"""Reproducer for invest.prices.tw_probe.fetch_tw_with_probe.

The legacy app/price_sources.py treats every bare TW symbol as a
two-stage problem:

  1. Look up symbol_market verdict.
     - 'twse'    -> fetch with .TW only
     - 'tpex'    -> fetch with .TWO only
     - 'unknown' -> SHORT-CIRCUIT, return [] without any client call
     - cache miss -> probe both suffixes
  2. On cache miss, probe .TW first; if empty, probe .TWO; persist
     the verdict so future calls skip the probe.

The new wrapper isolates this logic in a single function so
PriceService can call it for currency='TWD' without knowing the
caching mechanics.
"""
from __future__ import annotations

import pytest
from sqlmodel import Session, SQLModel, create_engine

from invest.persistence.models.symbol_market import SymbolMarket
from invest.persistence.repositories.symbol_market_repo import (
    SymbolMarketRepo,
)
from invest.prices import tw_probe


class StubClient:
    """Map suffixed-symbol -> response rows. Tracks all calls.

    A response of None on a key means 'no entry queued', which
    yields []. An Exception value would raise (not used here)."""

    def __init__(self, responses: dict[str, list[dict]] | None = None):
        self.responses = dict(responses or {})
        self.calls: list[dict] = []

    def fetch_prices(self, symbol: str, start: str, end: str) -> list[dict]:
        self.calls.append({"symbol": symbol, "start": start, "end": end})
        return list(self.responses.get(symbol, []))


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


@pytest.fixture
def market_repo(session):
    return SymbolMarketRepo(session)


# --- Cache hits ----------------------------------------------------------


class TestCacheHits:
    def test_twse_verdict_uses_dot_tw_suffix_only(self, market_repo):
        """On a 'twse' verdict, exactly one call is made — with
        symbol+'.TW'. .TWO must never be probed."""
        market_repo.upsert(SymbolMarket(symbol="2330", market="twse"))
        client = StubClient({
            "2330.TW": [{"date": "2026-04-30", "close": 980.0, "volume": 1}]
        })

        rows = tw_probe.fetch_tw_with_probe(
            "2330",
            "2026-04-30",
            "2026-04-30",
            client=client,
            market_repo=market_repo,
        )

        assert len(rows) == 1
        assert rows[0]["close"] == 980.0
        assert [c["symbol"] for c in client.calls] == ["2330.TW"]

    def test_tpex_verdict_uses_dot_two_suffix_only(self, market_repo):
        market_repo.upsert(SymbolMarket(symbol="5483", market="tpex"))
        client = StubClient({
            "5483.TWO": [{"date": "2026-04-30", "close": 250.0, "volume": 1}]
        })

        rows = tw_probe.fetch_tw_with_probe(
            "5483",
            "2026-04-30",
            "2026-04-30",
            client=client,
            market_repo=market_repo,
        )

        assert len(rows) == 1
        assert [c["symbol"] for c in client.calls] == ["5483.TWO"]

    def test_unknown_verdict_short_circuits_no_client_call(
        self, market_repo
    ):
        """The 'unknown' verdict is a NEGATIVE cache. The whole point
        is to not pay the round-trip cost again — so zero client
        calls must be made on this path."""
        market_repo.upsert(SymbolMarket(symbol="9999", market="unknown"))
        client = StubClient()

        rows = tw_probe.fetch_tw_with_probe(
            "9999",
            "2026-04-30",
            "2026-04-30",
            client=client,
            market_repo=market_repo,
        )

        assert rows == []
        assert client.calls == []


# --- Cache miss: probe order --------------------------------------------


class TestProbe:
    def test_probe_tw_first_persists_twse_on_success(self, market_repo):
        """First probe is .TW. If it returns rows, we persist 'twse'
        and never probe .TWO — saves one round-trip per TW-listed
        symbol on every cold-start backfill."""
        client = StubClient({
            "2330.TW": [{"date": "2026-04-30", "close": 980.0, "volume": 1}]
        })

        rows = tw_probe.fetch_tw_with_probe(
            "2330",
            "2026-04-30",
            "2026-04-30",
            client=client,
            market_repo=market_repo,
        )

        assert len(rows) == 1
        assert [c["symbol"] for c in client.calls] == ["2330.TW"]
        verdict = market_repo.find("2330")
        assert verdict is not None
        assert verdict.market == "twse"

    def test_probe_falls_back_to_two_when_tw_empty(self, market_repo):
        """Empty .TW response triggers .TWO probe (the TPEX fallback).
        Both calls should appear in order."""
        client = StubClient({
            # No entry for 2330.TW means yfinance returned []
            "5483.TWO": [{"date": "2026-04-30", "close": 250.0, "volume": 1}]
        })

        rows = tw_probe.fetch_tw_with_probe(
            "5483",
            "2026-04-30",
            "2026-04-30",
            client=client,
            market_repo=market_repo,
        )

        assert len(rows) == 1
        assert [c["symbol"] for c in client.calls] == ["5483.TW", "5483.TWO"]
        verdict = market_repo.find("5483")
        assert verdict is not None
        assert verdict.market == "tpex"

    def test_both_empty_persists_unknown_returns_empty(self, market_repo):
        """Both .TW and .TWO empty -> mark 'unknown'. The negative
        cache prevents any re-probe on subsequent calls."""
        client = StubClient()  # everything empty

        rows = tw_probe.fetch_tw_with_probe(
            "9999",
            "2026-04-30",
            "2026-04-30",
            client=client,
            market_repo=market_repo,
        )

        assert rows == []
        assert [c["symbol"] for c in client.calls] == ["9999.TW", "9999.TWO"]
        verdict = market_repo.find("9999")
        assert verdict is not None
        assert verdict.market == "unknown"


# --- Warrants (權證) ----------------------------------------------------


class TestWarrantClassifier:
    @pytest.mark.parametrize(
        "symbol",
        ["042900", "030001", "081234", "099999", "712345", "799999"],
    )
    def test_call_and_put_warrant_codes_recognized(self, symbol):
        assert tw_probe.is_tw_warrant(symbol) is True

    @pytest.mark.parametrize(
        "symbol",
        [
            "2330",     # 4-digit stock
            "0050",     # ETF (4 digits)
            "00631L",   # leveraged ETF (alpha suffix)
            "00981A",   # ETF
            "1234",     # any 4-digit
            "002330",   # second digit '0', not warrant
            "012345",   # second digit '1', not warrant (reserved range)
            "022345",   # second digit '2', not warrant
            "642900",   # first digit '6', not warrant
            "842900",   # first digit '8', not warrant
        ],
    )
    def test_non_warrant_codes_rejected(self, symbol):
        assert tw_probe.is_tw_warrant(symbol) is False


class TestWarrantProbe:
    def test_warrant_both_empty_skips_unknown_cache(self, market_repo):
        """A warrant whose .TW and .TWO probes both come back empty is
        the steady state — the symbol may simply have had no trades in
        the window. Persisting 'unknown' would blacklist it forever, so
        we deliberately leave the cache empty and re-probe next run."""
        client = StubClient()  # both empty

        rows = tw_probe.fetch_tw_with_probe(
            "042900",
            "2026-04-30",
            "2026-04-30",
            client=client,
            market_repo=market_repo,
        )

        assert rows == []
        assert [c["symbol"] for c in client.calls] == [
            "042900.TW",
            "042900.TWO",
        ]
        # Crucial: no negative cache so future backfills re-probe.
        assert market_repo.find("042900") is None
