"""TW symbol .TW / .TWO probe wrapper with symbol_market verdict cache.

Bare TW tickers (e.g. '2330', '5483') need a Yahoo suffix to fetch:
  TWSE-listed -> '.TW'
  TPEX/OTC    -> '.TWO'

The verdict for each symbol is cached in `symbol_market` so we only
probe once per symbol. The 'unknown' verdict is a NEGATIVE cache:
both suffixes were probed and both came back empty — never re-probe.
"""
from __future__ import annotations

from typing import Protocol

from invest.persistence.models.symbol_market import SymbolMarket
from invest.persistence.repositories.symbol_market_repo import (
    SymbolMarketRepo,
)


class PriceClient(Protocol):
    def fetch_prices(
        self, symbol: str, start: str, end: str
    ) -> list[dict]: ...


_SUFFIX_BY_VERDICT = {"twse": ".TW", "tpex": ".TWO"}


def fetch_tw_with_probe(
    bare_symbol: str,
    start: str,
    end: str,
    *,
    client: PriceClient,
    market_repo: SymbolMarketRepo,
) -> list[dict]:
    """Fetch yfinance rows for a bare TW symbol, picking .TW or .TWO
    based on the cached symbol_market verdict (probing if absent).

    Returns rows shaped like the underlying client (no tagging).
    Caller is responsible for persistence and currency stamping.
    """
    cached = market_repo.find(bare_symbol)

    if cached is not None:
        if cached.market == "unknown":
            # Negative cache: both suffixes already probed empty.
            return []
        suffix = _SUFFIX_BY_VERDICT.get(cached.market)
        if suffix is None:
            # Unrecognised verdict in cache (should not happen; treat as
            # unknown so we return [] rather than raising KeyError).
            return []
        return client.fetch_prices(f"{bare_symbol}{suffix}", start, end)

    # Cache miss: probe .TW first, .TWO second.
    twse_rows = client.fetch_prices(
        f"{bare_symbol}.TW", start, end
    )
    if twse_rows:
        market_repo.upsert(SymbolMarket(symbol=bare_symbol, market="twse"))
        return twse_rows

    tpex_rows = client.fetch_prices(
        f"{bare_symbol}.TWO", start, end
    )
    if tpex_rows:
        market_repo.upsert(SymbolMarket(symbol=bare_symbol, market="tpex"))
        return tpex_rows

    # Both empty: persist negative cache so future calls short-circuit.
    market_repo.upsert(SymbolMarket(symbol=bare_symbol, market="unknown"))
    return []
