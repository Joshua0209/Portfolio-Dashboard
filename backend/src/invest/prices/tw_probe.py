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


def is_tw_warrant(symbol: str) -> bool:
    """True if `symbol` looks like a Taiwan warrant (權證) code.

    Taiwan warrant numbering (per TWSE / TPEX):
      - 6-digit code, all numeric
      - Calls:  first digit '0', second digit '3'-'9'  (e.g. 042900, 081234)
      - Puts:   first digit '7'                        (e.g. 712345)

    ETFs (00xxxx, e.g. 0050, 00631L, 00981A) and stocks (4-digit) do not
    match. The check is intentionally strict — we only want the well-known
    warrant codespace, not anything that happens to be 6 digits.
    """
    if len(symbol) != 6 or not symbol.isdigit():
        return False
    return (symbol[0] == "0" and symbol[1] in "3456789") or symbol[0] == "7"


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
        suffix = _SUFFIX_BY_VERDICT[cached.market]
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

    # Both empty. For warrants (權證), skip the negative cache: a
    # zero-trade window is the steady state, not a signal that the
    # symbol is unknown. Future runs re-probe so that listings which
    # start trading get picked up.
    if is_tw_warrant(bare_symbol):
        return []

    # Genuine miss: persist negative cache so future calls short-circuit.
    market_repo.upsert(SymbolMarket(symbol=bare_symbol, market="unknown"))
    return []
