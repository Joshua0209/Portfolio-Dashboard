"""DLQ-aware price fetching service (middle-path rule).

Composes invest.prices.yfinance_client + PriceRepo + FailedTaskRepo
into a single facade with the middle-path DLQ rule chosen during
the Phase 2 design pause:

  Outcome A  exception                  -> DLQ insert + bump on retry
  Outcome B  empty, has prior data      -> silent miss
  Outcome C  empty, no prior data       -> DLQ insert ONCE
                                           (no auto-bump on repeat)

Plus: a successful fetch resolves any open DLQ row for the same
symbol — otherwise the operator sees stale "missing" rows
indefinitely after a transient outage clears.
"""
from __future__ import annotations

from datetime import date as _date
from decimal import Decimal
from typing import Optional, Protocol

from invest.persistence.models.failed_task import FailedTask
from invest.persistence.models.price import Price
from invest.persistence.repositories.failed_task_repo import FailedTaskRepo
from invest.persistence.repositories.price_repo import PriceRepo
from invest.persistence.repositories.symbol_market_repo import (
    SymbolMarketRepo,
)
from invest.prices.tw_probe import fetch_tw_with_probe, is_tw_warrant


_TASK_TYPE = "fetch_price"


class PriceClient(Protocol):
    def fetch_prices(
        self, symbol: str, start: str, end: str
    ) -> list[dict]: ...


def _open_task_for(
    dlq: FailedTaskRepo, symbol: str
) -> Optional[FailedTask]:
    """Find the (single) open DLQ row for `symbol`, if any.

    Filters in Python: payload is a JSON column, and at this scale
    (~50 symbols) avoiding portable-SQL JSON-extract is the right
    trade. Switch to a column-level index later if needed.
    """
    for t in dlq.find_by_type(_TASK_TYPE):
        if t.resolved_at is None and t.payload.get("symbol") == symbol:
            return t
    return None


def _has_prior_history(price_repo: PriceRepo, symbol: str) -> bool:
    return len(price_repo.find_prices(symbol)) > 0


def fetch_and_store(
    symbol: str,
    currency: str,
    on_date: _date,
    *,
    price_repo: PriceRepo,
    dlq: FailedTaskRepo,
    client: PriceClient,
    market_repo: Optional[SymbolMarketRepo] = None,
) -> Optional[Decimal]:
    """Fetch close for `symbol` on `on_date` and persist via `price_repo`.

    Returns the close as Decimal on success, None on miss/failure.
    On failure, applies the middle-path DLQ rule (see module docstring).

    For currency='TWD', routes through invest.prices.tw_probe to pick
    the right Yahoo suffix (.TW vs .TWO); requires `market_repo` so
    the verdict can be cached. The Price row is keyed on the BARE
    symbol regardless — the suffix is an implementation detail of
    the fetch, not part of the canonical identity.
    """
    iso = on_date.isoformat()
    payload = {"symbol": symbol, "currency": currency, "date": iso}

    if currency == "TWD" and market_repo is None:
        raise ValueError(
            "currency='TWD' requires market_repo for the .TW/.TWO probe; "
            "pass an SymbolMarketRepo or use a non-TWD currency"
        )

    try:
        if currency == "TWD":
            rows = fetch_tw_with_probe(
                symbol,
                iso,
                iso,
                client=client,
                market_repo=market_repo,
            )
        else:
            rows = client.fetch_prices(symbol, iso, iso)
    except Exception as exc:
        # Outcome A: real failure. Always bump.
        existing = _open_task_for(dlq, symbol)
        if existing is None:
            dlq.insert(
                FailedTask(
                    task_type=_TASK_TYPE, payload=payload, error=repr(exc)
                )
            )
        else:
            dlq.bump_attempt(existing.id, repr(exc))
        return None

    if not rows:
        if _has_prior_history(price_repo, symbol):
            # Outcome B: silent miss. We've priced this symbol before,
            # so an empty result is almost always a holiday.
            return None
        if currency == "TWD" and is_tw_warrant(symbol):
            # Outcome B': Taiwan warrant (權證) with no trades is the
            # steady state, not a failure. Clear any DLQ row left over
            # from before warrants were recognized as expected-empty.
            existing = _open_task_for(dlq, symbol)
            if existing is not None:
                dlq.mark_resolved(existing.id)
            return None
        # Outcome C: log once.
        if _open_task_for(dlq, symbol) is None:
            dlq.insert(
                FailedTask(
                    task_type=_TASK_TYPE,
                    payload=payload,
                    error=(
                        f"no rows for {symbol} on or before {iso}; "
                        "symbol may be delisted or unknown to yfinance"
                    ),
                )
            )
        # Else: already known to be missing — middle path means no bump.
        return None

    # Happy path: take the first row from the client. The wrapper
    # normalizes the response so for a single-day fetch, rows[0] is
    # the requested day (or the most recent priced day, on rare
    # weekend-fetch edge cases). Going through str() preserves the
    # human-readable float repr instead of carrying float binary
    # noise into Decimal.
    row = rows[0]
    close = Decimal(str(row["close"]))
    price_repo.upsert(
        Price(
            date=on_date,
            symbol=symbol,
            close=close,
            currency=currency,
            source="yfinance",
        )
    )

    # Recovery: resolve any open DLQ row so the /today banner clears.
    existing = _open_task_for(dlq, symbol)
    if existing is not None:
        dlq.mark_resolved(existing.id)

    return close


def fetch_and_store_range(
    symbol: str,
    currency: str,
    start: _date,
    end: _date,
    *,
    price_repo: PriceRepo,
    dlq: FailedTaskRepo,
    client: PriceClient,
    market_repo: Optional[SymbolMarketRepo] = None,
) -> int:
    """Fetch a date range for `symbol` and persist all rows via `price_repo`.

    Returns the number of rows persisted (0 on miss/failure). DLQ rules
    mirror :func:`fetch_and_store` but at the ``(symbol, range)``
    granularity — one DLQ row per failed range, not per failed date:

      Outcome A  exception                  -> insert/bump ONE DLQ row
      Outcome B  empty, has prior history   -> silent miss
      Outcome C  empty, no prior history    -> insert ONCE
                                                (no auto-bump on repeat)
      Non-empty -> persist + resolve any open DLQ row for the symbol

    For ``currency='TWD'``, routes through invest.prices.tw_probe to pick
    the right Yahoo suffix; requires ``market_repo`` so the verdict can
    be cached.
    """
    start_iso = start.isoformat()
    end_iso = end.isoformat()
    payload = {
        "symbol": symbol,
        "currency": currency,
        "start": start_iso,
        "end": end_iso,
    }

    if currency == "TWD" and market_repo is None:
        raise ValueError(
            "currency='TWD' requires market_repo for the .TW/.TWO probe; "
            "pass an SymbolMarketRepo or use a non-TWD currency"
        )

    try:
        if currency == "TWD":
            rows = fetch_tw_with_probe(
                symbol,
                start_iso,
                end_iso,
                client=client,
                market_repo=market_repo,
            )
        else:
            rows = client.fetch_prices(symbol, start_iso, end_iso)
    except Exception as exc:
        # Outcome A: real failure. Always bump.
        existing = _open_task_for(dlq, symbol)
        if existing is None:
            dlq.insert(
                FailedTask(
                    task_type=_TASK_TYPE, payload=payload, error=repr(exc)
                )
            )
        else:
            dlq.bump_attempt(existing.id, repr(exc))
        return 0

    if not rows:
        if _has_prior_history(price_repo, symbol):
            # Outcome B: silent miss.
            return 0
        if currency == "TWD" and is_tw_warrant(symbol):
            # Outcome B': Taiwan warrant (權證) with no trades is the
            # steady state, not a failure. Clear any DLQ row left over
            # from before warrants were recognized as expected-empty.
            existing = _open_task_for(dlq, symbol)
            if existing is not None:
                dlq.mark_resolved(existing.id)
            return 0
        # Outcome C: log once.
        if _open_task_for(dlq, symbol) is None:
            dlq.insert(
                FailedTask(
                    task_type=_TASK_TYPE,
                    payload=payload,
                    error=(
                        f"no rows for {symbol} in [{start_iso}..{end_iso}]; "
                        "symbol may be delisted or unknown to yfinance"
                    ),
                )
            )
        return 0

    # Happy path: persist every returned row. ``date`` may arrive as a
    # str (yfinance_client returns ISO strings) or as a date object — be
    # defensive on the boundary.
    persisted = 0
    for row in rows:
        raw_date = row.get("date")
        if isinstance(raw_date, _date):
            on_date = raw_date
        else:
            on_date = _date.fromisoformat(str(raw_date))
        close = Decimal(str(row["close"]))
        price_repo.upsert(
            Price(
                date=on_date,
                symbol=symbol,
                close=close,
                currency=currency,
                source="yfinance",
            )
        )
        persisted += 1

    # Recovery: resolve any open DLQ row for this symbol.
    existing = _open_task_for(dlq, symbol)
    if existing is not None:
        dlq.mark_resolved(existing.id)

    return persisted
