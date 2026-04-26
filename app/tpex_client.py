"""TPEX (OTC) tradingStock HTTP client.

Thin wrapper over the public TPEX endpoint:

  https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock
      ?stkno=XXXX&date=YYYY/MM/01&id=&response=json

Returns one calendar month of post-trading prices for one OTC stock at a
time. Mirrors the shape of `app.twse_client` so `price_sources.py` can
treat both backends uniformly.

Format quirks vs TWSE (pinned by tests/test_tpex_client.py):
  - stat lowercase "ok" (TWSE uses uppercase "OK") — accept either
  - data lives under tables[0].data
  - field label "日 期" has a regular space inside (TWSE uses "日期")
  - "not on this exchange" = stat==ok + tables[0].data==[] + code is null
"""
from __future__ import annotations

import logging
import random
import threading
import time
from typing import Callable

import requests

from app.twse_client import roc_to_iso

log = logging.getLogger(__name__)

BASE_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"

USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
)

# Module-level for test monkeypatching + connection pooling reuse.
_session = requests.Session()


# --- Pure parsers ----------------------------------------------------------


def _to_float(s: str) -> float:
    return float(s.replace(",", ""))


def _to_int(s: str) -> int:
    return int(s.replace(",", ""))


def _find_field_index(fields: list[str], label: str) -> int | None:
    """Find a field by exact match, then fallback to whitespace-stripped match.

    TPEX uses "日 期" with an inner space; TWSE uses "日期". Accepting both
    gives us a one-line defense against future label drift.
    """
    if label in fields:
        return fields.index(label)
    norm = label.replace(" ", "")
    for i, f in enumerate(fields):
        if f.replace(" ", "") == norm:
            return i
    return None


def parse_response(payload: dict) -> list[dict]:
    """Convert a TPEX JSON payload to [{date, close, volume}, ...].

    Returns [] when stat is non-ok, when tables/data is missing, or when
    the symbol is not on TPEX (data:[] + code:null is the canonical
    "not OTC" signal).
    """
    if not isinstance(payload, dict):
        return []
    stat = (payload.get("stat") or "").lower()
    if stat != "ok":
        return []
    tables = payload.get("tables") or []
    if not tables:
        return []
    table = tables[0] if isinstance(tables[0], dict) else {}
    fields = table.get("fields") or []
    rows = table.get("data") or []
    if not fields or not rows:
        return []

    i_date = _find_field_index(fields, "日期")
    i_volume = _find_field_index(fields, "成交張數")
    i_close = _find_field_index(fields, "收盤")
    if i_date is None or i_close is None or i_volume is None:
        log.warning("TPEX field labels changed: %s", fields)
        return []

    out: list[dict] = []
    for row in rows:
        try:
            out.append(
                {
                    "date": roc_to_iso(row[i_date]),
                    "close": _to_float(row[i_close]),
                    "volume": _to_int(row[i_volume]),
                }
            )
        except (ValueError, IndexError) as e:
            log.warning("skipping malformed TPEX row %r: %s", row, e)
    return out


# --- Stateful client -------------------------------------------------------


class TpexClient:
    """TPEX HTTP client with the same retry/backoff/UA-rotation contract as
    TwseClient. Sleep and jitter are injected for fast unit tests.
    """

    def __init__(
        self,
        base_sleep: float = 0.5,
        jitter_fn: Callable[[], float] | None = None,
        sleep_fn: Callable[[float], None] | None = None,
        max_attempts: int = 3,
    ):
        self._base_sleep = base_sleep
        self._current_sleep = base_sleep
        self._max_attempts = max_attempts
        self._lock = threading.Lock()
        self._ua_index = 0
        self._jitter = jitter_fn or (lambda: random.uniform(0.0, 0.2))
        self._sleep = sleep_fn or time.sleep

    def _next_ua(self) -> str:
        with self._lock:
            ua = USER_AGENTS[self._ua_index % len(USER_AGENTS)]
            self._ua_index += 1
        return ua

    def _on_success(self) -> None:
        self._current_sleep = max(self._base_sleep, self._current_sleep / 2)

    def _on_failure(self) -> None:
        self._current_sleep = max(self._base_sleep * 2, self._current_sleep * 2)

    def fetch_month(self, stock_no: str, year: int, month: int) -> list[dict]:
        date_param = f"{year:04d}/{month:02d}/01"
        url = f"{BASE_URL}?stkno={stock_no}&date={date_param}&id=&response=json"
        attempts = 0
        while attempts < self._max_attempts:
            attempts += 1
            ua = self._next_ua()
            try:
                resp = _session.get(
                    url, headers={"User-Agent": ua}, timeout=10
                )
                if resp.status_code == 200:
                    payload = resp.json()
                    self._on_success()
                    self._sleep(self._current_sleep + self._jitter())
                    return parse_response(payload)
                log.warning(
                    "TPEX non-200 (status=%s) for %s %d-%02d (attempt %d)",
                    resp.status_code, stock_no, year, month, attempts,
                )
            except Exception as e:
                log.warning(
                    "TPEX error for %s %d-%02d (attempt %d): %s",
                    stock_no, year, month, attempts, e,
                )
            self._on_failure()
            self._sleep(self._current_sleep + self._jitter())
        log.warning(
            "TPEX freeze: %s %d-%02d failed after %d attempts; returning []",
            stock_no, year, month, self._max_attempts,
        )
        return []


_singleton = TpexClient()


def fetch_month(stock_no: str, year: int, month: int) -> list[dict]:
    """Convenience wrapper around the module-level singleton."""
    return _singleton.fetch_month(stock_no, year, month)
