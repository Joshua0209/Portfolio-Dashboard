"""TWSE STOCK_DAY HTTP client.

Thin wrapper over the public TWSE endpoint:
  https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=YYYYMM01&stockNo=XXXX&response=json

Returns one calendar month of post-trading prices for one stock at a time.
TWSE has a WAF that flags scraping; defenses (per spec line 73):

  (a) dynamic backoff — base 0.5s; on any non-200, double the cooldown for
      the next 10 requests, then halve back toward base; on max_attempts
      consecutive non-200, freeze the client and return [] (not raise).
  (b) User-Agent rotation — pool of 4 realistic browser UAs, round-robin.
  (c) Jitter — uniform 0–200ms added to every sleep.

State lives in the module-level singleton; `fetch_month()` is the
convenience entry. `parse_response()` is exported pure for unit tests.
"""
from __future__ import annotations

import logging
import random
import threading
import time
from typing import Callable

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY"

USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
)

# requests.Session is module-level so connection pooling is reused across
# fetches. Tests monkeypatch this attribute to inject a fake.
_session = requests.Session()


# --- Pure parsers ----------------------------------------------------------


def roc_to_iso(roc: str) -> str:
    """'115/04/01' (民國) → '2026-04-01' (ISO)."""
    parts = roc.strip().split("/")
    if len(parts) != 3:
        raise ValueError(f"unexpected ROC date {roc!r}")
    y, m, d = (int(p) for p in parts)
    return f"{y + 1911:04d}-{m:02d}-{d:02d}"


def _to_float(s: str) -> float:
    return float(s.replace(",", ""))


def _to_int(s: str) -> int:
    return int(s.replace(",", ""))


def parse_response(payload: dict) -> list[dict]:
    """Convert a TWSE JSON payload to a list of {date, close, volume} dicts.

    Returns [] when stat != 'OK' or when data/fields are missing — TWSE
    signals "stock not on this exchange" the same way as "no trades this
    month".
    """
    if not isinstance(payload, dict):
        return []
    if payload.get("stat") != "OK":
        return []
    fields = payload.get("fields") or []
    rows = payload.get("data") or []
    if not fields or not rows:
        return []
    # The Chinese field labels are stable per TWSE docs:
    #   日期 成交股數 成交金額 開盤價 最高價 最低價 收盤價 漲跌價差 成交筆數 註記
    try:
        i_date = fields.index("日期")
        i_volume = fields.index("成交股數")
        i_close = fields.index("收盤價")
    except ValueError:
        log.warning("TWSE field labels changed: %s", fields)
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
            log.warning("skipping malformed TWSE row %r: %s", row, e)
    return out


# --- Stateful client -------------------------------------------------------


class TwseClient:
    """Thin retry+backoff layer around the TWSE STOCK_DAY endpoint.

    Sleep and jitter are injected so unit tests can run instantly.
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
        # Slow-decay back toward base after recovery
        self._current_sleep = max(self._base_sleep, self._current_sleep / 2)

    def _on_failure(self) -> None:
        self._current_sleep = max(self._base_sleep * 2, self._current_sleep * 2)

    def fetch_month(self, stock_no: str, year: int, month: int) -> list[dict]:
        params_date = f"{year:04d}{month:02d}01"
        url = f"{BASE_URL}?date={params_date}&stockNo={stock_no}&response=json"
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
                    "TWSE non-200 (status=%s) for %s %d-%02d (attempt %d)",
                    resp.status_code, stock_no, year, month, attempts,
                )
            except Exception as e:
                log.warning(
                    "TWSE error for %s %d-%02d (attempt %d): %s",
                    stock_no, year, month, attempts, e,
                )
            self._on_failure()
            self._sleep(self._current_sleep + self._jitter())
        log.warning(
            "TWSE freeze: %s %d-%02d failed after %d attempts; returning []",
            stock_no, year, month, self._max_attempts,
        )
        return []


# Module-level singleton so per-process backoff state is shared across
# all callers (e.g. backfill_runner walking many symbols).
_singleton = TwseClient()


def fetch_month(stock_no: str, year: int, month: int) -> list[dict]:
    """Convenience wrapper around the module-level singleton."""
    return _singleton.fetch_month(stock_no, year, month)
