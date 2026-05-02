"""JSON-backed monthly aggregate store — bridge from legacy portfolio.json.

Phase 6.5 prereq for Phase 9 cutover. Ports app/data_store.py:DataStore
verbatim with re-pointed imports. The shape of `data/portfolio.json`
is the parsed-PDF aggregate produced by scripts/parse_statements.py;
the same file is consumed today by the legacy Flask app on :8000 and
by the new FastAPI app on :8001 during the transition window.

Why JSON-backed in a Shioaji-canonical backend:
  PLAN §4 demotes PDFs to seeder + verifier roles, but they remain
  the source for pre-Shioaji history. Per the locked 7d probe outcome
  (PLAN §3), foreign trades stay PDF-canonical indefinitely (broker
  enrollment missing). portfolio.json is the parsed-PDF aggregate;
  PortfolioStore is the in-process bridge until the Trade-table
  aggregator lands (Phase 10+). The HTTP layer consumes month dicts
  either way — swapping the source-of-truth is a future concern that
  doesn't affect router code.

Why mtime-driven reload:
  scripts/parse_statements.py overwrites data/portfolio.json while
  the FastAPI process is running. Without reload-on-mtime, the
  /api/* endpoints serve stale data forever. The legacy DataStore
  was explicit about this; same invariant ports verbatim.

Threading note:
  The lock-protected reload is the only mutation point. Property
  reads call _maybe_reload() which is the lock owner; the lock is
  short-held (one stat() + one json.load()). No reader contention
  in practice — FastAPI's per-request session model means concurrent
  /api/* calls share the same store but each completes its own
  property fetch independently.
"""
from __future__ import annotations

import json
from pathlib import Path
from threading import Lock
from typing import Any


class PortfolioStore:
    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)
        self._lock = Lock()
        self._mtime: float | None = None
        self._raw: dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            self._raw = {"months": [], "summary": {}}
            self._mtime = None
            return
        with self._path.open("r", encoding="utf-8") as fh:
            self._raw = json.load(fh)
        self._mtime = self._path.stat().st_mtime

    def _maybe_reload(self) -> None:
        if not self._path.exists():
            return
        mtime = self._path.stat().st_mtime
        if mtime != self._mtime:
            with self._lock:
                # Double-checked under the lock — another thread may
                # have reloaded between the outer check and the lock
                # acquisition.
                if self._path.stat().st_mtime != self._mtime:
                    self._load()

    @property
    def raw(self) -> dict[str, Any]:
        self._maybe_reload()
        return self._raw

    @property
    def months(self) -> list[dict[str, Any]]:
        return self.raw.get("months", [])

    @property
    def summary(self) -> dict[str, Any]:
        return self.raw.get("summary", {})

    @property
    def kpis(self) -> dict[str, Any]:
        return self.summary.get("kpis", {})

    @property
    def by_ticker(self) -> dict[str, Any]:
        return self.summary.get("by_ticker", {})

    @property
    def all_trades(self) -> list[dict[str, Any]]:
        return self.summary.get("all_trades", [])

    @property
    def cumulative_flows(self) -> dict[str, Any]:
        return self.summary.get("cumulative_flows", {})

    @property
    def dividends(self) -> list[dict[str, Any]]:
        return self.summary.get("dividends", [])

    @property
    def venue_flows_twd(self) -> list[dict[str, Any]]:
        return self.summary.get("venue_flows_twd", [])

    @property
    def holdings_total_return(self) -> list[dict[str, Any]]:
        return self.summary.get("holdings_total_return", [])

    @property
    def latest_month(self) -> dict[str, Any]:
        m = self.months
        return m[-1] if m else {}

    @property
    def as_of(self) -> str | None:
        return self.kpis.get("as_of")
