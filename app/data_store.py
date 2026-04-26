"""Single source of truth for portfolio data.

Loads `data/portfolio.json` once, watches mtime, and re-reads if the parser
overwrites it. Everything in the dashboard reads from this — never from disk
directly — so analytics stay consistent across requests.
"""
from __future__ import annotations

import json
from pathlib import Path
from threading import Lock
from typing import Any


class DataStore:
    def __init__(self, path: Path):
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
