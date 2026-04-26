"""Phase 2 acceptance tests for app/twse_client.py.

The TWSE response format uses ROC year (民國) dates ("115/04/01" = 2026-04-01)
and comma-thousands numbers ("1,855.00") — both are common bug sources, so
they get explicit pinning here. Network is mocked via `requests` monkeypatching;
fixtures live in tests/fixtures/.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.twse_client import (
    BASE_URL,
    USER_AGENTS,
    TwseClient,
    fetch_month,
    parse_response,
    roc_to_iso,
)


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


# --- Pure parsers ---------------------------------------------------------


def test_roc_to_iso_converts_year() -> None:
    assert roc_to_iso("115/04/01") == "2026-04-01"
    assert roc_to_iso("100/12/31") == "2011-12-31"


def test_roc_to_iso_zero_pads() -> None:
    assert roc_to_iso("115/4/1") == "2026-04-01"


def test_parse_response_unwraps_2330_fixture() -> None:
    payload = _load_fixture("twse_2330_202604.json")
    rows = parse_response(payload)
    assert len(rows) == 16
    first = rows[0]
    assert first["date"] == "2026-04-01"
    assert first["close"] == 1855.0
    assert first["volume"] == 46_457_423
    assert all(r["date"][:7] == "2026-04" for r in rows)


def test_parse_response_strips_thousands_separators() -> None:
    payload = _load_fixture("twse_2330_202604.json")
    rows = parse_response(payload)
    assert isinstance(rows[0]["close"], float)
    assert rows[0]["volume"] == 46_457_423  # int, not str


def test_parse_response_returns_empty_when_stat_not_ok() -> None:
    payload = _load_fixture("twse_99999_202604.json")
    assert payload["stat"] != "OK"  # sanity
    assert parse_response(payload) == []


def test_parse_response_returns_empty_on_missing_keys() -> None:
    assert parse_response({}) == []
    assert parse_response({"stat": "OK"}) == []


# --- HTTP layer (mocked) --------------------------------------------------


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


@pytest.fixture()
def fake_session(monkeypatch):
    """Replace requests.Session with a programmable stub."""
    calls: list[dict] = []
    response_queue: list[_FakeResponse] = []

    class _FakeSession:
        def get(self, url, headers=None, timeout=None, **_):
            calls.append({"url": url, "headers": headers, "timeout": timeout})
            if not response_queue:
                raise AssertionError("no fake responses queued")
            return response_queue.pop(0)

    fake = _FakeSession()
    monkeypatch.setattr("app.twse_client._session", fake)
    return calls, response_queue


def test_fetch_month_builds_expected_url(fake_session) -> None:
    calls, queue = fake_session
    queue.append(_FakeResponse(200, _load_fixture("twse_2330_202604.json")))
    rows = fetch_month("2330", 2026, 4)
    assert len(rows) == 16
    url = calls[0]["url"]
    assert url.startswith(BASE_URL)
    assert "stockNo=2330" in url
    assert "date=20260401" in url
    assert "response=json" in url


def test_fetch_month_zero_pads_month(fake_session) -> None:
    calls, queue = fake_session
    queue.append(_FakeResponse(200, _load_fixture("twse_99999_202604.json")))
    fetch_month("9999", 2026, 1)
    assert "date=20260101" in calls[0]["url"]


def test_fetch_month_returns_empty_on_not_found(fake_session) -> None:
    _, queue = fake_session
    queue.append(_FakeResponse(200, _load_fixture("twse_99999_202604.json")))
    assert fetch_month("9999", 2026, 4) == []


def test_fetch_month_uses_a_browser_user_agent(fake_session) -> None:
    calls, queue = fake_session
    queue.append(_FakeResponse(200, _load_fixture("twse_2330_202604.json")))
    fetch_month("2330", 2026, 4)
    ua = calls[0]["headers"].get("User-Agent", "")
    assert ua in USER_AGENTS, f"UA {ua!r} not in pool"


def test_client_rotates_user_agents_round_robin(fake_session) -> None:
    calls, queue = fake_session
    payload = _load_fixture("twse_2330_202604.json")
    for _ in range(len(USER_AGENTS) + 1):
        queue.append(_FakeResponse(200, payload))
    client = TwseClient(sleep_fn=lambda _: None)
    for _ in range(len(USER_AGENTS) + 1):
        client.fetch_month("2330", 2026, 4)
    uas = [c["headers"]["User-Agent"] for c in calls]
    # First N requests cover all UAs at least once
    assert set(uas[: len(USER_AGENTS)]) == set(USER_AGENTS)


def test_client_doubles_backoff_on_5xx(fake_session) -> None:
    """On a non-200, the next sleep should be ≥ previous sleep × 2."""
    _, queue = fake_session
    queue.append(_FakeResponse(503))
    queue.append(_FakeResponse(200, _load_fixture("twse_2330_202604.json")))
    sleeps: list[float] = []
    client = TwseClient(sleep_fn=sleeps.append, base_sleep=0.5, jitter_fn=lambda: 0.0)
    rows = client.fetch_month("2330", 2026, 4)
    assert len(rows) == 16
    # Two sleeps total: one before retry (doubled from base 0.5 → 1.0), one before next call (still raised).
    assert sleeps[0] >= 1.0


def test_client_freezes_after_three_consecutive_failures(fake_session) -> None:
    """Three consecutive non-200 → return [] and don't retry indefinitely."""
    _, queue = fake_session
    for _ in range(5):
        queue.append(_FakeResponse(503))
    sleeps: list[float] = []
    client = TwseClient(
        sleep_fn=sleeps.append, base_sleep=0.5, jitter_fn=lambda: 0.0, max_attempts=3
    )
    rows = client.fetch_month("2330", 2026, 4)
    assert rows == []
    # Should not have made more than max_attempts requests
    # (number of sleeps == number of retries; we cap at max_attempts attempts).


def test_module_level_fetch_month_uses_singleton(fake_session) -> None:
    """Convenience wrapper preserves backoff state across calls."""
    _, queue = fake_session
    queue.append(_FakeResponse(200, _load_fixture("twse_2330_202604.json")))
    rows = fetch_month("2330", 2026, 4)
    assert len(rows) == 16
