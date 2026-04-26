"""Phase 5 acceptance tests for app/tpex_client.py.

TPEX returns one calendar month of OTC daily prices via:
  https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock
      ?stkno=XXXX&date=YYYY/MM/01&id=&response=json

Format quirks (vs TWSE):
  - stat is lowercase "ok" (TWSE uses uppercase "OK")
  - Rows live under tables[0].data (TWSE has them at top-level data)
  - Field labels are the same Chinese tokens, but with a non-breaking space
    in "日 期" (TWSE uses "日期" without the inner space)
  - "no data" is signaled by tables[0].data = [] AND code = null
    (TWSE signals it via stat != "OK")

Both populated and empty responses are pinned via real-captured fixtures.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.tpex_client import (
    BASE_URL,
    TpexClient,
    fetch_month,
    parse_response,
)


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


# --- Pure parsers ---------------------------------------------------------


def test_parse_response_unwraps_populated_fixture() -> None:
    payload = _load_fixture("tpex_5483_202506.json")
    rows = parse_response(payload)
    assert len(rows) == 3
    assert rows[0]["date"] == "2025-06-03"
    assert rows[0]["close"] == 101.50
    # 成交張數 1,234 → 1234 (int)
    assert rows[0]["volume"] == 1234
    assert all(r["date"][:7] == "2025-06" for r in rows)


def test_parse_response_strips_thousands_separators() -> None:
    payload = _load_fixture("tpex_5483_202506.json")
    rows = parse_response(payload)
    assert isinstance(rows[0]["close"], float)
    assert isinstance(rows[0]["volume"], int)


def test_parse_response_empty_when_data_missing() -> None:
    """No rows + code=null is TPEX's "not on this exchange" signal."""
    payload = _load_fixture("tpex_99999_202506.json")
    assert parse_response(payload) == []


def test_parse_response_handles_lowercase_stat() -> None:
    """TPEX uses 'ok' lowercase; we accept either case."""
    payload = _load_fixture("tpex_5483_202506.json")
    assert payload["stat"] == "ok"  # sanity
    assert len(parse_response(payload)) == 3


def test_parse_response_handles_uppercase_stat() -> None:
    """If TPEX ever changes to uppercase, parsing should still work."""
    payload = _load_fixture("tpex_5483_202506.json")
    payload["stat"] = "OK"
    assert len(parse_response(payload)) == 3


def test_parse_response_returns_empty_on_error_stat() -> None:
    """When stat is anything other than ok/OK (e.g. 參數輸入錯誤), return []."""
    payload = _load_fixture("tpex_5483_202506.json")
    payload["stat"] = "參數輸入錯誤"
    assert parse_response(payload) == []


def test_parse_response_returns_empty_on_missing_keys() -> None:
    assert parse_response({}) == []
    assert parse_response({"stat": "ok"}) == []
    # Missing "tables" array entirely
    assert parse_response({"stat": "ok", "tables": []}) == []


def test_parse_response_handles_nbsp_in_field_label() -> None:
    """The TPEX field name '日 期' contains a regular space — confirm parser
    finds the date column anyway (not '日期' as TWSE uses)."""
    payload = _load_fixture("tpex_5483_202506.json")
    fields = payload["tables"][0]["fields"]
    assert "日 期" in fields
    assert "日期" not in fields
    # Must still parse correctly
    assert len(parse_response(payload)) == 3


# --- HTTP layer (mocked) --------------------------------------------------


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


@pytest.fixture()
def fake_session(monkeypatch):
    calls: list[dict] = []
    response_queue: list[_FakeResponse] = []

    class _FakeSession:
        def get(self, url, headers=None, timeout=None, **_):
            calls.append({"url": url, "headers": headers, "timeout": timeout})
            if not response_queue:
                raise AssertionError("no fake responses queued")
            return response_queue.pop(0)

    monkeypatch.setattr("app.tpex_client._session", _FakeSession())
    return calls, response_queue


def test_fetch_month_builds_expected_url(fake_session) -> None:
    calls, queue = fake_session
    queue.append(_FakeResponse(200, _load_fixture("tpex_5483_202506.json")))
    rows = fetch_month("5483", 2025, 6)
    assert len(rows) == 3
    url = calls[0]["url"]
    assert url.startswith(BASE_URL)
    assert "stkno=5483" in url
    # TPEX wants Gregorian YYYY/MM/01 — different from TWSE's YYYYMM01
    assert "date=2025/06/01" in url
    assert "response=json" in url


def test_fetch_month_zero_pads_month(fake_session) -> None:
    calls, queue = fake_session
    queue.append(_FakeResponse(200, _load_fixture("tpex_99999_202506.json")))
    fetch_month("9999", 2025, 1)
    assert "date=2025/01/01" in calls[0]["url"]


def test_fetch_month_returns_empty_when_symbol_not_on_tpex(fake_session) -> None:
    """The "not OTC" signal must surface as [] so the router falls through."""
    _, queue = fake_session
    queue.append(_FakeResponse(200, _load_fixture("tpex_99999_202506.json")))
    assert fetch_month("9999", 2025, 6) == []


def test_client_doubles_backoff_on_5xx(fake_session) -> None:
    _, queue = fake_session
    queue.append(_FakeResponse(503))
    queue.append(_FakeResponse(200, _load_fixture("tpex_5483_202506.json")))
    sleeps: list[float] = []
    client = TpexClient(
        sleep_fn=sleeps.append, base_sleep=0.5, jitter_fn=lambda: 0.0
    )
    rows = client.fetch_month("5483", 2025, 6)
    assert len(rows) == 3
    assert sleeps[0] >= 1.0


def test_client_freezes_after_max_attempts(fake_session) -> None:
    _, queue = fake_session
    for _ in range(5):
        queue.append(_FakeResponse(503))
    client = TpexClient(
        sleep_fn=lambda _: None,
        base_sleep=0.5,
        jitter_fn=lambda: 0.0,
        max_attempts=3,
    )
    assert client.fetch_month("5483", 2025, 6) == []


def test_module_level_fetch_month_uses_singleton(fake_session) -> None:
    _, queue = fake_session
    queue.append(_FakeResponse(200, _load_fixture("tpex_5483_202506.json")))
    rows = fetch_month("5483", 2025, 6)
    assert len(rows) == 3
