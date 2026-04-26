"""Phase 11 — shioaji_client tests.

Hard requirements (per spec §11 + plan §3):
  1. No Order / order placement symbols ever imported.
  2. No CA-activation symbols ever imported.
  3. to_taipei_date() correctly localizes UTC → TPE.
  4. With credentials unset: app boots, INFO log written exactly once at
     startup, all endpoints return 200, no exception escapes.
  5. With credentials set: list_trades() returns [{date, code, side, qty,
     price, ccy, venue}] for the requested window. On session invalidation,
     reconnect-once-then-fail-quietly behavior holds.

These tests never hit the real Sinopac API — the shioaji module is
patched in via monkeypatch where present, and exercised in pure-Python
when not.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest


# --- Hard-requirement static checks (must run regardless of shioaji install) -

def test_no_order_symbols_imported_in_module_source():
    """Belt-and-suspenders: even if a future commit adds an `Order` import
    to app/shioaji_client.py, this test fires before it can ship."""
    src = (Path(__file__).resolve().parent.parent / "app" / "shioaji_client.py").read_text(
        encoding="utf-8"
    )
    # Mirror the spec's grep lines exactly.
    assert not re.search(
        r"from shioaji import .*Order|import shioaji.*\.Order|from shioaji\.order",
        src,
        re.IGNORECASE,
    ), "shioaji_client.py must not import Order symbols (read-only invariant)"


def test_no_order_or_ca_action_strings_in_module_source():
    src = (Path(__file__).resolve().parent.parent / "app" / "shioaji_client.py").read_text(
        encoding="utf-8"
    )
    forbidden = ("activate_ca", "place_order", "cancel_order", "update_order")
    for pat in forbidden:
        assert pat not in src, (
            f"shioaji_client.py must not reference {pat!r} (read-only invariant)"
        )


# --- to_taipei_date ---------------------------------------------------------

def test_to_taipei_date_localizes_utc_to_tpe():
    from app.shioaji_client import to_taipei_date

    # 2026-04-26 23:30 UTC → 2026-04-27 in TPE (UTC+8, no DST)
    utc_late = datetime(2026, 4, 26, 23, 30, tzinfo=timezone.utc)
    assert to_taipei_date(utc_late) == "2026-04-27"

    # 2026-04-26 00:00 UTC → still 2026-04-26 in TPE (08:00 local)
    utc_early = datetime(2026, 4, 26, 0, 0, tzinfo=timezone.utc)
    assert to_taipei_date(utc_early) == "2026-04-26"


def test_to_taipei_date_naive_datetime_treated_as_utc():
    from app.shioaji_client import to_taipei_date

    naive = datetime(2026, 4, 26, 23, 30)  # no tzinfo
    assert to_taipei_date(naive) == "2026-04-27"


# --- No-credentials boot path ----------------------------------------------

def test_client_unconfigured_when_creds_missing(monkeypatch):
    monkeypatch.delenv("SINOPAC_API_KEY", raising=False)
    monkeypatch.delenv("SINOPAC_SECRET_KEY", raising=False)

    from app.shioaji_client import ShioajiClient

    c = ShioajiClient()
    assert c.configured is False
    assert c.lazy_login() is False
    assert c.list_trades("2026-04-01", "2026-04-26") == []


def test_no_creds_logs_disabled_message_exactly_once(monkeypatch, caplog):
    monkeypatch.delenv("SINOPAC_API_KEY", raising=False)
    monkeypatch.delenv("SINOPAC_SECRET_KEY", raising=False)

    from app.shioaji_client import ShioajiClient

    caplog.set_level(logging.INFO, logger="app.shioaji_client")
    c = ShioajiClient()

    # Three calls — only one log line should appear.
    c.lazy_login()
    c.lazy_login()
    c.list_trades("2026-04-01", "2026-04-26")

    matches = [r for r in caplog.records if "credentials not configured" in r.message]
    assert len(matches) == 1
    assert matches[0].levelno == logging.INFO


def test_app_boots_without_creds(tmp_path, monkeypatch, empty_portfolio_json):
    """Phase 0 contract: dashboard fully functional without Shioaji creds."""
    monkeypatch.delenv("SINOPAC_API_KEY", raising=False)
    monkeypatch.delenv("SINOPAC_SECRET_KEY", raising=False)
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "phase11.db"))
    monkeypatch.delenv("BACKFILL_ON_STARTUP", raising=False)

    from app import create_app

    app = create_app(empty_portfolio_json)
    client = app.test_client()
    r = client.get("/api/health")
    assert r.status_code == 200
    assert r.get_json()["ok"] is True


# --- Credentialed path (with mocked shioaji session) -----------------------


class _FakeAPI:
    """Stand-in for `shioaji.Shioaji()` exposing only the read endpoints
    we use. Mirrors the shape we expect from the real SDK.
    """

    def __init__(self, trades=None, raise_on_first_call=False):
        self._trades = trades or []
        self.login_calls = 0
        self.list_trade_calls = 0
        self._raise_once = raise_on_first_call

    def login(self, api_key, secret_key):
        self.login_calls += 1

    def logout(self):
        pass

    def list_trades(self, start_date, end_date):
        self.list_trade_calls += 1
        if self._raise_once:
            self._raise_once = False
            raise RuntimeError("Session expired")
        return [
            t for t in self._trades
            if start_date <= t["ts"][:10] <= end_date
        ]


@pytest.fixture()
def fake_trades():
    """Two TW trades inside an arbitrary gap window, one outside."""
    return [
        {"ts": "2026-04-22T01:30:00Z", "code": "2330",
         "side": "Buy", "qty": 1000, "price": 920.0, "ccy": "TWD"},
        {"ts": "2026-04-23T05:10:00Z", "code": "0050",
         "side": "Sell", "qty": 2000, "price": 195.0, "ccy": "TWD"},
        {"ts": "2026-03-15T01:30:00Z", "code": "2330",
         "side": "Buy", "qty": 1000, "price": 880.0, "ccy": "TWD"},
    ]


def test_list_trades_returns_normalized_records_when_configured(monkeypatch, fake_trades):
    """With credentials + a working session, list_trades() should return
    the project-standard record shape: {date, code, side, qty, price, ccy,
    venue}."""
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    fake_api = _FakeAPI(trades=fake_trades)

    # Patch the shioaji module the client uses with one that returns fake_api
    import app.shioaji_client as mod

    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake_api)

    c = mod.ShioajiClient()
    out = c.list_trades("2026-04-01", "2026-04-30")

    assert fake_api.login_calls == 1
    assert {t["code"] for t in out} == {"2330", "0050"}
    assert all(t["venue"] == "TW" for t in out)
    assert all("date" in t and re.match(r"\d{4}-\d{2}-\d{2}", t["date"]) for t in out)
    # Side normalization: Buy → 普買, Sell → 普賣 (project convention)
    sides = {(t["code"], t["side"]) for t in out}
    assert ("2330", "普買") in sides
    assert ("0050", "普賣") in sides


def test_list_trades_reconnects_on_session_failure(monkeypatch, fake_trades):
    """First call raises (simulated session expired). Client should
    reconnect-once and return the data on the second attempt."""
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    fake_api = _FakeAPI(trades=fake_trades, raise_on_first_call=True)

    import app.shioaji_client as mod

    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake_api)

    c = mod.ShioajiClient()
    out = c.list_trades("2026-04-01", "2026-04-30")

    assert fake_api.login_calls == 2  # initial + reconnect
    assert fake_api.list_trade_calls == 2
    assert len(out) == 2  # data delivered post-reconnect


def test_list_trades_returns_empty_when_both_attempts_fail(monkeypatch):
    """Two consecutive failures → return [] without raising. Data layer
    must never be the reason the dashboard crashes."""
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    class _AlwaysFails:
        def __init__(self):
            self.login_calls = 0
            self.list_trade_calls = 0

        def login(self, api_key, secret_key):
            self.login_calls += 1

        def list_trades(self, start_date, end_date):
            self.list_trade_calls += 1
            raise RuntimeError("network down")

    fake = _AlwaysFails()
    import app.shioaji_client as mod

    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    c = mod.ShioajiClient()
    out = c.list_trades("2026-04-01", "2026-04-30")

    assert out == []
    assert fake.login_calls == 2
    assert fake.list_trade_calls == 2
