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

    # The test pins the once-per-lifetime invariant, not exact wording.
    # All disabled-reason branches share "trade overlay disabled" — match
    # that so log-message phrasing can evolve (e.g. distinguishing
    # "credentials not set" from "package not installed") without
    # rewriting this test.
    matches = [r for r in caplog.records if "trade overlay disabled" in r.message]
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

    def list_trades(self):
        # Mirrors shioaji 1.3.x: no args, returns the current session's
        # trades. Date-range filtering happens in the client, not here.
        self.list_trade_calls += 1
        if self._raise_once:
            self._raise_once = False
            raise RuntimeError("Session expired")
        return list(self._trades)


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


class _FakeContract:
    def __init__(self, code, currency="TWD"):
        self.code = code
        self.currency = currency


class _FakeAction:
    def __init__(self, value):
        self.value = value


class _FakeOrder:
    def __init__(self, action_value):
        self.action = _FakeAction(action_value)


class _FakeDeal:
    def __init__(self, ts, qty, price):
        self.ts = ts
        self.quantity = qty
        self.price = price


class _FakeStatus:
    def __init__(self, deals):
        self.deals = deals


class _FakeTrade:
    def __init__(self, code, action_value, deals, currency="TWD"):
        self.contract = _FakeContract(code, currency)
        self.order = _FakeOrder(action_value)
        self.status = _FakeStatus(deals)


def test_list_trades_extracts_fills_from_real_sdk_trade_shape(monkeypatch):
    """Pin the shioaji 1.3.x adapter: list_trades() returns Trade objects
    with .contract, .order.action.value, and .status.deals[]. Each Deal
    has (ts: float epoch, quantity, price). One Trade with two partial
    fills must emit two records.
    """
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    # 2026-04-26 01:30 UTC = 09:30 TPE — well inside business hours
    ts1 = datetime(2026, 4, 26, 1, 30, tzinfo=timezone.utc).timestamp()
    ts2 = datetime(2026, 4, 26, 5, 10, tzinfo=timezone.utc).timestamp()

    trades = [
        _FakeTrade("2330", "Buy", [
            _FakeDeal(ts1, qty=500, price=920.0),
            _FakeDeal(ts2, qty=500, price=921.5),  # 2nd partial fill
        ]),
        # Out-of-window trade — must be filtered out client-side
        _FakeTrade("0050", "Sell", [
            _FakeDeal(
                datetime(2025, 12, 1, 1, 0, tzinfo=timezone.utc).timestamp(),
                qty=2000, price=180.0,
            ),
        ]),
    ]

    class _SDKShapeAPI:
        def __init__(self):
            self.login_calls = 0

        def login(self, api_key, secret_key):
            self.login_calls += 1

        def list_trades(self):
            return trades

    fake = _SDKShapeAPI()
    import app.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    c = mod.ShioajiClient()
    out = c.list_trades("2026-04-26", "2026-04-26")

    # Both partial fills land as separate records; out-of-window dropped.
    assert len(out) == 2
    assert all(r["code"] == "2330" for r in out)
    assert all(r["side"] == "普買" for r in out)
    assert all(r["date"] == "2026-04-26" for r in out)
    qtys_prices = sorted((r["qty"], r["price"]) for r in out)
    assert qtys_prices == [(500.0, 920.0), (500.0, 921.5)]


class _FakeLot:
    """Stand-in for shioaji's StockPositionDetail.

    Per the Phase 0 probe (PLAN-shioaji-historical-trades.md §"Confirmed
    data model"):
      - lot.price      = TOTAL NTD cost for this lot (NOT per-share)
      - lot.last_price = TOTAL NTD market value for this lot
      - lot.quantity   = ALWAYS 0 (SDK quirk for 零股 / odd-lot)
      - lot.cond       = StockOrderCond-shaped (.value attr)
      - lot.direction  = Action enum (.value attr)
    """

    def __init__(
        self,
        date: str,
        code: str,
        cost_total: float,
        mv_total: float,
        cond_value: str = "Cash",
        direction_value: str = "Buy",
        currency: str = "TWD",
    ) -> None:
        self.date = date
        self.code = code
        self.price = cost_total            # SDK quirk: total, not per-share
        self.last_price = mv_total          # SDK quirk: total, not per-share
        self.quantity = 0                   # SDK quirk: always 0
        self.cond = _FakeAction(cond_value)
        self.direction = _FakeAction(direction_value)
        self.currency = _FakeAction(currency) if currency else None
        self.dseq = ""
        self.pnl = mv_total - cost_total
        self.fee = 0
        self.ex_dividends = 0
        self.interest = 0
        self.margintrading_amt = 0
        self.collateral = 0


def _stub_close_resolver(closes: dict[tuple[str, str], float]):
    """Build a (code, date) → close lookup for tests."""
    def _resolve(code: str, iso_date: str) -> float | None:
        return closes.get((code, iso_date))
    return _resolve


# --- list_open_lots() ------------------------------------------------------


def test_list_open_lots_returns_normalized_records_with_derived_qty(monkeypatch):
    """One lot of 2330 with cost=920_000 NTD, MV=1_050_000 NTD on 2026-04-22.
    Close on the entry date is 880.0/share (per portfolio entry), so derived
    qty = round(1_050_000 / 1050) = 1000. (Using last_price/today_close per
    plan §Path A.) Tests that list_open_lots emits the project record shape
    with qty derived via the injected resolver.
    """
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    lots = [
        _FakeLot(
            date="2025-11-15", code="2330",
            cost_total=920_000.0, mv_total=1_050_000.0,
            cond_value="Cash",
        ),
    ]

    class _LotsAPI:
        def __init__(self):
            self.login_calls = 0
            self.stock_account = object()  # opaque sentinel

        def login(self, api_key, secret_key):
            self.login_calls += 1

        def list_position_detail(self, account):
            assert account is self.stock_account
            return list(lots)

    fake = _LotsAPI()
    import app.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    # Resolver returns close=1050 for (2330, 2025-11-15) — odd-lot pattern:
    # MV = 1_050_000, close = 1050 → qty = 1000 shares.
    resolver = _stub_close_resolver({("2330", "2025-11-15"): 1050.0})

    c = mod.ShioajiClient()
    out = c.list_open_lots(close_resolver=resolver)

    assert len(out) == 1
    rec = out[0]
    assert rec["date"] == "2025-11-15"
    assert rec["code"] == "2330"
    assert rec["qty"] == 1000.0           # derived from MV/close
    assert rec["cost_twd"] == 920_000.0   # total NTD, passthrough
    assert rec["mv_twd"] == 1_050_000.0   # total NTD, passthrough
    assert rec["type"] == "現股"          # Cash → 現股
    assert rec["ccy"] == "TWD"
    assert rec["venue"] == "TW"


def test_list_open_lots_maps_cond_to_type(monkeypatch):
    """Cash → 現股, MarginTrading → 融資, ShortSelling → 融券.

    Mirrors the PDF parser's holdings_detail.type convention so the overlay
    feeds analytics layers that already handle 融資 cost-asymmetry.
    """
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    lots = [
        _FakeLot("2025-11-01", "2330", 920_000, 1_050_000, cond_value="Cash"),
        _FakeLot("2025-12-05", "00981A", 50_000, 60_000, cond_value="MarginTrading"),
        _FakeLot("2026-01-10", "2317", 30_000, 28_000, cond_value="ShortSelling"),
    ]

    class _LotsAPI:
        def __init__(self):
            self.stock_account = object()

        def login(self, api_key, secret_key):
            pass

        def list_position_detail(self, account):
            return list(lots)

    fake = _LotsAPI()
    import app.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    resolver = _stub_close_resolver({
        ("2330", "2025-11-01"): 1050.0,    # 1000 shares
        ("00981A", "2025-12-05"): 30.0,     # 2000 shares
        ("2317", "2026-01-10"): 70.0,       # 400 shares
    })

    c = mod.ShioajiClient()
    out = c.list_open_lots(close_resolver=resolver)

    types_by_code = {r["code"]: r["type"] for r in out}
    assert types_by_code == {"2330": "現股", "00981A": "融資", "2317": "融券"}


def test_list_open_lots_skips_lot_when_close_unavailable(monkeypatch, caplog):
    """If the resolver returns None for a lot's entry date (e.g., yfinance
    gap or pre-history symbol), skip the lot rather than emit qty=None.
    The merge layer can't do anything useful with qty=None, and silent
    filtering is safer than partial data. A warning is logged for triage.
    """
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    lots = [
        _FakeLot("2025-11-01", "2330", 920_000, 1_050_000),
        _FakeLot("1999-01-01", "OBSCURE", 1000, 1100),  # close not available
    ]

    class _LotsAPI:
        def __init__(self):
            self.stock_account = object()

        def login(self, api_key, secret_key):
            pass

        def list_position_detail(self, account):
            return list(lots)

    fake = _LotsAPI()
    import app.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    resolver = _stub_close_resolver({("2330", "2025-11-01"): 1050.0})

    caplog.set_level(logging.WARNING, logger="app.shioaji_client")
    c = mod.ShioajiClient()
    out = c.list_open_lots(close_resolver=resolver)

    codes = [r["code"] for r in out]
    assert codes == ["2330"]
    # Warning fired for the skipped lot
    skipped_msgs = [r for r in caplog.records if "OBSCURE" in r.message]
    assert len(skipped_msgs) == 1


def test_list_open_lots_returns_empty_when_unconfigured(monkeypatch):
    """Same no-creds invariant as list_trades: clean no-op."""
    monkeypatch.delenv("SINOPAC_API_KEY", raising=False)
    monkeypatch.delenv("SINOPAC_SECRET_KEY", raising=False)

    from app.shioaji_client import ShioajiClient

    c = ShioajiClient()
    assert c.list_open_lots(close_resolver=lambda *_: 100.0) == []


def test_list_open_lots_returns_empty_after_double_failure(monkeypatch):
    """Same reconnect-once-then-fail-quietly behavior as list_trades."""
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    class _AlwaysFails:
        def __init__(self):
            self.login_calls = 0
            self.list_calls = 0
            self.stock_account = object()

        def login(self, api_key, secret_key):
            self.login_calls += 1

        def list_position_detail(self, account):
            self.list_calls += 1
            raise RuntimeError("boom")

    fake = _AlwaysFails()
    import app.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    c = mod.ShioajiClient()
    out = c.list_open_lots(close_resolver=lambda *_: 100.0)

    assert out == []
    assert fake.login_calls == 2
    assert fake.list_calls == 2


class _FakePnL:
    """StockProfitLoss-shaped: closed-pair summary row.

    Per the Phase 0 probe (plan §"Confirmed data model"):
      - pl.date     = SELL date (close date of the pair)
      - pl.price    = SELL price per share
      - pl.pnl      = realized P&L (NTD)
      - pl.cond     = Cash | MarginTrading | ShortSelling enum-shaped
      - pl.quantity = ALWAYS 0 (do not trust)
    """

    def __init__(self, id, code, sell_date, sell_price, pnl, cond_value="Cash"):
        self.id = id
        self.code = code
        self.date = sell_date
        self.price = sell_price       # per-share NTD
        self.pnl = pnl
        self.cond = _FakeAction(cond_value)
        self.quantity = 0
        self.seqno = ""
        self.dseq = ""
        self.pr_ratio = 0


class _FakePnLDetail:
    """StockProfitDetail-shaped: one BUY-leg tranche.

    Per the probe: detail rows are buy legs only. trade_type does NOT
    distinguish buy/sell — every row IS a buy leg.
    """

    def __init__(self, buy_date, code, cost_total, price_per_share, cond_value="Cash"):
        self.date = buy_date
        self.code = code
        self.cost = cost_total          # total NTD per leg
        self.price = price_per_share    # per-share NTD
        self.quantity = 0               # SDK quirk: always 0
        self.fee = 0
        self.tax = 0
        self.cond = _FakeAction(cond_value)
        self.dseq = ""
        self.trade_type = _FakeAction("Common")
        self.currency = _FakeAction("TWD")
        self.rep_margintrading_amt = 0
        self.rep_collateral = 0
        self.rep_margin = 0
        self.shortselling_fee = 0
        self.ex_dividend_amt = 0
        self.interest = 0


def _build_pnl_api(pairs_with_legs: dict, raise_on_first_pl_call=False):
    """Helper: build a fake API where list_profit_loss returns the
    summary rows and list_profit_loss_detail returns legs keyed by id.

    pairs_with_legs: {(pl_summary, [legs])} list mapped onto API methods.
    """
    summaries = [pl for pl, _ in pairs_with_legs]
    leg_table = {pl.id: legs for pl, legs in pairs_with_legs}

    state = {"pl_calls": 0}

    class _API:
        def __init__(self):
            self.stock_account = object()
            self.login_calls = 0
            self.detail_calls = []

        def login(self, api_key, secret_key):
            self.login_calls += 1

        def list_profit_loss(self, account, begin_date, end_date):
            assert account is self.stock_account
            state["pl_calls"] += 1
            if raise_on_first_pl_call and state["pl_calls"] == 1:
                raise RuntimeError("session expired")
            # Filter SUMMARY rows by sell date inside [begin..end]
            return [
                pl for pl in summaries if begin_date <= pl.date <= end_date
            ]

        def list_profit_loss_detail(self, account, detail_id):
            assert account is self.stock_account
            self.detail_calls.append(detail_id)
            return list(leg_table.get(detail_id, []))

    return _API()


def test_list_realized_pairs_emits_buy_legs_and_sell_summary(monkeypatch):
    """The 7769-pattern probe case: one closed pair with 5 buy legs spanning
    2025-11 → 2026-02, sold on 2026-04-15. Emits 5 buy-leg records (each
    with qty=cost/price) plus 1 sell-summary record (qty = sum of legs).
    Buy legs may pre-date begin_date — decision #1 option C.
    """
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    pl = _FakePnL(id=101, code="7769", sell_date="2026-04-15",
                  sell_price=210.0, pnl=12_345.0, cond_value="Cash")
    legs = [
        # Five tranches, intentionally crossing the gap_start boundary
        # (gap_start would be 2026-04-01). Decision C says all legs
        # are emitted regardless of gap window.
        _FakePnLDetail("2025-11-15", "7769", cost_total=200_000, price_per_share=200.0),
        _FakePnLDetail("2025-12-03", "7769", cost_total=205_000, price_per_share=205.0),
        _FakePnLDetail("2026-01-12", "7769", cost_total=198_000, price_per_share=198.0),
        _FakePnLDetail("2026-02-08", "7769", cost_total=212_000, price_per_share=212.0),
        _FakePnLDetail("2026-02-20", "7769", cost_total=2_080,    price_per_share=208.0),
        # ↑ odd-lot leg: cost=2080, price=208 → qty=10 shares
    ]

    fake = _build_pnl_api([(pl, legs)])
    import app.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    c = mod.ShioajiClient()
    out = c.list_realized_pairs("2026-04-01", "2026-04-30")

    # 5 buy legs + 1 sell summary
    assert len(out) == 6

    buys = [r for r in out if r["side"] == "普買"]
    sells = [r for r in out if r["side"] == "普賣"]
    assert len(buys) == 5
    assert len(sells) == 1

    # Buy-leg qty derivation: round(cost/price)
    qty_by_date = {b["date"]: b["qty"] for b in buys}
    assert qty_by_date["2025-11-15"] == 1000.0   # 200000 / 200
    assert qty_by_date["2025-12-03"] == 1000.0   # 205000 / 205
    assert qty_by_date["2026-01-12"] == 1000.0   # 198000 / 198
    assert qty_by_date["2026-02-08"] == 1000.0   # 212000 / 212
    assert qty_by_date["2026-02-20"] == 10.0     # 2080 / 208 — odd lot

    # Sell summary qty = sum of buy-leg qtys
    assert sells[0]["qty"] == 4010.0
    assert sells[0]["date"] == "2026-04-15"
    assert sells[0]["code"] == "7769"
    assert sells[0]["price"] == 210.0
    assert sells[0]["pnl"] == 12_345.0

    # All records carry the pair_id for audit-hook linkage
    assert all(r["pair_id"] == 101 for r in out)
    assert all(r["ccy"] == "TWD" for r in out)
    assert all(r["venue"] == "TW" for r in out)
    assert all(r["type"] == "現股" for r in out)


def test_list_realized_pairs_filters_summary_by_window_not_legs(monkeypatch):
    """Two pairs: one with sell_date IN window, one OUT. Only the IN pair's
    legs (and summary) appear. begin_date/end_date filter SELLs, not BUYs.
    """
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    pl_in = _FakePnL(id=200, code="6442", sell_date="2026-04-20",
                     sell_price=145.0, pnl=500.0)
    pl_out = _FakePnL(id=201, code="2330", sell_date="2026-03-10",
                      sell_price=900.0, pnl=10_000.0)

    legs_in = [_FakePnLDetail("2026-04-15", "6442", 144_000, 144.0)]
    legs_out = [_FakePnLDetail("2025-09-01", "2330", 800_000, 800.0)]

    fake = _build_pnl_api([(pl_in, legs_in), (pl_out, legs_out)])
    import app.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    c = mod.ShioajiClient()
    out = c.list_realized_pairs("2026-04-01", "2026-04-30")

    codes = {r["code"] for r in out}
    assert codes == {"6442"}
    assert fake.detail_calls == [200]  # only the in-window pair was drilled


def test_list_realized_pairs_marks_degenerate_pair_with_empty_legs(monkeypatch):
    """If list_profit_loss_detail(id) returns 0 rows (rate-limit / partial
    response), the sell summary still emits — qty=0 — so the merge layer
    can fire the C-fallback reconcile event. Buy legs simply absent.

    Plan §Path A "C fallback (degenerate case)": "the overlay emits a
    reconcile event 'N broker pairs deferred'" — the trigger is qty=0 on
    a sell record, signalling 'pair acknowledged but legs unrecoverable'.
    """
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    pl = _FakePnL(id=300, code="2317", sell_date="2026-04-22",
                  sell_price=120.0, pnl=1_000.0)
    fake = _build_pnl_api([(pl, [])])  # empty legs — degenerate

    import app.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    c = mod.ShioajiClient()
    out = c.list_realized_pairs("2026-04-01", "2026-04-30")

    # Sell summary present; no buy legs
    sells = [r for r in out if r["side"] == "普賣"]
    buys = [r for r in out if r["side"] == "普買"]
    assert len(sells) == 1
    assert sells[0]["qty"] == 0.0   # signals C-fallback to merge layer
    assert sells[0]["pair_id"] == 300
    assert buys == []


def test_list_realized_pairs_maps_margin_trading_to_yong_zi(monkeypatch):
    """A 融資 (MarginTrading) closed pair: type='融資' on both legs and summary."""
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    pl = _FakePnL(id=400, code="00981A", sell_date="2026-04-25",
                  sell_price=32.0, pnl=200.0, cond_value="MarginTrading")
    legs = [_FakePnLDetail("2026-04-10", "00981A", 30_000, 30.0,
                           cond_value="MarginTrading")]
    fake = _build_pnl_api([(pl, legs)])

    import app.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    c = mod.ShioajiClient()
    out = c.list_realized_pairs("2026-04-01", "2026-04-30")

    assert all(r["type"] == "融資" for r in out)


def test_list_realized_pairs_returns_empty_when_unconfigured(monkeypatch):
    monkeypatch.delenv("SINOPAC_API_KEY", raising=False)
    monkeypatch.delenv("SINOPAC_SECRET_KEY", raising=False)

    from app.shioaji_client import ShioajiClient
    c = ShioajiClient()
    assert c.list_realized_pairs("2026-04-01", "2026-04-30") == []


def test_list_realized_pairs_reconnects_on_session_failure(monkeypatch):
    """First list_profit_loss raises → reconnect once → second call succeeds."""
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    pl = _FakePnL(id=500, code="2330", sell_date="2026-04-22",
                  sell_price=920.0, pnl=40_000.0)
    legs = [_FakePnLDetail("2026-04-10", "2330", 880_000, 880.0)]

    fake = _build_pnl_api([(pl, legs)], raise_on_first_pl_call=True)
    import app.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    c = mod.ShioajiClient()
    out = c.list_realized_pairs("2026-04-01", "2026-04-30")

    assert fake.login_calls == 2
    assert len(out) == 2  # 1 leg + 1 summary, post-reconnect


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

        def list_trades(self):
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
