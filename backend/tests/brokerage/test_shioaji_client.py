"""Phase 5 Cycle 36 — invest.brokerage.shioaji_client.

Pins the read-only client contract for the new modular layout.
Same invariants as the legacy app/shioaji_client.py:

  * Static-grep guard — no Order/CA/place/cancel/update_order/activate_ca
    symbols ever land in this file. Read-only forever.
  * No creds → empty everywhere; one INFO log per process lifetime.
  * Three surfaces (list_trades / list_open_lots / list_realized_pairs)
    each with reconnect-once-then-fail-quietly semantics.
  * Qty derivation: open lots from MV/close (SDK quirk: lot.quantity=0
    for 零股); realized-pair buy legs from cost/price.
  * Degenerate pair (legs unrecoverable) → sell summary still emits
    with qty=0 so the merge layer can fire a reconcile event.
  * pair_id propagated on every record so downstream audit hooks
    can correlate buy legs to their parent realized pair.

Tests never touch the network. The shioaji module is monkeypatched.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest


# --- Static-grep invariants (run regardless of shioaji install) -----------

_MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "src" / "invest" / "brokerage" / "shioaji_client.py"
)


def test_no_order_symbols_imported_in_module_source():
    src = _MODULE_PATH.read_text(encoding="utf-8")
    assert not re.search(
        r"from shioaji import .*Order|import shioaji.*\.Order|from shioaji\.order",
        src,
        re.IGNORECASE,
    ), "shioaji_client.py must not import Order symbols (read-only invariant)"


def test_no_order_or_ca_action_strings_in_module_source():
    src = _MODULE_PATH.read_text(encoding="utf-8")
    forbidden = ("activate_ca", "place_order", "cancel_order", "update_order")
    for pat in forbidden:
        assert pat not in src, (
            f"shioaji_client.py must not reference {pat!r} (read-only invariant)"
        )


# --- to_taipei_date --------------------------------------------------------

def test_to_taipei_date_localizes_utc_to_tpe():
    from invest.brokerage.shioaji_client import to_taipei_date

    # 23:30 UTC on 2026-04-26 → 07:30 TPE on 2026-04-27
    utc_late = datetime(2026, 4, 26, 23, 30, tzinfo=timezone.utc)
    assert to_taipei_date(utc_late) == "2026-04-27"

    # Naive treated as UTC
    naive = datetime(2026, 4, 26, 23, 30)
    assert to_taipei_date(naive) == "2026-04-27"


# --- No-creds path ---------------------------------------------------------

def test_unconfigured_returns_empty_everywhere(monkeypatch):
    monkeypatch.delenv("SINOPAC_API_KEY", raising=False)
    monkeypatch.delenv("SINOPAC_SECRET_KEY", raising=False)

    from invest.brokerage.shioaji_client import ShioajiClient

    c = ShioajiClient()
    assert c.configured is False
    assert c.list_trades("2026-04-01", "2026-04-30") == []
    assert c.list_open_lots(close_resolver=lambda *_: 100.0) == []
    assert c.list_realized_pairs("2026-04-01", "2026-04-30") == []


def test_no_creds_logs_disabled_message_exactly_once(monkeypatch, caplog):
    monkeypatch.delenv("SINOPAC_API_KEY", raising=False)
    monkeypatch.delenv("SINOPAC_SECRET_KEY", raising=False)

    from invest.brokerage.shioaji_client import ShioajiClient

    caplog.set_level(logging.INFO, logger="invest.brokerage.shioaji_client")
    c = ShioajiClient()

    # Multiple calls, single disabled-line.
    c.list_trades("2026-04-01", "2026-04-30")
    c.list_open_lots()
    c.list_realized_pairs("2026-04-01", "2026-04-30")

    matches = [r for r in caplog.records if "trade overlay disabled" in r.message]
    assert len(matches) == 1
    assert matches[0].levelno == logging.INFO


# --- Fakes shared across surfaces -----------------------------------------


class _FakeAction:
    def __init__(self, value):
        self.value = value


class _FakeContract:
    def __init__(self, code, currency="TWD"):
        self.code = code
        self.currency = currency


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


class _FakeLot:
    """StockPositionDetail-shaped. Per the Phase 0 probe:
    lot.price = TOTAL NTD cost, lot.last_price = TOTAL NTD MV,
    lot.quantity ALWAYS 0 (SDK quirk for 零股)."""

    def __init__(
        self, date, code, cost_total, mv_total,
        cond_value="Cash", currency="TWD",
    ):
        self.date = date
        self.code = code
        self.price = cost_total
        self.last_price = mv_total
        self.quantity = 0
        self.cond = _FakeAction(cond_value)
        self.direction = _FakeAction("Buy")
        self.currency = _FakeAction(currency) if currency else None


class _FakePnL:
    def __init__(self, id, code, sell_date, sell_price, pnl, cond_value="Cash"):
        self.id = id
        self.code = code
        self.date = sell_date
        self.price = sell_price
        self.pnl = pnl
        self.cond = _FakeAction(cond_value)
        self.quantity = 0


class _FakePnLDetail:
    """One BUY-leg tranche. Per the probe: every detail row IS a buy leg."""

    def __init__(self, buy_date, code, cost_total, price_per_share, cond_value="Cash"):
        self.date = buy_date
        self.code = code
        self.cost = cost_total
        self.price = price_per_share
        self.quantity = 0
        self.cond = _FakeAction(cond_value)


# --- list_trades ----------------------------------------------------------


def test_list_trades_extracts_partial_fills_with_side_normalization(monkeypatch):
    """One Trade with two partial fills emits two records. Buy → 普買.
    Out-of-window deals filtered. Side normalization mirrors PDF parser
    convention so trade_overlay merge can compare structurally.
    """
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    ts1 = datetime(2026, 4, 26, 1, 30, tzinfo=timezone.utc).timestamp()
    ts2 = datetime(2026, 4, 26, 5, 10, tzinfo=timezone.utc).timestamp()
    ts_old = datetime(2025, 12, 1, 1, 0, tzinfo=timezone.utc).timestamp()

    trades = [
        _FakeTrade("2330", "Buy", [
            _FakeDeal(ts1, qty=500, price=920.0),
            _FakeDeal(ts2, qty=500, price=921.5),
        ]),
        _FakeTrade("0050", "Sell", [_FakeDeal(ts_old, qty=2000, price=180.0)]),
    ]

    class _API:
        def __init__(self):
            self.login_calls = 0

        def login(self, api_key, secret_key):
            self.login_calls += 1

        def list_trades(self):
            return trades

    fake = _API()
    import invest.brokerage.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    c = mod.ShioajiClient()
    out = c.list_trades("2026-04-26", "2026-04-26")

    assert len(out) == 2
    assert all(r["code"] == "2330" for r in out)
    assert all(r["side"] == "普買" for r in out)
    assert all(r["date"] == "2026-04-26" for r in out)
    assert all(r["venue"] == "TW" for r in out)
    qtys_prices = sorted((r["qty"], r["price"]) for r in out)
    assert qtys_prices == [(500.0, 920.0), (500.0, 921.5)]


def test_list_trades_reconnect_once_on_session_failure(monkeypatch):
    """First fetch raises, second succeeds. Login called twice.
    Data layer must never be the reason the dashboard crashes — but
    a single transient failure shouldn't lose the whole window either."""
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    ts1 = datetime(2026, 4, 26, 1, 30, tzinfo=timezone.utc).timestamp()
    trades = [_FakeTrade("2330", "Buy", [_FakeDeal(ts1, qty=1000, price=920.0)])]

    class _API:
        def __init__(self):
            self.login_calls = 0
            self.list_calls = 0

        def login(self, api_key, secret_key):
            self.login_calls += 1

        def list_trades(self):
            self.list_calls += 1
            if self.list_calls == 1:
                raise RuntimeError("session expired")
            return list(trades)

    fake = _API()
    import invest.brokerage.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    c = mod.ShioajiClient()
    out = c.list_trades("2026-04-01", "2026-04-30")

    assert fake.login_calls == 2
    assert fake.list_calls == 2
    assert len(out) == 1


# --- list_open_lots -------------------------------------------------------


def test_list_open_lots_derives_qty_from_mv_and_close(monkeypatch):
    """SDK quirk: lot.quantity is 0 for 零股. Derive qty = round(mv/close).
    Cond → type maps Cash→現股, MarginTrading→融資, ShortSelling→融券."""
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    lots = [
        _FakeLot("2025-11-01", "2330", 920_000, 1_050_000, cond_value="Cash"),
        _FakeLot("2025-12-05", "00981A", 50_000, 60_000, cond_value="MarginTrading"),
        _FakeLot("2026-01-10", "2317", 30_000, 28_000, cond_value="ShortSelling"),
    ]

    class _API:
        def __init__(self):
            self.stock_account = object()

        def login(self, api_key, secret_key):
            pass

        def list_position_detail(self, account):
            assert account is self.stock_account
            return list(lots)

    fake = _API()
    import invest.brokerage.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    closes = {
        ("2330", "2025-11-01"): 1050.0,    # 1000 shares
        ("00981A", "2025-12-05"): 30.0,     # 2000 shares
        ("2317", "2026-01-10"): 70.0,       # 400 shares
    }
    resolver = lambda code, d: closes.get((code, d))

    c = mod.ShioajiClient()
    out = c.list_open_lots(close_resolver=resolver)

    by_code = {r["code"]: r for r in out}
    assert by_code["2330"]["qty"] == 1000.0
    assert by_code["00981A"]["qty"] == 2000.0
    assert by_code["2317"]["qty"] == 400.0
    assert by_code["2330"]["type"] == "現股"
    assert by_code["00981A"]["type"] == "融資"
    assert by_code["2317"]["type"] == "融券"
    assert all(r["venue"] == "TW" for r in out)


def test_list_open_lots_skips_lot_when_close_unavailable(monkeypatch, caplog):
    """No close → can't derive qty → skip + log warning. Silent partial
    data is worse than 'we don't know'; merge layer can't do anything
    useful with qty=None."""
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    lots = [
        _FakeLot("2025-11-01", "2330", 920_000, 1_050_000),
        _FakeLot("1999-01-01", "OBSCURE", 1000, 1100),  # close not in resolver
    ]

    class _API:
        def __init__(self):
            self.stock_account = object()

        def login(self, api_key, secret_key):
            pass

        def list_position_detail(self, account):
            return list(lots)

    fake = _API()
    import invest.brokerage.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    resolver = lambda code, d: 1050.0 if (code, d) == ("2330", "2025-11-01") else None

    caplog.set_level(logging.WARNING, logger="invest.brokerage.shioaji_client")
    c = mod.ShioajiClient()
    out = c.list_open_lots(close_resolver=resolver)

    assert [r["code"] for r in out] == ["2330"]
    assert any("OBSCURE" in r.message for r in caplog.records)


# --- list_realized_pairs --------------------------------------------------


def _build_pnl_api(pairs_with_legs, raise_first=False):
    summaries = [pl for pl, _ in pairs_with_legs]
    leg_table = {pl.id: legs for pl, legs in pairs_with_legs}
    state = {"pl_calls": 0}

    class _API:
        def __init__(self):
            self.stock_account = object()
            self.login_calls = 0
            self.detail_ids = []

        def login(self, api_key, secret_key):
            self.login_calls += 1

        def list_profit_loss(self, account, begin_date, end_date):
            state["pl_calls"] += 1
            if raise_first and state["pl_calls"] == 1:
                raise RuntimeError("session expired")
            return [pl for pl in summaries if begin_date <= pl.date <= end_date]

        def list_profit_loss_detail(self, account, detail_id):
            self.detail_ids.append(detail_id)
            return list(leg_table.get(detail_id, []))

    return _API()


def test_list_realized_pairs_emits_buy_legs_plus_sell_summary(monkeypatch):
    """One closed pair with 5 legs → 5 buy + 1 sell records. Sell qty =
    sum of leg qtys. Per locked decision #1 option C, buy legs may
    pre-date begin_date (the window filters SELL dates only)."""
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    pl = _FakePnL(101, "7769", "2026-04-15", 210.0, 12_345.0, "Cash")
    legs = [
        _FakePnLDetail("2025-11-15", "7769", 200_000, 200.0),
        _FakePnLDetail("2025-12-03", "7769", 205_000, 205.0),
        _FakePnLDetail("2026-01-12", "7769", 198_000, 198.0),
        _FakePnLDetail("2026-02-08", "7769", 212_000, 212.0),
        _FakePnLDetail("2026-02-20", "7769", 2_080,    208.0),  # odd-lot 10 sh
    ]

    fake = _build_pnl_api([(pl, legs)])
    import invest.brokerage.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    c = mod.ShioajiClient()
    out = c.list_realized_pairs("2026-04-01", "2026-04-30")

    buys = [r for r in out if r["side"] == "普買"]
    sells = [r for r in out if r["side"] == "普賣"]
    assert len(buys) == 5
    assert len(sells) == 1

    qty_by_date = {b["date"]: b["qty"] for b in buys}
    assert qty_by_date["2025-11-15"] == 1000.0
    assert qty_by_date["2026-02-20"] == 10.0  # odd lot

    sell = sells[0]
    assert sell["qty"] == 4010.0
    assert sell["price"] == 210.0
    assert sell["pnl"] == 12_345.0
    assert all(r["pair_id"] == 101 for r in out)


def test_list_realized_pairs_degenerate_emits_qty_zero_sell(monkeypatch):
    """list_profit_loss_detail returns 0 rows (rate-limit / partial
    response). Sell summary still emits with qty=0 — the merge layer
    keys on qty=0 to fire the C-fallback reconcile event 'N broker
    pairs deferred'. Without this row the pair would disappear silently
    and the operator would never know the broker acknowledged it."""
    monkeypatch.setenv("SINOPAC_API_KEY", "k")
    monkeypatch.setenv("SINOPAC_SECRET_KEY", "s")

    pl = _FakePnL(300, "2317", "2026-04-22", 120.0, 1_000.0)
    fake = _build_pnl_api([(pl, [])])

    import invest.brokerage.shioaji_client as mod
    monkeypatch.setattr(mod, "_SHIOAJI_AVAILABLE", True)
    monkeypatch.setattr(mod, "_make_session", lambda: fake)

    c = mod.ShioajiClient()
    out = c.list_realized_pairs("2026-04-01", "2026-04-30")

    sells = [r for r in out if r["side"] == "普賣"]
    buys = [r for r in out if r["side"] == "普買"]
    assert len(sells) == 1
    assert sells[0]["qty"] == 0.0
    assert sells[0]["pair_id"] == 300
    assert buys == []
