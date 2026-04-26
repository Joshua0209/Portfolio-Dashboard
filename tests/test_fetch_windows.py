"""Phase 3 acceptance tests for app/backfill_runner.compute_fetch_window().

These pin down the §6.1 per-symbol windowing rule end-to-end, covering all
five edge cases listed in the implementation plan §3 Phase 3:

  - ticker active throughout
  - ticker with first_trade before floor (clipped to floor)
  - ticker exited before floor (skipped)
  - ticker still held (fetch_end == today)
  - ticker sold and not re-bought (fetch_end == sale date)
"""
from __future__ import annotations

from app.backfill_runner import compute_fetch_window


FLOOR = "2025-08-01"
TODAY = "2026-04-27"
LATEST_DATA_MONTH = "2026-03"


def test_active_throughout() -> None:
    """Bought before floor, still held in latest month → clip start to floor,
    end to today."""
    w = compute_fetch_window(
        trade_dates=["2025-01-15", "2025-09-10"],
        held_months=["2025-08", "2025-09", "2025-10", "2025-11", "2025-12",
                     "2026-01", "2026-02", "2026-03"],
        latest_data_month=LATEST_DATA_MONTH,
        floor=FLOOR,
        today=TODAY,
    )
    assert w == (FLOOR, TODAY)


def test_first_trade_before_floor_clips_to_floor() -> None:
    """First trade in 2024-06, sold in 2025-09 — start clipped to floor,
    end at last_held_date or last_trade."""
    w = compute_fetch_window(
        trade_dates=["2024-06-01", "2025-09-15"],
        held_months=["2025-08", "2025-09"],
        latest_data_month=LATEST_DATA_MONTH,
        floor=FLOOR,
        today=TODAY,
    )
    assert w is not None
    start, end = w
    assert start == FLOOR
    # last_trade=2025-09-15, last_held month=2025-09 → month_end=2025-09-30
    assert end == "2025-09-30"


def test_exited_before_floor_is_skipped() -> None:
    """Ticker bought 2024-01, sold 2024-12, no holdings after that — entire
    active window precedes floor → return None (no fetch, no symbol_market row)."""
    w = compute_fetch_window(
        trade_dates=["2024-01-15", "2024-12-20"],
        held_months=["2024-01", "2024-02", "2024-12"],
        latest_data_month=LATEST_DATA_MONTH,
        floor=FLOOR,
        today=TODAY,
    )
    assert w is None


def test_still_held_fetch_end_is_today() -> None:
    """Symbol held in latest data month → fetch_end == today (not month-end)."""
    w = compute_fetch_window(
        trade_dates=["2025-08-20"],
        held_months=["2025-08", "2025-09", "2026-03"],
        latest_data_month=LATEST_DATA_MONTH,
        floor=FLOOR,
        today=TODAY,
    )
    assert w == ("2025-08-20", TODAY)


def test_sold_not_rebought_end_at_sale_date() -> None:
    """Bought after floor, sold before latest month, never reopened
    → fetch_end == max(last_trade, last_held_month_end)."""
    w = compute_fetch_window(
        trade_dates=["2025-09-01", "2025-12-15"],
        held_months=["2025-09", "2025-10", "2025-11", "2025-12"],
        latest_data_month=LATEST_DATA_MONTH,
        floor=FLOOR,
        today=TODAY,
    )
    assert w is not None
    start, end = w
    assert start == "2025-09-01"
    # last_trade=2025-12-15, last_held month=2025-12 → month_end=2025-12-31
    assert end == "2025-12-31"


def test_no_history_at_all_skips() -> None:
    """No trades, no holdings — should not be probed."""
    w = compute_fetch_window(
        trade_dates=[],
        held_months=[],
        latest_data_month=LATEST_DATA_MONTH,
        floor=FLOOR,
        today=TODAY,
    )
    assert w is None


def test_only_held_no_trades() -> None:
    """Edge case: held without trade-ledger entry (transferred-in position).
    Use month_end as both bounds."""
    w = compute_fetch_window(
        trade_dates=[],
        held_months=["2025-09", "2025-10"],
        latest_data_month=LATEST_DATA_MONTH,
        floor=FLOOR,
        today=TODAY,
    )
    assert w == (FLOOR, "2025-10-31")


def test_buy_sell_rebuy_window_includes_full_span() -> None:
    """Per §3 Phase 3 risks: bought 2024 (clipped), sold 2025-06,
    re-bought 2025-09. v1 fetches the full clipped window — the gap
    between 2025-06 and 2025-09 is acceptable cost."""
    w = compute_fetch_window(
        trade_dates=["2024-03-01", "2025-06-15", "2025-09-10"],
        held_months=["2024-03", "2025-06", "2025-09", "2025-10", "2026-03"],
        latest_data_month=LATEST_DATA_MONTH,
        floor=FLOOR,
        today=TODAY,
    )
    assert w == (FLOOR, TODAY)
