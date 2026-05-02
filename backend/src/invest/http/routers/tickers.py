"""GET /api/tickers (list) + /api/tickers/<code> (detail)."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from invest import benchmarks
from invest.analytics import monthly as analytics
from invest.http.deps import get_daily_store, get_portfolio_store
from invest.http.helpers import envelope, reprice_holdings_today
from invest.persistence.daily_store import DailyStore
from invest.persistence.portfolio_store import PortfolioStore


router = APIRouter()


def _normalize_trade_date(d: str) -> str:
    return d.replace("/", "-") if "/" in d else d


def _yahoo_symbol(code: str, venue: str | None, ccy: str | None) -> str | None:
    if venue == "TW":
        return f"{code}.TW"
    if venue == "Foreign" and ccy == "USD":
        return code
    return None


def _backfill_gap_prices(
    position_history: list[dict],
    code: str,
    venue: str | None,
    ccy: str | None,
) -> None:
    gaps = [h for h in position_history if h.get("ref_price") is None]
    if not gaps:
        return
    symbol = _yahoo_symbol(code, venue, ccy)
    if not symbol:
        return
    try:
        prices = benchmarks.fetch_monthly_prices(
            [symbol], position_history[0]["month"], position_history[-1]["month"],
        )
    except Exception:
        return
    sym_prices = prices.get(symbol, {})
    for h in gaps:
        p = sym_prices.get(h["month"])
        if p is not None:
            h["ref_price"] = p


def _daily_prices_for(
    code: str, start: str | None, end: str | None,
    s: PortfolioStore, daily: DailyStore,
) -> dict:
    points = daily.get_ticker_history(code, start=start, end=end)
    trades = []
    for t in s.all_trades or []:
        if t.get("code") != code:
            continue
        d = _normalize_trade_date(t.get("date", ""))
        if start and d < start:
            continue
        if end and d > end:
            continue
        trades.append({
            "date": d,
            "side": t.get("side"),
            "qty": t.get("qty"),
            "price": t.get("price"),
            "venue": t.get("venue"),
            "ccy": t.get("ccy"),
        })
    return {
        "points": points,
        "trades": trades,
        "empty": len(points) == 0,
    }


def _daily_position_history_for(
    code: str, start: str | None, end: str | None, daily: DailyStore,
) -> list[dict]:
    rows = daily.get_positions_for_ticker(code, start=start, end=end)

    def _row(r: dict) -> dict:
        mv_local = r.get("mv_local") or 0
        mv_twd = r.get("mv_twd") or 0
        cost_local = r.get("cost_local") or 0
        fx = (mv_twd / mv_local) if mv_local else 1.0
        cost_twd = cost_local * fx
        qty = r.get("qty") or 0
        return {
            "date": r["date"],
            "qty": qty,
            "cost_local": cost_local,
            "cost_twd": cost_twd,
            "mkt_value_local": mv_local,
            "mkt_value_twd": mv_twd,
            "ref_price": (mv_local / qty) if qty else None,
            "type": r.get("type"),
            "source": r.get("source"),
        }

    return [_row(r) for r in rows]


@router.get("/api/tickers")
def list_tickers(
    s: PortfolioStore = Depends(get_portfolio_store),
) -> dict[str, Any]:
    realized = analytics.realized_pnl_by_ticker(s.by_ticker)
    return envelope(realized)


@router.get("/api/tickers/{code}")
def ticker_detail(
    code: str,
    resolution: str = Query("monthly"),
    start: str | None = Query(default=None),
    end: str | None = Query(default=None),
    s: PortfolioStore = Depends(get_portfolio_store),
    daily: DailyStore = Depends(get_daily_store),
) -> Any:
    by_ticker = s.by_ticker
    if code not in by_ticker:
        return JSONResponse(
            status_code=404,
            content={"ok": False, "error": "not found", "data": {"error": "not found"}},
        )

    t = by_ticker[code]

    held: dict[str, dict] = {}
    last_venue: str | None = None
    last_ccy: str | None = None
    for m in s.months:
        fx = m.get("fx_usd_twd", 1) or 1
        ym = m["month"]
        for h in m.get("tw", {}).get("holdings", []) or []:
            if h.get("code") == code:
                held[ym] = {
                    "month": ym,
                    "venue": "TW",
                    "qty": h.get("qty"),
                    "avg_cost": h.get("avg_cost"),
                    "cost_twd": h.get("cost"),
                    "ref_price": h.get("ref_price"),
                    "mkt_value_twd": h.get("mkt_value"),
                    "unrealized_pnl_twd": h.get("unrealized_pnl"),
                    "type": h.get("type"),
                }
                last_venue = "TW"
                break
        else:
            for h in m.get("foreign", {}).get("holdings", []) or []:
                if h.get("code") == code:
                    rate = fx if h.get("ccy") == "USD" else 1.0
                    held[ym] = {
                        "month": ym,
                        "venue": "Foreign",
                        "ccy": h.get("ccy"),
                        "qty": h.get("qty"),
                        "avg_cost": (h.get("cost", 0) / h["qty"]) if h.get("qty") else 0,
                        "cost_local": h.get("cost"),
                        "cost_twd": (h.get("cost", 0) or 0) * rate,
                        "ref_price": h.get("close"),
                        "mkt_value_local": h.get("mkt_value"),
                        "mkt_value_twd": (h.get("mkt_value", 0) or 0) * rate,
                        "unrealized_pnl_local": h.get("unrealized_pnl"),
                        "unrealized_pnl_twd": (h.get("unrealized_pnl", 0) or 0) * rate,
                    }
                    last_venue = "Foreign"
                    last_ccy = h.get("ccy")
                    break

    position_history: list[dict] = []
    if held:
        all_months = [m["month"] for m in s.months]
        first_idx = min(all_months.index(ym) for ym in held)
        last_idx = max(all_months.index(ym) for ym in held)
        for ym in all_months[first_idx : last_idx + 1]:
            if ym in held:
                position_history.append(held[ym])
            else:
                gap = {
                    "month": ym,
                    "venue": last_venue,
                    "qty": 0,
                    "avg_cost": 0,
                    "cost_twd": 0,
                    "ref_price": None,
                    "mkt_value_twd": 0,
                    "unrealized_pnl_twd": 0,
                }
                if last_venue == "Foreign":
                    gap.update({
                        "ccy": last_ccy,
                        "cost_local": 0,
                        "mkt_value_local": 0,
                        "unrealized_pnl_local": 0,
                    })
                position_history.append(gap)

        _backfill_gap_prices(position_history, code, last_venue, last_ccy)

    trades = sorted(t.get("trades", []), key=lambda r: r.get("date", ""))

    name_norm = (t.get("name") or "").strip()
    dividends = []
    for ev in s.dividends:
        match_code = ev.get("code") and ev["code"] == code
        match_name = bool(name_norm) and (ev.get("name") or "").strip() == name_norm
        if match_code or match_name:
            dividends.append({
                "month": ev.get("month"),
                "date": ev.get("date"),
                "venue": ev.get("venue"),
                "ccy": ev.get("ccy"),
                "amount_local": ev.get("amount_local"),
                "amount_twd": ev.get("amount_twd"),
                "source": ev.get("source"),
            })

    realized_avg = analytics.realized_pnl_by_ticker({code: t})[0]
    realized_fifo = analytics.realized_pnl_by_ticker_fifo({code: t})[0]
    realized = {**realized_avg, "fifo": realized_fifo}

    last_month = s.months[-1]["month"] if s.months else None
    last_entry = position_history[-1] if position_history else None
    is_open = bool(last_entry and last_entry["month"] == last_month)
    current = last_entry if is_open else None

    if current:
        repriced = reprice_holdings_today([{**current, "code": code}], daily)
        if repriced and repriced[0].get("repriced_at"):
            current = {**current, **repriced[0]}

    payload: dict[str, Any] = {
        "code": code,
        "name": t.get("name"),
        "summary": realized,
        "trades": trades,
        "position_history": position_history,
        "dividends": dividends,
        "current": current,
        "is_open": is_open,
        "last_seen_month": last_entry["month"] if last_entry else None,
    }

    if (resolution or "").lower() == "daily":
        payload["daily_prices"] = _daily_prices_for(code, start=start, end=end, s=s, daily=daily)
        daily_history = _daily_position_history_for(code, start=start, end=end, daily=daily)
        if daily_history:
            payload["position_history_daily"] = daily_history

    return envelope(payload)
