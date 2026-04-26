"""Per-ticker drill-down: position history, all trades, dividend, P&L."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from flask import Blueprint

from .. import analytics
from ._helpers import envelope, store

bp = Blueprint("tickers", __name__, url_prefix="/api/tickers")


@bp.get("")
def list_tickers():
    s = store()
    by_ticker = s.by_ticker
    realized = analytics.realized_pnl_by_ticker(by_ticker)
    return envelope(realized)


@bp.get("/<code>")
def ticker_detail(code: str):
    s = store()
    by_ticker = s.by_ticker
    if code not in by_ticker:
        return envelope({"error": "not found"}), 404

    t = by_ticker[code]

    position_history = []
    for m in s.months:
        fx = m.get("fx_usd_twd", 1) or 1
        # try TW
        for h in m.get("tw", {}).get("holdings", []) or []:
            if h.get("code") == code:
                position_history.append({
                    "month": m["month"],
                    "venue": "TW",
                    "qty": h.get("qty"),
                    "avg_cost": h.get("avg_cost"),
                    "cost_twd": h.get("cost"),
                    "ref_price": h.get("ref_price"),
                    "mkt_value_twd": h.get("mkt_value"),
                    "unrealized_pnl_twd": h.get("unrealized_pnl"),
                    "type": h.get("type"),
                })
                break
        else:
            for h in m.get("foreign", {}).get("holdings", []) or []:
                if h.get("code") == code:
                    rate = fx if h.get("ccy") == "USD" else 1.0
                    position_history.append({
                        "month": m["month"],
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
                    })

    trades = sorted(t.get("trades", []), key=lambda r: r.get("date", ""))

    # Pull all dividends for this ticker from the unified dividend stream.
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

    # Is this position still open at the most recent month?
    last_month = s.months[-1]["month"] if s.months else None
    last_entry = position_history[-1] if position_history else None
    is_open = bool(last_entry and last_entry["month"] == last_month)
    current = last_entry if is_open else None

    return envelope({
        "code": code,
        "name": t.get("name"),
        "summary": realized,
        "trades": trades,
        "position_history": position_history,
        "dividends": dividends,
        "current": current,
        "is_open": is_open,
        "last_seen_month": last_entry["month"] if last_entry else None,
    })
