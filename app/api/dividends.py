"""Dividends: foreign dividend events + TW rebates."""
from __future__ import annotations

from collections import defaultdict

from flask import Blueprint

from ._helpers import envelope, store

bp = Blueprint("dividends", __name__, url_prefix="/api/dividends")


@bp.get("")
def dividends():
    s = store()
    rows = []
    by_ccy: dict[str, float] = defaultdict(float)
    by_ticker: dict[str, dict] = defaultdict(lambda: {"count": 0, "total_local": 0.0, "total_twd": 0.0, "ccy": "USD", "name": None})

    for m in s.months:
        fx = m.get("fx_usd_twd", 1) or 1
        for d in m.get("foreign", {}).get("dividends", []) or []:
            ccy = d.get("ccy", "USD")
            local = d.get("net_amount", 0) or 0
            rate = fx if ccy == "USD" else 1.0
            twd = local * rate
            row = {
                "month": m["month"],
                "date": d.get("date"),
                "code": d.get("code"),
                "name": d.get("name"),
                "ccy": ccy,
                "amount_local": local,
                "amount_twd": twd,
                "qty": d.get("qty"),
            }
            rows.append(row)
            by_ccy[ccy] += local
            t = by_ticker[d.get("code") or "?"]
            t["count"] += 1
            t["total_local"] += local
            t["total_twd"] += twd
            t["ccy"] = ccy
            if not t["name"]:
                t["name"] = d.get("name")

        for r in m.get("tw", {}).get("rebates", []) or []:
            rows.append({
                "month": m["month"],
                "date": None,
                "code": None,
                "name": r.get("type"),
                "ccy": "TWD",
                "amount_local": r.get("amount_twd", 0) or 0,
                "amount_twd": r.get("amount_twd", 0) or 0,
                "is_rebate": True,
            })
            by_ccy["TWD"] += r.get("amount_twd", 0) or 0

    rows.sort(key=lambda r: (r.get("month") or "", r.get("date") or ""), reverse=True)

    by_ticker_list = []
    for code, t in by_ticker.items():
        by_ticker_list.append({"code": code, **t})
    by_ticker_list.sort(key=lambda r: r["total_twd"], reverse=True)

    return envelope({
        "rows": rows,
        "totals_by_ccy": dict(by_ccy),
        "total_twd": sum(r["amount_twd"] for r in rows),
        "by_ticker": by_ticker_list,
        "count": len(rows),
    })
