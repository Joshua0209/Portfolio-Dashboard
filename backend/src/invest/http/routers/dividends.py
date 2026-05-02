"""GET /api/dividends — dividend events + rebates."""
from __future__ import annotations

from collections import defaultdict
from typing import Any

from fastapi import APIRouter, Depends

from invest.http.deps import get_portfolio_store
from invest.http.helpers import envelope
from invest.persistence.portfolio_store import PortfolioStore


router = APIRouter()


def _months_ago(month_str: str | None, n: int) -> str:
    if not month_str:
        return "0000/00/00"
    y, m = month_str.split("-")
    y, m = int(y), int(m)
    new_m = m - n + 1
    while new_m <= 0:
        new_m += 12
        y -= 1
    return f"{y:04d}/{new_m:02d}/01"


@router.get("/api/dividends")
def dividends(
    s: PortfolioStore = Depends(get_portfolio_store),
) -> dict[str, Any]:
    rows: list[dict] = []
    by_ccy: dict[str, float] = defaultdict(float)
    by_ticker: dict[str, dict] = defaultdict(lambda: {
        "code": None, "name": None, "venue": None,
        "count": 0, "total_local": 0.0, "total_twd": 0.0, "ccy": None,
        "first_date": None, "last_date": None,
    })
    monthly_by_venue: dict[str, dict] = defaultdict(lambda: {"TW": 0.0, "Foreign": 0.0})

    for ev in s.dividends:
        twd = ev.get("amount_twd", 0) or 0
        local = ev.get("amount_local", 0) or 0
        ccy = ev.get("ccy") or "TWD"
        rows.append({
            "month": ev.get("month"),
            "date": ev.get("date"),
            "venue": ev.get("venue"),
            "code": ev.get("code"),
            "name": ev.get("name"),
            "ccy": ccy,
            "amount_local": local,
            "amount_twd": twd,
            "source": ev.get("source"),
        })
        by_ccy[ccy] += local
        monthly_by_venue[ev.get("month") or ""][ev.get("venue") or "TW"] += twd
        key = ev.get("code") or ev.get("name") or "?"
        t = by_ticker[key]
        t["code"] = t["code"] or ev.get("code")
        t["name"] = t["name"] or ev.get("name")
        t["venue"] = t["venue"] or ev.get("venue")
        t["ccy"] = t["ccy"] or ccy
        t["count"] += 1
        t["total_local"] += local
        t["total_twd"] += twd
        d = ev.get("date")
        t["first_date"] = min(t["first_date"], d) if t["first_date"] else d
        t["last_date"] = max(t["last_date"], d) if t["last_date"] else d

    rebate_rows: list[dict] = []
    rebate_total = 0.0
    for m in s.months:
        for r in (m.get("tw") or {}).get("rebates", []) or []:
            amt = r.get("amount_twd", 0) or 0
            rebate_total += amt
            rebate_rows.append({
                "month": m["month"],
                "type": r.get("type"),
                "amount_twd": amt,
            })

    htr = s.holdings_total_return
    by_ticker_list = sorted(
        ({"code": code, **t} for code, t in by_ticker.items()),
        key=lambda r: r["total_twd"], reverse=True,
    )
    rows.sort(key=lambda r: (r.get("date") or ""), reverse=True)

    monthly = [
        {"month": m, "tw_twd": v["TW"], "foreign_twd": v["Foreign"], "total_twd": v["TW"] + v["Foreign"]}
        for m, v in sorted(monthly_by_venue.items())
    ]

    cutoff = _months_ago(s.as_of, 12)
    ttm_div = sum(
        r["amount_twd"] for r in rows
        if r.get("date") and r["date"] >= cutoff
    )
    invested_cost = sum(h.get("cost_twd", 0) or 0 for h in htr)
    ttm_yield = (ttm_div / invested_cost) if invested_cost else None

    n_months = max(1, len(s.months))
    total_div_twd = sum(r["amount_twd"] for r in rows)
    avg_monthly_div = total_div_twd / n_months
    annualized_yield = (avg_monthly_div * 12 / invested_cost) if invested_cost else None

    return envelope({
        "rows": rows,
        "rebates": rebate_rows,
        "rebates_total_twd": rebate_total,
        "totals_by_ccy": dict(by_ccy),
        "total_twd": total_div_twd,
        "by_ticker": by_ticker_list,
        "monthly": monthly,
        "count": len(rows),
        "yields": {
            "ttm_dividend_twd": ttm_div,
            "ttm_yield_on_cost": ttm_yield,
            "avg_monthly_twd": avg_monthly_div,
            "annualized_yield_on_cost": annualized_yield,
            "invested_cost_twd": invested_cost,
        },
        "holdings_total_return": htr,
    })
