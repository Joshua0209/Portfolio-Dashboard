"""Pure-Python analytics: drawdown, volatility, Sharpe, attribution.

Stateless functions so they're easy to test. All inputs are plain lists/dicts;
all outputs are JSON-serializable. Currency convention everywhere is TWD unless
the function name says otherwise.
"""
from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime
from typing import Iterable


# ────────────────────────────────────────────────────────────────────────────
# Time-series stats
# ────────────────────────────────────────────────────────────────────────────


def cumulative_curve(period_returns: Iterable[float]) -> list[float]:
    """Compound monthly returns into a cumulative-return curve starting at 0."""
    cum = []
    running = 1.0
    for r in period_returns:
        running *= 1.0 + (r or 0)
        cum.append(running - 1.0)
    return cum


def drawdown_curve(cum_returns: Iterable[float]) -> list[dict]:
    """For each point, return current drawdown from running peak.

    Returns a list of dicts with `value` (cumulative wealth = 1+r) and
    `drawdown` (signed, negative on the way down).
    """
    out = []
    peak = 1.0
    for r in cum_returns:
        wealth = 1.0 + (r or 0)
        if wealth > peak:
            peak = wealth
        dd = (wealth - peak) / peak if peak else 0.0
        out.append({"wealth": wealth, "drawdown": dd})
    return out


def max_drawdown(cum_returns: list[float]) -> float:
    if not cum_returns:
        return 0.0
    return min(p["drawdown"] for p in drawdown_curve(cum_returns))


def stdev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(var)


def annualize_return(monthly_return: float, periods: int = 12) -> float:
    """Compound a per-period rate to annual (e.g. monthly -> yearly)."""
    return (1.0 + monthly_return) ** periods - 1.0


def sharpe(period_returns: list[float], rf_period: float = 0.0, periods_per_year: int = 12) -> float:
    """Sharpe ratio using period (e.g. monthly) returns; risk-free defaults to 0."""
    if len(period_returns) < 2:
        return 0.0
    excess = [r - rf_period for r in period_returns]
    sd = stdev(excess)
    if sd == 0:
        return 0.0
    mean_excess = sum(excess) / len(excess)
    return (mean_excess / sd) * math.sqrt(periods_per_year)


def rolling_returns(period_returns: list[float], window: int) -> list[float | None]:
    """Compounded rolling returns with `window` periods. Pads early months with None."""
    out: list[float | None] = []
    for i in range(len(period_returns)):
        if i + 1 < window:
            out.append(None)
            continue
        slice_ = period_returns[i + 1 - window : i + 1]
        running = 1.0
        for r in slice_:
            running *= 1.0 + (r or 0)
        out.append(running - 1.0)
    return out


# ────────────────────────────────────────────────────────────────────────────
# Concentration / risk
# ────────────────────────────────────────────────────────────────────────────


def hhi(weights: Iterable[float]) -> float:
    """Herfindahl-Hirschman Index. 0=perfectly diversified, 1=single position.

    Caller should pass weights summing to ~1. Cash positions can be excluded
    or included as the caller wishes.
    """
    return sum((w or 0) ** 2 for w in weights)


def top_n_share(weights: list[float], n: int) -> float:
    sorted_w = sorted([w or 0 for w in weights], reverse=True)
    return sum(sorted_w[:n])


# ────────────────────────────────────────────────────────────────────────────
# P&L attribution
# ────────────────────────────────────────────────────────────────────────────


def realized_pnl_by_ticker(by_ticker: dict) -> list[dict]:
    """Realized P&L = sell_proceeds − (sell_qty × avg_buy_cost) − fees − tax.

    This uses average-cost convention (matches what Sinopac reports). It is an
    approximation when buy/sell tranches don't fully match — but for a
    statement-derived report, average-cost is the only basis we can compute.
    """
    out = []
    for code, t in by_ticker.items():
        buy_qty = t.get("buy_qty", 0) or 0
        sell_qty = t.get("sell_qty", 0) or 0
        buy_cost = t.get("buy_cost_twd", 0) or 0
        sell_proceeds = t.get("sell_proceeds_twd", 0) or 0
        fees = t.get("fees_twd", 0) or 0
        tax = t.get("tax_twd", 0) or 0

        avg_buy = (buy_cost / buy_qty) if buy_qty else 0
        cost_of_sold = avg_buy * sell_qty
        realized = sell_proceeds - cost_of_sold - fees - tax

        out.append({
            "code": code,
            "name": t.get("name"),
            "buy_qty": buy_qty,
            "sell_qty": sell_qty,
            "buy_cost_twd": buy_cost,
            "sell_proceeds_twd": sell_proceeds,
            "fees_twd": fees,
            "tax_twd": tax,
            "avg_buy_price_twd": avg_buy,
            "cost_of_sold_twd": cost_of_sold,
            "realized_pnl_twd": realized,
            "realized_pnl_pct": (realized / cost_of_sold) if cost_of_sold else None,
            "fully_closed": sell_qty >= buy_qty if buy_qty else False,
        })
    out.sort(key=lambda r: r["realized_pnl_twd"], reverse=True)
    return out


# ────────────────────────────────────────────────────────────────────────────
# Cashflow analysis
# ────────────────────────────────────────────────────────────────────────────


def monthly_flows(months: list[dict]) -> list[dict]:
    """Per-month inflow/outflow breakdown for the waterfall view."""
    out = []
    for m in months:
        flows = m.get("investment_flows_twd", {}) or {}
        out.append({
            "month": m["month"],
            "external_flow": m.get("external_flow_twd", 0),
            "tw_buy": flows.get("tw_buy", 0),
            "tw_sell": flows.get("tw_sell", 0),
            "foreign_buy": flows.get("foreign_buy", 0),
            "foreign_sell": flows.get("foreign_sell", 0),
            "rebate_in": flows.get("rebate_in", 0),
            "dividend_in": flows.get("dividend_in", 0),
            "fee": flows.get("fee", 0),
            "tax": flows.get("tax", 0),
        })
    return out


# ────────────────────────────────────────────────────────────────────────────
# FX
# ────────────────────────────────────────────────────────────────────────────


def fx_pnl(months: list[dict]) -> dict:
    """Estimate USD/TWD FX P&L: change in USD-TWD rate × cumulative USD held.

    This is a coarse measure (ignores intra-month flows), but for monthly
    statements it's the right granularity.
    """
    if len(months) < 2:
        return {"contribution_twd": 0, "monthly": []}

    monthly = []
    cumulative = 0.0
    for i in range(1, len(months)):
        prev = months[i - 1]
        curr = months[i]
        usd_held = prev.get("bank_usd_in_twd", 0)
        # USD held in TWD terms divided by previous fx gives USD amount
        prev_fx = prev.get("fx_usd_twd") or 1
        curr_fx = curr.get("fx_usd_twd") or prev_fx
        usd_amount = usd_held / prev_fx if prev_fx else 0
        delta_twd = usd_amount * (curr_fx - prev_fx)
        cumulative += delta_twd
        monthly.append({
            "month": curr["month"],
            "fx_usd_twd": curr_fx,
            "usd_amount": usd_amount,
            "fx_pnl_twd": delta_twd,
            "cumulative_fx_pnl_twd": cumulative,
        })
    return {"contribution_twd": cumulative, "monthly": monthly}


# ────────────────────────────────────────────────────────────────────────────
# Activity / movers
# ────────────────────────────────────────────────────────────────────────────


def top_movers(by_ticker: dict, latest_holdings: list[dict], top_n: int = 5) -> dict:
    """Top winners and losers by unrealized P&L (TWD)."""
    enriched = []
    for h in latest_holdings:
        enriched.append({
            "code": h.get("code"),
            "name": h.get("name"),
            "venue": h.get("venue"),
            "unrealized_pnl_twd": h.get("unrealized_pnl_twd", 0),
            "unrealized_pct": h.get("unrealized_pct", 0),
            "mkt_value_twd": h.get("mkt_value_twd", 0),
        })
    enriched.sort(key=lambda r: r["unrealized_pnl_twd"], reverse=True)
    winners = enriched[:top_n]
    losers = enriched[-top_n:][::-1]
    return {"winners": winners, "losers": losers}


def recent_activity(all_trades: list[dict], limit: int = 25) -> list[dict]:
    """Most recent trades, sorted descending by date."""
    def sort_key(t):
        try:
            return datetime.strptime(t.get("date", "1970/01/01"), "%Y/%m/%d")
        except ValueError:
            return datetime(1970, 1, 1)
    sorted_trades = sorted(all_trades, key=sort_key, reverse=True)
    return sorted_trades[:limit]


# ────────────────────────────────────────────────────────────────────────────
# Sector mapping (heuristic, no external API)
# ────────────────────────────────────────────────────────────────────────────


_TW_SECTOR_HINTS = {
    "0050": "ETF (TW broad)", "00631L": "ETF (TW leveraged)",
    "0051": "ETF (TW mid-cap)", "0056": "ETF (high dividend)",
    "00878": "ETF (high dividend)", "00929": "ETF (high dividend)",
    "00919": "ETF (high dividend)", "00713": "ETF (Smart Beta)",
    "00940": "ETF (high dividend)", "00713L": "ETF (TW leveraged)",
    "2330": "Semiconductors", "2317": "Hardware/EMS", "2454": "Semiconductors",
    "2308": "Hardware/EMS", "2382": "Hardware/EMS", "2603": "Shipping",
    "2609": "Shipping", "2615": "Shipping", "2002": "Steel",
    "1326": "Petrochemicals", "1303": "Petrochemicals", "1301": "Petrochemicals",
    "2412": "Telecom", "3008": "Optics", "3034": "Semiconductors",
    "2891": "Financials", "2882": "Financials", "2884": "Financials",
    "2885": "Financials", "2880": "Financials", "2890": "Financials",
    "1802": "Materials", "2912": "Retail", "1216": "Food",
    "5871": "Financials", "5880": "Financials",
}

_US_SECTOR_HINTS = {
    "NVDA": "Semiconductors", "AMD": "Semiconductors", "AVGO": "Semiconductors",
    "TSM": "Semiconductors", "INTC": "Semiconductors", "MU": "Semiconductors",
    "AAPL": "Hardware/Tech", "MSFT": "Software", "GOOGL": "Internet",
    "GOOG": "Internet", "META": "Internet", "AMZN": "Internet",
    "TSLA": "Auto/EV", "NFLX": "Media",
    "JPM": "Financials", "BAC": "Financials", "GS": "Financials",
    "V": "Financials", "MA": "Financials",
    "JNJ": "Healthcare", "UNH": "Healthcare", "LLY": "Healthcare",
    "PFE": "Healthcare", "MRK": "Healthcare",
    "XOM": "Energy", "CVX": "Energy",
    "WMT": "Consumer", "COST": "Consumer", "MCD": "Consumer",
    "DIS": "Media", "BA": "Industrials", "CAT": "Industrials",
    "SPY": "ETF (US broad)", "VOO": "ETF (US broad)", "QQQ": "ETF (US tech)",
    "VTI": "ETF (US broad)", "IVV": "ETF (US broad)",
    "LITE": "Semiconductors", "CRWD": "Software", "NET": "Software",
    "DDOG": "Software", "SNOW": "Software", "PLTR": "Software",
}


def sector_of(code: str, venue: str) -> str:
    if not code:
        return "Unknown"
    if venue == "TW":
        return _TW_SECTOR_HINTS.get(code, "TW Equity (other)")
    return _US_SECTOR_HINTS.get(code.upper(), "US Equity (other)")


def sector_breakdown(holdings: list[dict]) -> list[dict]:
    """Group holdings by heuristic sector. Returns list of {sector, value, weight, count}."""
    by_sector: dict[str, dict] = defaultdict(lambda: {"value": 0.0, "count": 0})
    total = 0.0
    for h in holdings:
        sec = sector_of(h.get("code", ""), h.get("venue", ""))
        v = h.get("mkt_value_twd", 0) or 0
        by_sector[sec]["value"] += v
        by_sector[sec]["count"] += 1
        total += v
    out = []
    for sec, agg in by_sector.items():
        out.append({
            "sector": sec,
            "value_twd": agg["value"],
            "count": agg["count"],
            "weight": (agg["value"] / total) if total else 0,
        })
    out.sort(key=lambda r: r["value_twd"], reverse=True)
    return out
