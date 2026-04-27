"""Pure-Python analytics: drawdown, volatility, Sharpe, Sortino, Calmar,
cashflow attribution, FIFO realized P&L, and FX P&L.

Stateless functions so they're easy to test. All inputs are plain lists/dicts;
all outputs are JSON-serializable. Currency convention everywhere is TWD unless
the function name says otherwise.
"""
from __future__ import annotations

import math
from calendar import monthrange
from collections import defaultdict, deque
from datetime import date, datetime
from typing import Iterable, Literal


# ────────────────────────────────────────────────────────────────────────────
# Period returns — choice of flow-timing assumption
# ────────────────────────────────────────────────────────────────────────────
#
# Modified Dietz returns: r = (V_end − V_start − F) / (V_start + Σ Wᵢ·Fᵢ)
#
#   Wᵢ = (D − dᵢ) / D      where D = days in month, dᵢ = day of flow
#
# The legacy parser uses W = 0.5 (mid-month assumption). When all flows
# happen near month-end (e.g. user sells everything on the last day) the
# mid-month assumption inflates the return because it treats half the
# withdrawn capital as having been absent for the whole period.
#
# `day_weighted` is the standard fix and is the dashboard default.
# `eom` (end-of-month) is the most conservative — it assumes ALL flows
# happened on the last day, so denominator is just V_start.
# `mid_month` reproduces the legacy number.

PeriodMethod = Literal["day_weighted", "mid_month", "eom"]


def _parse_trade_day(date_str: str) -> int | None:
    """Pull the day-of-month from a YYYY/MM/DD or YYYY-MM-DD string."""
    if not date_str:
        return None
    try:
        s = date_str.replace("-", "/")
        return int(s.split("/")[2])
    except (ValueError, IndexError):
        return None


def _trade_flow_events(month: dict) -> list[tuple[int, float]]:
    """Per-trade signed flow events as (day_of_month, F_twd).

    Sign convention matches Modified Dietz F: positive = cash INTO the
    investment portfolio (i.e. a buy that uses external bank cash);
    negative = cash OUT of the portfolio (a sell that returns cash to bank).

    For TW: F_i = -net_twd (because trade.net_twd is signed from the
        client side: negative when client paid for a buy).
    For Foreign: F_i = -(net_ccy × month FX), USD-only for now.
    """
    fx = month.get("fx_usd_twd") or 0.0
    events: list[tuple[int, float]] = []
    for t in (month.get("tw") or {}).get("trades", []) or []:
        d = _parse_trade_day(t.get("date"))
        if d is None:
            continue
        events.append((d, -float(t.get("net_twd") or 0)))
    for t in (month.get("foreign") or {}).get("trades", []) or []:
        if t.get("ccy") != "USD" or not fx:
            continue
        d = _parse_trade_day(t.get("date"))
        if d is None:
            continue
        events.append((d, -float(t.get("net_ccy") or 0) * fx))
    return events


def period_returns(months: list[dict], method: PeriodMethod = "day_weighted") -> list[dict]:
    """Compute Modified-Dietz period returns under a chosen flow-timing rule.

    Returns a list of dicts: {month, period_return, v_start, weighted_flow,
    flow_total, equity_end, days_in_month, method}.
    The first month has no prior equity so V_start is back-derived as
    V_end − F and its return is forced to 0 (no info to compare against).
    """
    if not months:
        return []
    out: list[dict] = []
    for i, m in enumerate(months):
        ym = m["month"]
        v_end = float(m.get("equity_twd") or 0)
        f_total = float(m.get("external_flow_twd") or 0)
        v_start = (v_end - f_total) if i == 0 else float(months[i - 1].get("equity_twd") or 0)

        year, mon = (int(x) for x in ym.split("-"))
        days = monthrange(year, mon)[1]

        if i == 0:
            r = 0.0
            wf = 0.0
        else:
            wf = _weighted_flow_sum(m, days, f_total, method)
            denom = v_start + wf
            r = (v_end - v_start - f_total) / denom if denom > 1e-6 else 0.0

        out.append({
            "month": ym,
            "period_return": r,
            "v_start": v_start,
            "equity_end": v_end,
            "flow_total": f_total,
            "weighted_flow": wf,
            "days_in_month": days,
            "method": method,
        })
    return out


def daily_external_flows(months: list[dict]) -> list[dict]:
    """Per-day external flow series (TWD) for daily Modified Dietz.

    "External" means: capital that crosses the boundary between you and
    the broker+bank as one system. Stock settlements, FX conversions, and
    dividends are internal rotations and are excluded — they do not change
    your invested capital, only its form.

    Source: bank ledger transactions (`m["bank"]["tx_twd"]` and
    `tx_foreign`), each carrying a `category` tag set by the parser.
    Foreign-account flows are converted to TWD using the same month FX
    that cashflows.py uses today (a per-day FX upgrade can come later via
    daily_store.get_fx_series — not blocking this implementation).

    Returns a sorted list of {date: 'YYYY-MM-DD', flow_twd: float}.
    Sign matches Modified Dietz: positive = inflow into the portfolio
    system (e.g. salary deposit), negative = outflow (e.g. transfer out).
    """
    EXCLUDED = {
        "tw_dividend",
        "foreign_dividend",
        "stock_settle_tw",
        "stock_settle_fx",
        "fx_convert",
    }
    by_date: dict[str, float] = defaultdict(float)
    for m in months:
        bank = m.get("bank") or {}
        fx = bank.get("fx") or {}
        for tx in bank.get("tx_twd", []) or []:
            if tx.get("category") in EXCLUDED:
                continue
            d = _normalize_iso_date(tx.get("date"))
            if d:
                by_date[d] += float(tx.get("signed_amount") or 0)
        for tx in bank.get("tx_foreign", []) or []:
            if tx.get("category") in EXCLUDED:
                continue
            d = _normalize_iso_date(tx.get("date"))
            if not d:
                continue
            ccy = tx.get("ccy") or "USD"
            rate = float(fx.get(ccy) or 0.0)
            by_date[d] += float(tx.get("signed_amount") or 0) * rate
    return [{"date": d, "flow_twd": by_date[d]} for d in sorted(by_date)]


def _normalize_iso_date(s: str | None) -> str | None:
    """'2026/03/15' or '2026-03-15' → '2026-03-15'. Returns None on garbage."""
    if not s:
        return None
    try:
        s = s.replace("/", "-")
        # validate via fromisoformat
        date.fromisoformat(s)
        return s
    except (ValueError, AttributeError):
        return None


def daily_twr(
    equity_series: list[dict],
    flow_series: list[dict],
    weight: float = 0.5,
) -> list[dict]:
    """Per-day Modified Dietz TWR, chained across all priced days.

    The standard daily Modified Dietz formula for one day d is:

        r_d = (V_d − V_{d-1} − F_d) / (V_{d-1} + weight · F_d)

    where:
      V_d        = end-of-day equity (TWD) for trading day d
      V_{d-1}    = end-of-day equity for the prior priced day
      F_d        = net external flow on day d (positive = inflow, signed)
      weight     = within-day flow timing assumption:
                     0.0 → flow at end of day (most conservative)
                     0.5 → flow at mid-day  (default; uniform-within-day)
                     1.0 → flow at start of day (most generous)

    Then chain across days:

        cum_twr_d = ∏_{i ≤ d} (1 + r_i) − 1

    Day 1 has no prior equity to compare against — return 0 for r_1 and
    use V_1 itself as the starting wealth_index baseline.

    Returns one dict per equity_series row:
        {date, equity_twd, flow_twd, period_return, cum_twr, wealth_index}
    """
    if not equity_series:
        return []

    flow_by_date: dict[str, float] = {f["date"]: float(f["flow_twd"]) for f in flow_series}
    out: list[dict] = []

    # Day 1: no prior equity. Force r_1 = 0; wealth_index starts at 1.0.
    first = equity_series[0]
    running_wealth = 1.0
    out.append({
        "date": first["date"],
        "equity_twd": float(first["equity_twd"]),
        "flow_twd": flow_by_date.get(first["date"], 0.0),
        "period_return": 0.0,
        "cum_twr": 0.0,
        "wealth_index": running_wealth,
    })

    for i in range(1, len(equity_series)):
        curr = equity_series[i]
        v_start = float(equity_series[i - 1]["equity_twd"])
        v_end = float(curr["equity_twd"])
        f_d = flow_by_date.get(curr["date"], 0.0)
        denom = v_start + weight * f_d
        r_d = (v_end - v_start - f_d) / denom if abs(denom) > 1e-6 else 0.0
        running_wealth *= 1.0 + r_d
        out.append({
            "date": curr["date"],
            "equity_twd": v_end,
            "flow_twd": f_d,
            "period_return": r_d,
            "cum_twr": running_wealth - 1.0,
            "wealth_index": running_wealth,
        })

    return out


def _weighted_flow_sum(month: dict, days: int, f_total: float, method: PeriodMethod) -> float:
    if method == "mid_month":
        return 0.5 * f_total
    if method == "eom":
        return 0.0
    # day_weighted — sum of per-trade Wᵢ·Fᵢ. Falls back to mid-month if
    # no per-trade dates (would only happen for a stub month).
    events = _trade_flow_events(month)
    if not events:
        return 0.5 * f_total
    wf = 0.0
    for day, f_i in events:
        w = max(0.0, (days - day) / days)
        wf += w * f_i
    return wf


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
    """For each point, return current drawdown from running peak."""
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


def drawdown_episodes(cum_returns: list[float], months: list[str] | None = None) -> list[dict]:
    """Identify each drawdown episode: peak, trough, depth, duration, recovery.

    A drawdown episode is the path from a new running peak down to its lowest
    trough and back up to the peak (or the present if not yet recovered).
    Returns list sorted by depth (most severe first).
    """
    if not cum_returns:
        return []
    months = months or [str(i) for i in range(len(cum_returns))]
    wealth = [1.0 + (r or 0) for r in cum_returns]

    episodes: list[dict] = []
    peak = wealth[0]
    peak_i = 0
    in_dd = False
    trough = peak
    trough_i = peak_i

    for i, w in enumerate(wealth):
        if w >= peak:
            if in_dd:
                episodes.append(_episode(peak, peak_i, trough, trough_i, w, i, months, recovered=True))
                in_dd = False
            peak = w
            peak_i = i
            trough = w
            trough_i = i
        else:
            if not in_dd:
                in_dd = True
                trough = w
                trough_i = i
            elif w < trough:
                trough = w
                trough_i = i

    if in_dd:
        episodes.append(_episode(peak, peak_i, trough, trough_i, wealth[-1], len(wealth) - 1, months, recovered=False))

    episodes.sort(key=lambda e: e["depth_pct"])
    return episodes


def _episode(peak, peak_i, trough, trough_i, last_w, last_i, months, recovered: bool) -> dict:
    return {
        "peak_month": months[peak_i],
        "peak_wealth": peak,
        "trough_month": months[trough_i],
        "trough_wealth": trough,
        "depth_pct": (trough - peak) / peak if peak else 0.0,
        "duration_months": last_i - peak_i,
        "drawdown_months": trough_i - peak_i,
        "recovery_months": (last_i - trough_i) if recovered else None,
        "recovered": recovered,
    }


def stdev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(var)


def downside_stdev(values: list[float], target: float = 0.0) -> float:
    """Stdev computed only over below-target observations (for Sortino)."""
    if not values:
        return 0.0
    sq_neg = [(v - target) ** 2 for v in values if v < target]
    if not sq_neg:
        return 0.0
    return math.sqrt(sum(sq_neg) / len(sq_neg))


def annualize_return(period_return: float, periods: int = 12) -> float:
    return (1.0 + period_return) ** periods - 1.0


def cagr_from_cum(cum_return: float, periods: int, periods_per_year: int = 12) -> float:
    """Compound Annual Growth Rate from a cumulative return over N periods."""
    if periods <= 0:
        return 0.0
    years = periods / periods_per_year
    if 1.0 + cum_return <= 0:
        return -1.0
    return (1.0 + cum_return) ** (1.0 / years) - 1.0


def sharpe(period_returns: list[float], rf_period: float = 0.0, periods_per_year: int = 12) -> float:
    if len(period_returns) < 2:
        return 0.0
    excess = [r - rf_period for r in period_returns]
    sd = stdev(excess)
    if sd == 0:
        return 0.0
    mean_excess = sum(excess) / len(excess)
    return (mean_excess / sd) * math.sqrt(periods_per_year)


def sortino(period_returns: list[float], rf_period: float = 0.0, periods_per_year: int = 12) -> float:
    if len(period_returns) < 2:
        return 0.0
    excess = [r - rf_period for r in period_returns]
    dsd = downside_stdev(excess, target=0.0)
    if dsd == 0:
        return 0.0
    mean_excess = sum(excess) / len(excess)
    return (mean_excess / dsd) * math.sqrt(periods_per_year)


def calmar(period_returns: list[float], periods_per_year: int = 12) -> float:
    """CAGR divided by |max drawdown|. Higher = better return per unit of pain."""
    if not period_returns:
        return 0.0
    cum = cumulative_curve(period_returns)
    mdd = abs(max_drawdown(cum))
    if mdd < 1e-9:
        return 0.0
    cagr = cagr_from_cum(cum[-1], len(period_returns), periods_per_year)
    return cagr / mdd


def rolling_returns(period_returns: list[float], window: int) -> list[float | None]:
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


def rolling_sharpe(period_returns: list[float], window: int, periods_per_year: int = 12) -> list[float | None]:
    out: list[float | None] = []
    for i in range(len(period_returns)):
        if i + 1 < window:
            out.append(None)
            continue
        out.append(sharpe(period_returns[i + 1 - window : i + 1], periods_per_year=periods_per_year))
    return out


# ────────────────────────────────────────────────────────────────────────────
# Concentration / risk
# ────────────────────────────────────────────────────────────────────────────


def hhi(weights: Iterable[float]) -> float:
    return sum((w or 0) ** 2 for w in weights)


def top_n_share(weights: list[float], n: int) -> float:
    sorted_w = sorted([w or 0 for w in weights], reverse=True)
    return sum(sorted_w[:n])


def effective_n(weights: Iterable[float]) -> float:
    """Effective number of holdings = 1 / HHI."""
    h = hhi(weights)
    return (1.0 / h) if h > 1e-9 else 0.0


# ────────────────────────────────────────────────────────────────────────────
# P&L attribution
# ────────────────────────────────────────────────────────────────────────────


def realized_pnl_by_ticker(by_ticker: dict) -> list[dict]:
    """Average-cost realized P&L. Matches Sinopac's reporting convention."""
    out = []
    for code, t in by_ticker.items():
        buy_qty = t.get("buy_qty", 0) or 0
        sell_qty = t.get("sell_qty", 0) or 0
        buy_cost = t.get("buy_cost_twd", 0) or 0
        sell_proceeds = t.get("sell_proceeds_twd", 0) or 0
        fees = t.get("fees_twd", 0) or 0
        tax = t.get("tax_twd", 0) or 0
        divs = t.get("dividends_twd", 0) or 0

        avg_buy = (buy_cost / buy_qty) if buy_qty else 0
        cost_of_sold = avg_buy * sell_qty
        realized = sell_proceeds - cost_of_sold - fees - tax

        out.append({
            "code": code,
            "name": t.get("name"),
            "venue": t.get("venue"),
            "buy_qty": buy_qty,
            "sell_qty": sell_qty,
            "buy_cost_twd": buy_cost,
            "sell_proceeds_twd": sell_proceeds,
            "fees_twd": fees,
            "tax_twd": tax,
            "dividends_twd": divs,
            "avg_buy_price_twd": avg_buy,
            "cost_of_sold_twd": cost_of_sold,
            "realized_pnl_twd": realized,
            "realized_pnl_with_div_twd": realized + divs,
            "realized_pnl_pct": (realized / cost_of_sold) if cost_of_sold else None,
            "fully_closed": sell_qty >= buy_qty if buy_qty else False,
        })
    out.sort(key=lambda r: r["realized_pnl_twd"], reverse=True)
    return out


def realized_pnl_by_ticker_fifo(by_ticker: dict) -> list[dict]:
    """FIFO realized P&L per ticker.

    Walks each ticker's trade log in chronological order, matching sells
    against the oldest open buy lots. This is closer to Taiwan tax basis
    (FIFO is the default) and produces accurate holding-period and
    win-rate stats.
    """
    out = []
    for code, t in by_ticker.items():
        trades = sorted(t.get("trades", []) or [], key=lambda r: r.get("date") or "")
        lots: deque[dict] = deque()
        realized = 0.0
        sell_proceeds = 0.0
        cost_of_sold = 0.0
        sell_qty_total = 0.0
        wins = losses = 0
        gross_win = gross_loss = 0.0
        holding_periods: list[float] = []
        for tr in trades:
            qty = tr.get("qty", 0) or 0
            if qty <= 0:
                continue
            side = (tr.get("side") or "")
            is_buy = "買" in side
            is_sell = ("賣" in side) and (side != "股利")
            if is_buy:
                gross = tr.get("gross_twd", 0) or 0
                fee = tr.get("fee_twd", 0) or 0
                tax = tr.get("tax_twd", 0) or 0
                px = ((gross + fee + tax) / qty) if qty else 0
                lots.append({
                    "qty": qty, "px_twd": px,
                    "date": _parse_iso_or_slash(tr.get("date")),
                })
            elif is_sell:
                gross = tr.get("gross_twd", 0) or 0
                fee = tr.get("fee_twd", 0) or 0
                tax = tr.get("tax_twd", 0) or 0
                proceeds_per_share = ((gross - fee - tax) / qty) if qty else 0
                sell_dt = _parse_iso_or_slash(tr.get("date"))
                remaining = qty
                trade_realized = 0.0
                while remaining > 1e-9 and lots:
                    lot = lots[0]
                    take = min(lot["qty"], remaining)
                    lot_cost = take * lot["px_twd"]
                    proceeds = take * proceeds_per_share
                    realized += proceeds - lot_cost
                    trade_realized += proceeds - lot_cost
                    sell_proceeds += proceeds
                    cost_of_sold += lot_cost
                    sell_qty_total += take
                    if sell_dt and lot["date"]:
                        holding_periods.append((sell_dt - lot["date"]).days)
                    lot["qty"] -= take
                    remaining -= take
                    if lot["qty"] <= 1e-9:
                        lots.popleft()
                if trade_realized > 0:
                    wins += 1
                    gross_win += trade_realized
                elif trade_realized < 0:
                    losses += 1
                    gross_loss += -trade_realized
        open_qty = sum(l["qty"] for l in lots)
        open_cost = sum(l["qty"] * l["px_twd"] for l in lots)
        avg_holding_days = (sum(holding_periods) / len(holding_periods)) if holding_periods else None
        win_rate = (wins / (wins + losses)) if (wins + losses) else None
        profit_factor = (gross_win / gross_loss) if gross_loss > 0 else None
        out.append({
            "code": code,
            "name": t.get("name"),
            "venue": t.get("venue"),
            "realized_pnl_twd": realized,
            "sell_proceeds_twd": sell_proceeds,
            "cost_of_sold_twd": cost_of_sold,
            "sell_qty": sell_qty_total,
            "open_qty": open_qty,
            "open_cost_twd": open_cost,
            "avg_open_cost_twd": (open_cost / open_qty) if open_qty else None,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "profit_factor": profit_factor,
            "avg_holding_days": avg_holding_days,
            "dividends_twd": t.get("dividends_twd", 0) or 0,
            "fully_closed": open_qty < 1e-6,
        })
    out.sort(key=lambda r: r["realized_pnl_twd"], reverse=True)
    return out


def _parse_iso_or_slash(s: str | None) -> date | None:
    if not s:
        return None
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


# ────────────────────────────────────────────────────────────────────────────
# Cashflow analysis
# ────────────────────────────────────────────────────────────────────────────


def monthly_flows(months: list[dict], venue_flows: list[dict] | None = None) -> list[dict]:
    """Per-month inflow/outflow breakdown for the cashflow waterfall.

    Pulls TW vs Foreign buy/sell from `venue_flows_twd` (broker-trade ground
    truth) and bank-side categorization from `investment_flows_twd`. Also
    derives three "view" totals so the UI can disambiguate what the user
    means by "net flow":

      * gross_in / gross_out — sum of bank movements tied to broker activity.
      * external_flow         — net broker↔bank cashflow (legacy).
      * deposits_net          — peer transfers + salary (capital actively
                                 moved IN from outside the investing system).
    """
    venue_by_month = {v["month"]: v for v in (venue_flows or [])}
    out = []
    for m in months:
        flows = m.get("investment_flows_twd", {}) or {}
        vf = venue_by_month.get(m["month"], {})
        bank = m.get("bank", {}) or {}

        gross_in = 0.0
        gross_out = 0.0
        for tx in bank.get("tx_twd", []) or []:
            cat = tx.get("category") or ""
            sgn = tx.get("signed_amount", 0) or 0
            if cat in ("stock_settle_tw", "rebate", "tw_dividend", "fx_convert"):
                if sgn > 0:
                    gross_in += sgn
                else:
                    gross_out += -sgn

        usd_rate = (bank.get("fx") or {}).get("USD") or m.get("fx_usd_twd") or 0.0
        for tx in bank.get("tx_foreign", []) or []:
            cat = tx.get("category") or ""
            sgn = tx.get("signed_amount", 0) or 0
            if cat in ("stock_settle_fx", "foreign_dividend", "fx_convert"):
                amt_twd = sgn * usd_rate
                if amt_twd > 0:
                    gross_in += amt_twd
                else:
                    gross_out += -amt_twd

        out.append({
            "month": m["month"],
            "external_flow": m.get("external_flow_twd", 0),
            "gross_in": gross_in,
            "gross_out": gross_out,
            "deposits_net": (
                (flows.get("transfer_net_twd", 0) or 0)
                + (flows.get("salary_in_twd", 0) or 0)
            ),
            # Venue split (broker-side, ground truth):
            "tw_buy": vf.get("tw_buy_twd", 0),
            "tw_sell": vf.get("tw_sell_twd", 0),
            "tw_fee": vf.get("tw_fee_twd", 0),
            "tw_tax": vf.get("tw_tax_twd", 0),
            "foreign_buy": vf.get("foreign_buy_twd", 0),
            "foreign_sell": vf.get("foreign_sell_twd", 0),
            "foreign_fee": vf.get("foreign_fee_twd", 0),
            # Bank-side (the path the cash actually took):
            "stock_buy_bank": flows.get("stock_buy_twd", 0),
            "stock_sell_bank": flows.get("stock_sell_twd", 0),
            "tw_dividend_in": flows.get("tw_dividend_in_twd", 0),
            "rebate_in": flows.get("rebate_in_twd", 0),
            "fx_to_usd": flows.get("fx_to_usd_twd", 0),
            "fx_to_twd": flows.get("fx_to_twd_twd", 0),
            "salary_in": flows.get("salary_in_twd", 0),
            "interest_in": flows.get("interest_in_twd", 0),
            "transfer_net": flows.get("transfer_net_twd", 0),
        })
    return out


# ────────────────────────────────────────────────────────────────────────────
# FX
# ────────────────────────────────────────────────────────────────────────────


def reprice_holdings_with_daily(
    holdings: list[dict],
    get_latest_close,
    current_fx_usd_twd: float | None = None,
) -> list[dict]:
    """Override each holding's ref_price/mv/unrealized with today's close.

    Source-of-truth fields preserved: code, name, qty, avg_cost, cost_*.
    Mutated fields: ref_price, mkt_value_local, mkt_value_twd,
    unrealized_pnl_local, unrealized_pnl_twd, unrealized_pct, repriced_at.

    Tickers with no daily price (e.g. delisted, very thinly-traded) keep
    their month-end values — graceful per-ticker fallback. The function
    is order-preserving and side-effect free (returns new list of new
    dicts).

    Args:
        holdings: rows from `_holdings_for_month(last)` (unified shape).
        get_latest_close: callable(symbol) -> {date, close, currency} or
            None. Pass `daily_store.get_latest_close` directly.
        current_fx_usd_twd: latest USD/TWD rate to convert foreign
            positions. Falls back to the holding's stale fx if None.
    """
    out: list[dict] = []
    for h in holdings:
        code = h.get("code")
        new = dict(h)
        if not code:
            out.append(new)
            continue
        latest = get_latest_close(code)
        if not latest:
            # Foreign holdings sometimes use ".TW" / ".HK" suffixes — daily
            # store keys foreign tickers as their bare Yahoo symbol.
            out.append(new)
            continue
        close = float(latest["close"])
        qty = float(h.get("qty") or 0)
        cost_local = float(h.get("cost_local") or h.get("cost_twd") or 0)
        new["ref_price"] = close
        venue = h.get("venue")
        if venue == "TW":
            mv_local = qty * close
            new["mkt_value_local"] = mv_local
            new["mkt_value_twd"] = mv_local
            upnl = mv_local - cost_local
            new["unrealized_pnl_local"] = upnl
            new["unrealized_pnl_twd"] = upnl
            new["unrealized_pct"] = (upnl / cost_local) if cost_local else 0
        else:  # Foreign — apply current FX
            mv_local = qty * close
            ccy = h.get("ccy") or "USD"
            rate = current_fx_usd_twd if ccy == "USD" and current_fx_usd_twd else 1.0
            new["mkt_value_local"] = mv_local
            new["mkt_value_twd"] = mv_local * rate
            upnl_local = mv_local - cost_local
            new["unrealized_pnl_local"] = upnl_local
            new["unrealized_pnl_twd"] = upnl_local * rate
            new["unrealized_pct"] = (upnl_local / cost_local) if cost_local else 0
        new["repriced_at"] = latest["date"]
        out.append(new)
    return out


def daily_fx_pnl(
    usd_exposure_series: list[dict],
    fx_series: list[dict],
) -> dict:
    """Day-over-day FX P&L using daily rates and daily USD exposure.

    For each priced day d (after the first):
        usd_amount_{d-1} = usd_mv_twd_{d-1} / rate_{d-1}
        fx_pnl_d         = usd_amount_{d-1} × (rate_d − rate_{d-1})

    USD cash from the bank statement is monthly-only and NOT included
    here — the daily layer tracks foreign equity exposure only. The
    /api/fx response calls this out via fx_pnl_resolution: "daily".

    Returns same shape as fx_pnl(): {contribution_twd, daily: [...]}.
    """
    if len(usd_exposure_series) < 2 or len(fx_series) < 2:
        return {"contribution_twd": 0, "daily": []}

    rate_by_date = {r["date"]: float(r["rate_to_twd"]) for r in fx_series}
    usd_by_date = {r["date"]: float(r["usd_mv_twd"]) for r in usd_exposure_series}
    dates = sorted(set(rate_by_date) & set(usd_by_date))
    if len(dates) < 2:
        return {"contribution_twd": 0, "daily": []}

    cumulative = 0.0
    daily = []
    for i in range(1, len(dates)):
        prev_d, curr_d = dates[i - 1], dates[i]
        prev_rate, curr_rate = rate_by_date[prev_d], rate_by_date[curr_d]
        prev_mv_twd = usd_by_date[prev_d]
        usd_amount = prev_mv_twd / prev_rate if prev_rate else 0.0
        delta_twd = usd_amount * (curr_rate - prev_rate)
        cumulative += delta_twd
        daily.append({
            "date": curr_d,
            "fx_usd_twd": curr_rate,
            "usd_amount": usd_amount,
            "fx_pnl_twd": delta_twd,
            "cumulative_fx_pnl_twd": cumulative,
        })
    return {"contribution_twd": cumulative, "daily": daily}


def fx_pnl(months: list[dict]) -> dict:
    """USD/TWD FX P&L on full USD exposure (bank + foreign equities).

    Old version only counted USD bank cash, missing the much-larger USD equity
    exposure. We now use total USD held = (bank_usd_in_twd + foreign_market_value_twd)
    converted back to USD via prior-month FX rate.
    """
    if len(months) < 2:
        return {"contribution_twd": 0, "monthly": []}

    monthly = []
    cumulative = 0.0
    for i in range(1, len(months)):
        prev = months[i - 1]
        curr = months[i]
        prev_fx = prev.get("fx_usd_twd") or 1
        curr_fx = curr.get("fx_usd_twd") or prev_fx
        usd_held_twd = (prev.get("bank_usd_in_twd", 0) or 0) + (prev.get("foreign_market_value_twd", 0) or 0)
        usd_amount = (usd_held_twd / prev_fx) if prev_fx else 0
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
    "00981A": "ETF (Active TW)",
    "2330": "Semiconductors", "2317": "Hardware/EMS", "2454": "Semiconductors",
    "2308": "Hardware/EMS", "2382": "Hardware/EMS", "2603": "Shipping",
    "2609": "Shipping", "2615": "Shipping", "2002": "Steel",
    "1326": "Petrochemicals", "1303": "Petrochemicals", "1301": "Petrochemicals",
    "2412": "Telecom", "3008": "Optics", "3034": "Semiconductors",
    "2891": "Financials", "2882": "Financials", "2884": "Financials",
    "2885": "Financials", "2880": "Financials", "2890": "Financials",
    "1802": "Materials", "2912": "Retail", "1216": "Food",
    "5871": "Financials", "5880": "Financials",
    "2360": "Semiconductors", "2376": "Hardware/EMS", "2369": "Optics",
    "3035": "Semiconductors",
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
    "LITE": "Semiconductors", "SNDK": "Hardware/Tech",
    "CRWD": "Software", "NET": "Software",
    "DDOG": "Software", "SNOW": "Software", "PLTR": "Software",
}


def sector_of(code: str, venue: str) -> str:
    if not code:
        return "Unknown"
    if venue == "TW":
        return _TW_SECTOR_HINTS.get(code, "TW Equity (other)")
    return _US_SECTOR_HINTS.get(code.upper(), "US Equity (other)")


def sector_breakdown(holdings: list[dict]) -> list[dict]:
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
