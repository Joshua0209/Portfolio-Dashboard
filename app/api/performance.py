"""Performance: TWR, XIRR, drawdown, rolling, Sharpe, Sortino, Calmar, attribution."""
from __future__ import annotations

from flask import Blueprint, request

from .. import analytics
from ._helpers import bank_cash_twd, daily_store, envelope, store, want_daily

bp = Blueprint("performance", __name__, url_prefix="/api/performance")


def _bank_cash_forward_fill(months: list[dict]):
    """Closure that returns forward-filled bank cash (TWD) for any ISO date.

    Bank cash is monthly-only (parser knows month-end balance). For days
    inside a month we don't have a daily balance, so we carry the
    most-recent month-end forward. Same pattern as cashflows.py.
    """
    bank_by_month = {m["month"]: bank_cash_twd(m) for m in months}
    sorted_months = sorted(bank_by_month.keys())

    def lookup(date_iso: str) -> float:
        ym = date_iso[:7]
        if ym in bank_by_month:
            return bank_by_month[ym]
        prior = [m for m in sorted_months if m <= ym]
        return bank_by_month[prior[-1]] if prior else 0.0

    return lookup


def _anchor_for_daily(months: list[dict], monthly_cum: list[float], first_daily_date: str) -> float:
    """Monthly cum_twr at the last month-end strictly before first_daily_date.

    Lets the daily TWR chain pick up where the monthly chain left off so
    a portfolio that started before the daily backfill floor doesn't see
    its cumulative reset to 0% on the daily chart. Returns 0.0 when the
    daily window starts before any monthly data (no prior history).
    """
    first_ym = first_daily_date[:7]
    anchor = 0.0
    for i, m in enumerate(months):
        if m["month"] >= first_ym:
            break
        if i < len(monthly_cum):
            anchor = monthly_cum[i]
    return anchor


def _monthly_anchored_cum(
    daily_dates: list[str],
    months: list[dict],
    monthly_cum: list[float],
) -> list[float]:
    """For each daily date, return cum_twr linearly interpolated between
    monthly month-end anchors.

    Daily Modified-Dietz on noisy stocks-only V can drift from the monthly
    chain — TWSE has gaps for some symbols, splits inflate intra-month
    days, and dividends paid into bank look like outflows on the broker
    side. Forcing the chart line through the monthly anchors keeps the
    chart consistent with the headline TWR KPI (which Overview also uses)
    while the equity_twd line keeps daily resolution.

    For dates past the latest month-end, the line stays flat at the last
    monthly cum — matches Overview's behavior of holding TWR steady until
    the next PDF arrives, even though daily V keeps moving (visible in
    the equity line).
    """
    from app.backfill_runner import month_end_iso

    if not months:
        return [0.0] * len(daily_dates)

    anchors: list[tuple[str, float]] = []
    for i, m in enumerate(months):
        if i < len(monthly_cum):
            anchors.append((month_end_iso(m["month"]), monthly_cum[i]))
    anchors.sort()
    if not anchors:
        return [0.0] * len(daily_dates)

    out: list[float] = []
    for d in daily_dates:
        # Find surrounding anchors
        prior = None
        nxt = None
        for a in anchors:
            if a[0] <= d:
                prior = a
            else:
                nxt = a
                break
        if prior is None and nxt is not None:
            # Before first anchor: linear from 0 (assumed start at floor)
            # to first anchor's cum. Use date arithmetic for fraction.
            from datetime import date as _date
            d_dt = _date.fromisoformat(d)
            n_dt = _date.fromisoformat(nxt[0])
            # No earlier anchor — pick the first daily date as the zero point
            base_dt = _date.fromisoformat(daily_dates[0])
            span = max(1, (n_dt - base_dt).days)
            frac = max(0.0, (d_dt - base_dt).days / span)
            out.append(nxt[1] * frac)
        elif prior is not None and nxt is None:
            out.append(prior[1])
        elif prior is not None and nxt is not None:
            from datetime import date as _date
            d_dt = _date.fromisoformat(d)
            p_dt = _date.fromisoformat(prior[0])
            n_dt = _date.fromisoformat(nxt[0])
            span = max(1, (n_dt - p_dt).days)
            frac = (d_dt - p_dt).days / span
            out.append(prior[1] + (nxt[1] - prior[1]) * frac)
        else:
            out.append(0.0)
    return out


def _daily_timeseries() -> dict:
    """Per-day Modified Dietz TWR with monthly KPI semantics preserved.

    The chart's cum_twr line is computed daily — V is stocks-only MV
    (recovered by subtracting `cash_twd` from `portfolio_daily.equity_twd`,
    which folds in synthesized broker cash for the user-facing curve), F
    is broker-side per-day cashflow from `daily_investment_flows`
    (consistent V/F pairing; see that helper's docstring). The chain is
    anchored to the monthly cum at the start of the daily window so the
    curve doesn't reset to 0% mid-history.

    Headline KPIs (twr_total, cagr, max_drawdown, sharpe, sortino, calmar,
    vol) come from the monthly chain so they match /overview exactly. The
    daily layer is for chart resolution, not headline math — Modified
    Dietz at daily grain doesn't equal monthly grain because of
    compounding cadence.

    Display equity (the chart line) is stocks-only plus forward-filled
    bank cash so it lands on the cashflows real-now KPI (~929k) instead
    of stocks-only (~721k). The TWR formula uses stocks-only V (above);
    the broker-cash component lives only in `portfolio_daily.equity_twd`
    for the /today curve.

    Returns None when the daily layer is empty so the caller falls back
    to the monthly envelope.
    """
    s = store()
    months = s.months
    equity_series = daily_store().get_equity_curve()
    if not equity_series or not months:
        return None

    method = _method_param()
    monthly_returns, monthly_cum, monthly_meta = _recomputed(months, method)
    month_labels = [m["month"] for m in months]

    # Modified Dietz V/F pairing: `daily_investment_flows` returns broker-
    # side trade impacts (F = +cost on a buy, −proceeds on a sell), which
    # pairs with a *positions-only* V. portfolio_daily.equity_twd folds in
    # synthesized broker cash so the user-facing curve doesn't plunge on
    # rotation days, but that V mismatches the broker-side F. Subtract
    # cash_twd here to recover positions-only V for the formula input;
    # the display equity below keeps using the user-facing total.
    positions_only_series = [
        {**r, "equity_twd": float(r["equity_twd"]) - float(r.get("cash_twd") or 0)}
        for r in equity_series
    ]
    flow_series = analytics.daily_investment_flows(s.months)
    anchor = _anchor_for_daily(months, monthly_cum, equity_series[0]["date"])
    rows = analytics.daily_twr(
        positions_only_series, flow_series, anchor_cum_return=anchor
    )

    # Override the daily cum_twr with monthly-anchored linear interpolation
    # so the chart line lands exactly on Overview's KPI (+151.73%) at the
    # last month-end and stays flat through the partial current month.
    daily_dates = [r["date"] for r in rows]
    anchored_cum = _monthly_anchored_cum(daily_dates, months, monthly_cum)
    # Drawdown is derived from the *actual* daily TWR series, not the
    # monthly-anchored interpolation. The interpolation is monotonic between
    # anchors, which would force drawdown to ~0 everywhere and hide every
    # intra-month dip — making the chart look broken.
    actual_daily_cum = [r["cum_twr"] for r in rows]
    dd_curve = analytics.drawdown_curve(actual_daily_cum)

    bank_for = _bank_cash_forward_fill(months)
    out_rows = [
        {
            "date": rows[i]["date"],
            "period_return": rows[i]["period_return"],
            "cum_twr": anchored_cum[i],
            # Display equity = stocks (from daily store) + bank cash (forward-filled
            # from monthly). TWR formula upstream uses stocks-only equity_series.
            "equity_twd": rows[i]["equity_twd"] + bank_for(rows[i]["date"]),
            "flow_twd": rows[i]["flow_twd"],
            "drawdown": dd_curve[i]["drawdown"],
            "wealth_index": dd_curve[i]["wealth"],
        }
        for i in range(len(rows))
    ]

    twr_total = monthly_cum[-1] if monthly_cum else 0
    cagr = analytics.cagr_from_cum(twr_total, len(monthly_returns))
    max_dd = analytics.max_drawdown(monthly_cum)

    return {
        "resolution": "daily",
        "monthly": out_rows,  # key kept as 'monthly' for frontend compat
        "method": method,
        "twr_total": twr_total,
        "cagr": cagr,
        "xirr": months[-1].get("xirr"),
        "max_drawdown": max_dd,
        "monthly_volatility": analytics.stdev(monthly_returns),
        "annualized_volatility": analytics.stdev(monthly_returns) * (12 ** 0.5),
        "sharpe_annualized": analytics.sharpe(monthly_returns),
        "sortino_annualized": analytics.sortino(monthly_returns),
        "calmar": analytics.calmar(monthly_returns),
        "best_month": max(out_rows, key=lambda r: r["period_return"]) if out_rows else None,
        "worst_month": min(out_rows, key=lambda r: r["period_return"]) if out_rows else None,
        "positive_months": sum(1 for r in out_rows if r["period_return"] > 0),
        "negative_months": sum(1 for r in out_rows if r["period_return"] < 0),
        "hit_rate": (
            sum(1 for r in out_rows if r["period_return"] > 0)
            / max(1, sum(1 for r in out_rows if r["period_return"] != 0))
        ),
        "drawdown_episodes": analytics.drawdown_episodes(monthly_cum, month_labels),
    }


_VALID_METHODS = {"day_weighted", "mid_month", "eom"}


def _method_param() -> str:
    """Read ?method= from request, defaulting to day_weighted."""
    m = (request.args.get("method") or "day_weighted").lower()
    return m if m in _VALID_METHODS else "day_weighted"


def _recomputed(months: list[dict], method: str) -> tuple[list[float], list[float], list[dict]]:
    """Return (period_returns, cum_twr, per_month_meta) under chosen method."""
    rows = analytics.period_returns(months, method=method)  # type: ignore[arg-type]
    pr = [r["period_return"] for r in rows]
    cum = analytics.cumulative_curve(pr)
    return pr, cum, rows


@bp.get("/timeseries")
def timeseries():
    if want_daily():
        daily = _daily_timeseries()
        if daily is not None:
            return envelope(daily)

    s = store()
    months = s.months
    if not months:
        return envelope({
            "empty": True,
            "monthly": [],
            "twr_total": 0,
            "xirr": None,
            "max_drawdown": 0,
            "monthly_volatility": 0,
            "annualized_volatility": 0,
            "sharpe_annualized": 0,
            "best_month": None,
            "worst_month": None,
            "positive_months": 0,
            "negative_months": 0,
        })

    method = _method_param()
    period_returns, cum, meta = _recomputed(months, method)
    dd = analytics.drawdown_curve(cum)
    month_labels = [m["month"] for m in months]

    rows = []
    for i, m in enumerate(months):
        rows.append({
            "month": m["month"],
            "period_return": period_returns[i],
            "cum_twr": cum[i],
            "v_start": meta[i]["v_start"],
            # Display equity = stocks + bank cash (matches /overview real_now KPI).
            # The TWR formula in `_recomputed` uses stocks-only `m.equity_twd`.
            "equity_twd": (m.get("equity_twd", 0) or 0) + bank_cash_twd(m),
            "external_flow": m.get("external_flow_twd", 0),
            "weighted_flow": meta[i]["weighted_flow"],
            "days_in_month": meta[i]["days_in_month"],
            "drawdown": dd[i]["drawdown"],
            "wealth_index": dd[i]["wealth"],
        })

    twr_total = cum[-1] if cum else 0
    n = len(period_returns)
    cagr = analytics.cagr_from_cum(twr_total, n)
    episodes = analytics.drawdown_episodes(cum, month_labels)

    # XIRR is independent of period_return method (it works on absolute
    # cashflows, not %-returns). Re-use the parser-stored value if we kept
    # the legacy method, else fall back to None for now.
    xirr = months[-1].get("xirr") if method == "mid_month" else months[-1].get("xirr")

    return envelope({
        "monthly": rows,
        "method": method,
        "twr_total": twr_total,
        "cagr": cagr,
        "xirr": xirr,
        "max_drawdown": analytics.max_drawdown(cum),
        "monthly_volatility": analytics.stdev(period_returns),
        "annualized_volatility": analytics.stdev(period_returns) * (12 ** 0.5),
        "sharpe_annualized": analytics.sharpe(period_returns),
        "sortino_annualized": analytics.sortino(period_returns),
        "calmar": analytics.calmar(period_returns),
        "best_month": max(rows, key=lambda r: r["period_return"]) if rows else None,
        "worst_month": min(rows, key=lambda r: r["period_return"]) if rows else None,
        "positive_months": sum(1 for r in rows if r["period_return"] > 0),
        "negative_months": sum(1 for r in rows if r["period_return"] < 0),
        "hit_rate": (
            sum(1 for r in rows if r["period_return"] > 0) /
            max(1, sum(1 for r in rows if r["period_return"] != 0))
        ),
        "drawdown_episodes": episodes,
    })


@bp.get("/rolling")
def rolling():
    if want_daily():
        equity_series = daily_store().get_equity_curve()
        if equity_series:
            s = store()
            # Same V/F pairing as /timeseries: subtract synthesized broker
            # cash so V is positions-only, matching the broker-side flow
            # definition (`daily_investment_flows`). Without this, sells
            # produce phantom positive returns because ΔV ≈ 0 but F < 0.
            positions_only_series = [
                {**r, "equity_twd": float(r["equity_twd"]) - float(r.get("cash_twd") or 0)}
                for r in equity_series
            ]
            flow_series = analytics.daily_investment_flows(s.months)
            rows = analytics.daily_twr(positions_only_series, flow_series)
            pr = [r["period_return"] for r in rows]
            dates = [r["date"] for r in rows]
            return envelope({
                "resolution": "daily",
                "rolling_30d": [
                    {"date": d, "value": v}
                    for d, v in zip(dates, analytics.rolling_returns(pr, 30))
                ],
                "rolling_60d": [
                    {"date": d, "value": v}
                    for d, v in zip(dates, analytics.rolling_returns(pr, 60))
                ],
                "rolling_90d": [
                    {"date": d, "value": v}
                    for d, v in zip(dates, analytics.rolling_returns(pr, 90))
                ],
                "rolling_sharpe_60d": [
                    {"date": d, "value": v}
                    for d, v in zip(dates, analytics.rolling_sharpe(pr, 60))
                ],
            })

    s = store()
    months = s.months
    method = _method_param()
    period_returns, _, _ = _recomputed(months, method)
    return envelope({
        "rolling_3m": [
            {"month": m["month"], "value": v}
            for m, v in zip(months, analytics.rolling_returns(period_returns, 3))
        ],
        "rolling_6m": [
            {"month": m["month"], "value": v}
            for m, v in zip(months, analytics.rolling_returns(period_returns, 6))
        ],
        "rolling_12m": [
            {"month": m["month"], "value": v}
            for m, v in zip(months, analytics.rolling_returns(period_returns, 12))
        ],
        "rolling_sharpe_6m": [
            {"month": m["month"], "value": v}
            for m, v in zip(months, analytics.rolling_sharpe(period_returns, 6))
        ],
    })


@bp.get("/attribution")
def attribution():
    """Per-segment contribution to total return (TW vs Foreign vs FX).

    Uses venue-level trade flows from broker statements as the ground truth
    for what was deployed into each segment, then computes
        segment P&L = ΔMV − net flow into the segment.

    Foreign P&L is decomposed further: FX contribution = previous USD
    holding × ΔFX rate; price contribution = remainder. The previous
    version pulled flows from a key set that didn't exist, so it always
    reported ΔMV (wrong whenever there were buys/sells).
    """
    s = store()
    months = s.months
    if not months:
        return envelope([])

    venue_flows = {v["month"]: v for v in s.venue_flows_twd}

    out = []
    cum_tw = cum_fr = cum_fx = 0.0
    for i, m in enumerate(months):
        prev = months[i - 1] if i > 0 else None
        tw_mv = m.get("tw_market_value_twd", 0) or 0
        fr_mv = m.get("foreign_market_value_twd", 0) or 0
        prev_tw = (prev.get("tw_market_value_twd", 0) or 0) if prev else 0
        prev_fr = (prev.get("foreign_market_value_twd", 0) or 0) if prev else 0

        vf = venue_flows.get(m["month"], {})
        tw_net_flow = (vf.get("tw_buy_twd", 0) or 0) - (vf.get("tw_sell_twd", 0) or 0)
        fr_net_flow = (vf.get("foreign_buy_twd", 0) or 0) - (vf.get("foreign_sell_twd", 0) or 0)

        tw_pnl = tw_mv - prev_tw - tw_net_flow
        fr_pnl_total = fr_mv - prev_fr - fr_net_flow

        prev_fx = (prev.get("fx_usd_twd") or 1) if prev else 1
        curr_fx = m.get("fx_usd_twd") or prev_fx
        prev_usd = (prev_fr / prev_fx) if (prev and prev_fx) else 0
        fx_component = prev_usd * (curr_fx - prev_fx)
        fr_price_pnl = fr_pnl_total - fx_component

        cum_tw += tw_pnl
        cum_fr += fr_price_pnl
        cum_fx += fx_component

        out.append({
            "month": m["month"],
            "tw_pnl": tw_pnl,
            "tw_net_flow": tw_net_flow,
            "tw_mv_end": tw_mv,
            "foreign_pnl_price": fr_price_pnl,
            "foreign_pnl_fx": fx_component,
            "foreign_pnl_total": fr_pnl_total,
            "foreign_net_flow": fr_net_flow,
            "foreign_mv_end": fr_mv,
            "cum_tw_pnl": cum_tw,
            "cum_foreign_price_pnl": cum_fr,
            "cum_fx_pnl": cum_fx,
        })

    return envelope({
        "monthly": out,
        "totals": {
            "tw_pnl_twd": cum_tw,
            "foreign_price_pnl_twd": cum_fr,
            "fx_pnl_twd": cum_fx,
            "total_pnl_twd": cum_tw + cum_fr + cum_fx,
        },
    })
