"""Jinja filters for currency, percent, and date formatting."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from flask import Flask


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fmt_twd(value: Any, decimals: int = 0) -> str:
    n = _to_float(value)
    if n is None:
        return "—"
    return f"NT${n:,.{decimals}f}"


def fmt_usd(value: Any, decimals: int = 2) -> str:
    n = _to_float(value)
    if n is None:
        return "—"
    return f"${n:,.{decimals}f}"


def fmt_int(value: Any) -> str:
    n = _to_float(value)
    if n is None:
        return "—"
    return f"{n:,.0f}"


def fmt_pct(value: Any, decimals: int = 2) -> str:
    n = _to_float(value)
    if n is None:
        return "—"
    return f"{n * 100:+.{decimals}f}%"


def fmt_pct_unsigned(value: Any, decimals: int = 2) -> str:
    n = _to_float(value)
    if n is None:
        return "—"
    return f"{n * 100:.{decimals}f}%"


def fmt_month(value: Any) -> str:
    if not value:
        return "—"
    s = str(value)
    try:
        dt = datetime.strptime(s, "%Y-%m")
        return dt.strftime("%b %Y")
    except ValueError:
        return s


def register(app: Flask) -> None:
    app.add_template_filter(fmt_twd, "twd")
    app.add_template_filter(fmt_usd, "usd")
    app.add_template_filter(fmt_int, "intcomma")
    app.add_template_filter(fmt_pct, "pct")
    app.add_template_filter(fmt_pct_unsigned, "pct_abs")
    app.add_template_filter(fmt_month, "month")
