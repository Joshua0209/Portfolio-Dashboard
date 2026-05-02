"""Sector classifier (heuristic, no external API).
Hand-curated lookup dicts mapping bare ticker codes to sector
labels for the /risk and /holdings concentration views. Two pure
functions:
  sector_of(code, venue)       -> str
  sector_breakdown(holdings)   -> list of per-sector aggregates
The dicts here are a verbatim port of app/analytics.py's
_TW_SECTOR_HINTS and _US_SECTOR_HINTS. No tests pin individual
entries — those drift over time and the right place to catch
staleness is review, not unit tests.
"""
from collections import defaultdict
from typing import Any
_TW_SECTOR_HINTS: dict[str, str] = {
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
_US_SECTOR_HINTS: dict[str, str] = {
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
    """Classify a single ticker into a sector label.
    Empty code returns 'Unknown' (data-quality flag, not a venue
    bucket — an empty code shouldn't skew TW or US weights).
    Venue fallbacks:
      TW  -> 'TW Equity (other)'   (for unmapped TW tickers)
      US  -> 'US Equity (other)'   (for unmapped US tickers)
      HK  -> 'HK Equity (other)'
      JP  -> 'JP Equity (other)'
      any other venue -> 'Unknown'
    """
    if not code:
        return "Unknown"
    if venue == "TW":
        return _TW_SECTOR_HINTS.get(code, "TW Equity (other)")
    if venue == "US":
        return _US_SECTOR_HINTS.get(code.upper(), "US Equity (other)")
    if venue == "HK":
        return "HK Equity (other)"
    if venue == "JP":
        return "JP Equity (other)"
    return "Unknown"
def sector_breakdown(holdings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate per-holding market values into per-sector totals.
    Each holding dict needs `code`, `venue`, `mkt_value_twd`. Missing
    or None mkt_value_twd is treated as 0.
    Returns a list sorted descending by `value_twd`, each entry:
      {sector, value_twd, count, weight}
    weight = value_twd / total_value, or 0 if total_value is 0
    (avoids ZeroDivisionError on cold-start backfill).
    """
    by_sector: dict[str, dict[str, float | int]] = defaultdict(
        lambda: {"value": 0.0, "count": 0}
    )
    total = 0.0
    for h in holdings:
        sec = sector_of(h.get("code", ""), h.get("venue", ""))
        v = h.get("mkt_value_twd", 0) or 0
        by_sector[sec]["value"] += v
        by_sector[sec]["count"] += 1
        total += v
    out: list[dict[str, Any]] = []
    for sec, agg in by_sector.items():
        out.append(
            {
                "sector": sec,
                "value_twd": agg["value"],
                "count": agg["count"],
                "weight": (agg["value"] / total) if total else 0,
            }
        )
    out.sort(key=lambda r: r["value_twd"], reverse=True)
    return out
