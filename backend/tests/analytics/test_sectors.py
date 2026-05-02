"""Reproducer for invest.analytics.sectors.

Hand-curated heuristic, no external API. Two pure functions over a
pair of lookup dicts:

  sector_of(code, venue)       -> str   (single-symbol classifier)
  sector_breakdown(holdings)   -> list  (per-sector aggregate with
                                          value, count, weight)

The lookup dicts are intentionally not test-mirror'd — pinning every
individual entry would be a maintenance burden with no real-world
regression value (ETF tickers and company classifications drift
slowly enough that the right place to catch staleness is review,
not unit tests). What IS pinned: the *shape* of dispatch (TW vs
US vs empty), the default fallthrough, and the breakdown
aggregation.
"""
from __future__ import annotations

from invest.analytics.sectors import sector_breakdown, sector_of


# --- sector_of -----------------------------------------------------------


class TestSectorOf:
    def test_known_tw_code_returns_curated_sector(self):
        assert sector_of("2330", "TW") == "Semiconductors"

    def test_known_us_code_returns_curated_sector(self):
        assert sector_of("NVDA", "US") == "Semiconductors"

    def test_us_lookup_is_case_insensitive(self):
        """Tickers in trade data sometimes arrive lowercased.
        US dispatch must upper before lookup."""
        assert sector_of("nvda", "US") == "Semiconductors"

    def test_unknown_tw_falls_through_to_other_bucket(self):
        assert sector_of("9999", "TW") == "TW Equity (other)"

    def test_unknown_us_falls_through_to_other_bucket(self):
        assert sector_of("ZZZZ", "US") == "US Equity (other)"

    def test_empty_code_returns_unknown(self):
        """An empty code is a data-quality issue (parser missed
        the ticker resolution); flag it as Unknown rather than
        slotting it into a venue bucket where it'd skew weights."""
        assert sector_of("", "TW") == "Unknown"
        assert sector_of("", "US") == "Unknown"

    def test_hk_venue_returns_hk_equity_other(self):
        """HK venue has no curated dict; must fall to HK bucket,
        not silently bleed into US Equity (other)."""
        assert sector_of("0700", "HK") == "HK Equity (other)"

    def test_jp_venue_returns_jp_equity_other(self):
        """JP venue must use its own fallback bucket."""
        assert sector_of("7203", "JP") == "JP Equity (other)"

    def test_unknown_venue_returns_venue_bucket(self):
        """A venue not in the curated set returns a venue-prefixed
        bucket rather than leaking into US Equity (other)."""
        result = sector_of("1234", "SG")
        assert result == "SG Equity (other)"


# --- sector_breakdown ----------------------------------------------------


class TestSectorBreakdown:
    def test_empty_input_returns_empty(self):
        assert sector_breakdown([]) == []

    def test_aggregates_by_sector_with_weight_summing_to_one(self):
        holdings = [
            {"code": "2330", "venue": "TW", "mkt_value_twd": 1_000_000},
            {"code": "2454", "venue": "TW", "mkt_value_twd": 500_000},
            {"code": "NVDA", "venue": "US", "mkt_value_twd": 250_000},
            {"code": "AAPL", "venue": "US", "mkt_value_twd": 250_000},
        ]
        result = sector_breakdown(holdings)

        # Three sectors expected: Semiconductors (2330+2454+NVDA),
        # Hardware/Tech (AAPL).
        sectors = {r["sector"]: r for r in result}
        assert "Semiconductors" in sectors
        assert sectors["Semiconductors"]["value_twd"] == 1_750_000
        assert sectors["Semiconductors"]["count"] == 3
        assert sectors["Hardware/Tech"]["value_twd"] == 250_000
        assert sectors["Hardware/Tech"]["count"] == 1

        # Weights sum to ~1 (within float precision).
        total_weight = sum(r["weight"] for r in result)
        assert abs(total_weight - 1.0) < 1e-9

    def test_sorted_descending_by_value(self):
        holdings = [
            {"code": "AAPL", "venue": "US", "mkt_value_twd": 100},
            {"code": "2330", "venue": "TW", "mkt_value_twd": 1000},
            {"code": "NVDA", "venue": "US", "mkt_value_twd": 500},
        ]
        result = sector_breakdown(holdings)
        values = [r["value_twd"] for r in result]
        assert values == sorted(values, reverse=True)

    def test_zero_total_avoids_division_by_zero(self):
        """Edge case: every holding has 0 market value (e.g. fresh
        backfill before prices arrive). Don't crash; return entries
        with weight=0."""
        holdings = [
            {"code": "2330", "venue": "TW", "mkt_value_twd": 0},
            {"code": "NVDA", "venue": "US", "mkt_value_twd": 0},
        ]
        result = sector_breakdown(holdings)
        assert all(r["weight"] == 0 for r in result)

    def test_missing_or_none_value_treated_as_zero(self):
        """Defensive: a holding with mkt_value_twd absent or None
        contributes 0 to the bucket without breaking aggregation."""
        holdings = [
            {"code": "2330", "venue": "TW", "mkt_value_twd": 100},
            {"code": "AAPL", "venue": "US"},  # missing
            {"code": "NVDA", "venue": "US", "mkt_value_twd": None},
        ]
        result = sector_breakdown(holdings)
        sectors = {r["sector"]: r for r in result}
        assert sectors["Semiconductors"]["value_twd"] == 100
        # Missing/None values contribute 0 but still count
        assert sectors["Hardware/Tech"]["count"] == 1
        assert sectors["Semiconductors"]["count"] == 2  # 2330 + NVDA(None)
