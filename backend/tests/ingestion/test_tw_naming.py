"""Reproducer for invest.ingestion.tw_naming.

Pure string + dict logic — no PDF, no SDK, no DB. Three layers:

  normalize_tw_name(s)        -> str
      Fullwidth (０-９Ａ-Ｚ＊) → halfwidth fold so '台灣５０' matches
      '台灣50' for downstream code-resolution. Single fold table; not a
      Unicode-NFC general normalizer.

  load_overrides(path)        -> dict[str, str]
      File I/O boundary. Reads data/tw_ticker_map.json, strips
      comment/private keys ('_'-prefixed), strips empty values, and
      normalizes every key the same way trade names will be normalized
      at lookup time. Pure dict out.

  build_name_to_code(holdings, overrides) -> dict[str, str]
      Pure composition. Holdings dicts (flat list across months) seed
      the map; overrides win on key collision. Why both: holdings cover
      anything held at any month-end; overrides plug intra-month
      round-trips and pre-window exits that never appear in a holdings
      table.

  resolve_tw_code(trade_name, name_to_code) -> str
      Exact normalized match preferred. Falls back to a guarded prefix
      match (length floor 3, ratio cap 2.5×) so '致茂' (4 chars) does
      NOT inherit '致茂富邦57購' (8 chars, ratio 2.0×) but '貿聯KY'
      (5 chars) DOES hit '貿聯-KY' (7 chars, ratio 1.4×).

The split (loader vs builder vs resolver) is intentional: the legacy
parse_statements.py interleaved file I/O with mapping construction,
which made the priority rule ('overrides win') harder to test without
a fixture JSON file on disk. Here load_overrides is the only boundary;
build_name_to_code and resolve_tw_code take literal dicts.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from invest.ingestion.tw_naming import (
    build_name_to_code,
    load_overrides,
    normalize_tw_name,
    resolve_tw_code,
)


# --- normalize_tw_name ---------------------------------------------------


class TestNormalizeTwName:
    def test_empty_string_returns_empty(self):
        assert normalize_tw_name("") == ""

    def test_none_returns_empty(self):
        """Trade rows occasionally arrive with name=None when the parser
        couldn't extract the column. Treat as empty rather than crash."""
        assert normalize_tw_name(None) == ""

    def test_halfwidth_passes_through(self):
        assert normalize_tw_name("台灣50") == "台灣50"

    def test_fullwidth_digits_folded(self):
        """The PDF print engine renders some lines with fullwidth ASCII
        (U+FF01–FF5E). Fold them so trade names match holdings names."""
        assert normalize_tw_name("台灣５０") == "台灣50"

    def test_fullwidth_letters_folded(self):
        assert normalize_tw_name("貿聯ＫＹ") == "貿聯KY"

    def test_fullwidth_asterisk_folded(self):
        """U+FF0A '＊' (CJK fullwidth asterisk) → ASCII '*'. Used by the
        margin-tagged-row name suffix in some statement layouts."""
        assert normalize_tw_name("國巨＊") == "國巨*"

    def test_strips_surrounding_whitespace(self):
        assert normalize_tw_name("  台積電 ") == "台積電"

    def test_cjk_passthrough_unchanged(self):
        """INVARIANT: pure CJK (no fullwidth-ASCII range) untouched."""
        assert normalize_tw_name("台積電") == "台積電"


# --- load_overrides ------------------------------------------------------


class TestLoadOverrides:
    def test_missing_file_returns_empty_dict(self, tmp_path: Path):
        """No override file is the common case for fresh installs.
        Must NOT raise — the parser still works without it."""
        assert load_overrides(tmp_path / "missing.json") == {}

    def test_skips_comment_and_underscore_keys(self, tmp_path: Path):
        """The real override file ships with a '_comment' key
        documenting itself. That must not pollute the map."""
        f = tmp_path / "tw.json"
        f.write_text(json.dumps({
            "_comment": "doc string here",
            "_private": "ignored",
            "台玻": "1802",
        }))
        out = load_overrides(f)
        assert out == {"台玻": "1802"}

    def test_skips_empty_values(self, tmp_path: Path):
        """Operators sometimes leave an empty string when they don't yet
        know the code. Skip — empty != mapping."""
        f = tmp_path / "tw.json"
        f.write_text(json.dumps({"台玻": "1802", "未知": ""}))
        out = load_overrides(f)
        assert out == {"台玻": "1802"}

    def test_normalizes_keys_at_load_time(self, tmp_path: Path):
        """INVARIANT: keys are normalized at load so callers can look up
        with the same normalize_tw_name pipeline trade names go through.
        Otherwise a fullwidth override key would silently never match."""
        f = tmp_path / "tw.json"
        f.write_text(json.dumps({"台灣５０": "0050"}))  # fullwidth digits
        out = load_overrides(f)
        assert out == {"台灣50": "0050"}

    def test_coerces_non_string_values_to_str(self, tmp_path: Path):
        """Defensive: a hand-edited JSON file might have an int code.
        Coerce so downstream string-comparison works."""
        f = tmp_path / "tw.json"
        f.write_text(json.dumps({"台玻": 1802}))
        out = load_overrides(f)
        assert out == {"台玻": "1802"}


# --- build_name_to_code --------------------------------------------------


class TestBuildNameToCode:
    def test_empty_inputs_return_empty(self):
        assert build_name_to_code([], {}) == {}

    def test_holdings_seed_the_map(self):
        holdings = [
            {"name": "台積電", "code": "2330"},
            {"name": "台灣50", "code": "0050"},
        ]
        out = build_name_to_code(holdings, {})
        assert out == {"台積電": "2330", "台灣50": "0050"}

    def test_holdings_first_occurrence_wins(self):
        """A name that appears in multiple month-end snapshots will be
        seen multiple times. setdefault-style: keep the first."""
        holdings = [
            {"name": "台積電", "code": "2330"},
            {"name": "台積電", "code": "WRONG"},
        ]
        out = build_name_to_code(holdings, {})
        assert out["台積電"] == "2330"

    def test_overrides_win_over_holdings(self):
        """INVARIANT: overrides are the manual-fix layer. They MUST
        beat holdings-derived entries — the operator added them
        precisely because the holdings derivation was wrong/missing."""
        holdings = [{"name": "台玻", "code": "WRONG"}]
        overrides = {"台玻": "1802"}
        out = build_name_to_code(holdings, overrides)
        assert out["台玻"] == "1802"

    def test_skips_holdings_with_missing_name_or_code(self):
        """Defensive against parser gaps. A holdings dict without a
        name or with an empty code can't contribute a mapping."""
        holdings = [
            {"name": "", "code": "2330"},
            {"name": "台積電", "code": ""},
            {"name": None, "code": "0050"},
            {"name": "台積電", "code": "2330"},
        ]
        out = build_name_to_code(holdings, {})
        assert out == {"台積電": "2330"}

    def test_normalizes_holdings_names(self):
        """Holdings names from the PDF holdings table CAN have fullwidth
        characters too. Normalize at build time so resolve_tw_code's
        normalized lookups hit them."""
        holdings = [{"name": "台灣５０", "code": "0050"}]
        out = build_name_to_code(holdings, {})
        assert out == {"台灣50": "0050"}


# --- resolve_tw_code -----------------------------------------------------


class TestResolveTwCode:
    def test_empty_name_returns_empty(self):
        assert resolve_tw_code("", {"台積電": "2330"}) == ""

    def test_exact_match_wins(self):
        assert resolve_tw_code("台積電", {"台積電": "2330"}) == "2330"

    def test_normalizes_input_before_lookup(self):
        """A trade row with fullwidth digits must hit a halfwidth-keyed
        map. The whole point of the normalize+map pipeline."""
        assert resolve_tw_code("台灣５０", {"台灣50": "0050"}) == "0050"

    def test_no_match_returns_empty(self):
        assert resolve_tw_code("不存在", {"台積電": "2330"}) == ""

    def test_short_name_below_floor_skips_prefix_search(self):
        """INVARIANT: prefix-match floor is 3 chars. Below that the
        false-match risk explodes (every two-char input would prefix-
        match too many candidates). Return empty rather than guess."""
        # '台' is 1 char — even though '台積電' starts with it, refuse.
        assert resolve_tw_code("台", {"台積電": "2330"}) == ""
        # '台積' is 2 chars — same reason.
        assert resolve_tw_code("台積", {"台積電": "2330"}) == ""

    def test_prefix_match_within_ratio_cap(self):
        """INVARIANT: prefix match accepted when len(holding)/len(trade)
        < 2.5. '貿聯KY' (5) → '貿聯-KY' (7), ratio 1.4 — OK."""
        assert resolve_tw_code("貿聯KY", {"貿聯-KY": "3665"}) == "3665"

    def test_prefix_match_rejected_when_ratio_too_loose(self):
        """INVARIANT: ratio cap 2.5× prevents '致茂' (4) from inheriting
        '致茂富邦57購' (8, ratio 2.0×) — actually ratio 2.0 < 2.5 so
        it'd match. Use 3-char trade vs 8-char holding (ratio 2.67×)
        to land OUTSIDE the cap."""
        # '致茂' (2 chars) is BELOW the floor anyway, so use a
        # 3-char trade against an 8-char holding (ratio 2.67×).
        m = {"致茂富邦57購": "042900"}
        assert resolve_tw_code("ABC", m) == ""  # no prefix
        # And a real ratio-cap rejection: 3-char trade, 8-char holding.
        m2 = {"AAABBBCCC": "9999"}  # 9 chars
        assert resolve_tw_code("AAA", m2) == ""  # 9/3 = 3.0× > 2.5

    def test_prefix_match_at_exact_ratio_boundary(self):
        """INVARIANT: ratio is strictly less than 2.5×. Exactly 2.5
        rejects (the legacy code was '<' not '<=')."""
        # 4-char trade, 10-char holding → 2.5× exactly → reject.
        assert resolve_tw_code("AAAA", {"AAAABBBBCC": "1111"}) == ""

    def test_first_matching_prefix_wins(self):
        """When multiple holdings have the same prefix, the first one
        encountered in the dict wins. Insertion-order-preserving since
        Python 3.7. Document the contract."""
        # Use 3-char trade against multi-prefix dict.
        m = {"AAAB": "0001", "AAAC": "0002"}  # both 4-char, ratio 1.33×
        # 'AAA' prefix-matches both; first wins.
        assert resolve_tw_code("AAA", m) == "0001"
