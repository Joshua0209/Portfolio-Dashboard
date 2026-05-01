"""TW name → ticker code resolution primitives.

Three pure layers + one I/O boundary, used by tw_parser.py to attach
ticker codes to TW trade rows (the trade table prints only the
abbreviated stock name, not the code).

Lookup priority during resolution:
  1. Exact match on normalized name.
  2. Guarded prefix match: holding name starts with trade name AND
     len(holding)/len(trade) < 2.5 AND len(trade) >= 3.

The guards exist because TW stock names share prefixes liberally —
e.g. '致茂' (代號 2360) vs '致茂富邦57購' (代號 042900). Without the
floor + ratio cap, the trade-name parser would silently inherit the
wrong code from a structured-product holding.

Source-priority during build:
  overrides > holdings.

Overrides come from data/tw_ticker_map.json — the operator-curated
fallback for trade names that never appear in any month-end holdings
table (intra-month round-trips, pre-window exits).
"""
from __future__ import annotations

import json
from pathlib import Path

_PREFIX_MATCH_FLOOR = 3
_PREFIX_MATCH_RATIO_CAP = 2.5


def normalize_tw_name(s: str | None) -> str:
    """Fullwidth ASCII fold so '台灣５０' matches '台灣50'.

    Folds U+FF01-FF5E (fullwidth ASCII) → U+0021-007E (halfwidth) and
    U+FF0A '＊' (CJK fullwidth asterisk) → '*'. Other code points pass
    through unchanged. Strips surrounding whitespace.

    None or empty → ''.
    """
    if not s:
        return ""
    out: list[str] = []
    for c in s:
        cp = ord(c)
        if 0xFF01 <= cp <= 0xFF5E:
            out.append(chr(cp - 0xFEE0))
        elif c == "＊":
            out.append("*")
        else:
            out.append(c)
    return "".join(out).strip()


def load_overrides(path: Path) -> dict[str, str]:
    """Read the manual TW name → code override file.

    Returns {} when the file does not exist (fresh installs are
    expected to operate without it). Strips '_'-prefixed keys (the
    canonical file ships with a '_comment' self-doc), strips empty
    values, normalizes keys via normalize_tw_name so callers can look
    up with their normalized trade names, and coerces non-string
    values to str.
    """
    if not path.exists():
        return {}
    raw = json.loads(path.read_text())
    out: dict[str, str] = {}
    for k, v in raw.items():
        if k.startswith("_") or not v:
            continue
        out[normalize_tw_name(k)] = str(v)
    return out


def build_name_to_code(
    holdings: list[dict],
    overrides: dict[str, str],
) -> dict[str, str]:
    """Compose holdings-derived names with overrides into one map.

    Holdings is a flat list of dicts (each with 'name' and 'code'),
    flattened across every parsed month before this call. Holdings
    seed the map with first-occurrence-wins semantics (so the
    earliest-month appearance fixes the binding). Overrides are then
    layered on top — they ALWAYS win on key collision.

    Holdings names are normalized at build time (PDF holdings tables
    occasionally print fullwidth characters too).
    """
    base: dict[str, str] = {}
    for h in holdings:
        n = normalize_tw_name(h.get("name"))
        code = h.get("code")
        if n and code:
            base.setdefault(n, str(code))
    return base | overrides


def resolve_tw_code(trade_name: str, name_to_code: dict[str, str]) -> str:
    """Look up a ticker code for a TW trade-row name.

    Empty/None input → ''. Exact normalized match wins. Falls back to
    a guarded prefix match: holding name must START WITH the
    normalized trade name, the trade name must be at least 3
    characters long, and the length ratio must be strictly less than
    2.5×. First match wins on prefix collisions (insertion order).

    No match → ''.
    """
    n = normalize_tw_name(trade_name)
    if not n:
        return ""
    if n in name_to_code:
        return name_to_code[n]
    if len(n) < _PREFIX_MATCH_FLOOR:
        return ""
    for holding_name, code in name_to_code.items():
        if holding_name.startswith(n) and len(holding_name) / len(n) < _PREFIX_MATCH_RATIO_CAP:
            return code
    return ""
