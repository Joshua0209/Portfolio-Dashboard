"""Phase 11 Path A — audit hook: SDK leg count vs PDF trade count.

Documents the originally-planned STRICT firing rule. As of 2026-05-01
the strict rule was retired — it produced 8 false positives on 8 closed
pairs because SDK FIFO leg counts and PDF all-time buy counts are
structurally apples-to-oranges.

Phase 14.5 (2026-05-03) moved the audit hook out of
``brokerage/trade_overlay.py`` and into
``reconciliation/shioaji_audit.py`` (Option B — PDF coverage gap by
``(date, qty)`` keys). ``trade_overlay.merge()`` no longer fires
reconcile events on the write path; the audit runs as a post-overlay
step inside ``jobs.snapshot.run``.

These tests stay in the codebase as scenario documentation for the
originally-planned strict rule. They are ``pytest.skip``-ped because
the strict rule is permanently abandoned. The lone active test
asserts merge() stays silent — a regression guard for the extraction.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from invest.brokerage import trade_overlay
from invest.persistence.daily_store import DailyStore
from invest.reconciliation import reconcile

_POLICY_SILENT_REASON = (
    "Strict count-mismatch rule permanently abandoned (false-positive "
    "surge 2026-05-01). Extracted audit policy lives in "
    "reconciliation/shioaji_audit.audit_realized_pairs (Option B)."
)


@pytest.fixture()
def store(tmp_path: Path) -> DailyStore:
    s = DailyStore(tmp_path / "audit.db")
    s.init_schema()
    # Seed a price so positions_daily can be written for the gap.
    with s.connect_rw() as conn:
        conn.execute(
            "INSERT INTO prices(date, symbol, close, currency, source, ingested_at)"
            " VALUES ('2026-04-22', '7769', 210.0, 'TWD', 'yfinance', '2026-04-22T00:00:00Z')"
        )
    return s


def _portfolio_with_pdf_trades(*trades) -> dict:
    """Build a portfolio.json shape with the given PDF trade rows.

    Each trade is a (date, code, side, qty, price) tuple.
    """
    return {
        "months": [{"month": "2026-03",
                    "tw": {"holdings": []},
                    "foreign": {"holdings": []}}],
        "summary": {
            "all_trades": [
                {"month": d[:7], "date": d.replace("-", "/"), "venue": "TW",
                 "side": s, "code": c, "qty": q, "price": p, "ccy": "TWD"}
                for (d, c, s, q, p) in trades
            ],
        },
    }


class _AuditFakeClient:
    """Fake client that lets each test wire just the data it needs."""

    def __init__(self, lots=None, pairs=None, trades=None):
        self._lots = lots or []
        self._pairs = pairs or []
        self._trades = trades or []
        self.configured = True

    def lazy_login(self):
        return True

    def list_trades(self, start, end):
        return [t for t in self._trades if start <= t["date"] <= end]

    def list_open_lots(self, close_resolver=None):
        return list(self._lots)

    def list_realized_pairs(self, start, end):
        # Return everything; the merge layer is expected to filter sells
        # by window itself if needed. Buy legs may pre-date start
        # (Decision C).
        return list(self._pairs)


# --- Strict firing rule tests --------------------------------------------


@pytest.mark.skip(reason=_POLICY_SILENT_REASON)
def test_audit_fires_when_sdk_has_more_legs_than_pdf(store):
    """SDK reports 5 buy legs; PDF has 3 buy trades for code '7769' before
    the sell date. Strict rule fires regardless of qty match."""
    portfolio = _portfolio_with_pdf_trades(
        ("2025-11-15", "7769", "普買", 1000, 200.0),
        ("2025-12-03", "7769", "普買", 1000, 205.0),
        ("2026-01-12", "7769", "普買", 1000, 198.0),
    )

    pairs = [
        {"date": d, "code": "7769", "side": "普買", "qty": q,
         "price": p, "cost_twd": q * p, "ccy": "TWD", "venue": "TW",
         "type": "現股", "pair_id": 101}
        for (d, q, p) in [
            ("2025-11-15", 1000, 200.0),
            ("2025-12-03", 1000, 205.0),
            ("2026-01-12", 1000, 198.0),
            ("2026-02-08", 1000, 212.0),
            ("2026-02-20", 10,   208.0),  # odd-lot leg PDF doesn't have
        ]
    ]
    pairs.append({
        "date": "2026-04-15", "code": "7769", "side": "普賣",
        "qty": 4010, "price": 210.0, "ccy": "TWD", "venue": "TW",
        "type": "現股", "pair_id": 101, "pnl": 12_345.0,
    })

    fake = _AuditFakeClient(pairs=pairs)
    trade_overlay.merge(
        store, portfolio, fake, gap_start="2026-04-01", gap_end="2026-04-26"
    )

    events = reconcile.get_open_events(store)
    audit = [
        e for e in events
        if json.loads(e["diff_summary"]).get("event_type")
           == "broker_pdf_buy_leg_mismatch"
    ]
    assert len(audit) == 1
    payload = json.loads(audit[0]["diff_summary"])
    detail = payload["detail"]
    assert detail["pair_id"] == 101
    assert detail["code"] == "7769"
    assert detail["sell_date"] == "2026-04-15"
    assert detail["sdk_leg_count"] == 5
    assert detail["pdf_trade_count"] == 3
    assert len(detail["sdk_legs"]) == 5
    assert len(detail["pdf_trades"]) == 3


@pytest.mark.skip(reason=_POLICY_SILENT_REASON)
def test_audit_fires_when_pdf_has_more_trades_than_sdk(store):
    """Mirror case: PDF has 3 buy trades, SDK has 1 leg → fire (count diff
    in either direction)."""
    portfolio = _portfolio_with_pdf_trades(
        ("2025-11-15", "7769", "普買", 1000, 200.0),
        ("2025-12-03", "7769", "普買", 1000, 205.0),
        ("2026-01-12", "7769", "普買", 1000, 198.0),
    )
    pairs = [
        {"date": "2026-01-12", "code": "7769", "side": "普買", "qty": 3000,
         "price": 201.0, "cost_twd": 603_000, "ccy": "TWD", "venue": "TW",
         "type": "現股", "pair_id": 102},
        {"date": "2026-04-15", "code": "7769", "side": "普賣", "qty": 3000,
         "price": 210.0, "ccy": "TWD", "venue": "TW",
         "type": "現股", "pair_id": 102, "pnl": 27_000.0},
    ]

    fake = _AuditFakeClient(pairs=pairs)
    trade_overlay.merge(
        store, portfolio, fake, gap_start="2026-04-01", gap_end="2026-04-26"
    )

    events = reconcile.get_open_events(store)
    audit = [
        e for e in events
        if json.loads(e["diff_summary"]).get("event_type")
           == "broker_pdf_buy_leg_mismatch"
    ]
    assert len(audit) == 1
    detail = json.loads(audit[0]["diff_summary"])["detail"]
    assert detail["sdk_leg_count"] == 1
    assert detail["pdf_trade_count"] == 3


def test_audit_silent_when_leg_counts_match_exactly(store):
    """SDK 3 legs + PDF 3 trades for same code/window → no audit event."""
    portfolio = _portfolio_with_pdf_trades(
        ("2025-11-15", "7769", "普買", 1000, 200.0),
        ("2025-12-03", "7769", "普買", 1000, 205.0),
        ("2026-01-12", "7769", "普買", 1000, 198.0),
    )
    pairs = [
        {"date": d, "code": "7769", "side": "普買", "qty": 1000,
         "price": p, "cost_twd": 1000 * p, "ccy": "TWD", "venue": "TW",
         "type": "現股", "pair_id": 103}
        for (d, p) in [("2025-11-15", 200.0), ("2025-12-03", 205.0),
                       ("2026-01-12", 198.0)]
    ]
    pairs.append({
        "date": "2026-04-15", "code": "7769", "side": "普賣", "qty": 3000,
        "price": 210.0, "ccy": "TWD", "venue": "TW",
        "type": "現股", "pair_id": 103, "pnl": 27_000.0,
    })

    fake = _AuditFakeClient(pairs=pairs)
    trade_overlay.merge(
        store, portfolio, fake, gap_start="2026-04-01", gap_end="2026-04-26"
    )

    events = reconcile.get_open_events(store)
    audit = [
        e for e in events
        if json.loads(e["diff_summary"]).get("event_type")
           == "broker_pdf_buy_leg_mismatch"
    ]
    assert audit == []


@pytest.mark.skip(reason=_POLICY_SILENT_REASON)
def test_audit_fires_per_pair_independently(store):
    """Two closed pairs, both mismatched → two audit events. The merge
    layer fires once per pair_id."""
    portfolio = _portfolio_with_pdf_trades(
        ("2025-11-15", "7769", "普買", 1000, 200.0),  # 1 PDF trade
        ("2026-01-10", "2330", "普買", 1000, 880.0),  # 1 PDF trade
    )
    pairs = []
    # Pair A: 7769, 3 SDK legs vs 1 PDF
    for (d, p) in [("2025-11-15", 200.0), ("2025-12-03", 205.0),
                   ("2026-01-12", 198.0)]:
        pairs.append({
            "date": d, "code": "7769", "side": "普買", "qty": 1000,
            "price": p, "cost_twd": 1000 * p, "ccy": "TWD", "venue": "TW",
            "type": "現股", "pair_id": 201,
        })
    pairs.append({
        "date": "2026-04-15", "code": "7769", "side": "普賣", "qty": 3000,
        "price": 210.0, "ccy": "TWD", "venue": "TW",
        "type": "現股", "pair_id": 201, "pnl": 30_000.0,
    })
    # Pair B: 2330, 2 SDK legs vs 1 PDF
    for (d, p) in [("2026-01-10", 880.0), ("2026-02-08", 920.0)]:
        pairs.append({
            "date": d, "code": "2330", "side": "普買", "qty": 1000,
            "price": p, "cost_twd": 1000 * p, "ccy": "TWD", "venue": "TW",
            "type": "現股", "pair_id": 202,
        })
    pairs.append({
        "date": "2026-04-20", "code": "2330", "side": "普賣", "qty": 2000,
        "price": 940.0, "ccy": "TWD", "venue": "TW",
        "type": "現股", "pair_id": 202, "pnl": 40_000.0,
    })

    fake = _AuditFakeClient(pairs=pairs)
    trade_overlay.merge(
        store, portfolio, fake, gap_start="2026-04-01", gap_end="2026-04-26"
    )

    events = reconcile.get_open_events(store)
    audit = [
        e for e in events
        if json.loads(e["diff_summary"]).get("event_type")
           == "broker_pdf_buy_leg_mismatch"
    ]
    assert len(audit) == 2
    pair_ids = sorted(
        json.loads(e["diff_summary"])["detail"]["pair_id"] for e in audit
    )
    assert pair_ids == [201, 202]


@pytest.mark.skip(reason=_POLICY_SILENT_REASON)
def test_audit_fires_degenerate_empty_legs_event(store):
    """C-fallback degenerate case: list_profit_loss_detail(id) returned
    empty so list_realized_pairs emitted only a sell summary with qty=0.
    Strict rule: 0 SDK legs vs N PDF trades → fire as a separate event
    type so the banner can show different copy ("exact buy date deferred
    to next PDF")."""
    portfolio = _portfolio_with_pdf_trades(
        ("2025-11-15", "7769", "普買", 1000, 200.0),
    )
    pairs = [
        # No buy legs, only the sell summary with qty=0
        {"date": "2026-04-15", "code": "7769", "side": "普賣", "qty": 0,
         "price": 210.0, "ccy": "TWD", "venue": "TW",
         "type": "現股", "pair_id": 301, "pnl": 10_000.0},
    ]

    fake = _AuditFakeClient(pairs=pairs)
    trade_overlay.merge(
        store, portfolio, fake, gap_start="2026-04-01", gap_end="2026-04-26"
    )

    events = reconcile.get_open_events(store)
    types_seen = {
        json.loads(e["diff_summary"]).get("event_type") for e in events
    }
    # The plan calls out this as a distinct event type — separate banner
    # copy. The Strict rule still fires the leg-count mismatch event
    # alongside (0 ≠ 1), so we accept either or both.
    assert (
        "broker_pdf_pair_legs_unrecoverable" in types_seen
        or "broker_pdf_buy_leg_mismatch" in types_seen
    )
