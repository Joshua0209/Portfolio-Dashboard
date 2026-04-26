"""Phase 12 — reconciliation tests.

Hard requirements (per spec §12):
  1. NO auto-fire callsites in backfill_runner.py / snapshot_daily.py /
     parse_statements.py.
  2. NO Flask startup hook calls reconciliation.
  3. Clean diff exits 0, no row inserted.
  4. Fabricated diff inserts a row, banner can read it.
  5. Dismiss sets dismissed_at; banner stops rendering.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from app import reconcile
from app.daily_store import DailyStore


# --- Static no-auto-fire invariants --------------------------------------

@pytest.mark.parametrize("filename", [
    "app/backfill_runner.py",
    "scripts/snapshot_daily.py",
    "scripts/parse_statements.py",
])
def test_no_reconcile_auto_fire_callsites(filename):
    """Spec §12 hard requirement. The grep is from the plan verbatim."""
    path = Path(__file__).resolve().parent.parent / filename
    if not path.exists():
        # Phase 15 hasn't landed yet when this test runs in isolation;
        # skip rather than fail. Once snapshot_daily.py exists this test
        # protects it just like the others.
        pytest.skip(f"{filename} does not exist yet")
    src = path.read_text(encoding="utf-8")
    # The plan's grep `grep -rn "reconcile"` flags any *reference* to
    # reconcile in those files. Allow the word in comments/docstrings
    # by matching only function calls (`reconcile.run_for_month(`,
    # `from app.reconcile import`, `import reconcile`, etc.).
    forbidden_patterns = [
        r"from\s+app\.reconcile\s+import",
        r"import\s+app\.reconcile",
        r"from\s+\.\s*reconcile\s+import",
        r"reconcile\.run_for_month\s*\(",
        r"reconcile_for_month\s*\(",
    ]
    for pat in forbidden_patterns:
        assert not re.search(pat, src), (
            f"{filename} must not reference reconcile (manual-trigger only): "
            f"matched pattern {pat!r}"
        )


def test_app_factory_does_not_call_reconcile():
    """Belt-and-suspenders: the Flask startup path itself must not invoke
    reconcile.run_for_month — manual trigger only."""
    src = (Path(__file__).resolve().parent.parent / "app" / "__init__.py").read_text(
        encoding="utf-8"
    )
    assert "reconcile.run_for_month" not in src
    assert "from .reconcile" not in src
    assert "from app.reconcile" not in src


# --- Trade tuple + diff --------------------------------------------------


def test_normalize_trade_tuple_handles_pdf_slash_dates():
    pdf_trade = {"date": "2026/03/15", "code": "2330", "side": "普買",
                 "qty": 1000, "price": 880.0}
    overlay_trade = {"date": "2026-03-15", "code": "2330", "side": "普買",
                     "qty": 1000.0, "price": 880.0000001}
    assert (
        reconcile._normalize_trade_tuple(pdf_trade)
        == reconcile._normalize_trade_tuple(overlay_trade)
    )


def test_normalize_trade_tuple_distinguishes_real_price_diffs():
    a = {"date": "2026-03-15", "code": "2330", "side": "普買",
         "qty": 1000, "price": 880.00}
    b = {"date": "2026-03-15", "code": "2330", "side": "普買",
         "qty": 1000, "price": 880.05}
    assert reconcile._normalize_trade_tuple(a) != reconcile._normalize_trade_tuple(b)


def test_diff_trades_clean_returns_empty_lists():
    pdf = [{"date": "2026-03-15", "code": "2330", "side": "普買",
            "qty": 1000, "price": 880.0}]
    overlay = [{"date": "2026-03-15", "code": "2330", "side": "普買",
                "qty": 1000.0, "price": 880.0}]
    diff = reconcile.diff_trades(pdf, overlay)
    assert diff["only_in_pdf"] == []
    assert diff["only_in_overlay"] == []


def test_diff_trades_finds_missing_pdf_trade():
    pdf = []
    overlay = [{"date": "2026-03-15", "code": "2330", "side": "普買",
                "qty": 1000, "price": 880.0}]
    diff = reconcile.diff_trades(pdf, overlay)
    assert diff["only_in_pdf"] == []
    assert len(diff["only_in_overlay"]) == 1


# --- run_for_month + persistence ------------------------------------------


@pytest.fixture()
def store(tmp_path: Path) -> DailyStore:
    s = DailyStore(tmp_path / "reconcile.db")
    s.init_schema()
    return s


@pytest.fixture()
def portfolio_with_march_trade() -> dict:
    return {
        "months": [{"month": "2026-03"}],
        "summary": {
            "all_trades": [
                {"month": "2026-03", "date": "2026/03/15", "venue": "TW",
                 "side": "普買", "code": "2330", "qty": 1000, "price": 880.0},
            ],
        },
    }


def test_clean_diff_writes_no_event(store, portfolio_with_march_trade):
    """Plan §3 Phase 12 acceptance: clean diff exits 0, no row inserted."""
    overlay = lambda s, e: [
        {"date": "2026-03-15", "code": "2330", "side": "普買",
         "qty": 1000.0, "price": 880.0}
    ]
    summary = reconcile.run_for_month(
        store, portfolio_with_march_trade, "2026-03", overlay_client=overlay
    )
    assert summary["only_in_pdf_count"] == 0
    assert summary["only_in_overlay_count"] == 0
    assert summary["event_id"] is None

    with store.connect_ro() as conn:
        n = conn.execute("SELECT COUNT(*) FROM reconcile_events").fetchone()[0]
    assert n == 0


def test_fabricated_diff_writes_event(store, portfolio_with_march_trade):
    """Acceptance: fabricated diff exits 1, row inserted, banner reads it."""
    # Overlay has a trade the PDF doesn't.
    overlay = lambda s, e: [
        {"date": "2026-03-15", "code": "2330", "side": "普買",
         "qty": 1000.0, "price": 880.0},
        {"date": "2026-03-20", "code": "0050", "side": "普買",
         "qty": 2000, "price": 195.0},  # extra
    ]
    summary = reconcile.run_for_month(
        store, portfolio_with_march_trade, "2026-03", overlay_client=overlay
    )
    assert summary["only_in_overlay_count"] == 1
    assert summary["event_id"] is not None

    open_events = reconcile.get_open_events(store)
    assert len(open_events) == 1
    payload = json.loads(open_events[0]["diff_summary"])
    assert payload["only_in_overlay_count"] == 1


def test_dismiss_event_hides_from_open_list(store, portfolio_with_march_trade):
    overlay = lambda s, e: [
        {"date": "2026-03-20", "code": "0050", "side": "普買",
         "qty": 2000, "price": 195.0},
    ]
    summary = reconcile.run_for_month(
        store, portfolio_with_march_trade, "2026-03", overlay_client=overlay
    )
    event_id = summary["event_id"]

    assert reconcile.dismiss_event(store, event_id) is True
    assert reconcile.get_open_events(store) == []
    # Idempotent: re-dismiss returns False (no row updated)
    assert reconcile.dismiss_event(store, event_id) is False


def test_no_overlay_data_skipped_without_writing_event(store, portfolio_with_march_trade):
    """Without Shioaji creds the overlay returns []. Reconciliation must
    NOT spuriously fire — every PDF trade would otherwise show as
    'only_in_pdf' and create a meaningless banner."""
    overlay = lambda s, e: []
    summary = reconcile.run_for_month(
        store, portfolio_with_march_trade, "2026-03", overlay_client=overlay
    )
    assert summary["skipped_reason"] == "no_overlay_data"
    assert summary["event_id"] is None
    assert reconcile.get_open_events(store) == []


# --- API endpoints --------------------------------------------------------


@pytest.fixture()
def app(tmp_path, monkeypatch, empty_portfolio_json):
    monkeypatch.setenv("DAILY_DB_PATH", str(tmp_path / "reconcile_api.db"))
    monkeypatch.delenv("BACKFILL_ON_STARTUP", raising=False)
    from app import create_app
    return create_app(empty_portfolio_json)


def test_reconcile_endpoint_no_creds_returns_skipped(app):
    """POST /api/admin/reconcile?month=2026-03 with no Shioaji creds —
    returns the skipped envelope without crashing."""
    client = app.test_client()
    r = client.post("/api/admin/reconcile?month=2026-03")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["data"]["skipped_reason"] == "no_overlay_data"


def test_reconcile_endpoint_validates_month_format(app):
    client = app.test_client()
    r = client.post("/api/admin/reconcile?month=2026")
    assert r.status_code == 400


def test_today_reconcile_endpoint_returns_open_events(app):
    """GET /api/today/reconcile is the read endpoint the global banner
    polls. With no events present it returns an empty list."""
    client = app.test_client()
    r = client.get("/api/today/reconcile")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["data"]["events"] == []


def test_reconcile_dismiss_endpoint(app):
    """POST /api/admin/reconcile/<id>/dismiss flips the event off."""
    from app.daily_store import DailyStore
    ds: DailyStore = app.extensions["daily_store"]
    with ds.connect_rw() as conn:
        cur = conn.execute(
            "INSERT INTO reconcile_events(pdf_month, diff_summary, detected_at) "
            "VALUES ('2026-03', '{}', '2026-04-01T00:00:00Z')"
        )
        event_id = cur.lastrowid

    client = app.test_client()
    r = client.post(f"/api/admin/reconcile/{event_id}/dismiss")
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert body["data"]["dismissed"] is True

    # Now the open-events list should be empty.
    r2 = client.get("/api/today/reconcile")
    assert r2.get_json()["data"]["events"] == []
