"""Phase 11 - Trade-table backfill from data/portfolio.json.
The Trade SQLModel table is the long-term source of truth for the
Shioaji-canonical PLAN section 4 design. Phase 11 starts the migration
by populating the table from the parsed-PDF aggregate (the same data
PortfolioStore exposes via summary.all_trades). Analytics keep reading
PortfolioStore until the per-metric byte-equality verifier ships in
Phase 11.2; this module is the source side of that verifier.
Idempotency contract:
  - Re-running against the same portfolio.json yields the same set of
    source='pdf' rows. Implemented by clearing source='pdf' first,
    then bulk-inserting.
  - source='overlay' rows are NEVER touched. Same invariant pattern as
    positions_daily - PDFs canonical for historical, overlay canonical
    for post-PDF broker activity, neither writer crosses the boundary.
Side mapping (8 strings observed across the live 312-trade dataset on
2026-05-02):
  TW main:  普買 -> CASH_BUY    普賣 -> CASH_SELL
            資買 -> MARGIN_BUY  資賣 -> MARGIN_SELL
  TW OTC:   櫃買 -> CASH_BUY    櫃賣 -> CASH_SELL
            (venue stays 'TW' - Side encodes direction+credit, not
             market. Adding TW_OTC would belong on `venue`, not Side.)
  Foreign:  買進 -> CASH_BUY    賣出 -> CASH_SELL
            (venue='Foreign'; currency carries USD/etc.)
Unknown side strings are counted in the summary but do not abort the
run - a parser regression that introduces a 9th string shouldn't block
the operator from getting the rest of the trades into the table.
"""
from __future__ import annotations
import logging
from datetime import date
from decimal import Decimal
from typing import Any
from sqlmodel import Session, delete, select
from invest.domain.trade import Side
from invest.persistence.models.trade import Trade
log = logging.getLogger(__name__)
_SIDE_MAP: dict[str, Side] = {
    "普買": Side.CASH_BUY,
    "普賣": Side.CASH_SELL,
    "資買": Side.MARGIN_BUY,
    "資賣": Side.MARGIN_SELL,
    "櫃買": Side.CASH_BUY,
    "櫃賣": Side.CASH_SELL,
    "買進": Side.CASH_BUY,
    "賣出": Side.CASH_SELL,
}
def side_from_string(s: str) -> Side:
    """Strict mapping: raises on unknown strings.
    Callers that want lenient handling (e.g. the run() backfill) catch
    ValueError and skip the row instead of aborting.
    """
    if not s:
        raise ValueError("unknown side: empty string")
    try:
        return _SIDE_MAP[s]
    except KeyError as e:
        raise ValueError(f"unknown side: {s!r}") from e
def _parse_date(s: str) -> date:
    """Accept YYYY-MM-DD and YYYY/MM/DD - the parser emits both."""
    sep = "-" if "-" in s else "/"
    y, m, d = (int(p) for p in s.split(sep))
    return date(y, m, d)
def _build_row(t: dict) -> Trade:
    """Project one all_trades dict into a Trade ORM instance.
    Raises ValueError on unknown side strings (caller's responsibility
    to count and continue).
    """
    side = side_from_string(t.get("side", ""))
    return Trade(
        date=_parse_date(t["date"]),
        code=str(t["code"]),
        side=int(side),
        qty=int(round(float(t.get("qty") or 0))),
        price=Decimal(str(t.get("price") or 0)),
        currency=str(t.get("ccy") or "TWD"),
        fee=Decimal(str(t.get("fee_twd") or 0)),
        tax=Decimal(str(t.get("tax_twd") or 0)),
        rebate=Decimal("0"),
        source="pdf",
        venue=str(t.get("venue") or "TW"),
    )
def run(session: Session, portfolio: dict) -> dict[str, Any]:
    """Backfill the trades table from portfolio['summary']['all_trades'].
    Returns a summary dict:
      pdf_rows_inserted - rows newly written under source='pdf'
      pdf_rows_deleted  - prior source='pdf' rows cleared first
      skipped_count     - rows skipped due to unknown side strings
    """
    raw_trades = portfolio.get("summary", {}).get("all_trades", [])
    rows: list[Trade] = []
    skipped = 0
    for t in raw_trades:
        try:
            rows.append(_build_row(t))
        except (ValueError, KeyError, TypeError) as e:
            skipped += 1
            log.warning("trade_backfill: skipping row %r: %s", t, e)
    prior = session.exec(select(Trade).where(Trade.source == "pdf")).all()
    pdf_rows_deleted = len(prior)
    if pdf_rows_deleted:
        session.exec(delete(Trade).where(Trade.source == "pdf"))
    for row in rows:
        session.add(row)
    session.commit()
    summary = {
        "pdf_rows_inserted": len(rows),
        "pdf_rows_deleted": pdf_rows_deleted,
        "skipped_count": skipped,
    }
    log.info("trade_backfill summary: %s", summary)
    return summary
