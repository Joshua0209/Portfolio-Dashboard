"""DLQ-aware FX-rate fetching service (middle-path rule + TWD identity).

Composes invest.prices.yfinance_client.fetch_fx + FxRepo +
FailedTaskRepo. Mirrors invest.prices.price_service for FX rates:
same Outcome A/B/C rule, same recovery behavior. Two FX-specific
properties:

  - TWD is an identity short-circuit: returns Decimal('1') without
    any client call (TWD->TWD is unity; no point pinging yfinance,
    and a transient TWD=X hiccup must not enter the DLQ).
  - No probe — currencies map 1:1 to Yahoo pairs (USD->TWD=X,
    HKD->HKDTWD=X, etc.), no .TW/.TWO ambiguity.
"""
from __future__ import annotations

from datetime import date as _date
from decimal import Decimal
from typing import Optional, Protocol

from invest.persistence.models.failed_task import FailedTask
from invest.persistence.models.fx_rate import FxRate
from invest.persistence.repositories.failed_task_repo import FailedTaskRepo
from invest.persistence.repositories.fx_repo import FxRepo


_TASK_TYPE = "fetch_fx"


class FxClient(Protocol):
    def fetch_fx(self, ccy: str, start: str, end: str) -> list[dict]: ...


def _open_task_for(
    dlq: FailedTaskRepo, ccy: str
) -> Optional[FailedTask]:
    for t in dlq.find_by_type(_TASK_TYPE):
        if t.resolved_at is None and t.payload.get("ccy") == ccy:
            return t
    return None


def _has_prior_history(fx_repo: FxRepo, ccy: str) -> bool:
    return len(fx_repo.find_rates(ccy, "TWD")) > 0


def fetch_and_store_fx(
    ccy: str,
    on_date: _date,
    *,
    fx_repo: FxRepo,
    dlq: FailedTaskRepo,
    client: FxClient,
) -> Optional[Decimal]:
    """Fetch ccy->TWD rate for on_date, persist via fx_repo.

    Returns the rate as Decimal on success, None on miss/failure.
    Special case: ccy='TWD' returns Decimal('1') without any
    client call or persistence side-effect.
    """
    if ccy == "TWD":
        # Identity short-circuit. No fetch, no row, no DLQ.
        return Decimal("1")

    iso = on_date.isoformat()
    payload = {"ccy": ccy, "date": iso}

    try:
        rows = client.fetch_fx(ccy, iso, iso)
    except Exception as exc:
        existing = _open_task_for(dlq, ccy)
        if existing is None:
            dlq.insert(
                FailedTask(
                    task_type=_TASK_TYPE, payload=payload, error=repr(exc)
                )
            )
        else:
            dlq.bump_attempt(existing.id, repr(exc))
        return None

    if not rows:
        if _has_prior_history(fx_repo, ccy):
            return None
        if _open_task_for(dlq, ccy) is None:
            dlq.insert(
                FailedTask(
                    task_type=_TASK_TYPE,
                    payload=payload,
                    error=(
                        f"no FX rows for {ccy}->TWD on or before {iso}; "
                        "currency may be exotic / unsupported by yfinance"
                    ),
                )
            )
        return None

    row = rows[0]
    rate = Decimal(str(row["rate"]))
    fx_repo.upsert(
        FxRate(
            date=on_date,
            base=ccy,
            quote="TWD",
            rate=rate,
            source="yfinance",
        )
    )

    existing = _open_task_for(dlq, ccy)
    if existing is not None:
        dlq.mark_resolved(existing.id)

    return rate


def fetch_and_store_range(
    ccy: str,
    start: _date,
    end: _date,
    *,
    fx_repo: FxRepo,
    dlq: FailedTaskRepo,
    client: FxClient,
) -> int:
    """Fetch a date range of ccy->TWD rates and persist all rows via fx_repo.

    Returns the number of rows persisted (0 on miss/failure). DLQ rules
    mirror :func:`fetch_and_store_fx` but at the ``(ccy, range)``
    granularity — one DLQ row per failed range, not per failed date:

      Outcome A  exception                  -> insert/bump ONE DLQ row
      Outcome B  empty, has prior history   -> silent miss
      Outcome C  empty, no prior history    -> insert ONCE
                                                (no auto-bump on repeat)
      Non-empty -> persist + resolve any open DLQ row for the ccy

    Special case: ``ccy='TWD'`` returns 0 immediately without any
    client call, persistence side-effect, or DLQ row (TWD->TWD is unity).
    """
    if ccy == "TWD":
        # Identity short-circuit. No fetch, no row, no DLQ.
        return 0

    start_iso = start.isoformat()
    end_iso = end.isoformat()
    payload = {"ccy": ccy, "start": start_iso, "end": end_iso}

    try:
        rows = client.fetch_fx(ccy, start_iso, end_iso)
    except Exception as exc:
        # Outcome A: real failure. Always bump.
        existing = _open_task_for(dlq, ccy)
        if existing is None:
            dlq.insert(
                FailedTask(
                    task_type=_TASK_TYPE, payload=payload, error=repr(exc)
                )
            )
        else:
            dlq.bump_attempt(existing.id, repr(exc))
        return 0

    if not rows:
        if _has_prior_history(fx_repo, ccy):
            # Outcome B: silent miss.
            return 0
        # Outcome C: log once.
        if _open_task_for(dlq, ccy) is None:
            dlq.insert(
                FailedTask(
                    task_type=_TASK_TYPE,
                    payload=payload,
                    error=(
                        f"no FX rows for {ccy}->TWD in [{start_iso}..{end_iso}]; "
                        "currency may be exotic / unsupported by yfinance"
                    ),
                )
            )
        return 0

    # Happy path: persist every returned row. ``date`` may arrive as a
    # str (yfinance_client returns ISO strings) or as a date object — be
    # defensive on the boundary.
    persisted = 0
    for row in rows:
        raw_date = row.get("date")
        if isinstance(raw_date, _date):
            on_date = raw_date
        else:
            on_date = _date.fromisoformat(str(raw_date))
        rate = Decimal(str(row["rate"]))
        fx_repo.upsert(
            FxRate(
                date=on_date,
                base=ccy,
                quote="TWD",
                rate=rate,
                source="yfinance",
            )
        )
        persisted += 1

    # Recovery: resolve any open DLQ row for this ccy.
    existing = _open_task_for(dlq, ccy)
    if existing is not None:
        dlq.mark_resolved(existing.id)

    return persisted
