# PLAN — Modularization (Phase 14)

**Status:** Drafted 2026-05-03. Ready for review.
**Predecessor:** [`PLAN-refactor-shioaji-canonical.md`](./PLAN-refactor-shioaji-canonical.md) — Phases 0–13 (shipped).
**Supersedes:** the deleted `PLAN-analytics-on-trades-migration.md`. That plan
proposed routing the metrics layer through per-request trade aggregation,
which conflicts with the locked dual-SoT model (see §1) and is rejected.
**Author:** Joshua Lau
**Scope:** Finish modularizing three monoliths whose pieces already exist as
parallel scaffolds. No data-model change; no request-path behavior change.

---

## 1. Architectural model (locked)

```
Trades log (PDF + Shioaji overlay)            ← SoT-1
        │
        ▼ walk day-by-day + apply prices + apply FX
positions_daily / portfolio_daily              ← SoT-2 (precomputed cache)
        │   ▲ verified at month-ends vs PDF monthly snapshots
        ▼
Metrics layer  (reads daily-portfolio rows directly — never re-aggregates trades)
```

- **Trades** are SoT because the broker API returns trade-level data only.
- **Daily portfolio** is *also* SoT because it is computed once, verified
  against the PDF month-end snapshot, and stored. Metrics consume it directly.
- The dual-SoT consistency invariant: month-end aggregate of `portfolio_daily`
  ≡ PDF monthly snapshot. Currently enforced *implicitly* by anchoring the
  daily walk at each prior PDF month-end inside
  `backfill_runner._qty_per_priced_date_for_symbol`.

This plan does NOT change any of the above. It only moves code into smaller,
testable, single-purpose modules.

## 2. What's already done (Phases 0–13)

- Backend/frontend split: FastAPI on `:8001`, Vite/TS SPA on `:5173`.
- Domain VOs (`Money`, `Trade`, `Side`, `Venue`, `Position`).
- SQLModel `trades` table populated from PDFs (`source='pdf'`) and from the
  Shioaji overlay (`source='overlay'`).
- Daily layer (`positions_daily`, `portfolio_daily`, `prices`, `fx_daily`)
  derived by walking trades, anchored at PDF month-ends.
- Read-only Shioaji client + 3-source overlay merge + audit-event hook on
  broker-vs-PDF buy-leg mismatch.
- 14-router HTTP layer + admin endpoints.
- Frontend SPA with all 12 pages.
- ~870 backend tests + 4 skipped.

## 3. What's not done

Three monoliths need to be replaced by their existing modularized rewrites:

| Monolith | LOC | Modularized rewrite (already on disk, tested, unwired) | LOC |
|---|---:|---|---:|
| `jobs/backfill_runner.py` | 1727 | `jobs/backfill.py` + `prices/price_service.py` + `prices/fx_provider.py` | 119 + 157 + 113 |
| `jobs/snapshot_workflow.py` | 433 | `jobs/snapshot.py` | 69 |
| inline float math in `analytics/monthly.py` (lines 381–540, ~160 LOC) | 1122 | `analytics/{twr,xirr,drawdown,concentration,attribution,sectors,tax_pnl,ratios}.py` (Decimal) | ~480 total |
| inline `_fire_audit_events` in `brokerage/trade_overlay.py` | (~80) | `reconciliation/shioaji_audit.py` | 203 |

Plus one optional cleanup:
- The trade walk inside `backfill_runner.py` reads
  `portfolio.json:summary.all_trades` (a JSON dict). The SQLModel `trades`
  table holds the same data via `trade_backfill.py`. Switching the walk
  source to the SQL table is functionally equivalent and simpler.

## 4. Phases (each ships as one PR unless noted)

Each phase preserves request-path behavior. Verification rule for every
phase: existing backend test suite green, plus the smoke test in §6.

### Phase 14.1 — Wire `monthly.py` through Decimal primitives

**Goal:** `monthly.py` calls the per-metric primitives instead of duplicating
math inline.

**Steps:**
- Replace `monthly.sharpe / sortino / calmar / stdev / downside_stdev` with
  calls to `analytics.ratios` (already imported elsewhere — extend usage).
- Replace `monthly.max_drawdown / drawdown_curve` with calls to
  `analytics.drawdown`.
- Replace `monthly.hhi / top_n_share / effective_n` with calls to
  `analytics.concentration`.
- Replace `monthly.realized_pnl_by_ticker_fifo` body with a call to
  `analytics.tax_pnl` (FIFO match logic lives there).
- Replace `monthly.fx_pnl` decomposition with calls to
  `analytics.attribution`.
- Replace `monthly.sector_of / sector_breakdown` calls with delegations to
  `analytics.sectors`.
- TWR primitive in `analytics.twr` takes Decimal; `monthly.period_returns` /
  `monthly.daily_twr` keep their float arithmetic for now (router parity).
  Wire only where the input shape matches — DO NOT change router output.

**Behavioral guarantee:** every router that currently calls `monthly.X`
continues to receive byte-identical output. Float→Decimal conversion happens
inside `monthly.py` at the boundary; the public API of `monthly.py` is
unchanged.

**Test:** snapshot the JSON envelope of every router endpoint against the
current `data/portfolio.json`, run after the wiring, diff. Zero diff allowed
on count/string/date fields; ≤ 1e-9 drift allowed on float fields (float→
Decimal→float round-trip noise).

**LOC delta:** -160 (delete inline math from `monthly.py`); ~+30 (boundary
adapters in `monthly.py`). `monthly.py`: 1122 → ~990.

**Risk:** LOW. Same data, same algorithm, narrower implementation.

### Phase 14.2 — Replace `snapshot_workflow.py` with `jobs/snapshot.py`

**Goal:** the incremental refresh path (`POST /api/admin/refresh`,
`scripts/snapshot_daily.py`) routes through `jobs.snapshot`.

**Steps:**
- Audit `jobs/snapshot.py` to confirm it covers the
  `meta.last_known_date → today` gap-fill semantic that
  `snapshot_workflow.py` provides.
- Identify any features in `snapshot_workflow.py` not yet in `jobs/snapshot.py`
  (FX gap-fill, overlay refresh, DLQ retry, freshness ping). Port what's
  missing into the modularized form — do NOT duplicate; refactor into
  small composable functions inside `invest.jobs.snapshot` and (where
  appropriate) `invest.prices.price_service` / `invest.brokerage`.
- Switch `scripts/snapshot_daily.py` and the admin router to call the new
  entry point.
- Delete `snapshot_workflow.py`.

**Behavioral guarantee:** `POST /api/admin/refresh` produces the same
side-effects on `dashboard.db` as before. Test by running
`scripts/snapshot_daily.py` in dry-run / read-only modes against the same
seed DB and diffing the result.

**LOC delta:** -433; +(missing-feature ports, est. 50–100). Net -300+.

**Risk:** MEDIUM. `snapshot_workflow.py` has accumulated integrations
(FX, overlay, DLQ); confirm the modular pieces cover them.

### Phase 14.3 — Replace `backfill_runner.py` with modularized pieces

**Goal:** the cold-start backfill (`scripts/backfill_daily.py`,
lifespan-startup path) routes through `jobs.backfill` +
`prices.price_service` + `prices.fx_provider`.

**Steps (split into 3 PRs to keep each revertible):**

- **14.3a** — Wire `prices.price_service` into `backfill_runner` as the price
  fetch facade. Replace the inline `prices.sources.get_prices` calls with
  the DLQ-aware service. No file deletions. CI sweep: existing backfill
  produces the same `prices` table contents.
- **14.3b** — Wire `prices.fx_provider` similarly for FX. Same CI rule.
- **14.3c** — Wire `jobs.backfill` as the state-machine wrapper. Move the
  `_qty_per_priced_date_for_symbol` walk + `_derive_positions_and_portfolio`
  into `jobs._positions` (where they conceptually belong). Delete
  `backfill_runner.py`.

**Behavioral guarantee:** running cold-start backfill against the same
`portfolio.json` produces a byte-identical `dashboard.db` (compared via
`sqlite3 .dump` diff, modulo non-deterministic timestamps).

**LOC delta:** -1727 (delete monolith); +(positions walk relocation, ~250).
Net -1400+. The remaining business logic is split across three named
modules with clear seams.

**Risk:** MEDIUM-HIGH. `backfill_runner.py` is the production cold-start
path. Split into 3 PRs; keep `backfill_runner.py` alive through 14.3a and
14.3b; delete only at the end of 14.3c. Each PR independently revertible.

### Phase 14.4 — Wire DLQ drainage through `jobs/retry_failed.py`

**Goal:** `POST /api/admin/retry-failed` and
`scripts/retry_failed_tasks.py` route through `jobs.retry_failed`.

**Steps:**
- Audit `jobs/retry_failed.py` against the inline retry logic in
  `backfill_runner.py` (likely already extracted in 14.3c).
- Wire admin endpoint + script to the modular entry point.
- Delete any remaining inline retry code.

**LOC delta:** small. Mostly a delete.

**Risk:** LOW.

### Phase 14.5 — Decide on `shioaji_audit.py`

The current overlay's `_fire_audit_events` (in `brokerage/trade_overlay.py`)
duplicates what `reconciliation/shioaji_audit.py` does, with one
architectural difference: the inline version is invoked from the write
path (overlay merge); the extracted version is positioned as a *post-sync*
hook against persisted `Trade` rows.

**Decision matrix:**

| Option | Effect |
|---|---|
| A. Extract: `trade_overlay.merge()` stops firing audit events. After overlay writes complete, `reconciliation/shioaji_audit.run()` is invoked separately (e.g. at end of `jobs.snapshot`). | Cleaner separation; matches the file's docstring intent. |
| B. Keep inline: delete `reconciliation/shioaji_audit.py`. | Simpler; one less module; preserves current call ordering. |

**Recommendation:** Option A. The file already exists with tests; the
docstring explicitly motivates separation ("a future change to the audit
policy should not require touching the write path"). Cost: ~30 LOC
plumbing change.

**LOC delta:** -80 (inline `_fire_audit_events` removal); audit module
already exists.

**Risk:** LOW. Audit-event idempotency rules are preserved (skip pair_ids
with existing OPEN events).

### Phase 14.6 — Optional: switch trade-walk source to SQLModel `trades`

**Goal:** the daily walk reads from the SQLModel `trades` table instead of
`portfolio.json:summary.all_trades`.

**Why optional:** functionally equivalent under your dual-SoT model
(`trade_backfill.py` already populates `trades` from
`portfolio.json:summary.all_trades`, so they hold the same data). The win
is architectural clarity — the trade-walk has a single canonical source
(SQL), and `portfolio.json` becomes purely the PDF aggregate verifier.

**Steps:**
- In the modularized backfill (post-14.3), replace
  `portfolio.get("summary", {}).get("all_trades", [])` reads with SQLModel
  queries against `trades` (filtered to `source='pdf'` for the historical
  walk; overlay merge handles `source='overlay'` separately as today).
- Confirm `trade_backfill` runs are idempotent (they are — UPSERT-by-natural-
  key).
- Add a backfill-time pre-step that ensures `trade_backfill` has run before
  daily walk, or fail loud.

**Risk:** LOW if 14.3 lands first.

### Phase 14.7 — Make the dual-SoT verification invariant explicit

**Goal:** convert the *implicit* "anchor at PDF month-end" check into an
*explicit* post-derivation verifier that fails loud on drift.

**Steps:**
- Add `analytics/_dual_sot_verifier.py` with one function:
  `verify_month_end(daily_rows, pdf_snapshot) -> list[Discrepancy]`.
- Compare per-(code, month-end) qty + cost-basis + market-value.
  Tolerance: `abs ≤ 1 NTD` per row (PDF rounds to whole TWD). Aggregate
  cap: `≤ 10 NTD` per month.
- Invoke at the end of every `jobs.backfill.run_full_backfill` and at the
  end of `jobs.snapshot.run`. Discrepancies above tolerance:
  - For backfill: log + fail the job (cold-start contract is "all-or-nothing").
  - For snapshot: log + emit a `reconcile_event` of type
    `dual_sot_drift` (operator review via the existing reconcile banner).
- Surface drift count in `/api/today/freshness` so the operator sees it.

**Why now:** under your model this invariant is load-bearing. Implicit
enforcement means a bug in the trade walk could silently produce wrong
month-ends.

**LOC delta:** +~150 (new module + tests).

**Risk:** LOW. New code paths only; existing flows unaffected unless drift
already exists (in which case you want to know).

## 5. Order of execution

Recommended order, smallest-blast-radius first:

1. **14.1** (monthly.py wiring) — pure refactor, request-path output unchanged.
2. **14.5** (shioaji_audit extraction) — small, isolated.
3. **14.4** (retry_failed wiring) — small.
4. **14.2** (snapshot_workflow → jobs.snapshot) — medium.
5. **14.3a / 14.3b / 14.3c** (backfill_runner replacement) — largest, last.
6. **14.7** (explicit verifier) — depends on 14.3 being done.
7. **14.6** (trade-walk source switch) — optional, after 14.3.

## 6. Per-phase smoke test (run after every PR)

```bash
# Backend tests
cd backend && pytest                                  # all green

# Cold-start byte-equality
rm data/dashboard.db
python scripts/backfill_daily.py
sqlite3 data/dashboard.db .dump > /tmp/after.sql
diff /tmp/before.sql /tmp/after.sql                   # only timestamp diffs

# Endpoint envelope diff (existing test or quick script)
for ep in summary holdings/current performance/timeseries risk fx tax cashflows/monthly; do
  curl -s :8001/api/$ep > /tmp/new/$ep.json
  diff /tmp/golden/$ep.json /tmp/new/$ep.json         # zero diff
done

# Dashboard smoke (manual)
# - Visit /, /today, /performance, /risk, /tax, /holdings
# - Confirm no banner errors, no obvious number drift
```

## 7. Out of scope

- Any change to the metrics layer's data source. Metrics keep reading
  `positions_daily` / `portfolio_daily`. Trades-on-the-fly aggregation is
  permanently rejected per the dual-SoT model.
- Any change to `PortfolioStore`'s role on the request path. It remains
  the PDF-aggregate loader and the verifier benchmark source.
- Any change to the Shioaji read-only invariant or the reconciliation
  destructive-form gating.
- Foreign-account broker enrollment (separate effort; tracked in
  `project_foreign_account_406.md`).

## 8. Done criteria

- `backfill_runner.py` deleted.
- `snapshot_workflow.py` deleted.
- `monthly.py` ≤ 1000 LOC, no inline math primitives.
- All eight `analytics/{twr,xirr,drawdown,concentration,attribution,
  sectors,tax_pnl,ratios}.py` modules consumed on the request path
  (directly or via `monthly.py` boundary adapters).
- `_fire_audit_events` removed from `trade_overlay.py`; audit fires from
  `reconciliation/shioaji_audit.py`.
- `jobs/{backfill,snapshot,retry_failed}.py` are the canonical entry points.
- Dual-SoT verifier runs at the end of every backfill / snapshot.
- Backend test suite green, with adjusted counts reflecting deleted/added
  test files.
- CLAUDE.md "In-progress migration" section deleted; replaced with a
  short note describing the dual-SoT consistency invariant and where it
  is enforced.

---

*End of plan. Awaiting confirmation before Phase 14.1 PR.*
