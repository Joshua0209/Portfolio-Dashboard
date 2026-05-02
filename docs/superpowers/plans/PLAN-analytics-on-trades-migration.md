# PLAN — Analytics-on-Trades Migration (Phase 11.2+)

**Status:** Approved 2026-05-02; verifier policy + starting slice locked. Ready to implement.
**Predecessor:** [`PLAN-refactor-shioaji-canonical.md`](./PLAN-refactor-shioaji-canonical.md) — Phases 0–11 (already cut over). This plan is the explicit Phase 11.2+ continuation that the parent plan deferred.
**Author:** Joshua Lau
**Scope:** Replace `PortfolioStore`-backed analytics with `trades`-table-backed analytics, behind a per-metric byte-equality verifier, then retire `PortfolioStore` + `monthly.py`.
**Estimated effort:** ~6–10 working days end-to-end across multiple PRs.

---

## 1. Why this is its own plan

The parent plan's Phase 11 cutover landed the *source side* of this migration:

- `invest.jobs.trade_backfill` populates the SQLModel `trades` table from
  `portfolio.json:summary.all_trades` (rows tagged `source='pdf'`).
- The overlay writes `source='overlay'` rows.
- The redesigned scaffolds in `invest.jobs.{backfill,snapshot,retry_failed}`
  + `invest.prices.{price_service,fx_provider}` +
  `invest.reconciliation.shioaji_audit` all coexist with the canonical paths.

What the parent plan **deferred** to "Phase 11.2+" is the read-side flip:
- Build a parallel aggregator that produces month-dict-shaped output from
  `trades` + `positions_daily` + FX rows.
- Verify byte-equality against `PortfolioStore` for every production month.
- Flip the 9 routers off `monthly.py`.
- Delete `monthly.py` (1122 LOC) and `PortfolioStore`.

This plan exists because that work is genuinely multi-day, multi-PR, and
the verifier-policy decision in §3 is consequential enough to call out
explicitly.

## 2. Current state (verified 2026-05-02)

| Component | LOC | State |
|---|---:|---|
| `invest.analytics.monthly` | 1122 | Canonical, on the request path |
| `invest.analytics.{twr,xirr,ratios,drawdown,concentration,attribution,tax_pnl,sectors}` | ~480 | Pure helpers, tested (8 test files), **off** the request path |
| `invest.analytics.holdings_today` | 216 | Canonical, used by `tax.py` and `today` router |
| Routers importing `monthly` | — | 9 files: `summary, holdings, performance, risk, fx, tax, cashflows, tickers, benchmarks` |
| Analytics-output byte-equality verifier | — | **Does not exist.** Grep returns zero hits. (The existing `invest.ingestion.trade_verifier` is PDF-vs-Shioaji at the trade level — a different concern.) |

`monthly.py` taxonomy by surface area (the migration cost lives in surface #2):

1. **Pass-throughs** (~10 funcs) — already wrap helpers verbatim.
   Examples: `cumulative_curve`, `drawdown_curve`, `max_drawdown`,
   `sharpe`, `sortino`, `calmar`, `hhi`, `top_n_share`, `effective_n`.
   Migration cost: near-zero (just route callers to helpers directly).

2. **Time-series builders** (~8 funcs) — walk `months: list[dict]` to
   produce per-month/per-day rows. **The real work.** Examples:
   `period_returns`, `daily_investment_flows`, `daily_external_flows`,
   `daily_twr`, `monthly_flows`, `daily_fx_pnl`, `monthly_anchored_cum`,
   `bank_cash_forward_fill`. Each needs a trades-backed analogue.

3. **Projections** (~5 funcs) — domain-specific aggregates.
   `realized_pnl_by_ticker_fifo` (FIFO over trades — clean reference),
   `realized_pnl_by_ticker` (avg-cost), `fx_pnl`, `top_movers`,
   `recent_activity`, `sector_breakdown`. Mixed effort; FIFO is easiest.

## 3. Verifier policy (locked)

**Disambiguation first.** Two verifiers exist; do not conflate them:

| Verifier | Scope | Status |
|---|---|---|
| `invest.ingestion.trade_verifier` | PDF rows ↔ `trades` table rows (per-trade audit) | Already exists. |
| **This plan's verifier** | `monthly.py` aggregate output ↔ new aggregator output | To build. |

Both sides of this plan's verifier ultimately derive from the same PDF
source, through different precision paths:

```
PDF → portfolio.json (floats, pre-rounded) → PortfolioStore → monthly.py        (OLD)
PDF → portfolio.json → trade_backfill → trades table (Decimal) → new aggregator (NEW)
```

The OLD path does float arithmetic on whole-TWD-rounded values. The NEW
path does Decimal arithmetic on the same values reified at higher
precision. Divergences come from three sources:

1. Float-vs-Decimal representation drift.
2. Sum-then-round vs round-then-sum order of operations.
3. **Latent bugs in either path** — the actual signal the verifier exists for.

Because the PDF parser destroys precision at ingestion (`float(s.replace(",", ""))`
on text like `"52,950"`), the verifier's tolerance floor is bounded by
the source data. This makes a per-field-class tolerance table tractable.

### Policy: P3 — field-typed tolerances

| Field class | Examples | Tolerance | Rationale |
|---|---|---:|---|
| **TWD money — per-row** | `gross_twd`, `net_twd`, `fee_twd` on a single trade | `abs ≤ 1.0` | PDF rounds to whole TWD; one-row drift ≤ 1 NTD. |
| **TWD money — aggregate** | monthly cost basis, per-ticker realized P&L | `abs ≤ N × 1.0` (N = contributing rows); soft cap `10 NTD` | Aggregates accumulate; tight enough to expose order-of-ops bugs in compound math. |
| **Percentage / ratio** | TWR, returns, Sharpe, HHI, drawdown | `abs ≤ 1e-4` (1 bp) | Money-over-money derivative; bp-level is standard. |
| **Count** | qty per trade, # holdings | `abs ≤ 1` | Odd-lot qty derivation (`cost/price`) can drift by 1 share (CLAUDE.md SDK quirk). |
| **Date** | trade date, month label | exact equality | Strings end-to-end. |
| **String** | code, ticker, side, name | exact equality | Categorical. |
| **List of dicts** | `realized_pnl_by_ticker` rows | structural: same length, same key set; then per-element field-typed comparison after sorting on a stable key | Order-independence handled in the comparator, not the aggregator. |
| **Optional / null** | `unrealized_pct` when no holding | both null OR both numeric-within-tolerance | Presence-matching is itself a signal. |

**The N-proportional rule for aggregate money** is the only non-obvious
piece: a monthly cost-basis sum over 200 trades is allowed up to ~10 NTD
of drift, while a 5-trade monthly sum allows ~5 NTD. Capping at 10 NTD
prevents the policy from absorbing arbitrarily large divergences as N
grows.

### On-divergence behavior

| Phase | Behavior | Rationale |
|---|---|---|
| Slices A / B / C verifier sweep | **Test failure, hard stop.** | The verifier is a quality gate at this stage. |
| Slice D router-flip PRs (per-router rollout) | **Warn-and-serve-old.** Run both implementations, compare with tolerance, on divergence emit a `reconcile_event` (reuse existing event store) and serve the OLD output. | Safe rollout: production sees old behavior; new path's divergences become observable signal. |
| Post-cutover (30 days zero divergences in prod) | Drop the runtime guard; verifier reverts to a CI sweep on PRs touching `invest/analytics/**`. | The gate's job is done. Keep CI sweep so future analytics changes can't silently regress. |

## 4. Slice strategy

The work decomposes into four slices of increasing scope. Each is a
ship-able PR; each later slice subsumes the earlier ones.

### Slice A — Verifier infrastructure only (no metric migrated)

**Goal:** Land the harness and policy decision; migrate nothing.

**Deliverables:**
- `backend/src/invest/analytics/_verifier.py` — pure-function
  `compare(old, new, schema) -> DiffReport` per the §3 policy.
- `backend/src/invest/analytics/_verifier_policy.py` — the field-typed
  tolerance table (or scalar tolerance if P2 chosen).
- `scripts/verify_analytics_parity.py` — CLI that loads
  `data/portfolio.json` + `data/dashboard.db`, picks a metric by name,
  runs both implementations across every month, prints a diff report.
- `backend/tests/analytics/test_verifier.py` — unit tests for the
  comparator itself (does it correctly catch a 0.02 TWD drift on a money
  field? Does it ignore a 1e-9 drift?).

**Out of scope:** No router flip. No new aggregator. The CLI runs against
*placeholder* trades-backed implementations that just return
`PortfolioStore.summary[month]` so the harness can be exercised end-to-end.

**LOC budget:** ~250 (verifier 80, policy 50, CLI 70, tests 50).
**Effort:** 1 day. **Risk:** low (no production code paths touched).

### Slice B — Verifier + smallest meaningful metric

**Goal:** Prove the migration loop end-to-end on the easiest metric.

**Recommended metric: `realized_pnl_by_ticker_fifo`.** Reasons:
- Already a pure FIFO-over-trades computation in `monthly.py:598`. The
  trades-backed analogue is a near-direct port — read the same trades
  from the SQLModel table instead of from `portfolio.json:by_ticker`.
- Single router consumer (`tax.py:25`).
- No daily-resolution branch; no FX cross-currency math.
- The clean reference makes divergences interpretable rather than
  forensic.

**Deliverables:**
- `invest.analytics.tax_pnl_aggregator` (or extend the existing
  `tax_pnl.py`): consumes `trades` rows, produces the same dict shape
  `monthly.realized_pnl_by_ticker_fifo` returns.
- Wire into the verifier CLI as the first concrete metric.
- Run across every production month; capture divergence report.
- **Do not** flip `tax.py` — leave it on `monthly.py`.
- Document any bugs found in either implementation.

**LOC budget:** ~400 (~150 aggregator, ~200 tests, ~50 verifier wiring).
**Effort:** 1–1.5 days. **Risk:** low–medium (real bugs may surface, which
is the point — that's information, not delay).

### Slice C — All 8 metric families migrated, no router flips

**Goal:** Every monthly.py public function has a trades-backed analogue
that passes the verifier across every production month. Routers still
import `monthly`.

**Deliverables (one per metric family, each can be its own PR):**

| # | Family | Source funcs | New module |
|---|---|---|---|
| C1 | TWR / period returns | `period_returns`, `daily_twr`, `cumulative_curve`, `monthly_anchored_cum` | `invest.analytics.twr_aggregator` |
| C2 | Cashflows | `daily_investment_flows`, `daily_external_flows`, `monthly_flows`, `bank_cash_forward_fill` | `invest.analytics.cashflows_aggregator` |
| C3 | Realized P&L | `realized_pnl_by_ticker`, `realized_pnl_by_ticker_fifo` | done in Slice B + extend |
| C4 | Drawdown / risk ratios | `drawdown_curve`, `max_drawdown`, `drawdown_episodes`, `sharpe`, `sortino`, `calmar`, `hhi`, `top_n_share`, `effective_n` | mostly pass-throughs to existing helpers |
| C5 | FX P&L | `fx_pnl`, `daily_fx_pnl` | `invest.analytics.fx_aggregator` |
| C6 | Holdings derivatives | `reprice_holdings_with_daily`, `top_movers`, `sector_breakdown` | `invest.analytics.holdings_aggregator` |
| C7 | Recent activity | `recent_activity` | trivial — reads trades directly |
| C8 | Anchoring utilities | `month_end_iso`, `anchor_for_daily` | move to `invest.core.dates` (no logic change) |

**Run after each PR:** verifier sweep across all months. **Block** the PR
if any divergence outside policy tolerance.

**LOC budget:** ~1500 added (~800 aggregator code, ~700 tests).
Subtractions deferred to Slice D.
**Effort:** 4–6 days across 6–8 PRs.
**Risk:** medium (this is where forensic bugs live — see §5).

### Slice D — Cutover

**Goal:** Routers consume trades-backed aggregators; `monthly.py` and
`PortfolioStore` retire.

**Deliverables:**
- Per-router PRs flipping `from invest.analytics import monthly as
  analytics` to the new aggregator imports. Done one router at a time so
  every PR is independently revertible.
- `PortfolioStore` use sites collapsed to:
  - The PDF aggregate JSON loader (still needed; it feeds `trade_backfill`).
  - **Nothing else** on the request path.
- Delete `monthly.py`. Delete unused `PortfolioStore` methods.
- Verifier CLI demoted to a CI sweep (run on PRs that touch analytics)
  rather than a request-path gate.
- Update `CLAUDE.md` "In-progress migration" section → "Migration complete".

**LOC budget:** -1100 (delete monthly.py); +50 (delete-only PRs that
touch routers).
**Effort:** 1–1.5 days. **Risk:** low if Slices B+C did their job.

## 5. Risks (likelihood × impact, ordered)

| Risk | Severity | Mitigation |
|---|---|---|
| **Rounding-policy mismatch** masquerades as correctness divergence in Slices B/C | HIGH | §3 decision must be made BEFORE Slice A ships. Re-run verifier sweep after every policy change to confirm the policy isn't hiding bugs. |
| **Trades table is incomplete vs. `summary.all_trades`** — e.g. odd-lot derivation, foreign trades pre-Shioaji-enrollment, manual override codes from `tw_ticker_map.json` | HIGH | The verifier sweep catches this. Treat any divergence as a `trade_backfill` bug first, aggregator bug second. Do not silently bridge the gap with a fallback to PortfolioStore — that defeats the point. |
| **Margin (融資) cost-vs-equity rule** drifts during reimplementation | MEDIUM | Lock with a regression test in Slice B's policy. Do NOT refactor the accounting rule; port verbatim. CLAUDE.md §Caveats already flags this. |
| **FX P&L double-counting** between trades-backed FX and `daily_fx_pnl` | MEDIUM | Slice C5 needs a verifier sweep specifically for the JPY/HKD/USD breakdowns, not just totals. |
| **`monthly_anchored_cum` drift** at month boundaries (off-by-one on the anchor day) | MEDIUM | Lock with golden-vector test on three production months that span quarter ends. |
| **Verifier becomes a rubber stamp** because every divergence gets whitelisted | MEDIUM | Code-review rule: any whitelist entry requires a linked GitHub issue with the divergence's root cause. No "investigate later" entries. |
| **Router flip in Slice D introduces a regression** the verifier didn't catch (because the verifier compares aggregator output, not router output) | LOW–MEDIUM | Each router-flip PR runs the relevant router's existing integration test against both implementations and diffs the response envelopes byte-for-byte. |
| **Performance regression** — trades-table aggregation per-request might be slower than reading `portfolio.json` | LOW | Benchmark in Slice B. If meaningful, cache aggregator output in the existing `dashboard.db` (write-through cache, invalidated on `trades` mutation). Defer to Slice D. |

## 6. Starting slice (locked): Slice B

**Slice B** — verifier infra + `realized_pnl_by_ticker_fifo` migrated, no
router flips — ships first as a single PR.

Rationale:
- The §3 policy table is ~50 LOC and small enough to land alongside the
  first concrete metric without pre-committing to scaffolding-only work.
- `realized_pnl_by_ticker_fifo` is the cleanest first metric: pure FIFO
  over `trades` rows, single router consumer (`tax.py:25`), no daily
  branch, no FX cross-currency math, clean reference at
  `monthly.py:598`.
- Exercising the policy against real production data immediately is
  the only way to validate the tolerances are correct. Slice A in
  isolation risks ratifying an untested policy.

Slice B PR contents:
1. `invest.analytics._verifier` (P3 comparator).
2. `invest.analytics._verifier_policy` (the table from §3).
3. `invest.analytics.tax_pnl_aggregator` (FIFO over `trades`).
4. `scripts/verify_analytics_parity.py` CLI.
5. `tests/analytics/test_verifier.py` — comparator unit tests
   (does it catch a 0.02 TWD drift on per-row money? Does it ignore
   1e-9 drift? Does the N-proportional rule fire correctly?).
6. `tests/analytics/test_tax_pnl_aggregator.py` — aggregator regression
   tests against locked snapshots from `realized_pnl_by_ticker_fifo` on
   real production data.
7. Verifier sweep across all production months — embedded as a pytest
   marker (`@pytest.mark.parity_sweep`) so it runs in CI but can be
   skipped during local dev.

Slice B does **not** flip `tax.py`. Router still imports `monthly`.

## 7. Cutover checklist (when Slice D ships)

- [ ] Verifier sweep across every production month: zero divergences
      outside policy tolerance.
- [ ] All 9 routers flipped, each in its own revertible PR.
- [ ] `monthly.py` deleted.
- [ ] `PortfolioStore` reduced to PDF-aggregate loader (no
      `summary[*]` consumers on request path).
- [ ] Backend test suite green (currently 869 passed + 4 skipped; expect
      a net delta as monthly tests retire and aggregator tests land).
- [ ] Smoke-test all 12 dashboard pages against live data; no visible
      regressions.
- [ ] Verifier CLI promoted to a CI step that runs on PRs touching
      `invest/analytics/**`.
- [ ] CLAUDE.md updated:
  - "In-progress migration: analytics on trades" section deleted.
  - "Source-of-truth split" reflects trades table as canonical.
  - "Layout" reflects deleted `monthly.py`.
- [ ] [`PLAN-refactor-shioaji-canonical.md`](./PLAN-refactor-shioaji-canonical.md)'s
      Phase 11.2+ deferral note replaced with a back-link to this plan
      marked "Complete".

## 8. Open questions

1. **`PortfolioStore.summary[*]` retains JSON-only fields** that aren't
   in the trades table (e.g. parsed PDF totals, bank ledger dumps). Are
   any of these on the request path? Audit needed in Slice A.
2. **Foreign trades** (`venue='US'` in trades; `pdf-foreign` provenance)
   — current verifier scope assumes they're in the trades table. Confirm
   `trade_backfill` writes them. (Spot-check: does `summary.all_trades`
   include foreign? If yes, they're in `trades`.)
3. **`holdings_today`** is independent of `monthly.py` and not part of
   this migration. Confirm during Slice D that no router accidentally
   imports `monthly` for a function that should be from `holdings_today`.
4. **Performance budget** — what's the request-path latency tolerance?
   `monthly.py` reads a parsed JSON dict; the aggregator hits SQLite per
   request. Probably fine (the existing daily layer already does this for
   prices), but worth measuring in Slice B.

---

*End of plan. Awaiting §3 verifier-policy decision and §6 slice selection
before code lands.*
