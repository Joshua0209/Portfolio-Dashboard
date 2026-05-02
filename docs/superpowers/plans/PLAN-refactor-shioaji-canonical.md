# PLAN — Backend/Frontend Split + Modular Rewrite, Shioaji-Canonical

**Status:** Approved (decisions locked 2026-05-01); not yet started.
**Author:** Joshua Lau
**Scope:** Large-scale rewrite of `/Users/joshua/investment` — physical separation
of backend and frontend, modularization of every backend domain, and inversion
of the source-of-truth contract from PDF-canonical to Shioaji-canonical.
**Estimated effort:** ~10 working days end-to-end.

---

## 1. Goals

1. **Physical separation** of backend (FastAPI server) and frontend (Vite + TS
   static SPA). Two processes, two folders, talking over HTTP.
2. **Modular backend** — each domain in its own package: brokerage, prices,
   persistence, domain models, analytics, ingestion, reconciliation, HTTP
   transport. No domain knows about another's internals.
3. **Frontend kept aesthetically.** Same design tokens, page structure,
   navigation, 12-page surface. Componentization improvements allowed; visual
   regressions are not.
4. **Single source of truth = Shioaji** for trades and positions, with PDFs
   demoted to two roles:
   - **Historical seeder** (one-shot) — hydrates the brokerage trade store
     before Shioaji's earliest available data.
   - **Monthly verifier** (recurring) — every monthly statement audits the
     prior month's Shioaji-written trades; mismatches surface as
     `reconcile_events` for operator review.
5. **Tests rewritten from scratch** per unit; existing 311-test suite is
   reference, not baseline.

## 2. Locked Decisions

| # | Decision | Choice | Rationale |
|---|---|---|---|
| 1 | Backend language | **Python (FastAPI + SQLModel)** | Shioaji SDK is Python-only; analytics already in numpy idiom; one process is simpler than a Python sidecar |
| 2 | Frontend stack | **Vanilla JS + Vite + TypeScript** | Preserves zero-build feel and current page-per-file structure; adds types and HMR without a framework |
| 3 | Foreign account handling | **Treat HTTP 406 as a fixme — attempt to fix; if unfixable, monthly PDF verification covers the gap (≤ 30-day staleness)** | See §3 |
| 4 | Database | **SQLite (WAL) + Alembic migrations** | Single-user, embedded, no ops; Alembic adds migration discipline |
| Sequencing | End-to-end | Run all 10 phases in sequence; no checkpoints |

## 3. Foreign Account 406 — Fixme + Probe Plan

The Shioaji SDK currently returns HTTP 406 ("Account Not Acceptable") for every
accounting query against `AccountType.H` (the 複委託 foreign account). The
hard-coded `venue='TW'` in the current `shioaji_client.py` is the workaround.

**This is no longer accepted as permanent.** Phase 0 includes a probe to
attempt a fix; if it works, foreign coverage extends Shioaji-canonical to all
venues. If it does not, the monthly PDF verifier ensures the foreign track is
never more than ~30 days stale.

**Probe hypotheses (run in Phase 0, document outcome in this file):**

1. **Account-context bug** — Shioaji's session-scoped `default_account` may
   need to be set to the H-account *before* calling
   `list_positions`/`list_profit_loss`. Test: call
   `api.set_default_account(h_account)` first, then retry.
2. **Endpoint permission gap** — H-accounts may need an explicit foreign
   trading flag enabled on the SinoPac side. Verify in the broker portal.
3. **API surface mismatch** — there may be a separate `list_foreign_*` family
   of endpoints not currently called. Read SDK source to confirm.
4. **CA-cert requirement** — foreign queries may require `activate_ca` to be
   called even for read-only access. **CRITICAL:** if this is the case, do
   NOT enable `activate_ca` in `shioaji_client.py` — the static-grep guard
   forbidding it must remain. Spin up a separate opt-in `foreign_client.py`
   that imports `activate_ca` and is gated by a feature flag.

### Probe outcome (2026-05-01, shioaji 1.3.3)

Probe extended in `scripts/probe_shioaji_pnl_detail.py` Step 7. All four
hypotheses tested in safe order. **Hypothesis #2 confirmed; #1, #3, #4
ruled out.** Verbatim findings:

| # | Probe | Result | Verdict |
|---|---|---|---|
| 7a | Inspect `h_account` fields | `signed=False`, no `_ACCTTYPE['H']` entry | H-account is NOT enrolled at the broker; no typed wrapper class either |
| 7b | Forge `StockAccount(account_id=H.account_id)` and retry | HTTP 406 | Solace transport keys on broker-side `account_id`, not the Python class. SDK class-dispatch is not the cause. |
| 7c | `set_default_account(h_account)` swap + retry | HTTP 406 (with `try/finally` snapshot restore) | Solace uses the explicit `account=` kwarg, not session state. The default-account swap is a no-op for this purpose. |
| 7d | `activate_ca(./Sinopac.pfx)` then retry | `activate_ca → True`, retry HTTP 406 | CA was successfully activated. Foreign reads do not require CA. The "spin up `foreign_client.py`" path is **unnecessary scaffolding** and is dropped from the plan. |

**Root cause:** `signed=False` on the H-account, observable locally in
`api.list_accounts()` without any broker call. SinoPac's foreign-trading API
(複委託 API) is a **separate enrollment** the user has not signed up for.
The fix is portal-side, not code-side. Once the user enrolls and `signed`
flips to `True`, re-run the probe to confirm reads start returning rows.

**Outcome contract:** the existing fallback stands and is now permanent
(unless the user enrolls). Foreign trades carry `source='pdf-foreign'` in
the `trades` table and are written exclusively by `ingestion/trade_seeder`
(implemented in Cycles 31–34). The hard-coded `venue='TW'` in
`shioaji_client.py` is no longer a workaround for a bug — it correctly
reflects the broker enrollment scope. Banner the operator if foreign
track exceeds 35 days stale.

**Detection helper (optional, low priority):** `shioaji_sync` could read
`h_account.signed` once at startup and log a one-time hint if it ever
flips to `True` ("foreign API now enrolled — probe foreign reads"). Not
implemented; defer until enrollment happens.

## 4. Architecture — PDF as Verifier (revised)

The original plan treated PDFs as a one-shot historical seeder. The corrected
model has two PDF roles:

```
                      ┌──────────────────────────────────┐
                      │  trades  (write-once, append)    │
                      │  source ∈ {shioaji, pdf, pdf-    │
                      │           foreign, manual}       │
                      └──────────────────────────────────┘
                                  ▲          ▲
                       writes     │          │  audits (monthly)
            ┌─────────────────────┘          └─────────────────────┐
            │                                                       │
   ┌────────────────────┐                            ┌──────────────────────────┐
   │  shioaji_sync      │                            │  pdf_verifier            │
   │  (every snapshot)  │                            │  (every monthly close)   │
   │  - list_trades     │                            │  - parse statements      │
   │  - list_open_lots  │                            │  - diff vs trades table  │
   │  - list_realized_  │                            │  - emit reconcile_events │
   │    pairs           │                            │  - seed pre-shioaji rows │
   └────────────────────┘                            └──────────────────────────┘
            ▲                                                       ▲
            │                                                       │
   ┌────────────────────┐                            ┌──────────────────────────┐
   │ Shioaji API        │                            │ PDFs (sinopac_pdfs/)     │
   │ (live, canonical)  │                            │ (verifier, ≤30d stale)   │
   └────────────────────┘                            └──────────────────────────┘
```

The audit hook from the existing Phase 11 Path A is preserved and elevated:
the monthly verifier compares every Shioaji-written trade against the
authoritative PDF for the same `(code, ≤month-end)` window and writes
`reconcile_events` for any divergence. The existing
`broker_pdf_buy_leg_mismatch` event flavor still fires; new flavors can be
added (e.g. `pdf_trade_missing_from_shioaji`,
`shioaji_trade_missing_from_pdf`).

**Resolution semantics on a verifier mismatch:**

- **Shioaji wrote first, PDF later disagrees** → PDF-canonical for that
  trade. Update the row, set `source='pdf'`, retain the original
  `reconcile_event` for audit trail.
- **PDF says trade exists but Shioaji never wrote it** → insert from PDF
  with `source='pdf'`. (Likely a Shioaji history retention gap.)
- **Foreign trades** (until the 406 fixme is solved) → PDF is the only
  writer; verifier upgrades to seeder for these rows.

In other words, **PDF still wins on disagreement** — but disagreements are
rare (Shioaji is canonical for the live track), and they surface as events
rather than silent overwrites.

## 5. Target Repository Layout

```
investment/
├── backend/                              # FastAPI app
│   ├── pyproject.toml
│   ├── alembic/
│   │   ├── env.py
│   │   └── versions/
│   ├── src/invest/
│   │   ├── core/
│   │   │   ├── config.py                 # pydantic Settings
│   │   │   ├── logging.py
│   │   │   └── errors.py                 # typed exceptions
│   │   ├── persistence/
│   │   │   ├── db.py                     # engine, session, WAL setup
│   │   │   ├── models/                   # SQLModel ORM tables
│   │   │   │   ├── trade.py              # NEW write-once trades aggregate
│   │   │   │   ├── price.py
│   │   │   │   ├── fx.py
│   │   │   │   ├── position_daily.py
│   │   │   │   ├── portfolio_daily.py
│   │   │   │   ├── failed_task.py
│   │   │   │   └── reconcile_event.py
│   │   │   └── repositories/             # one per aggregate
│   │   │       ├── trade_repo.py
│   │   │       ├── price_repo.py
│   │   │       ├── fx_repo.py
│   │   │       ├── position_repo.py
│   │   │       ├── portfolio_repo.py
│   │   │       └── reconcile_repo.py
│   │   ├── domain/                       # pure logic, no I/O
│   │   │   ├── money.py                  # Money(amount, ccy)
│   │   │   ├── trade.py                  # Trade, Side, Venue
│   │   │   ├── position.py               # FIFO lot tracking
│   │   │   └── cashflow.py
│   │   ├── prices/
│   │   │   ├── yfinance_client.py
│   │   │   ├── fx_provider.py
│   │   │   └── price_service.py          # caching + DLQ
│   │   ├── brokerage/
│   │   │   ├── shioaji_client.py         # read-only; static-grep guard preserved
│   │   │   ├── shioaji_sync.py           # writes Trade rows from 3 surfaces
│   │   │   └── (foreign_client.py)       # only if 406 fixme requires CA-cert
│   │   ├── ingestion/                    # PDF parsing
│   │   │   ├── pdf_decryptor.py
│   │   │   ├── statement_parser.py
│   │   │   ├── tw_parser.py
│   │   │   ├── foreign_parser.py
│   │   │   ├── bank_parser.py
│   │   │   ├── trade_seeder.py           # one-shot historical (date < shioaji_min)
│   │   │   └── trade_verifier.py         # monthly audit vs trades table
│   │   ├── analytics/                    # pure functions
│   │   │   ├── twr.py                    # day_weighted / mid_month / eom
│   │   │   ├── xirr.py
│   │   │   ├── ratios.py                 # sharpe, sortino, calmar
│   │   │   ├── concentration.py          # HHI, top-N share
│   │   │   ├── drawdown.py
│   │   │   ├── attribution.py            # FX, venue
│   │   │   ├── tax_pnl.py                # FIFO realized + unrealized
│   │   │   └── sectors.py
│   │   ├── reconciliation/
│   │   │   ├── service.py                # orchestrates verifier diffs
│   │   │   └── audit_events.py           # event_type encoding
│   │   ├── http/
│   │   │   ├── deps.py                   # DI: get_db, require_admin
│   │   │   ├── envelope.py               # {ok, data} response model
│   │   │   ├── routers/
│   │   │   │   ├── summary.py
│   │   │   │   ├── holdings.py
│   │   │   │   ├── performance.py
│   │   │   │   ├── transactions.py
│   │   │   │   ├── cashflows.py
│   │   │   │   ├── dividends.py
│   │   │   │   ├── risk.py
│   │   │   │   ├── fx.py
│   │   │   │   ├── tax.py
│   │   │   │   ├── tickers.py
│   │   │   │   ├── benchmarks.py
│   │   │   │   ├── daily.py
│   │   │   │   ├── today.py
│   │   │   │   └── admin.py
│   │   │   └── openapi.py
│   │   ├── jobs/
│   │   │   ├── backfill.py               # cold-start
│   │   │   ├── snapshot.py               # incremental (Shioaji + prices)
│   │   │   ├── verify_month.py           # PDF verifier cron
│   │   │   └── retry_failed.py           # DLQ drain
│   │   └── app.py                        # FastAPI() factory
│   └── tests/                            # pytest, mirrors src/
│
├── frontend/                             # Vite + TS + vanilla components
│   ├── package.json
│   ├── vite.config.ts
│   ├── tsconfig.json
│   ├── src/
│   │   ├── main.ts
│   │   ├── lib/
│   │   │   ├── api.ts                    # typed client (codegen from OpenAPI)
│   │   │   ├── format.ts
│   │   │   ├── charts.ts                 # Chart.js wrappers
│   │   │   ├── pagination.ts
│   │   │   └── help.ts
│   │   ├── components/                   # KpiCard, FreshnessDot, DataTable, Banner, Sparkline
│   │   ├── pages/                        # one per route, same DOM hooks
│   │   │   ├── overview.ts
│   │   │   ├── today.ts
│   │   │   ├── holdings.ts
│   │   │   ├── performance.ts
│   │   │   ├── risk.ts
│   │   │   ├── fx.ts
│   │   │   ├── transactions.ts
│   │   │   ├── cashflows.ts
│   │   │   ├── dividends.ts
│   │   │   ├── tax.ts
│   │   │   ├── ticker.ts
│   │   │   └── benchmark.ts
│   │   ├── styles/
│   │   │   ├── tokens.css                # COPIED VERBATIM from current
│   │   │   └── app.css                   # COPIED VERBATIM from current
│   │   └── templates/                    # static HTML fragments (or migrate to lit-html)
│   └── public/
│
├── data/                                 # unchanged (gitignored)
├── sinopac_pdfs/                         # unchanged (gitignored)
├── scripts/                              # thin CLI shims importing backend.jobs
│   ├── backfill.py
│   ├── snapshot.py
│   ├── verify_month.py
│   ├── reconcile.py
│   ├── retry_failed.py
│   └── seed_from_pdfs.py                 # one-shot
└── README.md
```

## 6. Implementation Phases

### Phase 0 — Decisions, scaffolding, 406 probe (½–1 day)
- Initialize `backend/` (`pyproject.toml`, `alembic init`, FastAPI factory).
- Initialize `frontend/` (`vite create`, TypeScript config).
- Old `app/` keeps running on port 8000 throughout the rewrite.
- **406 probe:** write `scripts/probe_shioaji_foreign.py` exercising the four
  hypotheses in §3. Document outcome at the end of this section.
- New schema migration: introduce `trades` table — write-once, append-only,
  PK `(date, code, side, qty, price, source)`. This is the new aggregate
  root for "what happened."

**Probe outcome (fill in after Phase 0):** _<TBD>_

### Phase 1 — Persistence & domain (1–1.5 days)
- Port existing SQLite tables to SQLModel under `persistence/models/`.
  Bring forward the existing `dashboard.db`; no data loss.
- Build repositories — pure data access, no business logic.
- Build domain value objects (`Money`, `Trade`, `Side`, `Venue`,
  `Position`, `Cashflow`). No I/O.
- **Tests:** per-repository (in-memory SQLite); per-VO (hypothesis where
  it pays off — e.g. `Money` arithmetic identities).

### Phase 2 — Prices module (½ day)
- Port `yfinance_client.py` + `price_sources.py` + FX into `prices/`.
- Single `PriceService` facade. DLQ writes via `failed_task` repo.
- **Tests:** stubbed yfinance fake; DLQ behavior on transient failure;
  TW `.TW`/`.TWO` probe order.

### Phase 3 — Analytics module (1 day) ← golden-vector lock
- Split `analytics.py` (995 lines) into per-metric files: `twr.py`,
  `xirr.py`, `ratios.py`, `concentration.py`, `drawdown.py`,
  `attribution.py`, `tax_pnl.py`, `sectors.py`.
- Each file is pure functions over typed inputs (`List[Trade]`,
  `List[Price]`, `List[FX]`). No DB, no Flask.
- **Tests:** golden-vector tests against the *current implementation's
  outputs* for the real `portfolio.json`. Lock these BEFORE rewriting
  any math — they catch silent drift between old and new.

### Phase 4 — PDF ingestion: seeder + verifier (1.5 days)
- Port `scripts/parse_statements.py` into `ingestion/` modules.
- `trade_seeder.py` — idempotent one-shot, writes pre-Shioaji history.
- `trade_verifier.py` — monthly audit. Diff-only by default; with
  `--apply` flag, resolves disagreements per §4 rules.
- Wire `trade_verifier` to fire `reconcile_events` for every divergence.

### Phase 5 — Brokerage authority flip (1.5 days) — **the risky one**
- `brokerage/shioaji_sync.py` pulls all three surfaces and writes Trade
  rows with `source='shioaji'`.
- Drop the PDF-canonical UPSERT guard. Shioaji wins the live track.
- Foreign trades: behavior depends on Phase 0 probe outcome —
  - probe succeeded → Shioaji writes foreign with `source='shioaji'`.
  - probe failed → foreign trades carry `source='pdf-foreign'` and are
    written only by the monthly verifier.
- Audit hook (`broker_pdf_buy_leg_mismatch`) preserved.
- **Tests:** integration tests with a fake Shioaji client exercising
  the 3-source merge + the verifier's resolution rules.

### Phase 6 — HTTP layer (1 day)
- Port each `app/api/*.py` blueprint to a FastAPI router. Same URL
  shapes (`/api/...`), same `{ok, data}` envelope.
- `?resolution=daily` becomes a typed query param.
- Admin endpoints gated by Depends-injected `require_admin` reading
  `ADMIN_TOKEN`.
- Generate OpenAPI; export schema to `frontend/openapi.json` for
  client codegen.

### Phase 7 — Jobs split (½ day)
- Split `backfill_runner.py` (1,725 lines — biggest single file in the
  repo) into `jobs/backfill.py`, `jobs/snapshot.py`,
  `jobs/verify_month.py`, `jobs/retry_failed.py`. Each runnable as
  CLI shim and as a programmatic call from FastAPI startup.

### Phase 8 — Frontend rebuild (2 days)
- Vite + TS + same vanilla pattern. One file per page, same DOM
  hooks as today so `tokens.css`/`app.css` are copied verbatim and
  carry the look unchanged.
- Generate typed client from OpenAPI
  (`openapi-typescript-codegen` or `orval`).
- Rebuild components: `KpiCard`, `FreshnessDot`, `DataTable`,
  `Banner` (DLQ + reconcile), `Sparkline`. Keep Chart.js.
- Page-by-page port: overview → today → holdings → performance →
  risk → fx → transactions → cashflows → dividends → tax → ticker →
  benchmark.
- Visual diff sanity check: side-by-side screenshots at 320 / 768 /
  1440 against current dashboard.

### Phase 9 — Cutover + retire (½ day)
- New backend on port 8001. Verify endpoint-by-endpoint parity.
- Switch frontend `API_BASE` to 8001.
- Delete `app/`, `templates/`, `static/`, `app.py`. Update
  `CLAUDE.md` (Layout, Refresh workflow, Invariants, API surface,
  Caveats sections all need rewrites).

## 7. New Dependencies

| Layer | Adds | Keeps |
|---|---|---|
| Backend | `fastapi`, `uvicorn`, `sqlmodel`, `alembic`, `pydantic-settings`, `httpx` (test client) | `shioaji`, `yfinance`, `pypdf`, `pdfplumber`, `numpy` |
| Frontend | `vite`, `typescript`, `openapi-typescript-codegen` (or `orval`) | `chart.js` |

## 8. Risks (likelihood × impact, ordered)

| Risk | Severity | Mitigation |
|---|---|---|
| **Analytics regression** — Modified Dietz / XIRR / Sharpe drift between old and new implementations | **HIGH** | Golden-vector tests in Phase 3 lock current outputs as ground truth before any rewrite. Any drift is a rewrite bug, full stop. |
| **406 probe fails AND foreign track grows stale** | **HIGH** | Verifier-only path documented in §4. Banner foreign track if > 35 days stale; surfaces in `/today`. |
| **Historical Shioaji data is incomplete** — `list_realized_pairs` returns only closed pairs; open lots have no buy-date | **MEDIUM** | PDF seeder fills history; Shioaji extends from there forward. `Trade.source` field tells you who wrote each row. |
| **Margin (融資) accounting drift** — current code has subtle "equity vs cost" rules called out in `CLAUDE.md` | **MEDIUM** | Lock with golden-vector tests; do NOT refactor the accounting rule itself in Phase 3. |
| **Backfill rewrite loses retry semantics** — current 1,725-line `backfill_runner.py` has battle-tested DLQ + state-machine logic | **MEDIUM** | Phase 7 ports module-by-module, NOT a logic rewrite. Keep the state machine intact; just split files. |
| **Frontend "vibe" drift** — pixel/spacing regressions during port | **LOW–MEDIUM** | Reuse `tokens.css` and `app.css` verbatim; only JS+HTML changes; visual diff at 3 breakpoints. |
| **OpenAPI codegen lag** — backend changes require frontend regeneration | **LOW** | npm script `gen:api` wired into dev workflow. |

## 9. Out of Scope (intentional)

- Authentication beyond `ADMIN_TOKEN` (keep current model).
- Multi-user.
- Cloud deployment / Docker.
- Frontend framework migration (staying vanilla).
- New features — this is a refactor, not a behavior rewrite.
- Live trading — `shioaji_client.py` stays read-only forever; the
  static-grep guard against `Order` / `place_order` / `activate_ca`
  remains in place. Any foreign-CA work goes in a separate opt-in
  module.

## 10. Cutover Checklist (Phase 9)

- [ ] All 13 routers respond on port 8001 with parity envelopes.
- [ ] Frontend `API_BASE` flipped to 8001.
- [ ] `/api/health` reports correct daily-state on new backend.
- [ ] Visual diff: 320 / 768 / 1440 across all 12 pages — no
      regressions.
- [ ] Golden-vector analytics tests still passing.
- [ ] Monthly verifier dry-run on the latest closed month: zero
      unexpected `reconcile_events`.
- [ ] DLQ drain works end-to-end on a synthetic failure.
- [ ] `ADMIN_TOKEN` gating works on every `POST /api/admin/*`.
- [ ] Old `app/`, `templates/`, `static/`, `app.py` deleted.
- [ ] `CLAUDE.md` rewritten: Layout, Refresh workflow, Environment
      variables, Invariants, API surface, Caveats, Files NOT to commit.
- [ ] `README.md` rewritten.

## 11. Notes Carried Forward From Current Implementation

These invariants must survive the rewrite verbatim:

1. **Shioaji is read-only forever.** Static-grep guard in tests
   forbids `Order`, `place_order`, `cancel_order`, `update_order`,
   `activate_ca` from `shioaji_client.py`. Foreign CA work, if any,
   goes in a separate opt-in module.
2. **Reconciliation is operator-triggered for the destructive form.**
   The verifier runs automatically (read-only, emits events); the
   `--apply` flag that mutates `trades` rows is gated behind the
   admin endpoint. PDF-vs-Shioaji audit events are surfaced via the
   existing `/today` reconcile banner.
3. **The daily-resolution layer is a regenerable cache.** Wipe
   `dashboard.db`, re-run backfill, get the same dashboard back.
   Backups still use `sqlite3 .backup` (not `cp`) because of WAL
   sidecars.
4. **TW ticker code overrides** — `data/tw_ticker_map.json` remains
   the manual override file for trade names that never appear in any
   holdings table (intra-month round-trips).
5. **Per-currency conversion** — only USD positions are TWD-converted
   today. The HKD/JPY extension point in `parse_statements.py:main`
   moves into `ingestion/foreign_parser.py`.
6. **Modified Dietz weighting variants** (`day_weighted` /
   `mid_month` / `eom`) — three TWR variants, switchable in UI and
   via `?method=`. Month 1 forced to 0% (no prior equity).
   Behavior is locked by the golden-vector tests.
7. **`/today` blueprint exception** — the FastAPI router for `today`
   continues to mount both `/api/today/*` (read) and `/api/admin/*`
   (operator writes) for cohesion. The other 12 routers are
   single-prefix.
8. **Health states (Phase 9 of original plan)** —
   READY / INITIALIZING / FAILED contract preserved end-to-end.
   Daily/today endpoints return 202 + progress while initializing,
   503 + error string on failure, 200 + payload when ready.

---

*End of plan.*
