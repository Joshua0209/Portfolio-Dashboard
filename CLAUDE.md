# Sinopac Investment Dashboard

Personal investment performance dashboard built from Sinopac (永豐金) monthly PDF statements.
Pipeline: encrypted PDFs → decrypt → parse → JSON → **FastAPI backend (:8001) + Vite/TS dashboard (:5173)**.

A second daily-resolution layer sits on top of the monthly base: yfinance
prices (TW symbols via `.TW`/`.TWO` suffix, foreign as bare tickers) and FX
rates cached in a local SQLite (`data/dashboard.db`, WAL mode), plus an
optional read-only Shioaji client that overlays trades that happened *after*
the most recent monthly statement closed. The daily layer is a regenerable
cache — wipe `dashboard.db` and the next backfill rebuilds it in 30–60s.

## Architecture (post-Phase 11 cutover, 2026-05-02)

```
                    ┌────────────────────────────────────┐
                    │  Vite + TypeScript SPA  (port 5173) │
                    │  frontend/                          │
                    └──────────────┬─────────────────────┘
                                   │  /api/* via proxy
                    ┌──────────────▼─────────────────────┐
                    │  FastAPI backend       (port 8001)  │
                    │  backend/src/invest/                │
                    └──────────────┬─────────────────────┘
                                   │
                ┌──────────────────┼──────────────────┐
                │                                       │
        ┌───────▼─────────┐                  ┌─────────▼────────┐
        │ data/portfolio  │                  │  data/dashboard. │
        │ .json (PDF agg) │                  │  db (daily SQLite)│
        └─────────────────┘                  └──────────────────┘
                ▲                                       ▲
                │ written by                            │ written by
        ┌───────┴─────────┐                  ┌─────────┴────────┐
        │ scripts/parse_  │                  │ scripts/{backfill│
        │ statements.py   │                  │ ,snapshot}_daily │
        └─────────────────┘                  │ .py + Shioaji    │
                                             │ overlay          │
                                             └──────────────────┘
```

**Cutover state**: The Flask backend on :8000 (legacy `app.py` + `app/api/`) was
deleted in Phase 9; templates and static assets (`templates/`, `static/`) are
gone — the dashboard is a Vite+TS SPA in `frontend/`. Phase 10 (2026-05-02)
deleted the entire transitional `app/` package and the top-level `tests/`
directory. Every legacy module was ported verbatim into the `invest.*`
namespace under `backend/src/invest/`, scripts/*.py were rewritten as thin
shims that import from `invest.jobs.*` / `invest.persistence.*` /
`invest.brokerage.*` / `invest.reconciliation.*`, and the legacy test
files moved to `backend/tests/legacy/` with their imports rewritten in
bulk. Phase 11 (2026-05-02) wired `POST /api/admin/refresh` to the
canonical `invest.jobs.snapshot_workflow.run` (was previously stubbed
on the SQLModel-backed `snapshot.run_incremental` scaffold) and shipped
`invest.jobs.trade_backfill` — the source-side feed for the future
Trade-table aggregator. `scripts/backfill_trades.py` populates the
SQLModel `trades` table from `data/portfolio.json` (312 PDF trades →
`source='pdf'` rows, idempotent). Analytics still read PortfolioStore
on the request path; Phase 11.2 ports analytics one metric at a time
with byte-equality verification before flipping the read path.

## Layout

```
investment/
├── credentials.json              # Google API creds for the downloader (gitignored)
├── token.json                    # OAuth token (gitignored)
├── sinopac_pdfs/                 # Encrypted source PDFs (gitignored)
│   └── decrypted/                # Decrypted copies (gitignored — sensitive)
├── data/                         # gitignored — real portfolio data
│   ├── portfolio.json            # Parsed dataset (PDF aggregate, canonical)
│   ├── tw_ticker_map.json        # Manual TW name→code overrides
│   ├── benchmarks.json           # yfinance price cache (7-day TTL)
│   └── dashboard.db              # Daily-resolution SQLite cache (WAL; regenerable)
├── logs/                         # gitignored — daily.log (rotating, 5 MB × 3)
│
├── backend/                      # FastAPI app — canonical
│   ├── pyproject.toml
│   ├── src/invest/
│   │   ├── app.py                # FastAPI factory + lifespan; module-level
│   │   │                         #   `app = create_app()` for `uvicorn invest.app:app`
│   │   ├── core/
│   │   │   ├── config.py         # pydantic Settings (DAILY_DB_PATH, ADMIN_TOKEN)
│   │   │   └── state.py          # Backfill state machine singleton (Phase 10
│   │   │                         #   port of app/backfill_state)
│   │   ├── persistence/
│   │   │   ├── portfolio_store.py   # JSON-backed monthly aggregate (mtime reload;
│   │   │   │                        #   port of app/data_store)
│   │   │   ├── daily_store.py    # SQLite WAL wrapper (verbatim port of app/daily_store)
│   │   │   ├── models/           # SQLModel ORM tables (Phase 11+ Trade-table source)
│   │   │   └── repositories/     # Per-aggregate data access
│   │   ├── analytics/            # Pure-function analytics
│   │   │   ├── monthly.py        # Verbatim port of app/analytics.py — month-dict input
│   │   │   ├── holdings_today.py # Warm/cold reprice resolver (port of app/holdings_today)
│   │   │   ├── twr.py / xirr.py / ratios.py / drawdown.py / concentration.py /
│   │   │   │ attribution.py / tax_pnl.py / sectors.py
│   │   │   │   (per-metric files for Phase 11+ Trade-typed inputs)
│   │   ├── domain/               # Money, Trade, Side, Venue, Position VOs
│   │   ├── prices/
│   │   │   ├── yfinance_client.py    # Verbatim port of app/yfinance_client
│   │   │   ├── sources.py            # get_prices / get_fx_rates / get_yfinance_prices
│   │   │   │                         #   (port of app/price_sources)
│   │   │   ├── price_service.py      # Redesigned (Trade-table aggregator) — coexists
│   │   │   ├── fx_provider.py        # Redesigned FX provider — coexists
│   │   │   └── tw_probe.py
│   │   ├── brokerage/
│   │   │   ├── shioaji_client.py     # READ-ONLY (static-grep guard, verbatim port)
│   │   │   ├── shioaji_sync.py
│   │   │   └── trade_overlay.py      # 3-source merge + audit-event hook (port of
│   │   │                             #   app/trade_overlay)
│   │   ├── ingestion/            # PDF parsing modules (seeder + verifier)
│   │   ├── reconciliation/
│   │   │   ├── reconcile.py          # diff_trades / record_event / get_open_events
│   │   │   │                         #   (port of app/reconcile)
│   │   │   └── shioaji_audit.py      # Redesigned audit pipeline — coexists
│   │   ├── benchmarks.py         # yfinance benchmark fetcher + STRATEGIES catalogue
│   │   ├── http/
│   │   │   ├── deps.py           # get_session / get_portfolio_store / get_daily_store /
│   │   │   │                     #   require_admin
│   │   │   ├── envelope.py       # {ok, data} response model
│   │   │   ├── helpers.py        # bank_cash_twd / today_repriced_totals / envelope
│   │   │   └── routers/          # 14 routers — health, summary, holdings, performance,
│   │   │                         #   transactions, dividends, fx, tax, risk, cashflows,
│   │   │                         #   tickers, benchmarks, daily, today (read+admin)
│   │   └── jobs/
│   │       ├── backfill_runner.py    # 1725-LOC verbatim port of app/backfill_runner —
│   │       │                         #   the production cold-start path
│   │       ├── snapshot_workflow.py  # Incremental refresh — backs both
│   │       │                         #   scripts/snapshot_daily.py AND
│   │       │                         #   POST /api/admin/refresh (Phase 11)
│   │       ├── trade_backfill.py     # PDF → SQLModel `trades` table (Phase 11)
│   │       ├── backfill.py / snapshot.py / retry_failed.py / verify_month.py
│   │       │                         # Redesigned scaffolds (Trade-table aggregator) —
│   │       │                         #   coexist with verbatim ports above
│   │       └── _positions.py / _dlq.py
│   └── tests/                    # ~870 tests (pytest)
│       ├── analytics/ brokerage/ core/ domain/ http/ ingestion/
│       ├── jobs/ persistence/ prices/ reconciliation/
│       └── legacy/               # 149 ports of the old top-level tests/ (Phase 10)
│
├── frontend/                     # Vite + TypeScript SPA
│   ├── package.json
│   ├── vite.config.ts            # API proxy → :8001
│   └── src/
│       ├── main.ts
│       ├── lib/                  # api.ts (typed client), charts.ts, format.ts, paint.ts
│       ├── components/           # KpiCard, FreshnessDot, DataTable, Banner, Sparkline
│       ├── pages/                # one per route (overview, today, holdings, …)
│       └── styles/               # tokens.css, app.css
│
└── scripts/                      # Thin shims importing invest.jobs.* / invest.persistence.*
    ├── download_sinopac_pdfs.py
    ├── decrypt_pdfs.py
    ├── parse_statements.py       # → data/portfolio.json
    ├── backfill_daily.py         # Cold-start daily layer (invest.jobs.backfill_runner)
    ├── backfill_trades.py        # Phase 11 — populate SQLModel `trades` from PDFs
    ├── snapshot_daily.py         # Incremental refresh (invest.jobs.snapshot_workflow)
    ├── reconcile.py              # Manual PDF-vs-overlay diff
    ├── retry_failed_tasks.py     # Drain DLQ
    └── validate_data.py          # Sanity checks
```

## Refresh workflow

When new monthly statements arrive:

```bash
cd path/to/investment
source .venv/bin/activate

# 1. (existing) pull new PDFs into sinopac_pdfs/
python3 scripts/download_sinopac_pdfs.py

# 2. unlock — passwords come from env (comma-separated candidates)
export SINOPAC_PDF_PASSWORDS="<id-or-birthdate>,<fallback>"
python3 scripts/decrypt_pdfs.py

# 3. parse → data/portfolio.json
python3 scripts/parse_statements.py

# 4. start the FastAPI backend
PYTHONPATH=backend/src uvicorn invest.app:app --port 8001
# (mtime-watched portfolio.json — re-parsing while up reloads on next request)

# 5. start the Vite dev server (proxies /api → :8001)
cd frontend && npm run dev
# then open http://127.0.0.1:5173/
```

**Daily layer refresh** — three ways:

1. **Cold start** — delete `data/dashboard.db` and run
   `python scripts/backfill_daily.py`. Endpoints under `/api/daily/*` and
   `/api/today/*` return HTTP 202 + progress until backfill completes.
2. **Incremental** — `python scripts/snapshot_daily.py` gap-fills from
   `meta.last_known_date` to today. Idempotent. Same code path as
   `POST /api/admin/refresh` (the "Refresh now" button on `/today`).
3. **Retry the DLQ** — failed external fetches land in `failed_tasks` and
   are drained by `python scripts/retry_failed_tasks.py` or
   `POST /api/admin/retry-failed`.

## Environment variables

`.env` (gitignored) holds local overrides. None are strictly required.

| Variable | Purpose |
| --- | --- |
| `SINOPAC_PDF_PASSWORDS` | Comma-separated PDF unlock candidates. The decrypter tries each per file. Different statement types use different passwords (brokerage = National ID, bank = birth date). |
| `SINOPAC_API_KEY` / `SINOPAC_SECRET_KEY` | Shioaji read-only credentials. When both set, the trade overlay folds post-PDF broker activity into the daily layer. Without them, dashboard runs in PDF-only mode. |
| `SINOPAC_CA_CERT_PATH` / `SINOPAC_CA_PASSWORD` | Sinopac PKCS#12 (`.pfx`) bundle. **Documented but not consumed** — the static-grep guard forbids `activate_ca` from `app/shioaji_client.py`. Foreign-CA work would go in a separate opt-in module. The `.pfx` file itself is gitignored. |
| `BACKFILL_ON_STARTUP` | Default `false`. Reserved for future FastAPI lifespan hook. |
| `ADMIN_TOKEN` | Default unset (admin POSTs unauthenticated). When set, `require_admin` requires `X-Admin-Token` header on every `POST /api/admin/*`. |
| `DAILY_DB_PATH` | Override the default `./data/dashboard.db` location. |

Never commit any of these values.

## Portfolio definition (important)

| Account | Treated as | Why |
|---|---|---|
| TW securities (證券月對帳單) | inside portfolio | the investments themselves |
| Foreign / 複委託 | inside portfolio | same |
| Bank (永豐銀行 綜合對帳單) | **external** | source of capital; only used for USD/TWD FX rate |

External cashflows = `客戶淨收付` (TW) + `應收/付` sum (foreign), TWD-converted.

## Performance metrics

- **TWR (Modified Dietz, monthly)** — three flow-weighting variants,
  switchable via `?method=` on `/api/performance/*`:
  - `day_weighted` (default): each per-trade flow weighted by `(D-d)/D`.
  - `mid_month`: legacy Modified Dietz, all flows weighted 0.5.
  - `eom`: end-of-month, flows weighted 0.0.
- **XIRR**: Newton-Raphson on cashflow dates. Money-weighted.
- **Sortino / Calmar / Sharpe** with reference bands.

TWR and XIRR often diverge — TWR ≫ XIRR usually means recent deposits
haven't had time to compound; that's normal, not a bug.

## TW ticker codes for trades

The TW monthly statement's *trade* table prints only the abbreviated stock
name. For positions held at any month-end the parser auto-derives the code
by matching name → holdings table. For intra-month round-trips,
`data/tw_ticker_map.json` is the manual override file.

## Caveats

- **Margin (融資)**: equity = market value of all positions, but cost includes
  only your portion (資自備款). Equity-based returns can look inflated.
- **Foreign FX**: only USD positions are TWD-converted today.
- **Dividends**: bank-derived per-event records are source of truth; the
  broker `海外股票現金股利明細` section is a backfill only.
- **Sector mapping**: hand-curated heuristic in `analytics.monthly.sector_of`.

## Dashboard pages (URL → purpose)

| URL | Purpose |
|---|---|
| `/today` | Tactical view — Δ vs prior session, top movers, sparkline, MTD/QTD/YTD/Inception, drawdown, risk-and-return tile, calendar heatmap, freshness, Developer Tools accordion (DLQ + reconcile) |
| `/` | KPI hero, equity curve, allocation donut, top movers, recent activity |
| `/holdings` | Sortable table, treemap, sector breakdown |
| `/performance` | TWR/XIRR, monthly returns, drawdown, rolling, attribution |
| `/risk` | Drawdown curve, HHI, top-5/10 share, leverage |
| `/fx` | USD/TWD curve, FX-attributable P&L, currency exposure |
| `/transactions` | Filterable trade log, monthly volume + fee charts |
| `/cashflows` | Real vs counterfactual, monthly waterfall, bank ledger |
| `/dividends` | Monthly income, top payers, full distribution log |
| `/tax` | Per-ticker realized + unrealized P&L, win rate |
| `/ticker/<code>` | Position over time, cost vs MV, trades, dividends |
| `/benchmark` | Portfolio TWR vs market strategies |

A global freshness widget renders in the sidebar footer of every page,
driven by `/api/today/freshness`. Network failure renders "—".

## API surface

All endpoints return `{"ok": true, "data": ...}`. Errors are HTTP non-200.
TWD unless field name says otherwise.

### Monthly base (always available)
```
GET  /api/health
       → {months_loaded, as_of, daily_state: READY|INITIALIZING|FAILED,
          daily_last_known, daily_progress, daily_error}
GET  /api/summary
GET  /api/holdings/{current,timeline,sectors,snapshot/<month>}
GET  /api/performance/{timeseries,rolling,attribution}[?method=…]
GET  /api/transactions[?venue=&side=&code=&month=&q=]
GET  /api/transactions/aggregates
GET  /api/cashflows/{monthly,cumulative,bank}
GET  /api/dividends
GET  /api/risk
GET  /api/fx
GET  /api/tax
GET  /api/tickers
GET  /api/tickers/<code>
GET  /api/benchmarks/{strategies,compare}
```

### `?resolution=daily` query parameter

Most monthly endpoints accept `?resolution=daily`. When the daily layer
has rows, the body switches to a daily-shape payload and adds
`"resolution": "daily"` to the envelope. When empty, the parameter is
silently ignored and monthly shape is returned.

Honoured by: `/api/summary`, `/api/holdings/timeline`, `/api/performance/*`,
`/api/risk`, `/api/fx`, `/api/cashflows/monthly`, `/api/benchmarks/compare`.

### Daily-resolution layer
```
GET  /api/daily/equity[?start=YYYY-MM-DD&end=YYYY-MM-DD]
GET  /api/daily/prices/<symbol>
```

### `/today` widgets
```
GET  /api/today/snapshot           # latest equity + Δ vs prior priced day
GET  /api/today/movers
GET  /api/today/sparkline
GET  /api/today/period-returns
GET  /api/today/drawdown
GET  /api/today/risk-metrics
GET  /api/today/calendar
GET  /api/today/freshness
GET  /api/today/reconcile
```

### Admin / operator (manual-trigger)
```
POST /api/admin/refresh                     # snapshot_daily.run
GET  /api/admin/failed-tasks
POST /api/admin/retry-failed
POST /api/admin/reconcile?month=YYYY-MM
POST /api/admin/reconcile/<event_id>/dismiss
```

## Invariants (read these before editing the daily layer)

### Shioaji is read-only — forever
`backend/src/invest/brokerage/shioaji_client.py` MUST NOT import `Order`,
`place_order`, `cancel_order`, `update_order`, or `activate_ca`. Static-
grep tests in `backend/tests/brokerage/test_shioaji_client.py` enforce
this. The dashboard reads broker state; it never modifies it.

The client exposes three read-only surfaces (Phase 11 Path A):
| Method | Returns |
|---|---|
| `list_trades(start, end)` | Session-only — typically just today's fills |
| `list_open_lots(close_resolver)` | Currently-held lots |
| `list_realized_pairs(begin, end)` | Buy legs + sell summary for closed pairs |

**SDK quirk**: `quantity` field is always 0 for 零股 (odd-lot < 1000 shares).
Qty derived from `cost / price` for legs and `mv_twd / close` for open lots.

**Foreign account walled off**: per the 2026-05-01 probe (PLAN §3),
`signed=False` on H-account → broker enrollment missing. Foreign trades
remain PDF-canonical. `venue='TW'` hard-coded in shioaji_client.

### Reconciliation is operator-triggered for the destructive form
`invest.reconciliation.reconcile.run_for_month` MUST NOT be invoked from
`invest.jobs.backfill_runner`, `invest.jobs.snapshot_workflow`,
`scripts/parse_statements.py`, or `scripts/snapshot_daily.py`. The diff
runs automatically (read-only, emits events); the `--apply` flag that
mutates `trades` rows is gated behind `POST /api/admin/reconcile`.
PDFs are canonical — auto-fired diffs would be noisy or destructive.

### PDF rows are canonical; overlay rows never overwrite them
`invest.brokerage.trade_overlay` writes `positions_daily` rows with
`source='overlay'`, but the UPSERT carries `WHERE
positions_daily.source='overlay'` so an existing `source='pdf'` row is
never overwritten.

### 3-source overlay merge (Phase 11 Path A)
`trade_overlay.merge()` pulls open lots + realized pairs + session trades,
unifies them into trade-shaped records, dedups by `(date, code, side,
int(round(qty)))` with priority `realized_pair > list_trades > open_lot`.

### Audit hook fires on broker-vs-PDF leg-count mismatch (STRICT)
For each `pair_id` from `list_realized_pairs`, `trade_overlay._fire_audit_events`
counts SDK buy legs vs PDF buy trades for the same `(code, ≤sell_date)`
window. Any divergence fires `invest.reconciliation.reconcile.record_event(
event_type='broker_pdf_buy_leg_mismatch', ...)` for the operator to review.

### The daily layer is a cache
`data/dashboard.db` (SQLite, WAL mode + busy_timeout=5000) is regenerable
from `portfolio.json` plus public APIs. Backups should use
`sqlite3 ... .backup` (atomic, transactionally consistent), never `cp`
because of WAL sidecars.

## Backend ↔ frontend dev

```bash
# Backend (port 8001)
PYTHONPATH=backend/src uvicorn invest.app:app --port 8001

# Frontend (port 5173)
cd frontend && npm run dev

# Override proxy target if backend runs elsewhere:
VITE_API_TARGET=http://127.0.0.1:9999 npm run dev
```

Tests:
```bash
# Whole backend suite (869 tests + 4 skipped)
cd backend && pytest

# Targeted subsets:
cd backend && pytest tests/legacy/      # 149 verbatim-ported coverage tests
cd backend && pytest tests/analytics/   # Pure-function analytics
cd backend && pytest tests/jobs/        # Backfill + snapshot orchestration
```

## Phase 11 cutover summary (2026-05-02)

**Done**:
- `POST /api/admin/refresh` now routes through the canonical
  `invest.jobs.snapshot_workflow.run` (yfinance + Shioaji + DailyStore +
  PortfolioStore) — the same code path as `python scripts/snapshot_daily.py`.
  Previously the endpoint was on the SQLModel-backed `snapshot.run_incremental`
  scaffold (a no-op orchestrator that never executed real fetches).
- Added `invest.jobs.trade_backfill` — populates the SQLModel `trades`
  table from `data/portfolio.json:summary.all_trades`. Maps the 8
  observed Chinese side strings (`普買/普賣/資買/資賣/櫃買/櫃賣/買進/賣出`)
  onto the canonical `Side` enum (`CASH_BUY/CASH_SELL/MARGIN_BUY/MARGIN_SELL`),
  with venue/currency carrying the TW-vs-Foreign and main-vs-OTC distinctions.
- Idempotent re-run: clears prior `source='pdf'` rows and reinserts.
  `source='overlay'` rows are never touched (same invariant as
  `positions_daily`).
- Smoke-tested against real `data/portfolio.json`: all 312 trades
  mapped (0 skipped); idempotent re-run round-trips at 312 rows.
- Added `scripts/backfill_trades.py` (thin shim invoking the backfill).
- 23 new unit tests in `backend/tests/jobs/test_trade_backfill.py`
  covering the side mapping, idempotency, overlay-row preservation,
  and unknown-side leniency.

**Deferred to Phase 11.2+** (the *aggregator* — the hard part):
- Rewrite analytics modules (`invest.analytics.{twr, xirr, ratios,
  drawdown, concentration, attribution, tax_pnl, sectors}`) to compute
  monthly aggregates from the `trades` table on-the-fly, replacing
  PortfolioStore as the source of truth on the request path. Each
  metric ships behind a byte-equality verifier (compare new output
  against `PortfolioStore.summary[*]` for every month in the
  production dataset) before flipping the corresponding router branch.
- Wire price + FX repositories on top of the `prices` / `fx_daily`
  SQLModel tables (the SQLModel scaffolds already exist; need population
  and read-side verification against `DailyStore`).
- Decommission `PortfolioStore` once all router branches are flipped.

## Phase 10 cutover summary (2026-05-02)

**Done**:
- Verbatim-ported the entire transitional `app/` package into the
  `invest.*` namespace under `backend/src/invest/`:
  - `app/yfinance_client` → `invest.prices.yfinance_client`
  - `app/price_sources`   → `invest.prices.sources`
  - `app/shioaji_client`  → `invest.brokerage.shioaji_client`
  - `app/trade_overlay`   → `invest.brokerage.trade_overlay`
  - `app/reconcile`       → `invest.reconciliation.reconcile`
  - `app/backfill_state`  → `invest.core.state`
  - `app/backfill_runner` → `invest.jobs.backfill_runner` (1725 LOC)
  - `app/daily_store`     → `invest.persistence.daily_store`
  - `app/data_store`      → `invest.persistence.portfolio_store`
  - `app/holdings_today`  → `invest.analytics.holdings_today`
  - `app/analytics`       → `invest.analytics.monthly`
  - `app/benchmarks`      → `invest.benchmarks`
  - `scripts/snapshot_daily.run` → `invest.jobs.snapshot_workflow.run`
- Rewrote `scripts/{backfill_daily,snapshot_daily,reconcile,
  retry_failed_tasks,validate_data}.py` as thin shims pointing at
  `invest.jobs.*` / `invest.persistence.*` / `invest.brokerage.*` /
  `invest.reconciliation.*`.
- Moved the top-level `tests/` directory (149 tests covering the legacy
  modules) into `backend/tests/legacy/`, rewriting every `from app.X
  import Y` import in bulk so coverage is preserved against the new
  namespace.
- Deleted the entire `app/` package and the top-level `tests/` directory.
- Deleted the byte-equality parity tests in `backend/tests/analytics/`
  (their job ended when `app/` was removed — the modules they pinned
  parity against no longer exist).

**Coexistence note**: The redesigned scaffolds in `invest.jobs.{backfill,
snapshot,retry_failed}` and `invest.prices.{price_service,fx_provider}`
and `invest.reconciliation.shioaji_audit` (a Trade-table / SQLModel /
Repo-pattern design) coexist with the verbatim ports. The verbatim
ports power production today; the redesigned scaffolds receive features
as Phase 11+ lands.

**Phase 11+ landed in Phase 11 (2026-05-02)** — see "Phase 11 cutover
summary" above. The remaining `PortfolioStore` → analytics-on-trades
migration is now Phase 11.2+.

## Files NOT to commit

- `sinopac_pdfs/` (encrypted statements)
- `sinopac_pdfs/decrypted/` (definitely)
- `data/` (real positions, benchmark cache, dashboard.db)
- `logs/` (daily.log)
- `credentials.json`, `token.json`
- `.env`
- `*.pfx` (Sinopac CA bundle)
- `shioaji.log` / `**/shioaji.log` (SDK auto-creates)
