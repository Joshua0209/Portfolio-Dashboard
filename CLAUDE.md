# Sinopac Investment Dashboard

Personal investment performance dashboard built from Sinopac (永豐金) monthly PDF statements.
Pipeline: encrypted PDFs → decrypt → parse → JSON → **Flask backend + 12-page dashboard**.

A second daily-resolution layer sits on top of the monthly base: yfinance
prices (TW symbols via `.TW`/`.TWO` suffix, foreign as bare tickers) and FX
rates cached in a local SQLite (`data/dashboard.db`, WAL mode), plus an
optional read-only Shioaji client that overlays trades that happened *after*
the most recent monthly statement closed. The daily layer is a regenerable
cache — wipe `dashboard.db` and the next backfill rebuilds it in 30–60s.

## Layout

```
investment/
├── credentials.json              # Google API creds for the downloader (gitignored)
├── token.json                    # OAuth token (gitignored)
├── sinopac_pdfs/                 # Encrypted source PDFs (gitignored)
│   └── decrypted/                # Decrypted copies (gitignored — sensitive)
├── data/                         # gitignored — real portfolio data
│   ├── portfolio.json            # Parsed dataset consumed by the dashboard (canonical)
│   ├── tw_ticker_map.json        # Manual TW name→code overrides (see below)
│   ├── benchmarks.json           # yfinance price cache (7-day TTL)
│   └── dashboard.db              # Daily-resolution SQLite cache (WAL; regenerable)
├── logs/                         # gitignored — daily.log (rotating, 5 MB × 3)
├── scripts/                      # Pipeline (run from any CWD; ROOT auto-resolved)
│   ├── download_sinopac_pdfs.py  # Pull statement PDFs from Gmail
│   ├── decrypt_pdfs.py           # Step 1: env-based password unlock
│   ├── parse_statements.py       # Step 2: extract holdings + flows → data/portfolio.json
│   ├── backfill_daily.py         # Cold-start full backfill of dashboard.db
│   ├── snapshot_daily.py         # Incremental refresh (gap-fills since meta.last_known_date)
│   ├── reconcile.py              # CLI mirror of POST /api/admin/reconcile
│   ├── retry_failed_tasks.py     # CLI mirror of POST /api/admin/retry-failed
│   └── validate_data.py          # Sanity checks across portfolio.json + dashboard.db
├── app.py                        # Flask entrypoint
├── app/                          # Backend application package
│   ├── __init__.py               # create_app(), routes, blueprint registration, layered /api/health
│   ├── data_store.py             # Mtime-cached portfolio.json loader
│   ├── daily_store.py            # SQLite (WAL) wrapper around data/dashboard.db
│   ├── analytics.py              # Drawdown, Sharpe, Sortino, Calmar, HHI, FX P&L, FIFO P&L, sectors
│   ├── benchmarks.py             # yfinance fetcher + cached strategy curves
│   ├── filters.py                # Jinja currency/percent/date filters
│   ├── backfill_runner.py        # Background daemon: cold-start fetch + DLQ wrapper
│   ├── backfill_state.py         # READY/INITIALIZING/FAILED state machine + progress
│   ├── price_sources.py          # TW (.TW/.TWO probe) + foreign yfinance router
│   ├── yfinance_client.py        # yfinance HTTP wrapper (TW + foreign + FX)
│   ├── shioaji_client.py         # Read-only Shioaji wrapper (no Order/CA imports — see below)
│   ├── trade_overlay.py          # Folds post-PDF Shioaji trades into the daily layer
│   ├── reconcile.py              # PDF-vs-overlay trade diff per month (manual trigger only)
│   └── api/                      # 13 blueprints, all under /api/*
│       ├── summary.py            # KPIs, equity curve, allocation
│       ├── holdings.py           # Current/historical positions, sectors
│       ├── performance.py        # TWR/XIRR/drawdown/rolling/attribution (3 weighting methods)
│       ├── transactions.py       # Trade log + monthly aggregates + rebates
│       ├── cashflows.py          # Real vs counterfactual, bank ledger, gross/net views
│       ├── dividends.py          # Distributions + rebates
│       ├── risk.py               # Concentration, leverage, drawdown, ratios
│       ├── fx.py                 # USD/TWD curve, FX P&L attribution
│       ├── tax.py                # Realized + unrealized P&L by ticker (FIFO)
│       ├── tickers.py            # Per-security drill-down
│       ├── benchmarks.py         # Strategy comparison vs portfolio
│       ├── daily.py              # /api/daily/equity + /api/daily/prices/<symbol>
│       └── today.py              # /api/today/* widgets + /api/admin/* (no url_prefix; see below)
├── templates/                    # Jinja2 page templates (12 pages + 2 partials)
│   ├── _developer_tools.html     # DLQ + reconcile accordion, included on /today
│   ├── _reconcile_banner.html    # Global banner included from base.html
└── static/                       # css/, js/ (vanilla; no build step)
    ├── css/{tokens,app}.css      # Design system + components
    └── js/{api,charts,format,help,pagination,app}.js + freshness.js + pages/*.js
```

### `today` blueprint exception
Unlike the other 12 blueprints, `app/api/today.py` registers with no
`url_prefix` and mounts both `/api/today/*` (read) and `/api/admin/*`
(operator-triggered writes). This is deliberate cohesion — the admin
endpoints (refresh, retry-failed, reconcile, dismiss) are reconciliation/
freshness primitives surfaced by the same `/today` page.

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

# 4. start the Flask dashboard (refreshes data automatically when JSON updates)
python3 app.py
# then open http://127.0.0.1:8000/
```

The Flask app watches `data/portfolio.json` mtime — re-running `parse_statements.py`
while the server is up reloads data on the next request without a restart.

**Daily layer refresh.** The monthly base updates from PDFs; the daily layer
(`data/dashboard.db`) updates from public market APIs. Three ways to refresh:

1. **Cold start** — delete `data/dashboard.db` and either set
   `BACKFILL_ON_STARTUP=true` (background daemon thread on `create_app()`) or
   run `python scripts/backfill_daily.py` directly. Endpoints under
   `/api/daily/*` and `/api/today/*` return HTTP 202 + progress while the
   backfill is running, then flip to 200 with no restart.
2. **Incremental** — `python scripts/snapshot_daily.py` gap-fills from
   `meta.last_known_date` to today. Idempotent — re-running with no new
   trading days writes 0 rows. Same code path as `POST /api/admin/refresh`
   (the "Refresh now" button on `/today`).
3. **Retry the DLQ** — failed external fetches land in `failed_tasks` and
   are drained by `python scripts/retry_failed_tasks.py` or
   `POST /api/admin/retry-failed`.

## Environment variables

`.env` (gitignored) holds local overrides. None are strictly required — the
dashboard runs in PDF-only mode when everything is unset.

| Variable | Purpose |
| --- | --- |
| `SINOPAC_PDF_PASSWORDS` | Comma-separated PDF unlock candidates. The decrypter tries each per file; first that opens wins. Different statement types use different passwords (brokerage = National ID, bank = birth date). |
| `SINOPAC_API_KEY` / `SINOPAC_SECRET_KEY` | Shioaji read-only credentials. When both are set, `app/trade_overlay.py` overlays post-PDF trades onto the daily layer. Without them, the dashboard is fully functional in PDF-only mode and the overlay logs `skipped_reason='shioaji_unconfigured'` once per process lifetime. |
| `BACKFILL_ON_STARTUP` | Default `false`. When `true`, `create_app()` spawns the cold-start backfill in a daemon thread. The Flask debug-reloader is detected via `WERKZEUG_RUN_MAIN` so the parent process is skipped (otherwise both parent and child would spawn a thread). |
| `DAILY_DB_PATH` | Override the default `data/dashboard.db` location. Used in tests to swap in a per-test temporary DB. |

Never commit any of these values.

## Portfolio definition (important)

| Account | Treated as | Why |
|---|---|---|
| TW securities (證券月對帳單) | inside portfolio | the investments themselves |
| Foreign / 複委託 | inside portfolio | same |
| Bank (永豐銀行 綜合對帳單) | **external** | source of capital; only used for USD/TWD FX rate |

External cashflows = `客戶淨收付` (TW) + `應收/付` sum (foreign), TWD-converted.

## Performance metrics

- **TWR (Modified Dietz, monthly)** — three flow-weighting variants, switchable
  in the UI and via `?method=` on `/api/performance/*`:
  - `day_weighted` (default): each per-trade flow weighted by `(D-d)/D`. A sell
    on the last day of the month barely shrinks the denominator. Most accurate
    when deposits/withdrawals cluster intra-month.
  - `mid_month`: legacy Modified Dietz, all flows weighted 0.5:
    `r = (V_end − V_start − F) / (V_start + 0.5·F)`.
  - `eom`: end-of-month assumption, flows weighted 0.0.
  - All three chain across months. Month 1 is forced to 0% (no prior equity).
- **XIRR**: Newton-Raphson on cashflow dates. Money-weighted; reflects what
  *your money* actually earned. Cashflows dated to month-mid; final equity
  treated as a terminal inflow.
- **Sortino / Calmar / Sharpe** — all three printed on the Performance page
  with reference bands (<1 weak, 1–2 acceptable, 2–3 good, 3–5 excellent,
  >5 elite or thin sample).

TWR and XIRR often diverge. TWR ≫ XIRR usually means recent deposits haven't
had time to compound; that's normal, not a bug.

## TW ticker codes for trades

The TW monthly statement's *trade* table prints only the abbreviated stock
name, not the ticker code — the code only appears in the *holdings* table.
For positions held at any month-end the parser auto-derives the code by
matching trade name → holdings name. For intra-month round-trips (bought
and sold within the same month) or pre-data-window exits, the name never
appears in any holdings table.

`data/tw_ticker_map.json` is the manual override file that fills those
gaps. Keys are normalized halfwidth names (`'台玻'`, `'貿聯KY'`); values
are codes (`'1802'`). When `個股分析` shows a blank 代號 for a closed
position, add an entry and re-run `scripts/parse_statements.py`.

## Caveats

- **Margin (融資)**: equity = market value of all positions, but cost includes
  only your portion (資自備款). Equity-based returns can look inflated. Read
  `holdings_detail.type == "融資"` rows with that in mind.
- **Foreign FX**: only USD positions are TWD-converted right now. Add HKD/JPY
  rates from the bank statement if those positions appear (extend the loop in
  `scripts/parse_statements.py:main`).
- **Dividends**: bank-derived per-event records are the source of truth.
  `summary.dividends[]` carries one row per cash credit (TW `ACH股息` and
  foreign `國外股息`), with the ticker resolved from the memo column.
  Per-holding lifetime dividend sits on `tw.holdings[i].cum_dividend`
  (parsed from `累計配息`). The broker `海外股票現金股利明細` section is
  used as a backfill only — it's frequently empty.
- **The fetch() requirement**: opening static HTML directly (file://) fails
  because browsers block local JSON fetches. Always use the Flask app or a local server.
- **Sector mapping**: `app/analytics.py` has a hand-curated heuristic in
  `_TW_SECTOR_HINTS` and `_US_SECTOR_HINTS`. Unmapped tickers fall through
  to "TW Equity (other)" / "US Equity (other)". Extend the dicts as needed
  — there's no external API call.

## Dashboard pages (URL → purpose)

| URL | Purpose |
|---|---|
| `/today` | Tactical view. Hero with weekday-named Δ vs prior session, top movers, 30-day sparkline, MTD/QTD/YTD/Inception strip, underwater drawdown curve, risk-and-return tile, daily-return calendar heatmap, freshness dot, Developer Tools accordion (DLQ + manual reconciliation). Markets-closed wallclock-context line shows when `today_in_tpe ≠ data_date` (weekend / pre-open). |
| `/` | KPI hero, equity curve, allocation donut, top movers, recent activity |
| `/holdings` | Sortable table, treemap-style position map, sector breakdown, CSV export |
| `/performance` | TWR/XIRR, monthly returns, drawdown, rolling 3/6/12M, venue attribution |
| `/risk` | Drawdown curve, HHI concentration, top-5/10 share, leverage exposure |
| `/fx` | USD/TWD curve, FX-attributable P&L, currency exposure stack |
| `/transactions` | Filterable trade log, monthly volume + fee charts, CSV export |
| `/cashflows` | Real vs counterfactual chart, monthly waterfall, bank ledger |
| `/dividends` | Monthly income, top payers, full distribution log |
| `/tax` | Per-ticker realized + unrealized P&L, win rate, CSV export |
| `/ticker/<code>` | Position over time, cost vs MV chart, trades, dividends |
| `/benchmark` | Portfolio TWR vs market strategies (TW + US, multi-tier) |

A global freshness widget renders in the sidebar footer of every page
(`templates/base.html` + `static/js/freshness.js`), driven by
`/api/today/freshness`. Network failure renders "—" instead of crashing
the page — never breaks navigation.

## API surface

All endpoints return `{"ok": true, "data": ...}`. Errors are HTTP non-200.
Convention: TWD unless field name says otherwise; foreign positions show
both `_local` and `_twd` values where relevant.

### Monthly base (always available)
```
GET  /api/health
       → {months_loaded, as_of, daily_state: READY|INITIALIZING|FAILED,
          daily_last_known, daily_progress, daily_error}
GET  /api/summary
GET  /api/holdings/{current,sectors,timeline}
GET  /api/holdings/snapshot/<month>
GET  /api/performance/{timeseries,rolling,attribution}[?method=day_weighted|mid_month|eom]
GET  /api/transactions[?venue=&side=&code=&month=&q=]
GET  /api/transactions/aggregates
GET  /api/cashflows/{monthly,cumulative,bank}
GET  /api/dividends
GET  /api/risk
GET  /api/fx
GET  /api/tax
GET  /api/tickers
GET  /api/tickers/<code>
GET  /api/benchmarks/strategies
GET  /api/benchmarks/compare?keys=tw_passive,us_passive
```

### Daily-resolution layer
```
GET  /api/daily/equity[?start=YYYY-MM-DD&end=YYYY-MM-DD]
GET  /api/daily/prices/<symbol>
```

### `/today` widgets
```
GET  /api/today/snapshot           # latest equity + Δ vs prior priced day
GET  /api/today/movers             # gainers/decliners between two priced dates
GET  /api/today/sparkline          # last 30 trading days of equity_twd
GET  /api/today/period-returns     # MTD / QTD / YTD / Inception
GET  /api/today/drawdown           # underwater curve + max-DD metadata
GET  /api/today/risk-metrics       # ann return/vol, Sharpe, Sortino, hit rate
GET  /api/today/calendar           # daily-return calendar heatmap
GET  /api/today/freshness          # data_date, today_in_tpe, stale_days, band
GET  /api/today/reconcile          # open reconciliation events (drives the global banner)
```

### Admin / operator (all manual-trigger; never auto-fired)
```
POST /api/admin/refresh                     # incremental gap-fill (calls snapshot_daily)
GET  /api/admin/failed-tasks                # open DLQ rows
POST /api/admin/retry-failed                # drain the DLQ → {resolved, still_failing}
POST /api/admin/reconcile?month=YYYY-MM     # PDF vs Shioaji-overlay diff for one month
POST /api/admin/reconcile/<event_id>/dismiss
```

### Health states (Phase 9 contract)
- `READY` — daily store has rows; daily/today endpoints return 200.
- `INITIALIZING` — backfill in progress *or* daily store empty. Daily/today
  endpoints return HTTP 202 with `progress` payload. Frontend `static/js/api.js`
  retries with exponential backoff.
- `FAILED` — backfill exception. Daily/today endpoints return HTTP 503 with
  the error string. Banner deep-links to `/today#developer-tools`.

## Invariants (read these before editing the daily layer)

### Shioaji is read-only — forever
`app/shioaji_client.py` MUST NOT import `Order`, `place_order`,
`cancel_order`, `update_order`, or `activate_ca` from the Shioaji SDK.
This is enforced by static-grep tests in `tests/test_shioaji_client.py`
and `tests/test_reconcile.py`. The dashboard reads broker state; it never
modifies it. Without credentials, the client logs once and returns `[]`
on every read — the daily layer treats unconfigured Shioaji as a clean
no-op, never as an error.

### Reconciliation is manual-trigger only
`app/reconcile.py` MUST NOT be invoked from `app/backfill_runner.py`,
`scripts/parse_statements.py`, or `scripts/snapshot_daily.py`. The diff
is *operator-triggered*: the user clicks "Reconcile this month" on
`/today` (POST `/api/admin/reconcile?month=YYYY-MM`) or runs
`python scripts/reconcile.py YYYY-MM`. This is enforced by static-grep
tests. Rationale: PDFs are the canonical source — auto-fired diffs would
either be noisy (statement still settling) or destructive (a half-known
month would banner spurious "missing trades").

### PDF rows are canonical; overlay rows never overwrite them
`app/trade_overlay.py` writes `positions_daily` rows with
`source='overlay'`, but the UPSERT carries `WHERE positions_daily.source =
'overlay'` so an existing `source='pdf'` row is never overwritten.
PDFs win every conflict.

### The daily layer is a cache
`data/dashboard.db` (SQLite, WAL mode + busy_timeout=5000) is regenerable
from `portfolio.json` plus public APIs. It contains no source-of-truth
data. Backups should use `sqlite3 ... .backup` (atomic, transactionally
consistent) — never `cp` because WAL keeps in-flight pages in
`dashboard.db-wal` / `dashboard.db-shm` sidecars.

## Adding a new statement type

The parser dispatches in `scripts/parse_statements.py:main` based on filename
substring (`證券月對帳單`, `複委託`, `銀行綜合`). To add a new type:

1. Write a `parse_<type>(pdf_path) -> dict` function returning a structured
   month record.
2. Add a filename branch in `main()` to populate `files_by_month[ym][...]`.
3. Decide if it's inside-portfolio (affects equity & flows) or external.
4. Surface the new fields in the relevant `app/api/*.py` blueprint and
   wire up a chart in the matching `templates/*.html` + `static/js/pages/*.js`.

## Files NOT to commit

- `sinopac_pdfs/` (encrypted statements)
- `sinopac_pdfs/decrypted/` (definitely)
- `data/` (real positions, benchmark cache, dashboard.db)
- `logs/` (daily.log)
- `credentials.json`, `token.json`
- `.env`
