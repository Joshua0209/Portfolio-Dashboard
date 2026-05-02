# Sinopac Portfolio Dashboard

A personal investment performance dashboard built on top of Sinopac (永豐金)
monthly PDF statements. The pipeline turns encrypted statement PDFs into a
single JSON dataset, which a Flask backend then serves to a 12-page,
no-build-step dashboard. A second daily-resolution layer (yfinance prices
and FX rates in a local SQLite cache) powers the `/today` tactical view
and the equity-curve / sparkline visualizations on top of the monthly base.

```
PDFs (Gmail) ──► decrypt ──► parse ──► data/portfolio.json ──► Flask + JS
```

Everything runs locally. Nothing is sent to a remote service. The encrypted
PDFs, the decrypted PDFs, and the parsed `data/portfolio.json` are all
gitignored — the repository ships *code only*.

---

## What's in the dashboard

Twelve pages, each focused on a different question you'd ask about your
portfolio. Every metric and chart has an info icon (`ⓘ`) you can hover for
a plain-English explanation aimed at someone less than a year into investing.

| Page | URL | What it answers |
| --- | --- | --- |
| Today | `/today` | "What did markets do for me today?" Hero with weekday-named Δ vs prior session, top movers, 30-day sparkline, MTD/QTD/YTD/Inception strip, underwater drawdown curve, risk-and-return tile, daily-return calendar heatmap, freshness dot, and a Developer Tools accordion (failed-task DLQ + manual reconciliation). |
| Overview | `/` | "How am I doing right now?" Hero KPIs (equity, profit, TWR, XIRR), equity curve, allocation donut, top winners/losers, recent activity. |
| Holdings | `/holdings` | "What do I own?" Sortable table, real squarified treemap (area = market value, color = unrealized %), sector breakdown, CSV export. |
| Performance | `/performance` | "How is the strategy actually performing?" TWR / CAGR / XIRR / Sharpe / Sortino / Calmar with reference bands, monthly returns, drawdown timeline, rolling 3/6/12M, venue attribution (TW vs Foreign price vs FX). Switch between **day-weighted**, **mid-month**, and **end-of-month** Modified Dietz from the page actions. |
| Risk | `/risk` | "Where could I get hurt?" Drawdown episodes table, HHI concentration donut, top-5/10 share, margin (融資) leverage timeline, risk-adjusted ratios. |
| Cashflows | `/cashflows` | "Where did the money come and go?" Real-vs-counterfactual equity, monthly waterfall, full bank ledger with filters + pagination. Toggle four views: venue split, gross in vs out, net broker flow, external deposits. |
| Transactions | `/transactions` | "What did I trade?" Filterable trade log (venue / side / code / month / search), stacked monthly volume, fee chart with rebate offset, CSV export. |
| Dividends | `/dividends` | "What's my passive income?" Monthly stacked income (TW + Foreign), top payers, total return on cost, rebate ledger separated from real distributions, TTM yield. |
| Tax | `/tax` | "What do I owe at year end?" Per-ticker realized + unrealized P&L (FIFO basis), win rate, holding days, top winners/losers, CSV export. |
| FX | `/fx` | "How much of my P&L is just exchange rate?" USD/TWD curve, FX-attributable P&L (cumulative + monthly), currency exposure stack. |
| Benchmark | `/benchmark` | "Am I beating the market?" Compare your TWR against eight strategies — passive index (0050, SPY), dividend tilt (0056), naive mega-cap pickers, 60/40 balanced. yfinance-backed prices, 7-day cache. |
| Per-ticker | `/ticker/<code>` | "Tell me everything about ONE position." Position over time, cost vs market value, full trade history, dividend log. |

### Notable analytics features

- **Day-weighted Modified Dietz** — each per-trade flow weighted by `(D-d)/D`
  so a sell on the last day of the month barely shrinks the denominator.
  Mid-month and EOM variants are kept for comparison.
- **FIFO realized P&L** — TW tax-convention realized gains per ticker, with
  win rate, profit factor, and average holding days.
- **Bank-derived dividend ledger** — TW (`ACH股息`) + foreign (`國外股息`)
  cash credits resolved per ticker; broker-side data used as backfill only.
- **Margin-aware leverage** — 融資 positions tracked separately so equity-based
  returns aren't conflated with self-funded ones.
- **Daily-resolution layer** — `data/dashboard.db` (SQLite, WAL mode) caches
  yfinance prices (TW symbols via `.TW`/`.TWO` suffix, foreign as bare
  tickers) and FX rates, then derives `positions_daily` and
  `portfolio_daily`. Powers `/today`, the equity-curve sparkline, and the
  drawdown / calendar widgets. Cold-start backfill takes ~30–60s; refresh
  between server restarts via `python scripts/snapshot_daily.py` or the
  "Refresh now" button on `/today`.
- **Post-PDF trade overlay (optional)** — when `SINOPAC_API_KEY` /
  `SINOPAC_SECRET_KEY` are set, the daily layer folds in trades that
  happened *after* the most recent monthly statement closed via a
  read-only Shioaji client. Three SDK surfaces are unified into one
  merge: `list_open_lots` (currently-held positions), `list_realized_pairs`
  (closed-pair buy legs + sell summaries with `list_profit_loss_detail`
  drill-down), and `list_trades` (session-only safety net). PDFs remain
  canonical; overlay rows never overwrite `source='pdf'` rows. A strict
  audit hook fires a reconcile event whenever the SDK's buy-leg count
  diverges from the PDF parser's trades for the same window.
- **Manual reconciliation** — `POST /api/admin/reconcile?month=YYYY-MM`
  diffs PDF trades vs Shioaji-overlay trades for a given month and
  surfaces a global banner when they disagree. Always operator-triggered;
  never auto-fires from the backfill or parser.

---

## Quick start (data already parsed)

If `data/portfolio.json` already exists and your `.venv/` is set up:

```bash
cd path/to/investment
source .venv/bin/activate
python app.py
# open http://127.0.0.1:8000
```

The Flask app watches `data/portfolio.json` mtime — re-running the parser
while the server is up reloads data on the next request without a restart.

---

## Full setup (first time on a fresh machine)

### 1. Python environment

```bash
git clone <this-repo> investment
cd investment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Gmail / Drive credentials (only if you want auto-download)

Skip this if you'll drop PDFs into `sinopac_pdfs/` manually.

1. In Google Cloud Console, enable the Gmail API for your account.
2. Create OAuth desktop credentials.
3. Download `credentials.json` and place it at the repo root.
4. The first run of `scripts/download_sinopac_pdfs.py` will pop a browser
   window for consent and write `token.json` next to it.

Both `credentials.json` and `token.json` are gitignored.

### 3. Statement passwords

Sinopac PDFs are password-protected, and different statement types may use
different passwords (e.g. brokerage = National ID, bank = birth date). The
decrypter takes a comma-separated candidate list and tries each per file:

```bash
export SINOPAC_PDF_PASSWORDS="<national-id>,<birth-date-yyyymmdd>"
```

Never commit this value. Put it in `.env` or set it in your shell profile.

---

## Environment variables

Copy `.env.example` to `.env` (gitignored) and fill in what you need. None
are strictly required — the dashboard runs in PDF-only mode when everything
is unset.

| Variable | Required | Purpose |
| --- | --- | --- |
| `SINOPAC_PDF_PASSWORDS` | for `decrypt_pdfs.py` | Comma-separated unlock candidates; the decrypter tries each per file. |
| `SINOPAC_API_KEY` | optional | Read-only Shioaji credential. When set, the daily layer overlays post-PDF trades. Never used to place orders — `app/shioaji_client.py` does not import any order/CA symbols. **Must be present before the Flask process starts** — editing `.env` while `app.py` is running has no effect on the running process. |
| `SINOPAC_SECRET_KEY` | optional | Pair of `SINOPAC_API_KEY`. Same restart caveat. |
| `SINOPAC_CA_CERT_PATH` | unused (read-only contract) | Absolute path to the PKCS#12 `.pfx` certificate. **Documented but not consumed today.** The CA pair (`activate_ca`) only matters if/when trading is added in a separate module — the current `shioaji_client.py` is grep-guarded against `activate_ca`. Keep the file outside source control (gitignored via `*.pfx`). |
| `SINOPAC_CA_PASSWORD` | unused (read-only contract) | Unlock password for the `.pfx`. Same status as `SINOPAC_CA_CERT_PATH`. |
| `BACKFILL_ON_STARTUP` | optional, default `false` | When `true`, `create_app()` spawns the cold-start backfill in a daemon thread. The Flask debug-reloader is detected and the parent process is skipped to avoid double-spawning. |
| `ADMIN_TOKEN` | optional, default unset | When set, every `POST /api/admin/*` request must echo the value back via the `X-Admin-Token` header or the server returns `401`. Off by default so the localhost workflow keeps working unauthenticated; opt in only when exposing the dashboard via tunnel / LAN / reverse proxy. |
| `DAILY_DB_PATH` | optional | Override the default `data/dashboard.db` location (useful for tests). |
| `FLASK_DEBUG` | optional | Standard Flask flag; required if you want hot-reload on `app.py`. |

---

## Refresh workflow (new statements arrived)

Run the four steps in order. Each one is idempotent — re-running it
does nothing for files already processed.

```bash
cd path/to/investment
source .venv/bin/activate
export SINOPAC_PDF_PASSWORDS="..."

# 1. Pull new PDFs from Gmail (skip if you placed them manually)
python scripts/download_sinopac_pdfs.py

# 2. Decrypt — env-based unlock, outputs to sinopac_pdfs/decrypted/
python scripts/decrypt_pdfs.py

# 3. Parse → data/portfolio.json
python scripts/parse_statements.py

# 4. Start (or reload — it watches the JSON's mtime)
python app.py
```

If you already had `app.py` running, you don't need to restart — open any
dashboard page and it will pick up the new data on the next API call.

### Enabling the Shioaji overlay (optional)

The daily layer can fold in trades that happened *after* your most recent
PDF statement closed, using a read-only Shioaji session. The overlay is
**off by default** — the dashboard runs in PDF-only mode and silently
skips the merge unless credentials are present.

**One-time setup.**

1. Confirm the `shioaji` package is installed in your venv. It's listed
   in `requirements.txt` but heavy enough (~30–50MB across pysolace,
   pydantic-core, etc.) that `pip install -r requirements.txt` may have
   skipped or errored on it without you noticing:

   ```bash
   source .venv/bin/activate
   python -c "import shioaji; print(shioaji.__version__)"   # should print 1.x
   # if ModuleNotFoundError:
   pip install "shioaji>=1.2"
   ```

   When this is missing, `ShioajiClient.configured` stays `False` even
   with valid credentials — the log line will say `"Shioaji package not
   installed (...); trade overlay disabled"`.

2. Put the read-only API key/secret in `.env`:

   ```bash
   echo 'SINOPAC_API_KEY=...' >> .env
   echo 'SINOPAC_SECRET_KEY=...' >> .env
   ```

   The CA cert variables (`SINOPAC_CA_CERT_PATH`, `SINOPAC_CA_PASSWORD`)
   are **not** needed — the read-only client deliberately does not call
   `activate_ca`, and a static-grep test enforces that.

**Restart Flask** so the new env is in the running process. Both
`app.py` (via `app/__init__.py:_load_env`) and the CLI scripts
(`scripts/snapshot_daily.py`, `scripts/backfill_daily.py`,
`scripts/reconcile.py`) load `.env` automatically with
`override=False`, so a real shell-exported value still wins.

**Trigger the merge.** There's no dedicated "fetch broker trades"
button — the overlay piggybacks on every refresh path. Pick one:

- **UI** — open `/today` and click *Refresh now* in Developer Tools.
  This calls `POST /api/admin/refresh` → `snapshot_daily.run()` →
  `trade_overlay.merge()`. The response now includes an `overlay`
  block with `overlay_trades`, `dates_written`, and `skipped_reason`.
- **CLI** — `python scripts/snapshot_daily.py`. Prints the same
  summary on stdout via `daily.log`.
- **Cold start** — delete `data/dashboard.db` and run
  `python scripts/backfill_daily.py`. The final summary line includes
  `Shioaji overlay: N trade(s) (...)`.

**Verify it actually fired.** Check the log and the DB:

```bash
# Should show "trade_overlay merged: trades=N dates_written=M ..."
# If it says "skipped: reason=shioaji_unconfigured" the env didn't reach
# the process — restart Flask in a shell that has the keys.
tail -50 logs/daily.log | grep trade_overlay

# Overlay rows are tagged source='overlay'; PDF rows are never touched.
sqlite3 data/dashboard.db \
  "SELECT date, symbol, qty, mv_local FROM positions_daily \
   WHERE source='overlay' ORDER BY date DESC LIMIT 20;"
```

The overlay only writes rows for dates **strictly after** your latest
PDF month-end. If today falls inside the latest PDF month,
`compute_gap_window` returns `None` and the merge is a clean no-op
(logged as `skipped: reason=no_gap`).

**Three complementary read surfaces (Phase 11 Path A).** The overlay
no longer relies on `api.list_trades()` alone. The shioaji 1.3.x SDK
exposes three methods, all read-only, all pulled by `trade_overlay.merge()`:

| Surface | Window | What you see |
|---|---|---|
| `list_open_lots()` | snapshot | Currently-held lots with derived qty (`mv_twd / close`). Locks in your current 2330 / 00981A positions even without trade history. |
| `list_realized_pairs(start, end)` | sells in window | Closed pairs whose **sell date** falls in the window. Each pair drills down via `list_profit_loss_detail` to recover all buy legs (which may pre-date `start`). |
| `list_trades()` | session-only | Today's session fills — the safety net for trades placed since the last refresh. |

So a trade from earlier this month **does** appear in the overlay if
it was part of a closed pair whose sell date is in the gap window, OR
if the position is still currently held. The "buy in March, hold
through April" pattern is recovered via `list_open_lots()`; the "buy
in March, sell in April" round-trip is recovered via
`list_realized_pairs()`.

The remaining limitation: a position **bought and sold inside the gap
window before today** is recoverable only via `list_realized_pairs()`
(if it's already a closed pair). If your refresh runs *between* the
buy and the sell, the buy shows up as an open lot; once the sell
clears, the broker's accounting catches up and both legs appear in
the closed-pair query the next refresh.

**Audit hook — strict mismatch firing.** When the SDK's buy-leg count
for a closed pair differs from the PDF parser's trade count for the
same `(code, ≤sell_date)` window — even by one (broker consolidation
of partial fills) — the overlay fires a reconcile event of type
`broker_pdf_buy_leg_mismatch`. The global reconcile banner surfaces
it; you dismiss after triage. PDFs still win on `positions_daily`
writes — the audit is informational.

**Foreign account walled off.** `api.list_accounts()` returns the
H-type 複委託 account but every accounting query against it returns
HTTP 406 "Account Not Acceptable". Foreign trades remain
PDF-canonical; the overlay only handles TW.

### Backing up the daily SQLite cache (optional)

The daily-resolution layer (`data/dashboard.db`, added in the daily-prices
work) is a regenerable cache — if you delete it, the next run rebuilds it
from `portfolio.json` plus the public yfinance API in roughly 30–60
seconds. So backups are nice-to-have, not required.

If you do want a snapshot anyway, **never `cp` the file**. WAL mode keeps
in-flight pages in `dashboard.db-wal` and `dashboard.db-shm` sidecars, so a
plain copy can capture an inconsistent view. Use SQLite's online backup
command instead:

```bash
sqlite3 data/dashboard.db ".backup data/dashboard.db.bak"
```

It is safe to run while Flask is up — the backup is an atomic, transactionally
consistent copy taken through the same connection pool.

---

## Repository layout

```
investment/
├── app.py                        # Flask entrypoint
├── app/                          # Backend application package
│   ├── __init__.py               # create_app(), routes, blueprint registration, layered health
│   ├── data_store.py             # Mtime-cached portfolio.json loader
│   ├── daily_store.py            # SQLite (WAL) wrapper around data/dashboard.db
│   ├── analytics.py              # Drawdown, Sharpe, HHI, FX P&L, sectors, FIFO
│   ├── benchmarks.py             # yfinance fetcher + cached strategy curves
│   ├── filters.py                # Jinja currency/percent/date filters
│   ├── backfill_runner.py        # Background daemon: cold-start fetch + DLQ wrapper
│   ├── backfill_state.py         # READY/INITIALIZING/FAILED state machine + progress
│   ├── price_sources.py          # TW (.TW/.TWO probe) + foreign yfinance router
│   ├── yfinance_client.py        # yfinance HTTP wrapper (TW + foreign + FX)
│   ├── shioaji_client.py         # Read-only Shioaji wrapper (no Order/CA imports)
│   ├── trade_overlay.py          # Folds post-PDF Shioaji trades into the daily layer
│   ├── reconcile.py              # PDF-vs-overlay trade diff per month (manual trigger)
│   └── api/                      # 13 blueprints, all under /api/*
│       ├── summary.py            # KPIs, equity curve, allocation
│       ├── holdings.py           # Current/historical positions, sectors
│       ├── performance.py        # TWR/XIRR/drawdown/rolling/attribution (3 methods)
│       ├── transactions.py       # Trade log + monthly aggregates
│       ├── cashflows.py          # Real vs counterfactual, bank ledger
│       ├── dividends.py          # Distributions + rebates
│       ├── risk.py               # Concentration, leverage, drawdown
│       ├── fx.py                 # USD/TWD curve, FX P&L attribution
│       ├── tax.py                # Realized + unrealized P&L by ticker
│       ├── tickers.py            # Per-security drill-down
│       ├── benchmarks.py         # Strategy comparison
│       ├── daily.py              # Daily equity curve + per-symbol price history
│       └── today.py              # /today widgets + /api/admin/* (refresh, DLQ, reconcile)
├── scripts/                      # Pipeline (run from any CWD)
│   ├── download_sinopac_pdfs.py  # Gmail → sinopac_pdfs/
│   ├── decrypt_pdfs.py           # Env-based password unlock
│   ├── parse_statements.py       # PDFs → data/portfolio.json
│   ├── backfill_daily.py         # CLI cold-start backfill of dashboard.db
│   ├── snapshot_daily.py         # Incremental refresh (gap-fill since last_known_date)
│   ├── reconcile.py              # CLI mirror of POST /api/admin/reconcile
│   ├── retry_failed_tasks.py     # CLI mirror of POST /api/admin/retry-failed
│   └── validate_data.py          # Sanity checks across portfolio.json + dashboard.db
├── templates/                    # Jinja2 page templates (12 pages + 3 partials)
│   ├── _developer_tools.html     # DLQ + reconcile accordion, included on /today
│   ├── _dlq_banner.html          # Global "fetches failed" banner from base.html
│   ├── _reconcile_banner.html    # Global PDF-vs-overlay banner from base.html
├── static/                       # css/, js/ (vanilla; no build step)
│   ├── css/{tokens,app}.css      # Design system tokens + components
│   └── js/{api,charts,format,help,pagination,app,data-table,
│       dlq-banner,reconcile-banner,freshness}.js + pages/*.js
├── data/                         # gitignored — actual portfolio data
│   ├── portfolio.json            # Parsed dataset consumed by Flask (canonical)
│   ├── tw_ticker_map.json        # Manual TW name → ticker overrides
│   ├── benchmarks.json           # yfinance price cache (7-day TTL)
│   └── dashboard.db              # Daily-resolution SQLite cache (WAL; regenerable)
├── logs/                         # gitignored — daily.log (rotating, 5 MB × 3)
├── sinopac_pdfs/                 # gitignored — encrypted source PDFs
│   └── decrypted/                # gitignored — decrypted copies
├── credentials.json              # gitignored — Gmail OAuth client
└── token.json                    # gitignored — Gmail OAuth token
```

---

## API surface

All endpoints return `{"ok": true, "data": ...}`. Errors are HTTP non-200.
Convention: TWD unless field name says otherwise; foreign positions show
both `_local` and `_twd` values where relevant.

### Monthly base layer (always available)

```
GET  /api/health
       → { months_loaded, as_of, daily_state: READY|INITIALIZING|FAILED,
           daily_last_known, daily_progress, daily_error }
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

Most monthly endpoints accept an optional `?resolution=daily` query
parameter. When the daily SQLite layer has rows, the body switches to a
daily-shape payload (per-day rows keyed by `date` instead of monthly rows
keyed by `month`) and adds `"resolution": "daily"` to the envelope.
When the daily layer is empty, the parameter is ignored and the monthly
shape is returned — pages that opt in via `static/js/api.js` therefore
render correctly even before the cold-start backfill finishes. Currently
honoured by `/api/summary`, `/api/holdings/timeline`, `/api/performance/*`,
`/api/risk`, `/api/fx`, `/api/cashflows/monthly`, and
`/api/benchmarks/compare` (overlay only — strategy curves stay monthly).

### Daily-resolution layer (returns 202 + progress while INITIALIZING)

```
GET  /api/daily/equity[?start=YYYY-MM-DD&end=YYYY-MM-DD]
GET  /api/daily/prices/<symbol>            # daily price history + trade markers
```

### `/today` widgets

```
GET  /api/today/snapshot                    # latest equity + Δ vs prior priced day
GET  /api/today/movers                      # gainers/decliners between two priced dates
GET  /api/today/sparkline                   # last 30 trading days of equity_twd
GET  /api/today/period-returns              # MTD / QTD / YTD / Inception
GET  /api/today/drawdown                    # underwater curve + max-DD metadata
GET  /api/today/risk-metrics                # ann return/vol, Sharpe, Sortino, hit rate
GET  /api/today/calendar                    # daily-return calendar heatmap
GET  /api/today/freshness                   # data_date, today_in_tpe, stale_days, band
GET  /api/today/reconcile                   # open reconciliation events for the banner
```

### Admin / operator (manual trigger only)

```
POST /api/admin/refresh                     # incremental gap-fill (calls snapshot_daily)
GET  /api/admin/failed-tasks                # open DLQ rows
POST /api/admin/retry-failed                # drain the DLQ; { resolved, still_failing }
POST /api/admin/reconcile?month=YYYY-MM     # PDF vs Shioaji-overlay diff for one month
POST /api/admin/reconcile/<event_id>/dismiss
```

While the daily layer is still warming up, daily/today endpoints return
HTTP 202 with a `progress` payload instead of 500. The frontend (`static/js/api.js`)
retries with exponential backoff; on `FAILED` the response is HTTP 503 with
the error string, and the warming banner deep-links to `/today#developer-tools`.

---

## Caveats and edge cases

- **Bank account is treated as external.** TW securities + Foreign brokerage
  are *inside* the portfolio (the investments themselves). The bank account
  is a *source of capital* — used only for the USD/TWD FX rate.
- **TW trade tickers** — the TW monthly statement's trade table prints only
  the abbreviated stock name, not the ticker code. The parser auto-derives
  it by matching against the holdings table at month-end. For intra-month
  round-trips (bought and sold within the same month), no holdings row ever
  exists; add a manual override to `data/tw_ticker_map.json`.
- **Margin (融資)** — equity = market value, but cost includes only your
  portion (資自備款). Equity-based returns can look inflated. The Risk page
  surfaces a leverage timeline so you can see this.
- **Foreign FX** — only USD positions are TWD-converted right now. If you
  hold HKD or JPY, extend the conversion loop in `scripts/parse_statements.py`.
- **Sector mapping** is heuristic, not API-backed. See the `_TW_SECTOR_HINTS`
  and `_US_SECTOR_HINTS` dicts in `app/analytics.py`. Unmapped tickers fall
  through to "TW Equity (other)" / "US Equity (other)".
- **Benchmarks need yfinance.** First run hits the network; subsequent runs
  use the 7-day cache in `data/benchmarks.json`.
- **No auth on read endpoints.** The server is bound to `127.0.0.1` by
  default. The Flask app does set conservative response headers
  (`X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`,
  `Referrer-Policy: strict-origin-when-cross-origin`, and a
  same-origin-only CSP) via `after_request`, and `POST /api/admin/*`
  can be gated by setting `ADMIN_TOKEN` (clients must echo
  `X-Admin-Token`). Read endpoints under `/api/*` remain unauthenticated.
  Do not expose the dashboard to a public network without an auth
  proxy in front, regardless of `ADMIN_TOKEN`.

---

## Extending the dashboard

To add a new metric:

1. Implement it in `app/analytics.py` (pure-Python, no I/O — keep it testable).
2. Surface it in the relevant blueprint under `app/api/`.
3. Wire a chart or KPI tile in `templates/<page>.html` and the matching
   `static/js/pages/<page>.js`.
4. Add an `info-icon` next to the title with a one-sentence rookie-friendly
   explanation (`<span tabindex="0" class="info-icon" data-info="..."></span>`).

To add a new statement type, see the dispatch in `scripts/parse_statements.py:main`
and the "Adding a new statement type" section of `CLAUDE.md`.

---

## Files NOT to commit

- `sinopac_pdfs/` (encrypted statements)
- `sinopac_pdfs/decrypted/`
- `data/` (contains real positions)
- `credentials.json`, `token.json`
- `.env`
