# Daily Prices, Today Page, and Read-Only Sinopac Integration — Design

**Date:** 2026-04-26
**Status:** Approved (2026-04-26 round 3 — five open questions resolved; see [implementation plan](../plans/2026-04-26-daily-prices-and-today-page-implementation-plan.md) §6 for the resolutions)
**Author:** Joshua + Claude (brainstorming session)

---

## Summary

Today the dashboard runs entirely off month-end Sinopac PDFs. The equity curve has ~7 monthly points, drawdown can only be measured month-to-month, and any trade since the last PDF is invisible. This design adds **daily-resolution price and portfolio data** by pulling free public APIs (TWSE, TPEX, yfinance) plus narrowly-scoped read-only Shioaji for the post-PDF trade gap. Storage moves from JSON-only to JSON + SQLite. A new `/today` page surfaces tactical intraday context (today's Δ, top movers, freshness). Existing pages auto-upgrade to daily resolution via a `?resolution=daily` parameter.

The architecture is deliberately additive: the existing PDF parser, `data/portfolio.json`, and all current endpoints stay untouched. New code lives in new modules. A user without Shioaji credentials still gets a fully functional dashboard with daily prices — they just lose the ~30-day post-PDF trade overlay.

---

## Goals

- Daily-resolution equity curve, drawdown, rolling Sharpe/Sortino, and per-ticker price charts.
- Historical backfill from `BACKFILL_FLOOR = 2025-08-01` (or each symbol's first_trade_date if later) to today, with per-symbol fetch windows clipped to actual position activity. Derivable purely from price history + existing trade ledger.
- "Today's portfolio value" with Δ vs prior trading day, surfaced on a dedicated `/today` page.
- Trade-log freshness for the gap between the last PDF and today, sourced from Shioaji read-only APIs.
- Zero-credential happy path: TWSE + TPEX + yfinance cover all price data with no API keys.
- Read-only by construction: Shioaji client never imports `Order`, never calls `activate_ca`, never calls `place_order` / `cancel_order` / `update_order`.
- Graceful degradation: missing Shioaji creds → no overlay (PDF-only trade data). Network failures → DLQ + retry-all.
- Self-healing: cold start backfills each symbol's `[max(first_trade_date, BACKFILL_FLOOR), max(last_trade_date, last_held_date)]` window. Warm restarts incrementally fill gaps. Manual `snapshot_daily.py` for between-restart refreshes.

## Non-goals

- **Order placement.** Explicitly excluded. The Shioaji client never gains write access; even if a future contributor wanted to issue an order from inside Flask, they would need to add the CA cert path and import order types themselves.
- **Intraday/live tick updates during market hours.** v1 treats "today" as "the latest completed trading day in the DB." Manual `snapshot_daily.py` refreshes pull current prices; the dashboard does not poll TWSE during market hours.
- **K-bar OHLC granularity.** Daily close is the unit. No minute-bars, no tick-level data.
- **Corporate-action handling for ticker code changes.** If a held symbol's code migrates (e.g., delisting + relisting), v1 does not attempt to follow it automatically. Manual override file required.
- **Replacing PDFs as the source of truth.** PDFs remain authoritative for cost basis, dividends, FX, fees, and realized P&L. Daily data is a layer on top.

---

## Architecture

### Module layout

```
investment/
├── data/
│   ├── portfolio.json          # unchanged (PDF parser output)
│   ├── benchmarks.json         # unchanged (yfinance benchmark cache)
│   └── dashboard.db            # NEW: SQLite, time-series tables
├── logs/
│   └── daily.log               # NEW: rotating log
├── scripts/
│   ├── parse_statements.py     # unchanged
│   ├── backfill_daily.py       # NEW: one-shot full historical fill
│   ├── snapshot_daily.py       # NEW: incremental fill + overlay refresh
│   ├── reconcile.py            # NEW: manual PDF-vs-overlay diff
│   ├── validate_data.py        # NEW: data integrity gate (run before any UI work)
│   └── retry_failed_tasks.py   # NEW: DLQ retry-all
└── app/
    ├── data_store.py           # unchanged (portfolio.json mtime cache)
    ├── daily_store.py          # NEW: SQLite reader + readiness state
    ├── price_sources.py        # NEW: symbol-class router
    ├── twse_client.py          # NEW: TWSE HTTP wrapper
    ├── tpex_client.py          # NEW: TPEX HTTP wrapper
    ├── shioaji_client.py       # NEW: read-only session singleton
    ├── trade_overlay.py        # NEW: PDF + Shioaji trade merge
    ├── backfill_runner.py      # NEW: background thread + state machine
    └── api/
        ├── daily.py            # NEW: /api/daily/* blueprint
        └── today.py            # NEW: /api/today/* blueprint
```

### Module responsibilities

- **`twse_client.py`** — thin HTTP wrapper over `https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY?date=YYYYMM01&stockNo=XXXX&response=json`. Returns `[(date, close, volume)]` for one stock for one calendar month. Empty response = "not on TWSE." Defenses against TWSE WAF during deep historical backfill: **(a) dynamic backoff** — base sleep 0.5s; on any non-200 (429, 5xx, timeout), double the cooldown for the next 10 requests, then halve back toward base; on three consecutive non-200, freeze the client for 60s and log WARN. **(b) User-Agent rotation** — pool of 4–5 realistic browser UAs (Chrome/Firefox/Safari mix), rotated round-robin per request. **(c) Jitter** — uniform 0–200ms added to every sleep to break exact-cadence detection signatures. No state beyond what the `prices` table provides; backoff state is in-memory per-client and resets on process restart.
- **`tpex_client.py`** — same shape against TPEX endpoints (`https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php`).
- **`price_sources.py`** — symbol-class router. `get_prices(symbol, currency, start, end)` dispatches to the right backend:
  - Foreign (currency != TWD) → yfinance
  - TW (currency == TWD): consult `symbol_market` cache → if cached, dispatch directly. If not cached, probe TWSE first; on empty, probe TPEX; on empty, fall back to yfinance with `.TW` suffix; on empty, mark `unknown`. Cache the verdict.
  - FX (`USD/TWD`) → yfinance `TWD=X` (always).
  - Returns uniform `[(date: ISO string, close: float, currency: str)]`.
- **`shioaji_client.py`** — singleton wrapping `sj.Shioaji()` and `api.login()`. Imports nothing from `shioaji` related to orders. Exposes `list_profit_loss(begin, end)` and `list_positions()` only. Constructor is `lazy_login()` — checks env vars; if absent, returns a no-op client where calls return empty lists. Read-only by construction. **Timezone handling**: every `begin_date` / `end_date` parameter is coerced to `Asia/Taipei` via `zoneinfo.ZoneInfo("Asia/Taipei")` before being passed to Shioaji. The Shioaji backend evaluates date boundaries in TPE wall-clock time; passing UTC dates causes T+2 settlement records near month-end boundaries to land on the wrong side of the cutoff. Even when `datetime.now()` already looks "correct" on a TPE-localized server, we explicitly localize to defend against future container/CI environments running in UTC. Helper: `to_taipei_date(d) -> date` lives here and is used by `trade_overlay.py`.
- **`trade_overlay.py`** — given `last_pdf_end_date` (read from `data/portfolio.json`) and `today`, returns a list of trades in the gap window. Implementation: `list_profit_loss(begin=last_pdf_end+1, end=today)` for closed trades + diff `list_positions()` against month-end PDF positions for open trades. All date arithmetic uses `to_taipei_date()` (see `shioaji_client.py`) so T+2 settlement boundaries align with the PDF's TPE-wall-clock day. Result merged with `portfolio.json` ledger when computing `positions_daily`.
- **`daily_store.py`** — owns the SQLite schema. Read-only on the request path. Exposes `get_equity_curve(start, end)`, `get_today_snapshot()`, `get_ticker_history(code, start, end)`, `get_failed_tasks()`. Uses `check_same_thread=False` connections; reads use per-thread connections, writes serialize through a single writer connection guarded by an `RLock`. The actual gap-fill logic is in `backfill_runner.py`.
- **`backfill_runner.py`** — encapsulates the readiness state machine and the gap-fill job. Three states: `INITIALIZING`, `READY`, `FAILED`. Spawned as a daemon thread on Flask startup. Holds the writer connection. Failures push rows to `failed_tasks`; one failure does not abort the run. Final state is `READY` if at least the price/positions tables are populated through yesterday; otherwise `FAILED`.
- **`app/api/daily.py`** — blueprint exposing `/api/daily/equity?start=&end=`, `/api/daily/positions/<date>`, `/api/daily/prices/<symbol>?start=&end=`. Existing endpoints (`/api/summary`, `/api/performance/timeseries`, `/api/risk`, `/api/fx`) gain a `?resolution=daily|monthly` parameter. Default stays `monthly` for backwards compatibility.
- **`app/api/today.py`** — blueprint for `/api/today/snapshot` (today's MV, Δ vs prior, top movers) and `/api/today/freshness` (last update timestamp, source counts).

### Data flow

#### Cold start (Flask boot, empty `dashboard.db`)

```
1. Flask app starts, registers blueprints.
2. backfill_runner.start() spawned as daemon thread; state=INITIALIZING.
3. Main thread continues serving (every data endpoint returns 202 with state).
4. Background thread:
   a. Reads data/portfolio.json. For each symbol, computes a **per-symbol fetch window** to minimize external API calls:
      - `fetch_start = max(symbol.first_trade_date, BACKFILL_FLOOR)` where `BACKFILL_FLOOR = 2025-08-01` is a hard project-wide floor (no daily price data is requested earlier than this date, regardless of trade history).
      - `fetch_end = max(symbol.last_trade_date, symbol.last_held_date)` — i.e. for closed positions, only fetch through the last day the position was held; for open positions, fetch through today. `last_held_date` is derived from the holdings tables across months in `portfolio.json`.
      - Symbols whose entire active window ends before `BACKFILL_FLOOR` are **skipped entirely** (no rows fetched, no row in `symbol_market` cache).
   b. For each in-scope symbol: price_sources.get_prices(symbol, currency, fetch_start, fetch_end).
      - On TW symbol with no cached market: probe TWSE → TPEX → yfinance.fallback.
      - Cache verdict in symbol_market.
   c. Writes prices and fx_daily rows (idempotent on PK). FX daily rows are also bounded by `[BACKFILL_FLOOR, today]`.
   d. Pulls trade_overlay (Shioaji or empty if no creds).
   e. Walks merged trade ledger × prices → derives positions_daily.
   f. Aggregates → portfolio_daily.
   g. State=READY (or FAILED if everything blew up).
5. Future page loads return 200 with daily series.

**Why the windowing matters:** A naive "earliest trade date to today" backfill for a 6-year-old account with 30 tickers would fire ~3000+ TWSE calls on cold start, well into WAF territory. The per-symbol window collapses this: a ticker sold in 2025-10 only fetches Aug–Oct (3 months); a ticker held continuously since 2025-08 fetches Aug–today; a ticker that exited before 2025-08 fetches nothing. Combined, the typical real cold-start budget is **~30–50 TWSE/TPEX calls and a handful of yfinance pulls**, not thousands.
```

Cold start runtime estimate (with `BACKFILL_FLOOR = 2025-08-01` and per-symbol windowing): typical case ~30–50 TWSE/TPEX month-batch calls × ~0.5s + yfinance daily-bulk for foreign + overlay = **~30–60 seconds on first ever launch**. Subsequent restarts hit only the gap (last_known_date → today), typically <5 seconds.

#### Warm start (Flask boot, `dashboard.db` populated)

Same as cold start but:
- Step 4a only collects symbols where `MAX(prices.date) < today` per symbol.
- Most symbols are already current — only a handful of fetches happen.
- `is_ready` flips quickly (often before user opens the dashboard).

#### Page load (request path, after `READY`)

```
Browser → /api/daily/equity
       → daily.py blueprint
       → daily_store.get_equity_curve(start, end)
       → SQL: SELECT date, equity_twd FROM portfolio_daily WHERE date BETWEEN ? AND ?
       → JSON response, no external calls
```

#### Manual refresh (between restarts)

```
$ python scripts/snapshot_daily.py
  → instantiates the same backfill_runner logic, runs synchronously
  → writes new rows for any new dates since last run
  → updates last_known_date
  → next page load picks up new data automatically (no Flask restart needed)
```

The Flask process holds its own writer connection; `snapshot_daily.py` opens a separate connection. SQLite handles the cross-process write coordination via its file lock (rare contention; the script normally runs while user is at terminal, not while loading dashboard).

---

## Schema

```sql
-- Daily close prices, the source of truth for time-series
CREATE TABLE IF NOT EXISTS prices (
    date         TEXT NOT NULL,        -- ISO YYYY-MM-DD
    symbol       TEXT NOT NULL,        -- bare code, e.g. "2330" or "SNDK"
    close        REAL NOT NULL,
    currency     TEXT NOT NULL,        -- "TWD" or "USD"
    source       TEXT NOT NULL,        -- "twse" | "tpex" | "yfinance" | "yfinance:TW"
    fetched_at   TEXT NOT NULL,        -- ISO timestamp
    PRIMARY KEY (date, symbol)
);
CREATE INDEX IF NOT EXISTS idx_prices_symbol_date ON prices(symbol, date);

-- Daily FX rates (USD/TWD primarily; extensible)
CREATE TABLE IF NOT EXISTS fx_daily (
    date         TEXT NOT NULL,
    ccy          TEXT NOT NULL,        -- "USD"
    rate_to_twd  REAL NOT NULL,
    source       TEXT NOT NULL,        -- "yfinance:TWD=X"
    fetched_at   TEXT NOT NULL,
    PRIMARY KEY (date, ccy)
);

-- Symbol-to-market cache (TWSE / TPEX / yfinance / unknown)
CREATE TABLE IF NOT EXISTS symbol_market (
    symbol            TEXT PRIMARY KEY,
    market            TEXT NOT NULL,   -- "twse" | "tpex" | "yfinance" | "unknown"
    resolved_at       TEXT NOT NULL,
    last_verified_at  TEXT NOT NULL    -- re-probe quarterly
);

-- Per-day per-symbol position state (denormalized cache)
CREATE TABLE IF NOT EXISTS positions_daily (
    date         TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    qty          REAL NOT NULL,        -- shares held end-of-day
    cost_local   REAL NOT NULL,        -- avg cost in local currency
    mv_local     REAL NOT NULL,        -- qty * close
    mv_twd       REAL NOT NULL,        -- mv_local * fx if foreign
    type         TEXT NOT NULL,        -- "現股" | "融資" | "foreign"
    source       TEXT NOT NULL,        -- "pdf" | "overlay"
    PRIMARY KEY (date, symbol)
);

-- Aggregated daily portfolio state (denormalized cache, regenerable)
CREATE TABLE IF NOT EXISTS portfolio_daily (
    date         TEXT PRIMARY KEY,
    equity_twd   REAL NOT NULL,
    cash_twd     REAL,                 -- if known from PDF; null between PDFs
    fx_usd_twd   REAL NOT NULL,
    n_positions  INTEGER NOT NULL,
    has_overlay  INTEGER NOT NULL      -- 1 if any positions sourced from overlay
);

-- Dead-letter queue for any failed external fetch
CREATE TABLE IF NOT EXISTS failed_tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_type       TEXT NOT NULL,     -- "price_fetch" | "fx_fetch" | "overlay_fetch" | "reconcile"
    target          TEXT NOT NULL,     -- e.g. "2330:2026-04-15" or "shioaji_overlay"
    error_message   TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 1,
    first_seen_at   TEXT NOT NULL,
    last_attempt_at TEXT NOT NULL,
    resolved_at     TEXT                -- null while open
);
CREATE INDEX IF NOT EXISTS idx_failed_open ON failed_tasks(resolved_at) WHERE resolved_at IS NULL;

-- Reconciliation discrepancies (surfaced on dashboard until dismissed)
CREATE TABLE IF NOT EXISTS reconcile_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pdf_month       TEXT NOT NULL,     -- "2026-04"
    diff_summary    TEXT NOT NULL,     -- structural diff, JSON
    detected_at     TEXT NOT NULL,
    dismissed_at    TEXT
);

-- Process metadata
CREATE TABLE IF NOT EXISTS meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);
-- keys: "last_known_date", "schema_version", "last_overlay_refresh_at", "backfill_floor"
```

### Concurrency mode

`init_schema()` enables `journal_mode=WAL` and `busy_timeout=5000` on first connection. WAL is required because two processes write to the same database: the Flask backfill thread and the standalone `scripts/snapshot_daily.py` CLI. Without WAL, readers would block writers and vice-versa.

### Backups

WAL mode means the on-disk `data/dashboard.db` file alone is **not** a consistent snapshot — uncommitted transactions live in the sidecar `data/dashboard.db-wal` file until they're checkpointed. **Never `cp data/dashboard.db backup.db` while the database is in use.** The supported backup procedure is:

```bash
sqlite3 data/dashboard.db ".backup backup-$(date +%Y%m%d).db"
```

This issues a SQLite-coordinated copy that captures a consistent point-in-time snapshot regardless of WAL state. Document this in the project README under the refresh workflow. (For this project, backups are optional — the entire SQLite layer is regenerable from `data/portfolio.json` + external APIs in ~30–60s — so this is a "nice to have" rather than a "must run nightly.")

---

## External APIs

### TWSE (Taiwan Stock Exchange) — main board

```
GET https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY
    ?date=20260401         # any date in the target month
    &stockNo=2330
    &response=json
```

Returns one calendar month of OHLCV for one stock. Response includes a `data` array of `[date, volume, value, open, high, low, close, change, transactions]`. We only need date + close. Empty `data` → not listed on TWSE for that month.

Rate limit: no published rate limit; observed safe at 1–2 req/sec. We use a 0.5s base sleep + 0–200ms jitter, with dynamic backoff on non-200 and User-Agent rotation (see `twse_client.py` description) to stay under WAF thresholds during deep historical backfill.

### TPEX (Taipei Exchange) — OTC

```
GET https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php
    ?d=115/04             # ROC year/month (115 = 2026)
    &stkno=6488
```

Returns one calendar month of daily data for one OTC stock. ROC year format requires `tpex_client.py` to convert from Gregorian.

### yfinance — foreign equities + FX

Already a project dependency (used in `app/benchmarks.py`). Used for:
- Foreign symbols: `yf.Ticker("SNDK").history(start, end, interval="1d")`
- FX: `yf.Ticker("TWD=X").history(start, end, interval="1d")`
- Tertiary fallback for stubborn TW symbols: `yf.Ticker("2330.TW").history(...)`.

### Shioaji — read-only

```python
api = sj.Shioaji()
api.login(api_key=os.environ["SINOPAC_API_KEY"],
          secret_key=os.environ["SINOPAC_SECRET_KEY"])
# NOTE: api.activate_ca(...) is intentionally NEVER called.

closed_pl = api.list_profit_loss(api.stock_account, begin_date, end_date)
positions = api.list_positions(api.stock_account)
```

`closed_pl` returns realized round-trip records with date/code/qty/price for each leg. `positions` returns currently-open lots with avg cost. Together they reconstruct the gap-period trade activity.

Rate limits: 25 accounting queries / 5s; we make at most 2 calls per overlay refresh. Comfortable.

---

## Auth & secrets

| Secret | Where | Required? |
|---|---|---|
| `SINOPAC_API_KEY` | `.env` (gitignored) | No — absent → overlay disabled, dashboard runs PDF-only for trades |
| `SINOPAC_SECRET_KEY` | `.env` | No — same |
| `SINOPAC_PDF_PASSWORDS` | env (existing) | Required for PDF decryption (existing flow) |

`.env` loaded via `python-dotenv` at app boot. `shioaji_client.py` checks for both keys at import; if either is missing, `lazy_login()` returns a stub client whose `list_profit_loss` and `list_positions` return empty lists. Logged at INFO once at startup, not per request.

**Shioaji install policy:** the `shioaji` package is included as a hard dependency in `requirements.txt` (not split into a `requirements-shioaji.txt` extras file). Rationale for v1: simpler install path; the ~200MB footprint (mostly pyzmq) is acceptable for a personal-use project; users without credentials still pay the install cost but get the trivially-stubbed `lazy_login()` no-op behavior. Revisit if v2 needs a slimmer container image.

---

## Background thread & readiness state

State machine:

```
INITIALIZING ──(success)─→ READY
       │
       └────(exception)──→ FAILED
```

Held in `backfill_runner` module-level dict, guarded by an `RLock`:

```python
_state = {
    "phase": "INITIALIZING",  # | "READY" | "FAILED"
    "started_at": "2026-04-26T10:30:01+08:00",
    "ready_at": None,
    "error": None,
    "progress": {"symbols_done": 0, "symbols_total": 0, "current": None},
}
```

Endpoint behavior:

| Endpoint | INITIALIZING | READY | FAILED |
|---|---|---|---|
| `/api/health` | 200 + state JSON | 200 + state JSON | 200 + state JSON |
| `/api/daily/*`, `/api/today/*` | 202 + `{warming_up: true, since, progress}` | 200 + data | 503 + `{error}` |
| Existing endpoints with `?resolution=monthly` (default) | 200 + data (unaffected) | 200 + data | 200 + data |
| Existing endpoints with `?resolution=daily` | 202 | 200 | 503 |

The 202-with-progress shape lets the frontend show "Processing 14 of 32 symbols (TSMC)…" instead of an opaque spinner.

Concurrency:
- One writer connection in `backfill_runner` (`check_same_thread=False`); writes serialize through a single `RLock`.
- Read connections per-request thread.
- Idempotent PKs (`(date, symbol)`) make double-writes safe even if locking is buggy.

---

## Reconciliation

### Trigger

Reconciliation is **explicitly manual** — it never auto-fires on PDF mtime change, on Flask restart, or as a side-effect of `snapshot_daily.py`. Two entry points:

- **CLI**: `python scripts/reconcile.py [--month YYYY-MM]` — runs the diff for a specific month (default: most recent PDF month). Prints the structural diff to stdout, inserts a `reconcile_events` row if non-empty, exits 0 (clean) or 1 (diff found). Useful for cron/CI checks if you want them.
- **UI button**: "Run Reconciliation" inside the Developer Tools accordion on `/today` POSTs to `/api/admin/reconcile?month=YYYY-MM`, which runs the same logic server-side and surfaces the result inline.

Rationale: the user explicitly controls when diffing happens. PDF parsing and trade-overlay refresh stay independent of reconciliation; you can run reconcile any time after both have updated, including never if you don't care to verify a particular month.

### Comparison method (exact, structural, zero-tolerance)

For each PDF month `M`:
1. Extract from PDF: list of trades during `M` as `(date, code, side, qty, price)` tuples.
2. Extract from merged ledger (overlay rows that were stored for `M` before the PDF arrived): same shape.
3. Set-equality check on the tuple lists.

If equal: silently advance, drop overlay rows for `M` (they're now in the PDF ledger).

If not equal: compute structural diff (`pdf_only`, `overlay_only`, mismatched), insert a row in `reconcile_events` with the diff as JSON. Do **not** auto-fix anything — surface to user, let them decide.

Crucially, the comparison is in the trade-log domain only:
- No FX (which yfinance and the bank statement might disagree on by 0.0001).
- No equity totals (which depend on FX).
- No prices (which depend on which day's close TWSE returned).

Pure share counts, prices, and dates. If anything differs, the diff is exactly localizable.

### Behavior on diff

Frontend reads from `/api/today/reconcile` (open events). On a non-empty result, render a banner at the top of every page:

> **Reconciliation issue for 2026-04**: 1 PDF trade missing from overlay
> — PDF: Buy 2330 1000@580.0 on 2026-04-15
> [Investigate] [Dismiss]

"Dismiss" sets `dismissed_at`; banner stops appearing. "Investigate" links to a detailed view.

Investigation usually means: check Shioaji's `list_profit_loss` query window, check date timezone handling, check whether the trade was in a different account.

---

## Failure modes & DLQ

Every external fetch is wrapped:

```python
def fetch_with_dlq(task_type, target, fetch_fn):
    for attempt in range(3):  # 1, 2, 3
        try:
            return fetch_fn()
        except (TimeoutError, ConnectionError, HTTPError) as e:
            if attempt < 2:
                time.sleep(1 << (2 * attempt))  # 1s, 4s, 16s
                continue
            insert_failed_task(task_type, target, str(e))
            return None
```

Failed tasks go to `failed_tasks` table. A daily run does NOT halt on individual failures — other symbols continue.

`scripts/retry_failed_tasks.py` reads all `WHERE resolved_at IS NULL`, retries each, marks resolved on success or increments `attempts` on failure.

Specific failure handling:

| Failure | DLQ entry | User-visible |
|---|---|---|
| TWSE 5xx after 3 retries | `task_type='price_fetch', target='2330:2026-04-15'` | Listed in Developer Tools accordion on `/today`; warming-up timeout banner deep-links to it |
| TPEX returns garbled HTML | same | same |
| yfinance returns no data for foreign symbol | same | same |
| FX fetch fails for a date | `task_type='fx_fetch', target='USD:2026-04-15'` | same |
| Shioaji login fails | `task_type='overlay_fetch', target='shioaji_login'` | "Trade overlay unavailable" notice |
| Shioaji returns inconsistent data | log + fall back to no-overlay for the day | same |
| Reconciliation diff | `reconcile_events` row (separate from DLQ) | Reconciliation banner |

---

## Logging

```python
# app/__init__.py
import logging
from logging.handlers import RotatingFileHandler

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

file_handler = RotatingFileHandler(
    "logs/daily.log", maxBytes=5_000_000, backupCount=5
)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

root = logging.getLogger()
root.setLevel(logging.INFO)
root.addHandler(file_handler)
root.addHandler(stream_handler)
```

- Backfill logs go to `logs/daily.log` and stdout simultaneously.
- DEBUG level for per-symbol fetches; INFO for backfill-run start/end and DLQ inserts; WARN for retries; ERROR for run failures.
- Container-ready: stdout stream means future `docker logs` works without changes.

---

## Dashboard upgrades

### Tier 1 — Free wins (auto-upgrades from existing endpoints)

Existing endpoints accept `?resolution=daily`:
- `/api/summary` → equity curve becomes daily
- `/api/performance/timeseries` → TWR/XIRR daily; rolling 3/6/12M Sharpe gets 60/120/250-sample windows
- `/api/performance/rolling` → daily-resolution rolling metrics
- `/api/risk` → drawdown, VaR daily
- `/api/fx` → daily USD/TWD curve, per-day FX P&L attribution
- `/api/benchmarks/compare` → apples-to-apples daily comparison

**Frontend behavior — auto-upgrade, no manual toggle:** the frontend does not expose a monthly/daily switch in the UI. Instead, on every page load `static/js/api.js` reads `/api/health` once. If `state == READY`, all subsequent fetches on that page send `?resolution=daily`. If `state == INITIALIZING`, fetches use the implicit monthly default and the warming-up component renders. If `state == FAILED`, fetches use monthly and a banner surfaces the error.

This means: a user who visits the dashboard before backfill completes sees the existing monthly charts (no regression), and on next page load (after backfill is `READY`) gets daily resolution silently. Backend default of `?resolution=monthly` is preserved for backwards compatibility — programmatic API consumers and bookmarks-without-query-params keep working unchanged.

### Tier 2 — New components

| Feature | Page | Source endpoint |
|---|---|---|
| Today's Δ KPI hero | `/today` | `/api/today/snapshot` — returns `data_date` (latest trading day, ISO + weekday name), `prior_date`, `mv_twd`, `mv_twd_prior`, `delta_abs`, `delta_pct`, 30-day sparkline. **`data_date` is rendered prominently at the top of the page** ("Performance for Friday, 2026-04-24") so weekend/holiday viewers don't confuse stale data with broken data. If `data_date != today_in_tpe`, also surface the wall-clock today as context ("latest trading day; wall clock is Sun 2026-04-26"). |
| Top movers (today) | `/today` | `/api/today/movers` — top 5 gainers and top 5 losers by today's % change |
| Per-ticker daily chart | `/ticker/<code>` | `/api/daily/prices/<symbol>` + existing trade endpoint — chart.js line with buy/sell markers from trade log overlaid on price |
| Freshness indicator | global header/footer | `/api/today/freshness` — last update date, time, source breakdown ("from TWSE/Shioaji/yfinance"); color-coded by staleness |

### Tier 3 — Operational UI

For v1, **no dedicated `/admin` page** is built. All admin/operator controls live inside a collapsible **"Developer Tools"** accordion at the bottom of `/today` (collapsed by default). This keeps power-user controls out of the way for normal viewing while making them one click deep when needed. A separate `/admin` page can be extracted in v2 if these controls grow.

| Feature | Description |
|---|---|
| Warming-up state | Frontend handles 202 responses: show spinner with "Loading initial data ({progress})" + auto-retry every 5s. After ~2min without ready (revised down from 5min given the new ~30–60s cold-start estimate), show "Backfill seems stuck — check `logs/daily.log`" with a "View failed tasks" link that opens the Developer Tools accordion on `/today`. |
| Reconciliation banner | Polls `/api/today/reconcile` on page load. If non-empty, render dismissable banner at top of every page with the structural diff. |
| Failed tasks panel | Inside `/today` Developer Tools accordion: lists open `failed_tasks` rows with task type, target, error, attempts, last_attempt_at. **"Retry all"** button POSTs to `/api/admin/retry-failed`, which runs `scripts/retry_failed_tasks.py` logic in-process and returns a summary (resolved count, still-failing count). |
| Manual reconciliation trigger | Inside the same Developer Tools accordion: month picker + **"Run Reconciliation"** button POSTs to `/api/admin/reconcile?month=YYYY-MM`. Returns the structural diff inline; if non-empty, also inserts a `reconcile_events` row so the global banner appears on next page load. |
| Manual refresh | Inside the same Developer Tools accordion: **"Refresh now"** button (also surfaced in the freshness widget for convenience) POSTs to `/api/admin/refresh`, runs `snapshot_daily.py` logic synchronously, returns when done. |

### Tier 4 — `/today` page

New top-level page combining Tier 2 components into a tactical/intraday view:

```
┌─────────────────────────────────────────────────────┐
│  Performance for Friday, 2026-04-24                 │
│  (latest trading day — wall clock: Sun 2026-04-26)  │
│                                                     │
│  ┌──────────────────────────────────────────────┐   │
│  │ MV TWD: 4,284,103                            │   │
│  │ Δ vs prior session (Thu 2026-04-23):         │   │
│  │   +14,201 (+0.33%)                           │   │
│  │ Sparkline (last 30 trading days):            │   │
│  │   ───────∿∿─────/─                           │   │
│  └──────────────────────────────────────────────┘   │
│                                                     │
│  Top Movers (Friday 2026-04-24 vs Thursday)         │
│  ┌──────────────────┬──────────────────┐            │
│  │ Gainers          │ Losers           │            │
│  │ 2330  +2.14%     │ SNDK  -1.81%     │            │
│  │ 0050  +0.84%     │ 6488  -0.62%     │            │
│  │ 2317  +0.71%     │ 2454  -0.43%     │            │
│  └──────────────────┴──────────────────┘            │
│                                                     │
│  Data freshness                                     │
│  Through 2026-04-25 16:30 TPE                       │
│  TW: TWSE+TPEX | Foreign: yfinance | Trades: PDF    │
│  [Refresh now]                                      │
│                                                     │
│  [Reconciliation banner if non-empty]               │
│                                                     │
│  ▶ Developer Tools (collapsed by default)           │
│    ─────────────────────────────────────            │
│    │ Failed tasks (3 open)                          │
│    │ ─ price_fetch  2330:2026-04-15  3 attempts     │
│    │ ─ fx_fetch     USD:2026-04-15   2 attempts     │
│    │ ─ overlay      shioaji_login    1 attempt      │
│    │ [Retry all]                                    │
│    │                                                │
│    │ Reconciliation                                 │
│    │ Month: [2026-04 ▾]   [Run Reconciliation]      │
│    │                                                │
│    │ Refresh                                        │
│    │ [Run snapshot_daily now]                       │
│    └─────────────────────────────────────           │
└─────────────────────────────────────────────────────┘
```

The "Refresh now" button (both in the freshness block and inside Developer Tools) POSTs to `/api/admin/refresh`, which runs `snapshot_daily.py` server-side and returns when done (simple full-wait response acceptable for v1; progress streaming deferred). The Developer Tools accordion is collapsed by default — the panel-body markup is rendered but hidden until expanded, so accidental "Retry all" / "Run Reconciliation" clicks are not a risk.

---

## Testing strategy

- **Unit tests** for `twse_client.py` and `tpex_client.py`: mock HTTP responses (real captured fixtures from public APIs), assert parsing of OHLCV and empty-response handling.
- **Unit tests** for `price_sources.py`: mock the per-source clients, assert routing logic (cache hit, cache miss with TWSE success, cache miss with TPEX fallback, unknown).
- **Unit tests** for `trade_overlay.py`: synthetic Shioaji response fixtures + synthetic `portfolio.json` → assert merged ledger correctness, including the "current open lot in Shioaji that opened during the gap" case.
- **Unit tests** for reconciliation diff: hand-crafted PDF trade list + overlay trade list, assert `pdf_only` / `overlay_only` / matched buckets are exactly correct on equality, on missing trade, on differing qty.
- **Integration test** for backfill: small fixture (2 TW tickers, 1 foreign, 5 trading days), end-to-end through SQLite, assert `portfolio_daily` rows are correct.
- **Smoke test** for Flask startup: empty DB → boot Flask → assert 202 within 1s → wait → assert 200 with data within timeout.
- **Manual test** for reconciliation cycle: drop a known PDF, run snapshot to populate overlay, drop a slightly-different PDF, run reconciliation, assert banner appears.

Test fixtures live under `tests/fixtures/`. Integration test uses a temp SQLite file; teardown removes it.

---

## Implementation order

The plan is sequenced so each step is independently testable and produces a visible checkpoint. Each step is roughly one PR / one session of work.

1. **Schema + `daily_store.py` skeleton** (read-only API on empty tables). Just SQL DDL and stub methods. No external calls yet.
2. **`twse_client.py` + `price_sources.py` (TW-only path)**. Write a test that fetches one month of 2330 prices and parses correctly. No DB writes yet.
3. **`backfill_runner.py` + `scripts/backfill_daily.py`**. Cold backfill for TW symbols only, hardcoded test ticker list. Implement the per-symbol fetch-window logic with `BACKFILL_FLOOR = 2025-08-01` and `[max(first_trade_date, BACKFILL_FLOOR), max(last_trade_date, last_held_date)]`. Symbols whose last activity precedes `BACKFILL_FLOOR` are skipped entirely. Populate `prices`, derive simple `portfolio_daily` (no foreign yet, no overlay). End-to-end DB populated.
4. **`/api/daily/equity` blueprint**. Read from `portfolio_daily`. Frontend: equity curve on `/` switches to daily resolution. **First visible win.**
5. **`tpex_client.py` + dynamic discovery**. Add TPEX fallback in `price_sources`; populate `symbol_market` table.
6. **yfinance for foreign + FX**. Add `fx_daily` population, foreign price fetch. `portfolio_daily.equity_twd` now correct including foreign holdings.
7. **Data integrity validation gate (ship gate before any UI work)**. Run `scripts/validate_data.py` against the populated `dashboard.db`:
   - (a) Every held symbol has a `prices` row for every trading day between first-trade-date and yesterday — assert no gaps per symbol.
   - (b) `symbol_market` resolution is correct: spot-check ≥5 TWSE symbols, ≥5 TPEX symbols (if any held), all foreign symbols. Assert market verdicts match the actual exchange.
   - (c) `fx_daily` has no gaps in the held-position window — assert dense daily series.
   - (d) Cross-source agreement spot-check: pick 5 TW symbols, fetch the same date from yfinance with `.TW` suffix, compare to TWSE/TPEX close. Assert agreement within 0.5% (catches symbol-routing bugs).
   - (e) For the most recent month-end, derived `portfolio_daily.equity_twd` matches the corresponding PDF month-end equity within structural diff zero tolerance on positions × prices (FX may differ slightly — log delta but accept).
   - **Do not advance to step 8 until this script exits 0.** Fix any discrepancies first.
8. **`/ticker/<code>` upgrade (Tier 2)**. Daily chart with trade markers. **Biggest single visual upgrade.**
9. **Background thread + warming-up state (Tier 3)**. Move `backfill_runner` from synchronous-script-only to Flask-startup-thread. Add 202 handling. Frontend warming-up component.
10. **Failed tasks DLQ (Tier 3)**. Wrap all fetches with `fetch_with_dlq`. Add `/api/admin/failed-tasks`, `scripts/retry_failed_tasks.py`. Frontend failed-tasks panel.
11. **`shioaji_client.py` + `trade_overlay.py`**. Read-only Shioaji login (with strict `Asia/Taipei` date coercion), gap-period trade fetch, merged ledger. Test with real account in a separate spike.
12. **Reconciliation (Tier 3)**. `scripts/reconcile.py` CLI + `/api/admin/reconcile` POST endpoint + "Run Reconciliation" UI button. **Manual trigger only** — no auto-fire on PDF mtime or restart. Populates `reconcile_events`; frontend banner reads from there.
13. **`/today` page (Tier 2 + Tier 4)**. New blueprint, new template, top-movers and Δ KPI components. Render `data_date` prominently with weekday name; surface wall-clock today as context when they differ.
14. **Freshness indicator (Tier 2)**. Global header/footer component, polls `/api/today/freshness`.
15. **`scripts/snapshot_daily.py`**. CLI for manual refreshes between Flask restarts.

Natural ship points: **steps 4 / 8 / 10 / 12 / 13** — each is a standalone usable improvement. Step 7 (validation gate) is non-negotiable but not a "ship" milestone — it's a quality check before UI work begins. Steps 11–12 are the "Shioaji + reconciliation" work; if Shioaji API access is blocked, the dashboard ships through step 10 with PDF-only trade data and is still a major upgrade over today.

---

## Open questions / future work

- **Intraday refresh during market hours.** v1 explicitly defers. Could later add a separate `/api/today/live` that hits TWSE's intraday endpoints (or Shioaji `api.snapshots()`) for current prices, without touching the daily-close storage.
- **Corporate actions.** Splits, dividends-as-shares, code migrations. Manual override file for v1; could add automated yfinance corporate-action fetch later.
- **Multi-currency beyond USD.** If user adds HKD or JPY positions, extend `fx_daily` and the FX router. Schema already supports it.
- **Performance metrics methodology.** TWR and XIRR computed at daily resolution may diverge from monthly results. Not a bug — different sample frequency. Worth a doc note for users.
- **Schema migrations.** v1 ships schema_version=1. Future schema changes need a `migrations/` directory or a stored-meta-version-check at startup. Out of scope for v1; one-shot drop-and-rebuild is acceptable until then.
