# Implementation Plan: Daily Prices, /today Page, and Read-Only Sinopac Integration

**Date:** 2026-04-26
**Status:** Approved 2026-04-26 — 5 open questions resolved in §6; ready to start at Phase 0
**Source spec:** [`docs/superpowers/specs/2026-04-26-daily-prices-and-today-page-design.md`](../specs/2026-04-26-daily-prices-and-today-page-design.md)
**Author:** Joshua + Claude (planner agent, /plan command)

---

## 1. Requirements restatement

- Add daily-resolution price/equity/drawdown data on top of the existing month-end PDF pipeline, without disturbing what already works (`portfolio.json`, the 11 existing blueprints, the parser scripts).
- Storage moves from JSON-only to JSON + SQLite (`data/dashboard.db`); the SQLite layer is a regenerable cache, not a new source of truth.
- Daily prices come from free public APIs first (TWSE, TPEX, yfinance) so a user with zero credentials still gets a fully working daily dashboard.
- Shioaji is added strictly read-only and only fills the post-PDF trade gap; missing creds must degrade gracefully, never crash the app, and never block the happy path.
- Existing endpoints (`/api/summary`, `/api/performance/*`, `/api/risk`, `/api/fx`, `/api/benchmarks/compare`) gain an opt-in `?resolution=daily` flag with `monthly` as the backwards-compatible default.
- Two new blueprints (`/api/daily/*`, `/api/today/*`) and a new `/today` page surface tactical/intraday context (Δ vs prior session, top movers, freshness, reconciliation banner).
- Cold-start backfill runs in a background thread with an `INITIALIZING / READY / FAILED` readiness state; data endpoints return 202 with progress while warming up.
- Every external fetch is wrapped in a retry+DLQ pattern; failed tasks land in `failed_tasks` and are retryable from CLI and UI.
- Reconciliation between PDF trades and Shioaji-overlay trades is **manual-trigger only** (CLI + UI button); it never auto-fires.
- A non-negotiable validation gate (`scripts/validate_data.py`) sits between the data layer (steps 1–6) and any UI work (steps 8+).

## 2. What already exists vs what's net-new

| Spec module / file | Exists? | Notes |
|---|---|---|
| `data/portfolio.json` | Yes | Untouched. Authoritative for cost basis, dividends, FX, fees. |
| `data/benchmarks.json` | Yes | Untouched yfinance monthly cache. |
| `data/dashboard.db` | **No** | New SQLite file; created on first backfill. Parent `data/` already gitignored. |
| `logs/daily.log` | **No** | New rotating log; `logs/` directory must be created and gitignored. |
| `scripts/parse_statements.py` | Yes | Untouched. |
| `scripts/{backfill_daily,snapshot_daily,reconcile,validate_data,retry_failed_tasks}.py` | **No** | All new. |
| `app/__init__.py` | Yes | Modified: register 2 new blueprints, init DB, kick off `backfill_runner` thread, configure `RotatingFileHandler` + stdout, load `.env`. |
| `app/data_store.py` | Yes | Untouched. New `daily_store.py` mirrors its mtime-cached read-only pattern but for SQLite. |
| `app/{daily_store,price_sources,twse_client,tpex_client,shioaji_client,trade_overlay,backfill_runner}.py` | **No** | All new. |
| `app/benchmarks.py` | Yes | Reuse pattern; not modified. New foreign+FX fetcher in `price_sources.py` does its own yfinance calls (different shape: daily, SQLite-cached). |
| `app/api/_helpers.py` | Yes | Extended with `daily_envelope_or_warming(...)` returning 202 with progress when `state != READY`. |
| `app/api/{summary,performance,risk,fx,benchmarks}.py` | Yes | Each gains a `?resolution=daily` branch reading from `daily_store`. Default unchanged. |
| `app/api/{daily,today}.py` | **No** | New blueprints. |
| `templates/today.html` | **No** | New page. |
| `templates/base.html` | Yes | Modified: freshness widget, reconciliation banner partial, `/today` nav link. |
| `static/js/pages/today.js`, `static/js/freshness.js` | **No** | New. |
| `static/js/pages/ticker.js` | Yes | Modified to use `/api/daily/prices/<symbol>` with trade markers. |
| `requirements.txt` | Yes (5 lines) | Adds `python-dotenv`, `requests`, optionally `shioaji`. `yfinance` already pinned. `sqlite3`/`zoneinfo` are stdlib. |
| `tests/` | **No** | New. Phase 0 sets up `pytest` + fixtures. |
| `.env.example` | **No** | New, documents `SINOPAC_API_KEY` / `SINOPAC_SECRET_KEY` as optional. |

## 3. Phased plan

### Phase 0 — Project plumbing (prerequisite, not in spec's 15 steps)
**Goal:** Add deps, dotenv, logging, `tests/` scaffold, `.gitignore` updates so subsequent phases have somewhere to land.
**Files:** `requirements.txt` (add `python-dotenv`, `requests`, `shioaji`, `pytest`), `.env.example` (documents optional `SINOPAC_API_KEY` / `SINOPAC_SECRET_KEY`), `.gitignore` (add `logs/`, `data/dashboard.db`, `data/dashboard.db-wal`, `data/dashboard.db-shm`, `.env`), `tests/__init__.py`, `tests/conftest.py`, `app/__init__.py` (logging + `load_dotenv()`), `README.md` (document `sqlite3 data/dashboard.db ".backup ..."` backup procedure under refresh workflow — see §6.3).
**Acceptance criteria:**
- `pip install -r requirements.txt` succeeds inside `.venv` (note: pulls ~200MB for shioaji + pyzmq — expected, see §6.2).
- `pytest -q` runs (zero tests pass; framework wired).
- `python app.py` boots; `logs/daily.log` is created on first request; INFO line written to both stdout and file.
- Dashboard renders identically to today (no regressions).
- README contains the `.backup` documentation paragraph.

**Complexity:** S · **Dependencies:** none
**Risks:** `python-dotenv` could shadow real shell env vars — load with `override=False`.

### Phase 1 — Schema + `daily_store.py` skeleton (spec step 1)
**Goal:** SQL DDL + read-only stub methods; no external calls.
**Files:** `app/daily_store.py` (new), `app/__init__.py` (register `DailyStore`), `tests/test_daily_store.py`.
**Acceptance criteria:**
- `python -c "from app import create_app; create_app()"` creates `data/dashboard.db` with all 8 tables (`prices`, `fx_daily`, `symbol_market`, `positions_daily`, `portfolio_daily`, `failed_tasks`, `reconcile_events`, `meta`).
- `sqlite3 data/dashboard.db ".schema"` matches spec §Schema verbatim (PKs, indexes included).
- `sqlite3 data/dashboard.db "PRAGMA journal_mode"` returns `wal` (set in `init_schema()` per §6.3).
- `sqlite3 data/dashboard.db "PRAGMA busy_timeout"` returns `5000`.
- `meta` table contains `backfill_floor = '2025-08-01'` row at init (per §6.1).
- `pytest tests/test_daily_store.py -q` passes.

**Complexity:** S · **Dependencies:** Phase 0
**Risks:** SQLite path collision on fresh checkout — `init_schema()` must `mkdir(parents=True, exist_ok=True)`. WAL pragma must be applied **before** any writes on a brand-new DB (some platforms ignore late `journal_mode` changes).

### Phase 2 — `twse_client.py` + `price_sources.py` (TW-only path) (spec step 2)
**Goal:** Fetch one month of TWSE prices; symbol-class router with TWSE-only branch.
**Files:** `app/twse_client.py`, `app/price_sources.py`, `tests/fixtures/twse_2330_202604.json`, `tests/test_twse_client.py`, `tests/test_price_sources.py`.
**Acceptance criteria:**
- `pytest tests/test_twse_client.py tests/test_price_sources.py -q` passes with mocked HTTP.
- One real probe: `python -c "from app.twse_client import fetch_month; print(fetch_month('2330', 2026, 4))"` returns ≥15 rows.
- No DB writes in this phase.

**Complexity:** M · **Dependencies:** Phase 1
**Risks:** TWSE WAF could block dev — keep dev probes <10; commit fixtures so tests don't hit network.

### Phase 3 — `backfill_runner.py` + `scripts/backfill_daily.py` (TW-only) (spec step 3)
**Goal:** End-to-end TW backfill with **per-symbol fetch windowing** (per §6.1) writing to `prices` and a simplified `portfolio_daily`.
**Files:** `app/backfill_runner.py`, `scripts/backfill_daily.py`, `tests/test_backfill_runner.py`, `tests/test_fetch_windows.py`.
**Per-symbol window logic (from §6.1):**
- Read `BACKFILL_FLOOR` from `meta.backfill_floor` (default `2025-08-01`).
- For each TW symbol in `portfolio.json`:
  - Compute `first_trade_date` from the trade ledger (earliest date the symbol appears).
  - Compute `last_trade_date` from the trade ledger (latest date).
  - Compute `last_held_date` from the holdings tables (latest month-end the symbol had qty > 0; coerced to that month's last trading day).
  - `fetch_start = max(first_trade_date, BACKFILL_FLOOR)`.
  - `fetch_end = max(last_trade_date, last_held_date)`.
  - **Skip the symbol entirely** if `fetch_end < BACKFILL_FLOOR` (no calls, no `symbol_market` row).
  - Otherwise call `price_sources.get_prices(symbol, currency, fetch_start, fetch_end)`.

**Acceptance criteria:**
- `python scripts/backfill_daily.py --tw-only` populates `prices` correctly bounded by per-symbol windows; for in-scope tickers ≥1 row per trading day in window; for tickers entirely before `BACKFILL_FLOOR` zero rows.
- Total wall-clock for typical portfolio (~30–50 in-scope month-batches): ≤90 seconds.
- Re-running is idempotent (UPSERT).
- `pytest tests/test_fetch_windows.py -q` covers: ticker active throughout, ticker with first_trade before floor (clipped to floor), ticker exited before floor (skipped), ticker still held (fetch_end == today), ticker sold and not re-bought (fetch_end == sale date).

**Complexity:** L · **Dependencies:** Phase 2
**Risks:** Edge case — a ticker bought in 2024, sold in 2025-06, re-bought in 2025-09. Trade ledger has both legs; `first_trade_date=2024`, `last_trade_date=2025-09+`, but the 2025-06 → 2025-09 gap should ideally not be fetched. v1 keeps it simple: fetch the full clipped window `[max(first, FLOOR), max(last, last_held)]` even if that includes a no-position gap. Cost is a few extra month-batches per re-bought ticker; correctness is preserved (positions_daily for the gap shows qty=0 from the trade ledger, regardless of whether prices exist for those days).

### Phase 4 — `/api/daily/equity` blueprint (FIRST SHIP POINT) (spec step 4)
**Goal:** Frontend equity curve switches to daily resolution **automatically** when backend is `READY` (per §6.5 — no manual toggle).
**Files:** `app/api/daily.py`, `app/__init__.py` (register), `app/api/summary.py` (resolution branch), `static/js/api.js` (one-shot `/api/health` check + module-level `RESOLUTION` flag, all fetch helpers append `?resolution=${RESOLUTION}`), `static/js/pages/overview.js` (no opt-in flag needed — `api.js` handles it globally), `tests/test_api_daily.py`.
**Acceptance criteria:**
- `curl localhost:8000/api/daily/equity` returns 200 with daily rows after backfill.
- `curl localhost:8000/api/summary?resolution=daily` returns daily points (count depends on per-symbol windows from Phase 3).
- `curl localhost:8000/api/summary` (no param) returns identical bytes to baseline (regression-safe — backwards compatibility preserved).
- Browser load with `READY` state: Network tab shows `/api/summary?resolution=daily`, `/api/performance/timeseries?resolution=daily`, etc. without any per-page code change.
- Browser load with `INITIALIZING` state: Network tab shows `/api/summary` (no resolution param), warming-up component renders, no UI toggle visible.

**Complexity:** M · **Dependencies:** Phase 3 · **Ship point:** Yes
**Risks:** Daily point count may need downsampling on low-DPI screens — punt to v2. The single-shot health check on page load means a user with a long-lived tab won't auto-flip to daily after backfill completes mid-session; that's acceptable for v1 (next hard refresh handles it).

### Phase 5 — `tpex_client.py` + dynamic discovery (spec step 5)
**Goal:** TPEX fallback; populate `symbol_market` cache.
**Files:** `app/tpex_client.py`, `app/price_sources.py` (extend), `tests/fixtures/tpex_*.json`, tests.
**Acceptance criteria:**
- Held OTC symbols (if any) resolve to `tpex` in `symbol_market`; gets prices.
- Re-running `backfill_daily.py` does not re-probe cached symbols (verify via log count).

**Complexity:** M · **Dependencies:** Phase 4
**Risks:** TPEX response format historically less stable — capture multiple fixtures.

### Phase 6 — yfinance for foreign + FX (spec step 6)
**Goal:** Foreign equities + `fx_daily` populated; `portfolio_daily.equity_twd` correct including foreign.
**Files:** `app/price_sources.py` (foreign + FX branches), `app/backfill_runner.py` (extend), tests.
**Acceptance criteria:**
- `sqlite3 data/dashboard.db "SELECT COUNT(*) FROM fx_daily"` ≥ trading days in window.
- Held foreign tickers have `prices` rows with `currency='USD'`.
- Most-recent-month-end `portfolio_daily.equity_twd` within 1% of corresponding `portfolio.json` month.

**Complexity:** M · **Dependencies:** Phase 5
**Risks:** yfinance returns stale `TWD=X` rows on Asia weekends — forward-fill within trading window.

### Phase 7 — `scripts/validate_data.py` (NON-NEGOTIABLE GATE) (spec step 7)
**Goal:** Verify data integrity before any UI work proceeds.
**Files:** `scripts/validate_data.py` implementing all 5 checks (per-symbol gaps, market resolution, FX gaps, cross-source agreement ≤0.5%, month-end equity reconciliation).
**Acceptance criteria:**
- `python scripts/validate_data.py` exits 0 against the populated DB after phase 6.
- Deliberately corrupting one row makes it exit 1 with the row identified.
- **Hard gate: Phase 8 must not begin until phase 7 exits 0 against real data.**

**Complexity:** M · **Dependencies:** Phase 6
**Risks:** Cross-source check (d) can flag false positives on dividend-adjusted yfinance vs unadjusted TWSE — 0.5% tolerance documented.

### Phase 8 — `/ticker/<code>` daily upgrade (SHIP POINT) (spec step 8)
**Goal:** Per-ticker daily price chart with buy/sell markers.
**Files:** `app/api/daily.py` (add `/api/daily/prices/<symbol>`), `app/api/tickers.py` (extend), `static/js/pages/ticker.js`, `templates/ticker.html`.
**Acceptance criteria:**
- `/ticker/2330?resolution=daily` shows daily price line with trade markers aligned to `portfolio.json` trade dates.
- Closed positions still show full lifetime price line.

**Complexity:** M · **Dependencies:** Phase 7 exits 0 · **Ship point:** Yes
**Risks:** Existing gap-fill fix (commit `041bf7f`) must be preserved when daily resolution is on; weekend/holiday gaps render as continuous lines.

### Phase 9 — Background thread + warming-up state (spec step 9)
**Goal:** Move `backfill_runner` from sync-only to Flask-startup daemon thread; 202 handling.
**Files:** `app/backfill_runner.py` (`start()`), `app/__init__.py` (call), `app/api/_helpers.py` (`require_ready_or_warming` decorator), `static/js/api.js` (handle 202 with poll/backoff), `static/js/pages/overview.js`.
**Acceptance criteria:**
- Delete `data/dashboard.db`, boot Flask: `/api/daily/equity` returns 202 with progress within 1s.
- After ~5 min, same endpoint returns 200 without restart.
- `/api/summary` (no resolution) returns 200 throughout.
- Forcing `state = FAILED` returns 503.

**Complexity:** L · **Dependencies:** Phase 8
**Risks:**
- Flask debug-mode reloads spawn double threads — guard with `WERKZEUG_RUN_MAIN` check.
- Backfill exception must transition state to FAILED, not silently die — top-level `try/except`.
- **Feature-flag suggestion: gate phase 9 behind `BACKFILL_ON_STARTUP=true` env var so it can be disabled in production while keeping manual `backfill_daily.py` working.**

### Phase 10 — Failed-tasks DLQ (SHIP POINT) (spec step 10)
**Goal:** All external fetches wrapped with `fetch_with_dlq`; admin endpoints + retry CLI + failed-tasks panel inside the **`/today` Developer Tools accordion** (per §6.4 — no `/admin` page in v1).
**Files:** `app/backfill_runner.py` (`fetch_with_dlq`), wrap call sites, `app/api/today.py` (`/api/admin/failed-tasks` GET, `/api/admin/retry-failed` POST), `scripts/retry_failed_tasks.py`, `templates/_developer_tools.html` (new partial — failed-tasks list + Retry-all button), `static/js/pages/today.js` (mount accordion, fetch + render failed tasks), `tests/test_dlq.py`.
**Acceptance criteria:**
- Simulated TWSE 5xx for one symbol writes `failed_tasks` row, backfill continues for other symbols.
- `python scripts/retry_failed_tasks.py` retries open rows; on success sets `resolved_at`.
- `POST /api/admin/retry-failed` returns `{resolved: N, still_failing: M}` summary.
- Developer Tools accordion on `/today` is collapsed by default; expanding reveals failed-tasks list with task type, target, error, attempts, last_attempt_at columns.

**Complexity:** M · **Dependencies:** Phase 9 · **Ship point:** Yes
**Risks:** Don't DLQ-wrap reads from DB — only external fetches. Accordion collapsed-by-default means users may not notice failures; the warming-up timeout banner (Phase 9) and the freshness widget (Phase 14) both need to deep-link to `/today#developer-tools` and auto-expand the accordion when there are open tasks.

### Phase 11 — `shioaji_client.py` + `trade_overlay.py` (spec step 11)
**Goal:** Read-only Shioaji session + gap-period trade overlay.
**Files:** `app/shioaji_client.py`, `app/trade_overlay.py`, `app/backfill_runner.py` (merge overlay), `tests/test_shioaji_client.py`, `tests/test_trade_overlay.py`.
**Acceptance criteria (all hard requirements):**
- **`grep -E "from shioaji import|import shioaji" app/shioaji_client.py | grep -E "Order|order"` returns nothing.**
- **`grep -E "activate_ca|place_order|cancel_order|update_order" app/shioaji_client.py` returns nothing.**
- **TPE-tz unit test for `to_taipei_date()`: passes a UTC datetime, asserts the TPE-localized date.**
- With `SINOPAC_*` unset: app boots, stub returns, INFO log "Shioaji credentials not configured; trade overlay disabled" written exactly once at startup, all `/api/*` endpoints return 200, dashboard fully functional.
- With creds set: trades appear in `positions_daily` with `source='overlay'` for the gap window.

**Complexity:** L · **Dependencies:** Phase 10
**Risks:**
- Shioaji session can be invalidated server-side; reconnect on first API failure.
- (Heavy install footprint of ~200MB is acknowledged and accepted per §6.2 — `shioaji` is in `requirements.txt` as a hard dependency.)

### Phase 12 — Reconciliation (SHIP POINT) (spec step 12)
**Goal:** Manual-trigger reconciliation: CLI + button inside the **`/today` Developer Tools accordion** (per §6.4) + global banner.
**Files:** `scripts/reconcile.py`, `app/api/today.py` (`POST /api/admin/reconcile?month=YYYY-MM`, `GET /api/today/reconcile`), `templates/_developer_tools.html` (extend with month picker + "Run Reconciliation" button), `templates/base.html` (banner partial — global on every page), `static/js/reconcile-banner.js`, `tests/test_reconcile.py`.
**Acceptance criteria (all hard requirements):**
- **`grep -rn "reconcile" app/backfill_runner.py scripts/snapshot_daily.py scripts/parse_statements.py` returns no auto-fire callsites.**
- **No PDF mtime watcher; no Flask startup hook calls reconciliation.**
- Clean diff: exits 0, no `reconcile_events` row inserted.
- Fabricated diff: exits 1, row inserted, banner appears on next page load.
- "Dismiss" sets `dismissed_at`; banner stops rendering.

**Complexity:** M · **Dependencies:** Phase 11 · **Ship point:** Yes (conditional on Phase 11)
**Risks:** Set-equality on float prices is brittle — round to 4 decimals before comparison.

### Phase 13 — `/today` page (SHIP POINT) (spec step 13)
**Goal:** New blueprint + template combining Tier 2 components plus the Developer Tools accordion (introduced as a partial in Phase 10).
**Files:** `app/api/today.py` (snapshot + movers), `templates/today.html` (with weekday-named `data_date` heading + wall-clock context line when different + `{% include "_developer_tools.html" %}` at the bottom), `static/js/pages/today.js`, `app/__init__.py` (route), `templates/base.html` (nav).
**Acceptance criteria:**
- `/today` renders hero with weekday-named `data_date` ("Performance for Friday, 2026-04-24").
- Top movers populated from `positions_daily` % delta.
- Sparkline shows last 30 trading days.
- "Refresh now" (in the freshness block AND inside the Developer Tools accordion) POSTs to `/api/admin/refresh` → runs `snapshot_daily` synchronously → page reloads.
- On weekends: `data_date` shows Friday's date; wall-clock context line visible.
- Developer Tools accordion is rendered, collapsed by default; deep link `/today#developer-tools` opens the page with it expanded.

**Complexity:** L · **Dependencies:** Phase 12 · **Ship point:** Yes
**Risks:** Sparkline canvas sizing on mobile — test 320/768/1024. Accordion expand-on-hash needs to fire after the failed-tasks fetch resolves so users don't see an empty accordion that then populates.

### Phase 14 — Freshness indicator (global) (spec step 14)
**Goal:** Global header/footer freshness widget polling `/api/today/freshness`.
**Files:** `app/api/today.py` (endpoint), `templates/base.html` (footer slot), `static/js/freshness.js`.
**Acceptance criteria:**
- Every page shows freshness widget in footer.
- Color matches staleness (green <1d / yellow <3d / red >3d).
- Network failure → "—" instead of crash.

**Complexity:** S · **Dependencies:** Phase 13
**Risks:** Fetch once on `DOMContentLoaded` only — don't tight-loop poll.

### Phase 15 — `scripts/snapshot_daily.py` (spec step 15)
**Goal:** CLI for manual incremental refreshes between Flask restarts.
**Files:** `scripts/snapshot_daily.py`, `app/api/today.py` (`POST /api/admin/refresh`).
**Acceptance criteria:**
- After 3-day pause, `python scripts/snapshot_daily.py` writes ~3 trading days' rows; updates `meta.last_known_date`.
- Re-running immediately is no-op.
- Running while Flask is up: Flask picks up new rows on next request without restart.

**Complexity:** S · **Dependencies:** Phase 14
**Risks:** SQLite write contention between Flask thread and script — enable `journal_mode=WAL` in `init_schema()`; busy-timeout 5s.

## 4. Cross-cutting risks

- **TWSE WAF during deep backfill** — A new user with 3 years of trade history could trigger ~3000 TWSE calls on cold start. Mitigations baked in (UA rotation, jitter, dynamic backoff, per-symbol DLQ); fallback is to spread across sessions or cap window to last 12 months on first run.
- **SQLite write contention between Flask thread and `snapshot_daily.py`** — Two processes both want the writer lock. Enable `journal_mode=WAL` in `init_schema()` so readers never block writers; busy-timeout 5s on writers.
- **Shioaji creds unavailable** — Phase 11's stub-client design must be tested without creds before merging; full pytest suite must pass with `SINOPAC_API_KEY` unset.
- **`portfolio.json` schema drift** — Parser is untouched, but if it ever changes trade record shape, `trade_overlay.merge()` and `validate_data.py` step (e) break silently. Add a `portfolio_json_version` check.
- **Cold-start UX** — With the per-symbol windowing from §6.1 cutting cold-start to ~30–60s, the warming-up window is far more tolerable than the original ~3–5min estimate. Phase 9's progress shape still matters (users need to know something is happening), but the timeout-banner trigger is revised down from 5min to 2min. Manual UX check at `--no-cache` browser load required.
- **Time zone consistency** — `data_date`, "today_in_tpe", and `portfolio.json` trade dates all need consistent TPE handling. Consider centralizing `to_taipei_date()` in `app/timezones.py` early to avoid drift.
- **Ship-point reversibility** — If phase 9 (background thread) introduces a startup regression, every later phase is blocked. Feature-flag it behind `BACKFILL_ON_STARTUP=true`.

## 5. Natural ship points (spec verification)

- **Step 4** ✅ Ships standalone (TW-only acceptable; foreign holdings under-represented until step 6).
- **Step 8** ✅ Ships standalone *only after* phase 7 gate passes.
- **Step 10** ✅ Ships standalone — purely additive.
- **Step 12** ⚠️ Conditional — only if step 11 (Shioaji) succeeded. Without overlay, nothing to reconcile.
- **Step 13** ✅ Ships standalone (sparkline + Δ KPI work with PDF-only data; reconciliation banner is no-op when no events).

**Confirmed ship points:** 4, 8, 10, 13. **Conditional:** 12 (only if 11 succeeded). **Fallback ship point:** 10 if Shioaji is blocked entirely.

## 6. Resolved decisions (2026-04-26)

The 5 open questions from the original plan are now resolved. Each decision below is also reflected in the spec.

### 6.1 Backfill window depth — per-symbol windowing with hard floor

**Decision:** No backfill goes earlier than `BACKFILL_FLOOR = 2025-08-01`. Per-symbol fetch window is `[max(first_trade_date, BACKFILL_FLOOR), max(last_trade_date, last_held_date)]`. Symbols whose entire active window precedes `BACKFILL_FLOOR` are skipped entirely (no rows fetched, no `symbol_market` row inserted).

**Implications:**
- Phase 3's `backfill_runner.run_backfill()` must compute the per-symbol window from `portfolio.json`'s trade ledger and holdings tables, not pass a single global `(start, end)` tuple.
- Cold-start API budget revised down: typical ~30–60s instead of ~3–5min. WAF risk drops from "real" to "structural impossibility" given the small call count.
- `meta.backfill_floor` is stored at schema init (so the constant is auditable / changeable later without code edit).
- Phase 6's FX backfill is bounded by `[BACKFILL_FLOOR, today]` regardless of trade activity (FX series needs to be dense across the whole equity-curve window, not per-symbol).
- Phase 7 (`validate_data.py`) check (a) is scoped to `[BACKFILL_FLOOR, today]` rather than per-symbol full history.

### 6.2 Shioaji install — hard dependency in requirements.txt

**Decision:** `shioaji` is in `requirements.txt`, not split into a `requirements-shioaji.txt` extras file.

**Implications:**
- Phase 0 adds `shioaji` to `requirements.txt` directly.
- Phase 11 risk note about "make it optional in `requirements.txt`" is dropped.
- Users without credentials still install the ~200MB pyzmq dependency but get the trivially-stubbed `lazy_login()` no-op behavior. Acceptable for v1 personal-use scope.

### 6.3 SQLite WAL backup procedure — use `.backup` command

**Decision:** WAL stays on (required for two-process write safety). Backups must use `sqlite3 [db] ".backup [file]"`, never `cp`.

**Implications:**
- Phase 1's `init_schema()` enables `journal_mode=WAL` and `busy_timeout=5000` on first connection.
- README must document the `.backup` procedure under the refresh workflow section. Phase 0 or Phase 1 adds this docs entry.
- `.gitignore` already covers `data/`, so the WAL sidecar files (`dashboard.db-wal`, `dashboard.db-shm`) are not a git concern.
- Backups are optional for this project (the SQLite layer is regenerable from `portfolio.json` + APIs in ~30–60s) — documented as nice-to-have, not nightly cron.

### 6.4 `/admin` page scope — deferred; use Developer Tools accordion on `/today`

**Decision:** No dedicated `/admin` page for v1. All admin/operator controls live inside a collapsible "Developer Tools" accordion at the bottom of `/today`, collapsed by default.

**Implications:**
- Phase 10's "frontend failed-tasks panel" lands inside the accordion, not on a separate page.
- Phase 12's "Run Reconciliation" UI button lives in the same accordion next to the failed-tasks panel.
- Phase 13's `/today` template gains the accordion section. A small reusable accordion partial (`templates/_developer_tools.html` or similar) is acceptable.
- "View failed tasks" links from warming-up timeout messages should deep-link to `/today#developer-tools` and auto-expand the accordion.
- Warming-up timeout messaging revised from 5min to 2min given the new ~30–60s cold-start estimate.
- Endpoints `/api/admin/failed-tasks`, `/api/admin/retry-failed`, `/api/admin/reconcile`, `/api/admin/refresh` exist as endpoints regardless of where the UI lands — the namespace is preserved for future `/admin` page extraction.

### 6.5 Frontend resolution toggle — auto-upgrade once READY, no manual toggle

**Decision:** No user-facing monthly/daily toggle in v1. Frontend reads `/api/health` once on page load; if `state == READY`, all subsequent fetches use `?resolution=daily`. If `state != READY`, fetches use the implicit monthly default and the warming-up component renders.

**Implications:**
- Phase 4's frontend change becomes "in `static/js/api.js`, add a one-shot `/api/health` check on page load that sets a module-level `RESOLUTION` flag; all fetch helpers append `?resolution=${RESOLUTION}` automatically." No per-page opt-in flag, no UI toggle component.
- Backend `?resolution=monthly` default stays — programmatic API consumers and bookmarks-without-query-params keep working.
- Phase 9's frontend warming-up handling is unchanged: 202 → spinner with progress, 200 → render data.
- A user who visits before backfill completes sees existing monthly charts (no regression). Next page load after `READY` silently upgrades to daily.

---

## 7. Status

**Approved 2026-04-26.** Ready to begin at Phase 0. No further confirmation needed before execution; phase-by-phase confirmation is at the executor's discretion (see §3 phase acceptance criteria for go/no-go signals).
