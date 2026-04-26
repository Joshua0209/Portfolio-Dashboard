# Sinopac Portfolio Dashboard

A personal investment performance dashboard built on top of Sinopac (永豐金)
monthly PDF statements. The pipeline turns encrypted statement PDFs into a
single JSON dataset, which a Flask backend then serves to an 11-page,
no-build-step dashboard.

```
PDFs (Gmail) ──► decrypt ──► parse ──► data/portfolio.json ──► Flask + JS
```

Everything runs locally. Nothing is sent to a remote service. The encrypted
PDFs, the decrypted PDFs, and the parsed `data/portfolio.json` are all
gitignored — the repository ships *code only*.

---

## What's in the dashboard

Eleven pages, each focused on a different question you'd ask about your
portfolio. Every metric and chart has an info icon (`ⓘ`) you can hover for
a plain-English explanation aimed at someone less than a year into investing.

| Page | URL | What it answers |
| --- | --- | --- |
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

### Backing up the daily SQLite cache (optional)

The daily-resolution layer (`data/dashboard.db`, added in the daily-prices
work) is a regenerable cache — if you delete it, the next run rebuilds it
from `portfolio.json` plus the public TWSE/TPEX/yfinance APIs in roughly
30–60 seconds. So backups are nice-to-have, not required.

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
│   ├── __init__.py               # create_app(), routes, blueprint registration
│   ├── data_store.py             # Mtime-cached portfolio.json loader
│   ├── analytics.py              # Drawdown, Sharpe, HHI, FX P&L, sectors, FIFO
│   ├── benchmarks.py             # yfinance fetcher + cached strategy curves
│   ├── filters.py                # Jinja currency/percent/date filters
│   └── api/                      # 11 blueprints, all under /api/*
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
│       └── benchmarks.py         # Strategy comparison
├── scripts/                      # Pipeline (run from any CWD)
│   ├── download_sinopac_pdfs.py  # Gmail → sinopac_pdfs/
│   ├── decrypt_pdfs.py           # Env-based password unlock
│   └── parse_statements.py       # PDFs → data/portfolio.json
├── templates/                    # Jinja2 page templates (11 pages)
├── static/                       # css/, js/ (vanilla; no build step)
│   ├── css/{tokens,app}.css      # Design system tokens + components
│   └── js/{api,charts,format,help,pagination,app}.js + pages/*.js
├── data/                         # gitignored — actual portfolio data
│   ├── portfolio.json            # Parsed dataset consumed by Flask
│   ├── tw_ticker_map.json        # Manual TW name → ticker overrides
│   └── benchmarks.json           # yfinance price cache (7-day TTL)
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

```
GET /api/health
GET /api/summary
GET /api/holdings/{current,sectors,timeline}
GET /api/holdings/snapshot/<month>
GET /api/performance/{timeseries,rolling,attribution}[?method=day_weighted|mid_month|eom]
GET /api/transactions[?venue=&side=&code=&month=&q=]
GET /api/transactions/aggregates
GET /api/cashflows/{monthly,cumulative,bank}
GET /api/dividends
GET /api/risk
GET /api/fx
GET /api/tax
GET /api/tickers
GET /api/tickers/<code>
GET /api/benchmarks/strategies
GET /api/benchmarks/compare?strategies=passive_tw,passive_us,...
```

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
- **No auth.** The server is bound to `127.0.0.1` only. Do not expose it to
  a public network without an auth proxy in front.

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
