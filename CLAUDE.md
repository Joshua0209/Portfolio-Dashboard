# Sinopac Investment Dashboard

Personal investment performance dashboard built from Sinopac (永豐金) monthly PDF statements.
Pipeline: encrypted PDFs → decrypt → parse → JSON → **Flask backend + multi-page dashboard**.

## Layout

```
investment/
├── credentials.json              # Google API creds for the downloader (gitignored)
├── token.json                    # OAuth token (gitignored)
├── sinopac_pdfs/                 # Encrypted source PDFs (gitignored)
│   └── decrypted/                # Decrypted copies (gitignored — sensitive)
├── data/                         # gitignored — real portfolio data
│   ├── portfolio.json            # Parsed dataset consumed by the dashboard
│   ├── tw_ticker_map.json        # Manual TW name→code overrides (see below)
│   └── benchmarks.json           # yfinance price cache (7-day TTL)
├── scripts/                      # Pipeline (run from any CWD; ROOT auto-resolved)
│   ├── download_sinopac_pdfs.py  # Pull statement PDFs from Gmail
│   ├── decrypt_pdfs.py           # Step 1: env-based password unlock
│   └── parse_statements.py       # Step 2: extract holdings + flows → data/portfolio.json
├── app.py                        # Flask entrypoint
├── app/                          # Backend application package
│   ├── __init__.py               # create_app(), routes, blueprint registration
│   ├── data_store.py             # Mtime-cached portfolio.json loader
│   ├── analytics.py              # Drawdown, Sharpe, Sortino, Calmar, HHI, FX P&L, FIFO P&L, sectors
│   ├── benchmarks.py             # yfinance fetcher + cached strategy curves
│   ├── filters.py                # Jinja currency/percent/date filters
│   └── api/                      # 11 blueprints, all under /api/*
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
│       └── benchmarks.py         # Strategy comparison vs portfolio
├── templates/                    # Jinja2 page templates (11 pages)
├── static/                       # css/, js/ (vanilla; no build step)
│   ├── css/{tokens,app}.css      # Design system + components
│   └── js/{api,charts,format,help,pagination,app}.js + pages/*.js
└── legacy/index.html             # Pre-rebuild single-page dashboard (kept for reference)
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

# 4. start the Flask dashboard (refreshes data automatically when JSON updates)
python3 app.py
# then open http://127.0.0.1:8000/
```

The Flask app watches `data/portfolio.json` mtime — re-running `parse_statements.py`
while the server is up reloads data on the next request without a restart.

## Password env

`SINOPAC_PDF_PASSWORDS` is a comma-separated list. The decrypter tries each
password per file; the first that opens it wins. Different statement types may
need different passwords (e.g. National ID for brokerage, birth-date for bank).
Never commit the value.

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

## API surface

All endpoints return `{"ok": true, "data": ...}`. Errors are HTTP non-200.
Convention: TWD unless field name says otherwise; foreign positions show
both `_local` and `_twd` values where relevant.

```
GET /api/health
GET /api/summary
GET /api/holdings/{current,sectors,timeline}
GET /api/holdings/snapshot/<month>
GET /api/performance/{timeseries,rolling,attribution}
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
GET /api/benchmarks/compare?keys=tw_passive,us_passive
```

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
- `data/` (contains real positions and benchmark cache)
- `credentials.json`, `token.json`
- `.env`
