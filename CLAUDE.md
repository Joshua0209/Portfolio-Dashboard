# Sinopac Investment Dashboard

Personal investment performance dashboard built from Sinopac (永豐金) monthly PDF statements.
Pipeline: encrypted PDFs → decrypt → parse → JSON → static HTML/Chart.js dashboard.

## Layout

```
investment/
├── credentials.json              # Google API creds for the downloader (existing)
├── token.json                    # OAuth token (existing)
├── download_sinopac_pdfs.py      # Existing: pulls statement PDFs from Gmail/Drive
├── sinopac_pdfs/                 # Encrypted source PDFs
│   └── decrypted/                # Decrypted copies (gitignored — sensitive)
├── decrypt_pdfs.py               # Step 1: env-based password unlock
├── parse_statements.py           # Step 2: extract holdings + flows → data/portfolio.json
├── data/portfolio.json           # Parsed dataset consumed by the dashboard
├── data/tw_ticker_map.json       # Manual TW name→code overrides (see below)
└── index.html                    # Step 3: dashboard (Chart.js via CDN)
```

## Refresh workflow

When new monthly statements arrive:

```bash
cd path/to/investment
source .venv/bin/activate

# 1. (existing) pull new PDFs into sinopac_pdfs/
python3 download_sinopac_pdfs.py

# 2. unlock — passwords come from env (comma-separated candidates)
export SINOPAC_PDF_PASSWORDS="<id-or-birthdate>,<fallback>"
python3 decrypt_pdfs.py

# 3. parse → data/portfolio.json
python3 parse_statements.py

# 4. view dashboard (must serve over HTTP — fetch() blocks file://)
python3 -m http.server 8000
# then open http://localhost:8000/
```

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

- **TWR (Modified Dietz, monthly)**: `r = (V_end − V_start − F) / (V_start + 0.5·F)`,
  chained across months. Measures investment skill independent of deposit timing.
  - Month 1 is forced to 0% (no prior equity to compare against).
- **XIRR**: Newton-Raphson on cashflow dates. Money-weighted; reflects what
  *your money* actually earned. Cashflows dated to month-mid; final equity
  treated as a terminal inflow.

The two often diverge significantly. TWR ≫ XIRR usually means recent deposits
haven't had time to compound; that's normal, not a bug.

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
position, add an entry and re-run `parse_statements.py`.

## Caveats

- **Margin (融資)**: equity = market value of all positions, but cost includes
  only your portion (資自備款). Equity-based returns can look inflated. Read
  `holdings_detail.type == "融資"` rows with that in mind.
- **Foreign FX**: only USD positions are TWD-converted right now. Add HKD/JPY
  rates from the bank statement if those positions appear (extend the loop in
  `parse_statements.py:main`).
- **Dividends (TW)**: 累計配息 column captured per holding but not yet flowed
  through the cashflow ledger. Foreign dividends ARE counted via 應收/付.
- **The fetch() requirement**: opening `index.html` directly (file://) fails
  because browsers block local JSON fetches. Always use a local server.

## Adding a new statement type

The parser dispatches in `parse_statements.py:main` based on filename
substring (`證券月對帳單`, `複委託`, `銀行綜合`). To add a new type:

1. Write a `parse_<type>(pdf_path) -> dict` function returning a structured
   month record.
2. Add a filename branch in `main()` to populate `files_by_month[ym][...]`.
3. Decide if it's inside-portfolio (affects equity & flows) or external.
4. Bump dashboard's `index.html` if you want a new visualization.

## Files NOT to commit

- `sinopac_pdfs/` (encrypted statements)
- `sinopac_pdfs/decrypted/` (definitely)
- `data/portfolio.json` (contains real positions)
- `credentials.json`, `token.json`
