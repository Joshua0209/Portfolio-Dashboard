# Sinopac Portfolio Dashboard

Personal investment dashboard for [Sinopac](https://www.sinopac.com/) (永豐金) statements.
Pipeline: encrypted PDF → decrypt → parse → JSON → Flask backend + multi-page UI.

Quick start (assumes `data/portfolio.json` already exists from `parse_statements.py`):

```bash
cd path/to/investment
source .venv/bin/activate
pip install -r requirements.txt
python3 app.py
# open http://127.0.0.1:8000
```

For the refresh workflow when new statements arrive, see [CLAUDE.md](./CLAUDE.md).

## Pages

| URL | Purpose |
| --- | --- |
| `/` | KPI hero, equity curve, allocation, top movers, recent activity |
| `/holdings` | Sortable position table, treemap, sector breakdown, CSV export |
| `/performance` | TWR/XIRR, monthly returns, drawdown, rolling 3/6/12M, attribution |
| `/risk` | Drawdown curve, HHI concentration, top-N share, leverage exposure |
| `/fx` | USD/TWD curve, FX-attributable P&L, currency exposure stack |
| `/transactions` | Filterable trade log, monthly volume + fees, CSV export |
| `/cashflows` | Real vs counterfactual chart, monthly waterfall, bank ledger |
| `/dividends` | Monthly income, top payers, full distribution log |
| `/tax` | Per-ticker realized + unrealized P&L, win rate, CSV export |
| `/ticker/<code>` | Position over time, cost vs market value, trades, dividends |

## Architecture

- **Backend** — Flask app in `app/`. Single `DataStore` reads `data/portfolio.json`
  on import and reloads on mtime change. Ten blueprints under `/api/*` provide
  read-only JSON endpoints with a consistent `{ok, data}` envelope.
- **Analytics** — `app/analytics.py` contains pure-Python implementations of
  drawdown, Sharpe, HHI, FX P&L attribution, sector mapping, and realized P&L
  by ticker. No external API calls.
- **Frontend** — Vanilla JS, no build step. Chart.js + chartjs-chart-treemap
  via CDN. Design tokens in `static/css/tokens.css` drive both dark and light
  themes (toggle via the sidebar). Each page has its own JS file under
  `static/js/pages/`.

## Adding a metric

1. Add a function to `app/analytics.py`.
2. Surface it in the relevant blueprint under `app/api/`.
3. Wire a chart or KPI tile in `templates/<page>.html` and the matching
   `static/js/pages/<page>.js`.

## Notes

- `data/portfolio.json` and the encrypted PDFs are gitignored — they contain
  real positions and never enter version control.
- The server is bound to `127.0.0.1` only. Do not expose it to a network
  without auth in front.
