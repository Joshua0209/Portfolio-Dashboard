# Plan: Recover historical broker trades for the post-PDF overlay

**Status**: All decisions locked. Ready to implement on user's go-ahead.
- Phase 0 probe: complete. Path A confirmed. Foreign rejected by SDK (HTTP 406).
- Closed-pair scope: option C (all detail rows, no date filter, audit-hook for PDF disagreement).
- 融資 lots: option A (write to overlay with `type='融資'`).
- Implementation footprint: ~120 lines new in `app/shioaji_client.py`, ~80 lines refactor in `app/trade_overlay.py`, ~150 lines new tests, no schema changes, no cron.

**Branch**: `feature/daily-resolution-layer` (current)
**Last updated**: 2026-05-01

## Phase 0: Results (probe executed 2026-05-01)

| Test | Result | Implication |
|---|---|---|
| `api.list_accounts()` | 3 accounts: StockAccount, FutureAccount, **`Account(account_type=AccountType.H)`** | Foreign-equity account *exists* in account list — but see next row |
| `api.list_profit_loss(h_account, ...)` | **HTTP 406 "Account Not Acceptable"** | SDK rejects H-account for accounting queries. Foreign trades remain PDF-canonical. |
| `api.list_positions(h_account)` | **HTTP 406 "Account Not Acceptable"** | Same. No SDK route to foreign positions. |
| `api.list_profit_loss(stock_account, '2026-03-02', '2026-05-01')` | 19 closed pairs returned | TW historical query works. |
| `api.list_profit_loss_detail(stock_account, detail_id=id)` | Returns BUY-leg tranches only (e.g., 5 legs for one closed pair, dates spanning 2025-11 → 2026-02) | Buy-side history recoverable. Sell info stays on summary row. |
| `api.list_positions(stock_account)` | 2 positions: `2330` Cash + `00981A` MarginTrading | TW open positions visible. |
| `api.list_position_detail(stock_account)` (default call) | 2+ rows with `date, price (=total cost TWD), last_price (=current MV TWD), currency` | Currently-open lots queryable via the simple no-arg call. |
| `quantity` field across `list_profit_loss*` and `list_position_detail` | Always `0` (SDK quirk for 零股 / odd-lot trades) | Derive qty from `cost / price` or from `pnl` reconciliation. |

### Confirmed data model

For the TW stock account:

```
list_profit_loss(stock_account, begin, end) → List[StockProfitLoss]
  pl.id        → handle for list_profit_loss_detail
  pl.code      → ticker
  pl.date      → SELL date (close date of the pair)
  pl.price     → SELL price per share
  pl.pnl       → realized P&L (NTD)
  pl.cond      → Cash | MarginTrading | ShortSelling
  pl.quantity  → ALWAYS 0 (do not trust)

list_profit_loss_detail(stock_account, detail_id=pl.id) → List[StockProfitDetail]
  Returns BUY legs only (entry tranches, FIFO-closed by the sell on pl.date).
  leg.date     → BUY date for this tranche
  leg.price    → BUY price per share
  leg.cost     → BUY cost in NTD (per-tranche)
  leg.fee, leg.tax → per-tranche
  leg.trade_type → TradeType.Common (does NOT distinguish buy/sell — all detail rows ARE buy legs)
  leg.quantity → ALWAYS 0 (use cost/price)

list_position_detail(stock_account)  # default call works fine
  → List[StockPositionDetail]
  lot.date     → entry date
  lot.code     → ticker
  lot.price    → TOTAL COST in NTD for this lot (NOT per-share)
  lot.last_price → CURRENT MV in NTD for this lot
  lot.currency → Currency.TWD
  lot.cond     → Cash | MarginTrading | ShortSelling
  lot.direction → Action.Buy (long lots) / Action.Sell (short lots)
  lot.quantity → ALWAYS 0
```

### Quantity derivation (SDK quirk)

Every `quantity` field comes back as `0` — confirmed across `list_profit_loss`, `list_profit_loss_detail`, and `list_position_detail`. Cause: the SDK reports qty as int張 (1000-share lots) and the user's account contains 零股 (odd-lot, e.g., ~3 shares of 7769 in one tranche), which truncates to 0.

Derivation rules:

| Surface | Qty formula |
|---|---|
| `list_profit_loss_detail` (buy legs) | `qty_shares = round(cost / price)` |
| `list_position_detail` (open lots) | `qty_shares = round(last_price / quoted_close)` where `quoted_close` comes from our `prices` table for that day's closing price of `code` |
| `list_profit_loss` (sell summary) | `qty_shares = sum(leg.cost/leg.price for leg in detail_rows)` (matches FIFO total) |

`list_position_detail.price` and `.last_price` are *total NTD* values, not per-share. This is asymmetric with `list_profit_loss_detail` (per-share `price`, total `cost`). The adapter must handle both.

## Goal

Surface broker trades from "after the most recent PDF month-end" through "today"
in `data/dashboard.db` so the user sees post-PDF trades on `/today` and the
equity curve, without waiting for the next monthly PDF to land.

Concrete success: after running `POST /api/admin/refresh` on 2026-05-01, the
user's April 1–April 27 trades appear in `positions_daily` rows with
`source='overlay'`, the `/today` snapshot reflects them, and PDF-sourced rows
are never overwritten.

## Background

Current state (`app/shioaji_client.py:242-289`): the client calls
`api.list_trades()` (Shioaji 1.3.x — no args, session-scoped). For any date
before "today TPE", the SDK returns nothing on this call. The 825cece commit
fixed the 1.2.x → 1.3.x signature drift but did not address the historical-data
gap.

Prior session's proposal (`broker_deals` table + daily cron) was over-built
because it assumed `list_trades` was the only read surface. The Shioaji API
exposes two additional historical surfaces that change the calculation.

## Research findings

Source: https://sinotrade.github.io/llms-full.txt (fetched 2026-05-01)
and https://sinotrade.github.io/tutor/accounting/profit_loss/ (search index).

### Three relevant SDK surfaces

| Method | Signature | Returns | Scope |
|---|---|---|---|
| `api.list_trades()` | no args | `List[Trade]` with `.status.deals[]` | Session only (today) |
| `api.list_profit_loss(account, begin_date, end_date)` | dates as `'YYYY-MM-DD'` | `List[StockProfitLoss]` | Historical — **closed pairs** in window |
| `api.list_profit_loss_detail(account, detail_id)` | `detail_id` from above | `List[StockProfitDetail]` | Per-pair leg detail |
| `api.list_position_detail(account)` | no date args | `List[StockPositionDetail]` | **Currently-open lots** |

### Critical attribute lists (live docs, not the cached skill)

```
StockProfitLoss:        id, code, seqno, dseq, quantity, price, pnl,
                        pr_ratio, cond, date
StockProfitDetail:      date, code, quantity, dseq, fee, tax, currency,
                        price, cost, rep_margintrading_amt, rep_collateral,
                        rep_margin, shortselling_fee, ex_dividend_amt,
                        interest, trade_type, cond
StockPositionDetail:    date, code, quantity, price, last_price, dseq,
                        direction, pnl, currency, fee, cond, ex_dividends,
                        interest, margintrading_amt, collateral
```

### Coverage matrix for "see April 1–today trades"

| Trade pattern | Recoverable from | Buy date | Sell date |
|---|---|---|---|
| Bought before April, still held | March PDF + `list_position_detail` (sanity) | from PDF | n/a (open) |
| Bought in April, still held | `list_position_detail.date` (entry date) | exact | n/a (open) |
| Bought before April, sold in April | March PDF + `list_profit_loss.date` (sell) | from PDF | exact |
| Bought AND sold in April (round-trip) | `list_profit_loss` + **`list_profit_loss_detail`** | **probe required** | exact |

The bottom row is the source of the architectural decision.

### Open question (drives Phase 0)

The docs **imply** but do not state that `list_profit_loss_detail` returns
both legs of a closed pair (a buy row and a sell row distinguished by
`trade_type`), each with its own `date`. The plural return type
(`List[StockProfitDetail]`) and the presence of `trade_type` are the signals.

If true → Path A. If false (returns only the close leg or only an opaque
identifier) → Path B.

## Phase 0: Probe (executed — see Results above)

Write a one-off script that exercises the historical surfaces against the
user's real account and prints the raw response shape. ~50 lines. Throwaway.

**File**: `scripts/probe_shioaji_pnl_detail.py` (gitignored)

**Probe steps**:

1. Login with `SINOPAC_API_KEY` / `SINOPAC_SECRET_KEY` from env.
2. **Account enumeration**: print `api.list_accounts()`. Confirms TW-only
   surface — anything beyond `StockAccount` / `FutureAccount` would change
   the plan; the docs say nothing else exists but the user's account may
   reveal otherwise.
3. **`list_profit_loss` historical query**:
   `api.list_profit_loss(api.stock_account, begin_date='2026-03-01', end_date='2026-04-30')`.
   For each row, print `id, code, quantity, price, pnl, date`.
4. **`list_profit_loss_detail` per-pair drill-down** (the key Path A test):
   for each `id` from step 3, call
   `api.list_profit_loss_detail(api.stock_account, detail_id=id)` and print
   every returned record's `date, code, quantity, price, cost, trade_type, dseq`.
5. **`list_position_detail` open-lot snapshot**:
   `api.list_position_detail(api.stock_account)`. Print `date, code, quantity, price, cond` for each row.
6. **`list_trades` session probe** (Path B sanity check):
   `api.list_trades()`. Print count and (if non-empty) first few records'
   `.contract.code`, `.order.action.value`, `.status.deals[]`.
7. Logout.

**Decision criteria**:

| Probe outcome | Path |
|---|---|
| Step 4 returns ≥2 records per `id` with distinct `date`s and `trade_type` distinguishing buy/sell legs | **Path A** |
| Step 4 returns 1 record (close-side only), 0 records, or rate-limits consistently | **Path B** |
| Step 2 reveals an unexpected account type (foreign-account) | **Out-of-scope expansion** — pause and rescope before either path |

**Estimated time to run**: 5 minutes once user has logged in to read the
script's output.

## Path A — confirmed by probe, this is the implementation

### Locked decisions (resolved 2026-05-01)

| # | Decision | Choice | Rationale |
|---|---|---|---|
| 1 | Closed-pair coverage scope | **C — all detail rows, no date filter** | Cross-checks PDF parser; lets reconcile banner surface broker-vs-PDF disagreements. PDF rows always win conflicts (existing `WHERE source='overlay'` UPSERT guard), so writes outside the gap window are silently shadowed. |
| 2 | `MarginTrading` (融資) lots | **A — write with `type='融資'`** | Matches existing PDF-parser convention. Analytics layer already handles 融資 cost-asymmetry (CLAUDE.md §"Caveats"). 00981A becomes visible on `/today` immediately. |

### No schema changes
Existing `positions_daily(date, symbol, qty, cost_local, mv_local, mv_twd, type, source)` accommodates everything. The `type` field carries `現股` / `融資` / `融券` per the PDF parser's convention; the overlay maps `StockOrderCond.Cash` → `現股`, `StockOrderCond.MarginTrading` → `融資`, `StockOrderCond.ShortSelling` → `融券`.

### Code changes

**`app/shioaji_client.py`** (~120 lines added, 0 changed):

```python
def list_open_lots(self) -> list[dict[str, Any]]:
    """Return currently-held TW lots as project-shape records.

    Calls api.list_position_detail(stock_account). Each lot becomes one
    record:

        {date, code, qty, cost_twd, mv_twd, type, ccy, venue}

    Where:
      qty       = round(lot.last_price / close_on_date_from_prices_table)
                  with fallback to round(lot.price / pl_implied_price) if
                  the prices table has no entry for that day.
      cost_twd  = lot.price       (already total NTD, NOT per-share)
      mv_twd    = lot.last_price  (already total NTD)
      type      = '現股' | '融資' | '融券'   (mapped from lot.cond)
      ccy       = 'TWD'           (Currency.TWD per probe; foreign rejected)
      venue     = 'TW'

    Returns [] if not configured / login fails / both attempts fail.
    Never raises.
    """

def list_realized_pairs(
    self, begin_date: str, end_date: str
) -> list[dict[str, Any]]:
    """Return closed-pair fills (buy legs + sell summary) in
    [begin_date, end_date].

    NOTE: 'begin_date' / 'end_date' filter the SUMMARY rows (sell dates)
    only. Buy legs returned by list_profit_loss_detail may pre-date
    begin_date — this is intentional (decision #1, option C). Detail-row
    audit comparison happens in trade_overlay.merge(), not here.

    Internally:
      pl_rows = api.list_profit_loss(stock_account, begin, end)
      for each pl in pl_rows:
          legs = api.list_profit_loss_detail(stock_account, pl.id)
          for leg in legs:
              # Buy leg (per probe finding: detail rows are buy legs only)
              emit {
                date  = leg.date,
                code  = leg.code,
                side  = '普買',
                qty   = round(leg.cost / leg.price),  # SDK quirk
                price = leg.price,                    # per-share NTD
                cost_twd = leg.cost,                  # total NTD per leg
                ccy   = 'TWD',
                venue = 'TW',
                type  = map(pl.cond),
                pair_id = pl.id,                      # for audit linkage
              }
          # Sell summary (one per closed pair)
          emit {
            date   = pl.date,
            code   = pl.code,
            side   = '普賣',
            qty    = sum(buy_leg.qty for buy_leg in legs),
            price  = pl.price,                  # per-share NTD
            ccy    = 'TWD',
            venue  = 'TW',
            type   = map(pl.cond),
            pair_id = pl.id,
            pnl    = pl.pnl,                    # for analytics cross-check
          }

    Returns []: not configured / login fails / both attempts fail.
    Never raises. If list_profit_loss_detail(id) returns empty for any id,
    that pair's buy legs are skipped and a reconcile event is fired by
    the merge layer (C-fallback).
    """
```

The legacy `list_trades(start_date, end_date)` stays as a session-only
catch — it picks up trades placed *after* the last `list_profit_loss` call
but before refresh runs. No removals.

**`app/trade_overlay.py`** (~80 lines changed):

`merge()` consumes three sources with `(date, code, side, qty)` dedup:

1. `list_open_lots()` → buy-side fills for currently-held positions (locks in `2330` cash + `00981A` margin per probe state).
2. `list_realized_pairs(gap_start, gap_end)` → all buy legs + sell summary for closed pairs whose **sell date** is in the window. Buy legs may pre-date `gap_start` (decision #1).
3. `list_trades()` → session-only safety net for trades placed since the last refresh.

**Audit hook (decision #1, option C)**:
For each `pair_id` returned by source 2 where the SDK's buy-leg count
diverges from the PDF parser's trade rows for the same `(code, ≤sell_date)`
window, emit a reconcile event via `app/reconcile.py:record_event(...)`:

```
event_type = 'broker_pdf_buy_leg_mismatch'
detail = {
  pair_id, code, sell_date,
  sdk_leg_count, pdf_trade_count,
  sdk_legs: [...],
  pdf_trades: [...],
}
```

The existing reconcile banner (`templates/_reconcile_banner.html`) surfaces
this; operator dismisses via `POST /api/admin/reconcile/<event_id>/dismiss`
once they've decided which side is right. PDF still wins on `positions_daily`
writes — the audit is informational, not corrective.

**`app/daily_store.py`**: no changes.

### Tests
- `tests/test_shioaji_client.py`: new fake-SDK fixtures shaped after the
  probe output. Specifically:
  - `_FakePnL` and `_FakePnLDetail` fixtures matching the 7769 5-leg case
    and the 6442 1-leg same-month round-trip case from the probe.
  - `_FakePositionDetail` matching the 2330 / 00981A current-state probe.
  - Test: qty derivation (cost/price for legs; mv/quoted_close for lots).
  - Test: 融資 mapping (`StockOrderCond.MarginTrading` → `type='融資'`).
  - Test: list_profit_loss_detail empty for one id → that pair's legs
    skipped; reconcile event fires (mocked); merge continues for other ids.
  - Existing static-grep guards remain; new methods are read-only by
    SDK design (no Order / activate_ca anywhere in the diff).
- `tests/test_trade_overlay.py`: new merge tests covering all four trade
  patterns from the §"Coverage matrix":
  - Bought-and-still-held in gap window → row from `list_open_lots`
  - Bought-pre-gap, sold-in-gap → buy from PDF (untouched), sell from
    `list_realized_pairs` summary
  - Round-trip in gap (the 6442 pattern, transposed forward): buy + sell
    both from `list_realized_pairs`
  - Pre-gap multi-leg pair (the 7769 pattern): buy legs cross gap_start
    boundary; verify they're written but PDF rows for pre-gap dates are
    unchanged.
- New file `tests/test_trade_overlay_audit.py`: pin the audit hook —
  SDK reports 5 buy legs for code X, parser-stored portfolio.json has
  3 trades for code X in the same window → reconcile event fires with
  exactly the diff payload, banner shows up, dismiss works.

### C fallback (degenerate case in Path A)
If `list_profit_loss_detail(id)` returns 0 rows for any `id` in the window
(rate-limit, partial response, transient error), the overlay emits a
reconcile event "N broker pairs deferred — exact dates unavailable from
SDK; will reconcile against next PDF" via the existing `app/reconcile.py`
event log. Banner already infrastructure'd in `templates/_reconcile_banner.html`.

This is a defensive belt-and-suspenders — should be rare or never in steady state.

### Estimated work
~150 lines new code, ~40 lines test setup, ~120 lines tests. One-day implementation.

## Path B — REJECTED by probe (kept for reference only)

The probe confirmed `list_profit_loss_detail` returns BUY-leg tranches with
distinct dates, eliminating the original motivation for the daily cron.
The schema migration / launchd plist / `broker_deals` table below are NOT
needed and will not be implemented. Section retained as historical reference
in case a future SDK regression breaks `list_profit_loss_detail`.

---

### Schema migration

Add to `data/dashboard.db`:

```sql
CREATE TABLE IF NOT EXISTS broker_deals(
  date         TEXT NOT NULL,        -- TPE business date
  code         TEXT NOT NULL,
  side         TEXT NOT NULL,        -- 普買 / 普賣 (project convention)
  qty          REAL NOT NULL,
  price        REAL NOT NULL,
  ccy          TEXT NOT NULL DEFAULT 'TWD',
  source       TEXT NOT NULL DEFAULT 'shioaji_session',
  captured_at  TEXT NOT NULL,        -- ISO timestamp of when we captured
  PRIMARY KEY(date, code, side, qty, price)
);
CREATE INDEX IF NOT EXISTS broker_deals_date_idx ON broker_deals(date);
```

Idempotent INSERT OR IGNORE on the PK. Captured rows are immutable.

### `app/daily_store.py` additions
- `record_broker_deals(deals: list[dict]) -> int`
- `query_broker_deals(start: str, end: str) -> list[dict]`

### Daily capture mechanism
- New script: `scripts/snapshot_broker_deals.py`
  - Loads `.env`, instantiates `ShioajiClient`, calls `list_trades()`, persists fills via `record_broker_deals`.
  - Logs N rows captured to `logs/broker_snapshot.log`.
  - Idempotent: re-running on the same day is a no-op.
- launchd plist: `~/Library/LaunchAgents/com.investment.shioaji-snapshot.plist`
  - Fires daily at **06:00 TPE**.
  - Calls `<repo>/.venv/bin/python <repo>/scripts/snapshot_broker_deals.py`.
  - StandardOutPath/ErrorPath → `<repo>/logs/broker_snapshot.log`.
- Documented in `CLAUDE.md` "Daily layer refresh" section + README.

### Why 06:00 TPE (cron timing rationale)

The cron's job is to capture trades that have *already settled in the broker
back-end* — i.e., are stable and won't be amended. TWSE has a layered close
sequence:

| Time TPE | Event |
|---|---|
| 13:30 | TWSE regular session closes |
| 14:00–14:30 | After-hours odd-lot fixing window |
| 14:30 | All exchange activity ends; broker reconciliation begins |
| Evening | Broker may adjust fills (rare — error corrections, partial-fill consolidation) |
| 06:00 next day | All adjustments settled. SDK returns clean state. |

Same-day cron times (14:30, 17:00) sometimes miss late broker amendments;
the rare row that gets adjusted overnight then needs special handling. 06:00
sidesteps this entirely. The cost is a uniform 1-day lag — a Monday trade
appears in `broker_deals` at Tuesday 06:00, surfaces in the dashboard on
Tuesday's first refresh.

This timing is **TW-only**. Foreign / 複委託 trades are not queryable via
Shioaji 1.3.x at all (see Out of scope) — they remain canonical-from-PDFs
regardless of cron schedule.

### `app/trade_overlay.py` changes
Three-source merge identical to Path A in shape, but source #1 (open lots) is
augmented by `query_broker_deals(gap_start, gap_end)` for round-trip buy
recovery. `list_profit_loss` still provides closes; `list_position_detail`
still provides currently-open entries; `broker_deals` covers the "bought and
sold same window" gap.

Dedup key remains `(date, code, side, qty)`.

### C fallback (Path B)
- If a date `d` in the window has neither a PDF row nor a `broker_deals` row
  (cron didn't fire, laptop asleep, manual run skipped) AND `list_profit_loss`
  shows a closed pair on `d`, emit reconcile event "round-trip on `d` with
  unrecoverable buy date — defer to next PDF".
- Operator can manually run `python scripts/snapshot_broker_deals.py` any time;
  it captures *today's* deals. Past gaps are SDK-permanent, only PDFs heal them.

### Operational considerations
- **Failure modes**: launchd may not fire if laptop is asleep at 14:30 TPE. Acceptable — C fallback covers this; we banner the gap.
- **Backup**: `broker_deals` is append-only; SQLite WAL backup via `.backup` already covered in CLAUDE.md.
- **Bootstrapping**: `broker_deals` will be empty on day-1. The first useful data lands the next trading day after deployment. Not a blocker — the user's question was forward-looking.

### Estimated work
~250 lines new code (script + store methods + plist + integration), ~150 lines tests, plus operational setup. Roughly 1.5 days.

## Tests (both paths)

### Read-only invariant
Extend `tests/test_shioaji_client.py:43-51` static-grep `forbidden` tuple to
keep `place_order`, `cancel_order`, `update_order`, `activate_ca` blocked. New
methods are read-only by SDK design — no special guard needed beyond what's
there.

### Unit
- New fake SDK fixtures: `_FakePnL`, `_FakePnLDetail`, `_FakePositionDetail`.
- Cover: empty result, single closed pair, round-trip pair (Path A only),
  rate-limit-then-empty, login retry on session expiry.

### Integration
- `tests/test_trade_overlay.py`: end-to-end with seeded portfolio.json (last
  PDF = 2026-03), Shioaji fixture returning known April activity, assert
  resulting `positions_daily` rows match the four trade patterns.
- Verify `source='pdf'` rows are never overwritten (existing invariant).

### Manual / E2E
- Run dashboard with creds set after probe + implementation.
- Hit `/today`, verify Δ vs prior session reflects current April positions.
- Hit `/api/today/movers` for a date that had a closed pair, verify gain/loss attribution.

## Decision points (all resolved)

| # | Question | Resolution |
|---|---|---|
| 1 | Phase 0 probe | ✅ Written, executed 2026-05-01. Path A confirmed. |
| 2 | C-fallback banner copy | ✅ "*N broker round-trip(s) since last statement —* exact buy date deferred to next PDF" — used only when `list_profit_loss_detail(id)` returns empty. The audit-hook banner (decision #1 below) uses different copy: "*N broker pair(s) disagree with PDF parser — review_." |
| 3 | Path B cron time | ❌ N/A — Path B rejected. |
| 4 | Probe data leakage | ✅ User confirmed raw output OK. |
| 5 | Closed-pair coverage scope | ✅ **Option C**: all detail rows, no date filter. PDF wins conflicts; audit events surface disagreements. |
| 6 | `MarginTrading` lots in overlay | ✅ **Option A**: write with `type='融資'`. |

## Out of scope

- **Foreign / 複委託 / US trades** — Empirically confirmed by the Phase 0
  probe: although `api.list_accounts()` returns the user's H-type account
  (`AccountType.H`, `account_id='00102926'`), every accounting query against
  it (`list_positions`, `list_profit_loss`) responds with HTTP 406
  *"Account Not Acceptable"*. The H account exists at the auth layer but is
  walled off from the read APIs. Foreign trades (currently TSMX, SNXX, SNDK)
  remain PDF-canonical, sourced from the 複委託 monthly statements parsed
  by `scripts/parse_statements.py`. The C-fallback banner naturally covers
  "foreign trades since last PDF" as a deferred-to-next-statement state.
  `app/shioaji_client.py` already hard-codes `venue='TW'` in `_extract_fills`
  and stays that way.
- Trading (placing/canceling orders) — explicitly forbidden by the read-only
  invariant in `app/shioaji_client.py:1-7` and the static-grep guards.
- Real-time tick streaming — `/today` reads day-end closes, not intraday quotes.
- CA certificate activation — orthogonal, see CLAUDE.md "Shioaji is read-only — forever".

## Sequencing

1. ~~Phase 0 probe~~ — **done 2026-05-01**, Path A confirmed.
2. **Implementation** on `feature/daily-resolution-layer`:
   - 2a. Extend `ShioajiClient` with `list_realized_pairs()` and `list_open_lots()`. Keep `list_trades()` for the same-session sanity catch.
   - 2b. Implement qty-derivation helpers (cost/price for legs; mv-vs-prices-table for open lots).
   - 2c. Refactor `app/trade_overlay.py:merge()` to consume the three sources, with `(date, code, side, qty)` dedup.
   - 2d. Tests covering: each new client method (fake SDK fixtures mirroring the probe output), the round-trip case (pl[2] code 6442 pattern), the 0-leg degenerate case (C fallback path).
3. Tests green: `pytest tests/ -q` (current 284 → +N).
4. Manual verification on `/today` with creds set — should now show 2330 + 00981A in current positions and the April closed pairs in /transactions.
5. Update `CLAUDE.md` (data-model section + Shioaji surface table) + README.
6. Commit + PR.

## Sources

- [list_profit_loss attributes (StockProfitLoss)](https://sinotrade.github.io/tutor/accounting/profit_loss/)
- [list_position_detail attributes (StockPositionDetail)](https://sinotrade.github.io/tutor/accounting/position/)
- Shioaji llms-full.txt (live, fetched 2026-05-01) — primary source for `list_profit_loss_detail` signature.
- `app/shioaji_client.py:242-289` — current 1.3.x adapter.
- `app/trade_overlay.py:132-273` — current merge() integration point.
- `tests/test_shioaji_client.py:36-51` — static-grep read-only guards.
