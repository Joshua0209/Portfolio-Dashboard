"""Phase 0 probe — verify Shioaji historical-trade query surface.

Run once. Throwaway. Prints raw SDK responses for:
  1. account enumeration
  2. list_profit_loss (historical closed pairs)
  3. list_profit_loss_detail (per-pair leg drill-down — the Path A test)
  4. list_position_detail (currently-open lots)
  5. list_trades (session-only sanity check)

Usage:
    source .venv/bin/activate
    python scripts/probe_shioaji_pnl_detail.py

Decision criteria printed at the end. See PLAN-shioaji-historical-trades.md
"Phase 0: Probe" for what to do with the output.
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from pathlib import Path

# Load .env from repo root before importing anything that reads env vars.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env", override=False)
except ImportError:
    pass

API_KEY = os.environ.get("SINOPAC_API_KEY", "")
SECRET_KEY = os.environ.get("SINOPAC_SECRET_KEY", "")

if not (API_KEY and SECRET_KEY):
    print("ERROR: SINOPAC_API_KEY / SINOPAC_SECRET_KEY not set in .env or environment")
    sys.exit(1)


def hr(label: str) -> None:
    print(f"\n{'=' * 8} {label} {'=' * (60 - len(label))}")


def safe_attrs(obj, names):
    """Print named attributes if present; tolerate missing fields."""
    return {n: getattr(obj, n, "<missing>") for n in names}


def main() -> None:
    import shioaji as sj

    api = sj.Shioaji()
    print(f"shioaji version: {sj.__version__}")
    api.login(api_key=API_KEY, secret_key=SECRET_KEY)

    try:
        # --- Step 1: account enumeration ---------------------------------
        hr("STEP 1: api.list_accounts()")
        accounts = api.list_accounts()
        print(f"  count: {len(accounts)}")
        for i, acct in enumerate(accounts):
            print(f"  [{i}] type={type(acct).__name__} repr={acct!r}")
        print(f"  api.stock_account:  {api.stock_account!r}")
        print(f"  api.futopt_account: {api.futopt_account!r}")

        # --- Step 2: list_profit_loss ------------------------------------
        end = date.today()
        begin = end - timedelta(days=60)
        hr(f"STEP 2: list_profit_loss({begin} → {end})")
        try:
            pl_rows = api.list_profit_loss(
                api.stock_account,
                begin_date=begin.isoformat(),
                end_date=end.isoformat(),
            )
        except Exception as exc:
            print(f"  ERROR: {type(exc).__name__}: {exc}")
            pl_rows = []
        print(f"  rows returned: {len(pl_rows)}")
        attrs = ["id", "code", "seqno", "dseq", "quantity", "price", "pnl",
                 "pr_ratio", "cond", "date"]
        for i, pl in enumerate(pl_rows[:5]):
            print(f"  [{i}] {safe_attrs(pl, attrs)}")
        if len(pl_rows) > 5:
            print(f"  ... ({len(pl_rows) - 5} more rows omitted)")

        # --- Step 3: list_profit_loss_detail (THE KEY TEST) --------------
        hr("STEP 3: list_profit_loss_detail (Path A pivotal test)")
        if not pl_rows:
            print("  SKIPPED — no closed pairs in window. Either user had no")
            print("  closed positions in last 60 days, or the API returned empty.")
            print("  Probe inconclusive on Path A vs B; widen window or pick Path B.")
        else:
            probe_targets = pl_rows[:3]
            detail_attrs = ["date", "code", "quantity", "price", "cost",
                            "trade_type", "dseq", "fee", "tax", "cond"]
            for i, pl in enumerate(probe_targets):
                pl_id = getattr(pl, "id", None)
                print(f"\n  --- detail for pl[{i}] id={pl_id} code={getattr(pl, 'code', '?')} ---")
                if pl_id is None:
                    print("    SKIPPED — no .id attribute on this pl row")
                    continue
                try:
                    legs = api.list_profit_loss_detail(
                        api.stock_account, detail_id=pl_id
                    )
                except Exception as exc:
                    print(f"    ERROR: {type(exc).__name__}: {exc}")
                    continue
                print(f"    leg count: {len(legs)}")
                for j, leg in enumerate(legs):
                    print(f"    leg[{j}] {safe_attrs(leg, detail_attrs)}")

        # --- Step 4a: list_positions (TW summary) ------------------------
        hr("STEP 4a: list_positions(stock_account) — TW summary")
        try:
            tw_positions = api.list_positions(api.stock_account)
        except Exception as exc:
            print(f"  ERROR: {type(exc).__name__}: {exc}")
            tw_positions = []
        print(f"  TW positions: {len(tw_positions)}")
        pos_attrs = ["id", "code", "direction", "quantity", "price",
                     "last_price", "pnl", "cond", "yd_quantity"]
        for i, pos in enumerate(tw_positions[:10]):
            print(f"  [{i}] {safe_attrs(pos, pos_attrs)}")

        # --- Step 4b: list_position_detail (TW per-lot, both call shapes) -
        hr("STEP 4b: list_position_detail(stock_account) — both call shapes")
        try:
            tw_lots_default = api.list_position_detail(api.stock_account)
        except Exception as exc:
            print(f"  default-call ERROR: {type(exc).__name__}: {exc}")
            tw_lots_default = []
        print(f"  default call (no detail_id): {len(tw_lots_default)} rows")
        lot_attrs = ["date", "code", "quantity", "price", "last_price",
                     "direction", "cond", "currency"]
        for i, lot in enumerate(tw_lots_default[:5]):
            print(f"    [{i}] {safe_attrs(lot, lot_attrs)}")

        # Now try the chained pattern: feed each position's id into detail
        if tw_positions:
            print("\n  chained call (per-position id):")
            for i, pos in enumerate(tw_positions[:3]):
                pos_id = getattr(pos, "id", None)
                code = getattr(pos, "code", "?")
                if pos_id is None:
                    print(f"    pos[{i}] code={code} has no .id, skipping")
                    continue
                try:
                    legs = api.list_position_detail(
                        api.stock_account, detail_id=pos_id
                    )
                except Exception as exc:
                    print(f"    pos[{i}] code={code} id={pos_id} ERROR: "
                          f"{type(exc).__name__}: {exc}")
                    continue
                print(f"    pos[{i}] code={code} id={pos_id}: "
                      f"{len(legs)} lot(s)")
                for j, lot in enumerate(legs):
                    print(f"      lot[{j}] {safe_attrs(lot, lot_attrs)}")

        # --- Step 5: list_trades (session-only sanity) -------------------
        hr("STEP 5: list_trades() (session-only — should be 0 on fresh login)")
        try:
            trades = api.list_trades()
        except Exception as exc:
            print(f"  ERROR: {type(exc).__name__}: {exc}")
            trades = []
        print(f"  session trades: {len(trades)}")
        for i, tr in enumerate(trades[:3]):
            contract = getattr(tr, "contract", None)
            order = getattr(tr, "order", None)
            status = getattr(tr, "status", None)
            print(f"  [{i}] code={getattr(contract, 'code', '?')} "
                  f"action={getattr(getattr(order, 'action', None), 'value', '?')} "
                  f"deals={len(getattr(status, 'deals', []) or [])}")

        # --- Step 6: H-account (複委託 / foreign) reachability -----------
        hr("STEP 6: H-account (複委託 foreign) — does the SDK accept it?")
        h_account = None
        for acct in accounts:
            atype = getattr(acct, "account_type", None)
            atype_val = getattr(atype, "value", None) if atype is not None else None
            if atype_val == "H" or str(atype) == "AccountType.H":
                h_account = acct
                break
        if h_account is None:
            print("  no H-type account found in api.list_accounts()")
        else:
            print(f"  H account: {h_account!r}")

            # 6a: list_positions on H
            print("\n  --- 6a: list_positions(h_account) ---")
            try:
                h_positions = api.list_positions(h_account)
            except Exception as exc:
                print(f"    ERROR: {type(exc).__name__}: {exc}")
                h_positions = []
            print(f"    H positions: {len(h_positions)}")
            for i, pos in enumerate(h_positions[:10]):
                print(f"    [{i}] {safe_attrs(pos, pos_attrs)}")

            # 6b: list_position_detail on H (chained)
            print("\n  --- 6b: list_position_detail(h_account, detail_id=pos.id) ---")
            for i, pos in enumerate(h_positions[:5]):
                pos_id = getattr(pos, "id", None)
                code = getattr(pos, "code", "?")
                if pos_id is None:
                    print(f"    pos[{i}] code={code} has no .id, skipping")
                    continue
                try:
                    legs = api.list_position_detail(h_account, detail_id=pos_id)
                except Exception as exc:
                    print(f"    pos[{i}] code={code} id={pos_id} ERROR: "
                          f"{type(exc).__name__}: {exc}")
                    continue
                print(f"    pos[{i}] code={code} id={pos_id}: "
                      f"{len(legs)} lot(s)")
                for j, lot in enumerate(legs):
                    print(f"      lot[{j}] {safe_attrs(lot, lot_attrs)}")

            # 6c: list_profit_loss on H (does foreign closed-pair query work?)
            print(f"\n  --- 6c: list_profit_loss(h_account, {begin} → {end}) ---")
            try:
                h_pl = api.list_profit_loss(
                    h_account,
                    begin_date=begin.isoformat(),
                    end_date=end.isoformat(),
                )
            except Exception as exc:
                print(f"    ERROR: {type(exc).__name__}: {exc}")
                h_pl = []
            print(f"    H closed pairs: {len(h_pl)}")
            pl_attrs = ["id", "code", "seqno", "dseq", "quantity", "price",
                        "pnl", "pr_ratio", "cond", "date"]
            for i, pl in enumerate(h_pl[:3]):
                print(f"    [{i}] {safe_attrs(pl, pl_attrs)}")

            # 6d: list_profit_loss_detail per H pair (if any)
            if h_pl:
                print("\n  --- 6d: list_profit_loss_detail per H pair ---")
                detail_attrs = ["date", "code", "quantity", "price", "cost",
                                "trade_type", "dseq", "fee", "tax", "cond",
                                "currency"]
                for i, pl in enumerate(h_pl[:2]):
                    pl_id = getattr(pl, "id", None)
                    code = getattr(pl, "code", "?")
                    if pl_id is None:
                        continue
                    try:
                        legs = api.list_profit_loss_detail(
                            h_account, detail_id=pl_id
                        )
                    except Exception as exc:
                        print(f"    pl[{i}] code={code} id={pl_id} ERROR: "
                              f"{type(exc).__name__}: {exc}")
                        continue
                    print(f"    pl[{i}] code={code} id={pl_id}: "
                          f"{len(legs)} leg(s)")
                    for j, leg in enumerate(legs):
                        print(f"      leg[{j}] {safe_attrs(leg, detail_attrs)}")

        # --- Step 7: 406 fix attempts (only if H account exists) ----------
        # Tests the four hypotheses in PLAN-refactor-shioaji-canonical.md §3,
        # in order of safety. Each step short-circuits on success.
        h_fix_succeeded = False
        if h_account is not None:
            hr("STEP 7a: H-account introspection (zero-cost, no broker calls)")
            try:
                from shioaji.account import _ACCTTYPE  # type: ignore[attr-defined]
            except ImportError:
                _ACCTTYPE = {}
            print(f"  type(h_account):       {type(h_account).__name__}")
            print(f"  account_type:          {h_account.account_type!r}")
            print(f"  account_type.value:    "
                  f"{getattr(h_account.account_type, 'value', '<n/a>')}")
            print(f"  signed:                {getattr(h_account, 'signed', '<missing>')}")
            print(f"  person_id:             "
                  f"{getattr(h_account, 'person_id', '<missing>')}")
            print(f"  broker_id:             "
                  f"{getattr(h_account, 'broker_id', '<missing>')}")
            print(f"  account_id:            "
                  f"{getattr(h_account, 'account_id', '<missing>')}")
            try:
                astyped = h_account.astype()
                print(f"  astype():              "
                      f"{type(astyped).__name__} (no _ACCTTYPE['H'] entry → "
                      f"falls back to base class)")
            except Exception as exc:
                print(f"  astype() ERROR:        {type(exc).__name__}: {exc}")
            print(f"  _ACCTTYPE has 'H':     {'H' in _ACCTTYPE}")
            signed = bool(getattr(h_account, "signed", False))
            print()
            if not signed:
                print("  ⚠  signed=False → broker-side foreign API enrollment is")
                print("     missing. This is hypothesis #2 from PLAN §3 and the")
                print("     fix is in the SinoPac portal, not in code. The")
                print("     remaining 7b/7c/7d probes will likely all fail with")
                print("     401/403 even if the SDK dispatch is fixed.")
            else:
                print("  ✓  signed=True → broker has enrolled foreign API. The")
                print("     406 is then either an SDK class-dispatch issue (7b)")
                print("     or a session-account-context issue (7c) or a CA-cert")
                print("     requirement (7d).")

            # --- 7b: cast H account to a StockAccount-typed wrapper -------
            hr("STEP 7b: cast H → StockAccount-typed (Solace dispatch test)")
            print("  Hypothesis: the C++ Solace transport keys on the typed")
            print("  account class. Account(account_type=H) has no typed")
            print("  wrapper, so it falls into a default route the server")
            print("  rejects. If we forge a StockAccount with H's id fields,")
            print("  the dispatch picks the /stocks/positions route — server")
            print("  may then accept or reject based on the account_id alone.")
            try:
                from shioaji.account import StockAccount as _StockAccount
                forged = _StockAccount(
                    person_id=h_account.person_id,
                    broker_id=h_account.broker_id,
                    account_id=h_account.account_id,
                    signed=getattr(h_account, "signed", False),
                    username=getattr(h_account, "username", ""),
                )
                # Note: StockAccount sets account_type=AccountType.Stock by
                # default, overriding H. Override back so we can compare.
                print(f"  forged: {forged!r}")
                try:
                    forged_pos = api.list_positions(forged)
                    print(f"  list_positions(forged_S): {len(forged_pos)} rows")
                    for i, pos in enumerate(forged_pos[:5]):
                        print(f"    [{i}] {safe_attrs(pos, pos_attrs)}")
                    if forged_pos:
                        h_fix_succeeded = True
                        print("  ✓✓ FIX FOUND: forge as StockAccount works.")
                except Exception as exc:
                    print(f"  list_positions(forged_S) ERROR: "
                          f"{type(exc).__name__}: {exc}")
            except Exception as exc:
                print(f"  forge ERROR: {type(exc).__name__}: {exc}")

            # --- 7c: set_default_account swap (with try/finally restore) --
            if not h_fix_succeeded:
                hr("STEP 7c: set_default_account(h_account) swap + retry")
                print("  Hypothesis: the Solace layer reads `default_stock_account`")
                print("  from session state, not the explicit `account=` kwarg.")
                print("  Swap, retry, then RESTORE — never leave the session in")
                print("  a corrupted state.")
                snapshot_default = None
                try:
                    snapshot_default = getattr(
                        api._solace, "default_stock_account", None
                    )
                    print(f"  snapshot default_stock_account: {snapshot_default!r}")
                    try:
                        api.set_default_account(h_account)
                        print(f"  swapped → default_stock_account: "
                              f"{api._solace.default_stock_account!r}")
                    except Exception as exc:
                        print(f"  set_default_account ERROR: "
                              f"{type(exc).__name__}: {exc}")
                        raise
                    try:
                        swapped_pos = api.list_positions(h_account)
                        print(f"  list_positions(h, default-swapped): "
                              f"{len(swapped_pos)} rows")
                        for i, pos in enumerate(swapped_pos[:5]):
                            print(f"    [{i}] {safe_attrs(pos, pos_attrs)}")
                        if swapped_pos:
                            h_fix_succeeded = True
                            print("  ✓✓ FIX FOUND: default-account swap works.")
                            print("  Caveat: production must do the same swap")
                            print("  every read, with try/finally restore.")
                    except Exception as exc:
                        print(f"  list_positions(h) ERROR: "
                              f"{type(exc).__name__}: {exc}")
                finally:
                    if snapshot_default is not None:
                        try:
                            api._solace.default_stock_account = snapshot_default
                            api.stock_account = snapshot_default
                            print(f"  RESTORED default_stock_account: "
                                  f"{api._solace.default_stock_account!r}")
                        except Exception as exc:
                            print(f"  RESTORE ERROR: "
                                  f"{type(exc).__name__}: {exc}")

            # --- 7d: activate_ca and retry --------------------------------
            # NOT permitted in shioaji_client.py (static-grep guard) but OK
            # in this throwaway probe script. If this is the fix, plan §3
            # mandates a separate opt-in foreign_client.py module.
            if not h_fix_succeeded:
                hr("STEP 7d: activate_ca + retry (probe-only — not the client)")
                ca_path = os.environ.get("SINOPAC_CA_CERT_PATH", "")
                ca_passwd = os.environ.get("SINOPAC_CA_PASSWORD", "")
                if not (ca_path and ca_passwd):
                    print("  SKIPPED — SINOPAC_CA_CERT_PATH / "
                          "SINOPAC_CA_PASSWORD not set")
                else:
                    print(f"  ca_path: {ca_path}")
                    try:
                        ok = api.activate_ca(  # noqa: probe-only
                            ca_path=ca_path, ca_passwd=ca_passwd,
                        )
                        print(f"  activate_ca returned: {ok!r}")
                    except Exception as exc:
                        print(f"  activate_ca ERROR: "
                              f"{type(exc).__name__}: {exc}")
                        ok = False
                    if ok:
                        try:
                            ca_pos = api.list_positions(h_account)
                            print(f"  list_positions(h, post-CA): "
                                  f"{len(ca_pos)} rows")
                            for i, pos in enumerate(ca_pos[:5]):
                                print(f"    [{i}] {safe_attrs(pos, pos_attrs)}")
                            if ca_pos:
                                h_fix_succeeded = True
                                print("  ✓✓ FIX FOUND: foreign reads need CA.")
                                print("  Plan §3 mandates a SEPARATE opt-in")
                                print("  foreign_client.py — must NOT enable")
                                print("  activate_ca on the read-only client.")
                        except Exception as exc:
                            print(f"  list_positions(h, post-CA) ERROR: "
                                  f"{type(exc).__name__}: {exc}")

            hr("STEP 7 SUMMARY")
            if h_fix_succeeded:
                print("  ✓ At least one fix path returned rows. See logs above.")
            else:
                print("  ✗ All four hypotheses failed. Foreign stays PDF-canonical.")
                print("  This is the existing PLAN §3 fallback — no code change")
                print("  needed; foreign trades are already written by")
                print("  ingestion/trade_seeder with source='pdf-foreign'.")

        # --- Decision summary --------------------------------------------
        hr("DECISION SUMMARY")
        print("Path A is locked in for TW (Step 3 confirmed buy-leg recovery).")
        print("Remaining branches to confirm from THIS run:")
        print("  - Step 4b: which call shape returns the TW open lots?")
        print("    * default returned non-zero  → use that")
        print("    * chained per-position-id   → use that pattern")
        print("    * both still 0              → something else is wrong; investigate")
        print("  - Step 6: did the H account accept list_profit_loss + detail?")
        print("    * yes, with valid rows → SCOPE EXPANDS: foreign in the overlay")
        print("    * permission error / empty → foreign stays PDF-canonical")
        print("\nPaste this entire output back.")

    finally:
        try:
            api.logout()
        except Exception:
            pass


if __name__ == "__main__":
    main()
