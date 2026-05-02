"""Reproducer for invest.ingestion.bank_parser.

Three pure layers exposed:

  categorize(summary)     -> str
      9-bucket keyword classifier: tw_dividend / foreign_dividend /
      stock_settle_tw / stock_settle_fx / rebate / fx_convert /
      salary / interest / transfer / other.

  parse_bank_tx_line(line) -> Optional[ParsedBankTx]
      Date + summary + first two money tokens (amount, balance) +
      trailing memo. The money-token regex deliberately rejects long
      digit strings (account numbers, stock codes) so they don't
      accidentally fill amount/balance.

  parse_bank_text(text)    -> ParsedBankStatement
      Full statement: 對帳單期間 period + FX rates from the cross-
      currency table + balance triple (total/TWD/foreign cash) +
      per-account tx lists. Account routing keyed off
      '帳號:NNN(新臺幣)' / '帳號:NNN(美元)' headers. Sign inferred
      from balance delta (balance went up → credit; down → debit).
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from invest.ingestion.bank_parser import (
    ParsedBankStatement,
    ParsedBankTx,
    categorize,
    parse_bank_text,
    parse_bank_tx_line,
)


# --- categorize ----------------------------------------------------------


class TestCategorize:
    def test_ach_dividend_tw(self):
        """ACH (case-insensitive) marks TW listed-stock dividends
        from 集保. Distinct from foreign dividends."""
        assert categorize("ACH股息") == "tw_dividend"
        assert categorize("ach 股息") == "tw_dividend"  # case insensitive

    def test_foreign_dividend(self):
        assert categorize("國外股息") == "foreign_dividend"
        assert categorize("海外股息") == "foreign_dividend"

    def test_stock_settle_tw(self):
        assert categorize("股票款") == "stock_settle_tw"
        assert categorize("預扣股款") == "stock_settle_tw"

    def test_stock_settle_fx(self):
        """Foreign-broker settlement on the USD account."""
        assert categorize("股款交割") == "stock_settle_fx"

    def test_rebate(self):
        assert categorize("折讓款") == "rebate"

    def test_fx_convert(self):
        assert categorize("手機換匯") == "fx_convert"

    def test_salary(self):
        assert categorize("薪資轉入") == "salary"

    def test_interest(self):
        assert categorize("利息存入") == "interest"

    def test_transfer(self):
        assert categorize("手機轉帳") == "transfer"
        assert categorize("跨行轉帳") == "transfer"

    def test_bare_transfer_keyword_is_stock_settlement(self):
        """INVARIANT: a bare '轉帳' (no other keyword) is the
        bank's edge-case label for refunds of 預扣股款 — treat as
        stock settlement, NOT a peer transfer. The legacy code's
        most-specific-rule-last ordering relies on this."""
        assert categorize("轉帳") == "stock_settle_tw"

    def test_unknown_falls_through_to_other(self):
        assert categorize("未知摘要") == "other"
        assert categorize("") == "other"


# --- parse_bank_tx_line --------------------------------------------------


class TestParseBankTxLine:
    def test_minimal_tx(self):
        line = "2024/03/15 股票款 100,000 1,500,000"
        out = parse_bank_tx_line(line)
        assert out is not None
        assert out.date == date(2024, 3, 15)
        assert out.summary == "股票款"
        assert out.amount == Decimal("100000")
        assert out.balance == Decimal("1500000")
        assert out.memo == ""

    def test_with_memo(self):
        line = "2024/03/15 股票款 100,000 1,500,000 證券交割"
        out = parse_bank_tx_line(line)
        assert out is not None
        assert out.memo == "證券交割"

    def test_long_digit_string_in_memo_excluded_from_amount_walk(self):
        """INVARIANT: account numbers (10+ digits) and stock codes
        must NOT be picked up as amount/balance. The MONEY_TOKEN
        regex requires comma-grouped form for >7-digit numbers, so
        '1234567890' (raw 10 digits) is rejected."""
        line = "2024/03/15 跨行轉帳 50,000 1,000,000 帳號 1234567890"
        out = parse_bank_tx_line(line)
        assert out is not None
        assert out.amount == Decimal("50000")
        assert out.balance == Decimal("1000000")

    def test_decimal_amount(self):
        """USD account txs print decimal amounts (e.g. 1,234.56)."""
        line = "2024/03/15 股款交割 1,234.56 9,876.54"
        out = parse_bank_tx_line(line)
        assert out is not None
        assert out.amount == Decimal("1234.56")
        assert out.balance == Decimal("9876.54")

    def test_no_date_returns_none(self):
        assert parse_bank_tx_line("just some text") is None

    def test_one_amount_only_returns_none(self):
        """A line with only one money token isn't a valid tx (we need
        both amount and balance). Skip rather than guess."""
        line = "2024/03/15 摘要 1,000"
        assert parse_bank_tx_line(line) is None

    def test_empty_returns_none(self):
        assert parse_bank_tx_line("") is None


# --- parse_bank_text -----------------------------------------------------


_BANK_FULL = """\
永豐銀行 綜合對帳單
對帳單期間：2024/03/01-2024/03/31

存款 1,500,000
臺幣 1,000,000
外幣 500,000

匯率表
USD(USD) 1.0000 31.50 1.0000
HKD(HKD) 1.0000 4.05 1.0000

帳號:001234567890(新臺幣)
交易日 摘要 支出/存入 餘額 備註
2024/03/01 薪資轉入 100,000 1,100,000
2024/03/15 股票款 50,000 1,050,000 證券交割

帳號:001234567899(美元)
交易日 摘要 支出/存入 餘額 備註
2024/03/10 股款交割 1,000.00 9,000.00
2024/03/20 國外股息 50.00 9,050.00 NVDA
"""


class TestFullBankStatement:
    def test_parses_period(self):
        out = parse_bank_text(_BANK_FULL)
        assert out.month == "2024-03"

    def test_parses_balances(self):
        out = parse_bank_text(_BANK_FULL)
        assert out.cash_total_twd == Decimal("1500000")
        assert out.cash_twd == Decimal("1000000")
        assert out.cash_foreign_twd == Decimal("500000")

    def test_parses_fx_rates(self):
        out = parse_bank_text(_BANK_FULL)
        # FX table format: middle column is TWD rate.
        assert out.fx_rates["USD"] == Decimal("31.50")
        assert out.fx_rates["HKD"] == Decimal("4.05")

    def test_routes_twd_account_txs(self):
        out = parse_bank_text(_BANK_FULL)
        assert len(out.tx_twd) == 2
        salary, settle = out.tx_twd
        assert salary.summary == "薪資轉入"
        assert salary.ccy == "TWD"
        assert salary.category == "salary"
        # Balance went UP from prev (None → 1,100,000): credit, +amount.
        assert salary.signed_amount == Decimal("100000")

        assert settle.summary == "股票款"
        # Balance went DOWN: 1,100,000 → 1,050,000: debit, -amount.
        assert settle.signed_amount == Decimal("-50000")
        assert settle.category == "stock_settle_tw"

    def test_routes_usd_account_txs(self):
        out = parse_bank_text(_BANK_FULL)
        assert len(out.tx_foreign) == 2
        for tx in out.tx_foreign:
            assert tx.ccy == "USD"

    def test_balance_delta_sign_inference(self):
        """INVARIANT: amount sign comes from the balance delta, not
        from a +/- in the amount column. The bank's '支出/存入'
        column is unsigned; we infer direction by comparing the
        new balance to the previous one in the same account."""
        out = parse_bank_text(_BANK_FULL)
        # USD: prev=None, then 9000 (deposit, no prior known →
        # heuristic: stock_settle_fx has 股款交割 keyword which is
        # in the outflow heuristic list, so first tx is signed -)
        first, second = out.tx_foreign
        assert first.signed_amount == Decimal("-1000.00")  # 股款交割 outflow
        # Second: prev=9000, new=9050: balance UP → credit +50
        assert second.signed_amount == Decimal("50.00")
        assert second.category == "foreign_dividend"

    def test_returns_immutable_tuples(self):
        out = parse_bank_text(_BANK_FULL)
        assert isinstance(out.tx_twd, tuple)
        assert isinstance(out.tx_foreign, tuple)


class TestEmpty:
    def test_period_only(self):
        text = "對帳單期間：2024/03/01-2024/03/31\n"
        out = parse_bank_text(text)
        assert out.month == "2024-03"
        assert out.tx_twd == ()
        assert out.tx_foreign == ()

    def test_missing_period_raises(self):
        with pytest.raises(ValueError):
            parse_bank_text("no period here\n")


class TestDataclassShapes:
    def test_tx_frozen(self):
        t = ParsedBankTx(
            date=date(2024, 1, 1),
            summary="x",
            amount=Decimal("1"),
            balance=Decimal("1"),
            memo="",
            signed_amount=Decimal("1"),
            category="other",
            ccy="TWD",
        )
        try:
            t.amount = Decimal("2")  # type: ignore[misc]
        except Exception:
            return
        raise AssertionError("ParsedBankTx must be frozen")

    def test_statement_frozen(self):
        out = parse_bank_text("對帳單期間：2024/03/01-2024/03/31\n")
        try:
            out.month = "X"  # type: ignore[misc]
        except Exception:
            return
        raise AssertionError("ParsedBankStatement must be frozen")
