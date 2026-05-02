"""TW 證券交易明細 trade-row parser.

Single-line regex parser for the four TW trade families:

  普買, 櫃買        →  cash buy   (Side.CASH_BUY,    8 columns)
  普賣, 櫃賣        →  cash sell  (Side.CASH_SELL,   9 columns; +tax)
  資買              →  margin buy (Side.MARGIN_BUY, 11 columns; +loan)
  資賣              →  margin sell(Side.MARGIN_SELL,12 columns; +loan,+collateral)

Each line in the trade table maps to at most one ParsedTwTrade. Lines
that do not match any of the four shapes return None — they are
column headers, page footers, holdings-table rows, or other non-trade
content that the caller skips.

Code resolution (trade name → ticker code) happens later in
statement_parser.py, after every month's holdings have been parsed
and the name_to_code map is built. Storing the raw name on
ParsedTwTrade defers that resolution to the orchestrator layer.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from invest.domain.trade import Side

_NUM = r"-?[\d,]+(?:\.\d+)?"
_DATE = r"\d{4}/\d{2}/\d{2}"

_TRADE_BUY_RE = re.compile(
    rf"^({_DATE})\s+(普買|櫃買)\s+(.+?)\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s*$"
)
_TRADE_SELL_RE = re.compile(
    rf"^({_DATE})\s+(普賣|櫃賣)\s+(.+?)\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s*$"
)
_TRADE_MARGIN_BUY_RE = re.compile(
    rf"^({_DATE})\s+(資買)\s+(.+?)\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_DATE})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s*$"
)
_TRADE_MARGIN_SELL_RE = re.compile(
    rf"^({_DATE})\s+(資賣)\s+(.+?)\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_DATE})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s*$"
)

_HOLDING_RE = re.compile(
    rf"^(現股|融資|融券)\s+(\S+)\s+(.+?)\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+(-?[\d.]+)%\s+({_NUM})\s+({_NUM})\s+(-?[\d.]+)%\s*$"
)
_SUBTOTAL_RE = re.compile(
    rf"^小計\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+({_NUM})\s+(-?[\d.]+)%"
)

_SIDE_MAP: dict[str, Side] = {
    "普買": Side.CASH_BUY,
    "櫃買": Side.CASH_BUY,
    "普賣": Side.CASH_SELL,
    "櫃賣": Side.CASH_SELL,
    "資買": Side.MARGIN_BUY,
    "資賣": Side.MARGIN_SELL,
}


@dataclass(frozen=True)
class ParsedTwRebate:
    """One nonzero rebate entry from the top of the statement.

    Sinopac prints two rebate lines (電子折讓金 / 一般折讓金); zero
    values are excluded by the parser so an empty tuple means
    'nothing rebated this month'.
    """

    type: str
    amount_twd: Decimal


@dataclass(frozen=True)
class ParsedTwHolding:
    """One row from the 證券庫存 (holdings) table.

    Percentages stored as fractions (2.35% → 0.0235); the format
    layer multiplies by 100 only at presentation time.

    'type' encodes direction (現股 cash long / 融資 margin long /
    融券 short). The same code appears at most once per type per
    statement.
    """

    type: str
    code: str
    name: str
    qty: int
    avg_cost: Decimal
    cost: Decimal
    ref_price: Decimal
    mkt_value: Decimal
    unrealized_pnl: Decimal
    unrealized_pct: Decimal
    cum_dividend: Decimal
    unrealized_pnl_with_div: Decimal
    unrealized_pct_with_div: Decimal


@dataclass(frozen=True)
class ParsedTwSubtotal:
    """The 小計 row at the bottom of the 證券庫存 table.

    The redundant percentage column from the PDF is dropped — it's
    derivable from the four raw values, and storing it would let it
    drift out of sync with them.
    """

    qty: int
    cost: Decimal
    mkt_value: Decimal
    unrealized_pnl: Decimal


@dataclass(frozen=True)
class ParsedTwTrade:
    """Trade-row intermediate. Carries the basics plus settlement-only
    fields the seeder/verifier may use for cashflow reconciliation.

    Fields default to zero/None on the families where they don't apply
    (e.g. tax=0 on margin buys, self_funded=0 on margin sells) so
    every parsed trade has a uniform shape regardless of family.
    """

    date: date
    name: str
    side: Side
    qty: int
    price: Decimal
    gross: Decimal
    fee: Decimal
    net_twd: Decimal
    tax: Decimal = field(default_factory=lambda: Decimal("0"))
    margin_loan: Decimal = field(default_factory=lambda: Decimal("0"))
    self_funded: Decimal = field(default_factory=lambda: Decimal("0"))
    collateral: Decimal = field(default_factory=lambda: Decimal("0"))
    interest_start: date | None = None


def _dec(s: str) -> Decimal:
    return Decimal(s.replace(",", ""))


def _date(s: str) -> date:
    y, m, d = s.split("/")
    return date(int(y), int(m), int(d))


def parse_tw_trade_line(line: str) -> ParsedTwTrade | None:
    """Parse a single trade-table row. Returns None for non-trade lines."""
    if not line or not line.strip():
        return None

    m = _TRADE_BUY_RE.match(line)
    if m:
        gross = _dec(m.group(6))
        fee = _dec(m.group(7))
        owed = _dec(m.group(8))  # 客戶應付
        return ParsedTwTrade(
            date=_date(m.group(1)),
            name=m.group(3).strip(),
            side=_SIDE_MAP[m.group(2)],
            qty=int(_dec(m.group(4))),
            price=_dec(m.group(5)),
            gross=gross,
            fee=fee,
            net_twd=-owed,  # client pays out
        )

    m = _TRADE_SELL_RE.match(line)
    if m:
        return ParsedTwTrade(
            date=_date(m.group(1)),
            name=m.group(3).strip(),
            side=_SIDE_MAP[m.group(2)],
            qty=int(_dec(m.group(4))),
            price=_dec(m.group(5)),
            gross=_dec(m.group(6)),
            fee=_dec(m.group(7)),
            tax=_dec(m.group(8)),
            net_twd=_dec(m.group(9)),  # client receives 客戶應收
        )

    m = _TRADE_MARGIN_BUY_RE.match(line)
    if m:
        owed = _dec(m.group(11))  # 客戶應付
        return ParsedTwTrade(
            date=_date(m.group(1)),
            name=m.group(3).strip(),
            side=_SIDE_MAP[m.group(2)],
            qty=int(_dec(m.group(4))),
            price=_dec(m.group(5)),
            gross=_dec(m.group(6)),
            fee=_dec(m.group(7)),
            interest_start=_date(m.group(8)),
            margin_loan=_dec(m.group(9)),
            self_funded=_dec(m.group(10)),
            net_twd=-owed,
        )

    m = _TRADE_MARGIN_SELL_RE.match(line)
    if m:
        return ParsedTwTrade(
            date=_date(m.group(1)),
            name=m.group(3).strip(),
            side=_SIDE_MAP[m.group(2)],
            qty=int(_dec(m.group(4))),
            price=_dec(m.group(5)),
            gross=_dec(m.group(6)),
            fee=_dec(m.group(7)),
            tax=_dec(m.group(8)),
            interest_start=_date(m.group(9)),
            margin_loan=_dec(m.group(10)),
            collateral=_dec(m.group(11)),
            net_twd=_dec(m.group(12)),  # 客戶應收
        )

    return None


def parse_tw_holding_row(line: str) -> ParsedTwHolding | None:
    """Parse one row from the 證券庫存 (holdings) table. None if no match."""
    if not line or not line.strip():
        return None
    m = _HOLDING_RE.match(line.strip())
    if not m:
        return None
    return ParsedTwHolding(
        type=m.group(1),
        code=m.group(2),
        name=m.group(3).strip(),
        qty=int(_dec(m.group(4))),
        avg_cost=_dec(m.group(5)),
        cost=_dec(m.group(6)),
        ref_price=_dec(m.group(7)),
        mkt_value=_dec(m.group(8)),
        unrealized_pnl=_dec(m.group(9)),
        unrealized_pct=Decimal(m.group(10)) / Decimal("100"),
        cum_dividend=_dec(m.group(11)),
        unrealized_pnl_with_div=_dec(m.group(12)),
        unrealized_pct_with_div=Decimal(m.group(13)) / Decimal("100"),
    )


def parse_tw_subtotal_row(line: str) -> ParsedTwSubtotal | None:
    """Parse the 小計 row at the bottom of the holdings table."""
    if not line or not line.strip():
        return None
    m = _SUBTOTAL_RE.match(line.strip())
    if not m:
        return None
    return ParsedTwSubtotal(
        qty=int(_dec(m.group(1))),
        cost=_dec(m.group(2)),
        mkt_value=_dec(m.group(3)),
        unrealized_pnl=_dec(m.group(4)),
    )


@dataclass(frozen=True)
class ParsedSecuritiesStatement:
    """Full TW 證券月對帳單, post-parse.

    Section tuples are immutable; the aggregate is frozen. Consumers
    that need to filter/transform should build new collections rather
    than mutate.
    """

    month: str
    holdings: tuple[ParsedTwHolding, ...]
    subtotal: ParsedTwSubtotal | None
    trades: tuple[ParsedTwTrade, ...]
    rebates: tuple[ParsedTwRebate, ...]
    net_cashflow_twd: Decimal


_PERIOD_RE = re.compile(r"成交年月：(\d{6})")
_REBATE_RE = re.compile(rf"^(電子折讓金|一般折讓金)：\s*({_NUM})")
_NET_CASHFLOW_RE = re.compile(rf"客戶淨收付：\s*幣別：臺幣\s*({_NUM})")


def parse_securities_text(text: str) -> ParsedSecuritiesStatement:
    """Parse a full TW 證券月對帳單 from extracted text.

    Raises ValueError if 成交年月 (period) cannot be extracted — the
    period is the canonical month key downstream; missing it would
    let unrelated months merge into a single bucket.
    """
    period_m = _PERIOD_RE.search(text)
    if not period_m:
        raise ValueError("Missing 成交年月 (period) in TW securities statement")
    period = period_m.group(1)
    month = f"{period[:4]}-{period[4:]}"

    rebates: list[ParsedTwRebate] = []
    for line in text.splitlines():
        rm = _REBATE_RE.match(line.strip())
        if rm:
            amt = _dec(rm.group(2))
            if amt != 0:
                rebates.append(ParsedTwRebate(type=rm.group(1), amount_twd=amt))

    holdings: list[ParsedTwHolding] = []
    trades: list[ParsedTwTrade] = []
    subtotal: ParsedTwSubtotal | None = None

    in_holdings = False
    in_trades = False

    for raw in text.splitlines():
        line = raw.strip()

        if "證券庫存" in line and "證券交易明細" not in line:
            in_holdings, in_trades = True, False
            continue
        if "證券交易明細" in line:
            in_holdings, in_trades = False, True
            continue
        if "客戶淨收付" in line or "電子折讓金額明細" in line:
            in_holdings = in_trades = False

        if in_holdings:
            if h := parse_tw_holding_row(line):
                holdings.append(h)
                continue
            if s := parse_tw_subtotal_row(line):
                subtotal = s
                continue

        if in_trades:
            if t := parse_tw_trade_line(line):
                trades.append(t)

    cf_m = _NET_CASHFLOW_RE.search(text)
    net_cashflow = _dec(cf_m.group(1)) if cf_m else Decimal("0")

    return ParsedSecuritiesStatement(
        month=month,
        holdings=tuple(holdings),
        subtotal=subtotal,
        trades=tuple(trades),
        rebates=tuple(rebates),
        net_cashflow_twd=net_cashflow,
    )
