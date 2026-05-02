from dataclasses import dataclass
from datetime import date as _date
from enum import IntEnum, StrEnum
from typing import Optional

from invest.domain.money import Money


class Side(IntEnum):
    """Trade side encoding direction + credit type.

    Numeric layout reserves decimal blocks per credit family so new
    variants slot in without renumbering existing values:
      1-9   : 普 cash equity
      10-19 : 資 margin  (融資)
      20-29 : 券 short   (融券)
      30-39 : reserved (e.g. 借券 SBL)
      100+  : reserved (futures, options, FX)
    """

    CASH_BUY = 1       # 普買
    CASH_SELL = 2      # 普賣
    MARGIN_BUY = 11    # 資買
    MARGIN_SELL = 12   # 資賣
    SHORT_SELL = 21    # 券賣 — opens a short
    SHORT_COVER = 22   # 券買 — covers a short

    @property
    def is_buy(self) -> bool:
        # Direction-of-flow is buy when the trader is acquiring shares:
        # ordinary buy, margin buy, or covering a short.
        return self in (Side.CASH_BUY, Side.MARGIN_BUY, Side.SHORT_COVER)

    @property
    def is_sell(self) -> bool:
        return not self.is_buy


class Venue(StrEnum):
    TW = "TW"
    US = "US"
    HK = "HK"
    JP = "JP"


@dataclass(frozen=True)
class Trade:
    """Pure domain VO; no persistence concerns.

    Maps to/from invest.persistence.models.trade.Trade via a converter
    at the persistence boundary. Analytics consume this form only.
    """

    date: _date
    code: str
    side: Side
    qty: int
    price: Money
    venue: Venue
    fee: Optional[Money] = None
    tax: Optional[Money] = None
    rebate: Optional[Money] = None

    def gross_value(self) -> Money:
        """qty * price; always positive (direction lives in `side`)."""
        return self.price * self.qty
