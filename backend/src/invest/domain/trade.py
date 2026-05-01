from enum import IntEnum


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
