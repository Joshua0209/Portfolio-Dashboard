"""invest.analytics — pure-function analytics layer.

Public API re-exports.  Import from here to avoid deep module paths:

    from invest.analytics import modified_dietz, twr_chain, xirr, sharpe, ...
"""
from invest.analytics.attribution import fx_attribution
from invest.analytics.concentration import hhi, top_n_share
from invest.analytics.drawdown import max_drawdown, underwater_curve
from invest.analytics.ratios import calmar, sharpe, sortino
from invest.analytics.sectors import sector_breakdown, sector_of
from invest.analytics.tax_pnl import (
    build_positions,
    realized_pnl_per_position,
    unrealized_pnl_per_position,
)
from invest.analytics.twr import modified_dietz, twr_chain
from invest.analytics.xirr import xirr

__all__ = [
    # attribution
    "fx_attribution",
    # concentration
    "hhi",
    "top_n_share",
    # drawdown
    "max_drawdown",
    "underwater_curve",
    # ratios
    "sharpe",
    "sortino",
    "calmar",
    # sectors
    "sector_of",
    "sector_breakdown",
    # tax_pnl
    "build_positions",
    "realized_pnl_per_position",
    "unrealized_pnl_per_position",
    # twr
    "modified_dietz",
    "twr_chain",
    # xirr
    "xirr",
]
