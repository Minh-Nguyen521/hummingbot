from decimal import Decimal
from typing import Dict

from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.perpetual_market_making.data_types import PriceSize, Proposal  # noqa: F401 re-exported


class LegState:
    """Mutable per-leg timing and exit-order state for HedgeMMStrategy."""

    __slots__ = (
        "market_info",
        "is_long",
        "account_role",
        "create_ts",
        "cancel_ts",
        "entry_orders",
        "exit_orders",
        "next_close_ts",
    )

    def __init__(self, market_info: MarketTradingPairTuple, is_long: bool, account_role: str = "default"):
        self.market_info: MarketTradingPairTuple = market_info
        self.is_long: bool = is_long
        self.account_role: str = account_role
        self.create_ts: float = 0.0
        self.cancel_ts: float = 0.0
        # Maps client_order_id → creation timestamp for OPEN (entry) orders on this leg.
        self.entry_orders: Dict[str, float] = {}
        # Maps client_order_id → creation timestamp for CLOSE (exit) orders on this leg.
        self.exit_orders: Dict[str, float] = {}
        # Rate-limit timestamp: don't emit another close order before this time.
        self.next_close_ts: float = 0.0
