import logging
import os
from decimal import Decimal
from typing import Dict, List

from pydantic import Field

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import MarketDict, OrderType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase


class OrderbookBalanceConfig(StrategyV2ConfigBase):
    script_file_name: str = os.path.basename(__file__)
    controllers_config: List[str] = []

    exchange: str = Field("exchange_sim")
    trading_pair: str = Field("BTC-USDT")

    # Reference price used when the orderbook has no data yet
    ref_price: Decimal = Field(Decimal("1000"))

    # Per-level base order size
    order_amount: Decimal = Field(Decimal("1.0"))
    # Additional size added per level (level 1 = order_amount, level 2 = order_amount + order_level_amount, ...)
    order_level_amount: Decimal = Field(Decimal("0.3"))
    # Number of order levels on each side
    order_levels: int = Field(10)
    # Spread between levels as a fraction (e.g. 0.0015 = 0.15%)
    order_level_spread: Decimal = Field(Decimal("0.0015"))
    # First-level distance from mid as a fraction (e.g. 0.0012 = 0.12%)
    bid_spread: Decimal = Field(Decimal("0.0012"))
    ask_spread: Decimal = Field(Decimal("0.0012"))

    # How aggressively to skew orders toward the lighter side.
    # 0 = symmetric (no rebalancing), 1 = fully skew (0 orders on heavy side when completely one-sided).
    balance_factor: Decimal = Field(Decimal("1.0"))

    # How often to cancel and refresh orders (seconds)
    order_refresh_time: int = Field(5)

    def update_markets(self, markets: MarketDict) -> MarketDict:
        markets[self.exchange] = markets.get(self.exchange, set()) | {self.trading_pair}
        return markets


class OrderbookBalance(StrategyV2Base):
    """
    Orderbook-balancing market maker for exchange_sim.

    On every refresh it:
    1. Reads the current orderbook to measure total bid volume vs ask volume.
    2. Computes a skew value: +1 means all volume is on the bid side (asks are empty),
       -1 means all volume is on the ask side (bids are empty), 0 means perfectly balanced.
    3. Scales each side's order amount by (1 ∓ skew * balance_factor) so that the
       lighter side receives proportionally more volume.
    4. Cancels all existing orders and places the new proposal.
    """

    create_timestamp = 0

    def __init__(self, connectors: Dict[str, ConnectorBase], config: OrderbookBalanceConfig):
        super().__init__(connectors, config)
        self.config = config

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def on_tick(self):
        if self.create_timestamp <= self.current_timestamp:
            self.cancel_all_orders()
            proposal = self.create_proposal()
            adjusted = self.adjust_proposal_to_budget(proposal)
            self.place_orders(adjusted)
            self.create_timestamp = self.current_timestamp + self.config.order_refresh_time

    # ------------------------------------------------------------------
    # Proposal
    # ------------------------------------------------------------------

    def create_proposal(self) -> List[OrderCandidate]:
        connector = self.connectors[self.config.exchange]

        # --- 1. Measure orderbook imbalance ---
        try:
            ob = connector.get_order_book(self.config.trading_pair)
            bid_vol = Decimal(str(sum(row.amount for row in ob.bid_entries())))
            ask_vol = Decimal(str(sum(row.amount for row in ob.ask_entries())))
        except Exception:
            bid_vol = Decimal("0")
            ask_vol = Decimal("0")

        total_vol = bid_vol + ask_vol
        # skew in [-1, +1]: positive → bids are heavier → place more asks
        if total_vol > 0:
            skew = (bid_vol - ask_vol) / total_vol
        else:
            skew = Decimal("0")

        # --- 2. Determine reference (mid) price ---
        ref_price = self._get_ref_price(connector)
        if ref_price is None or ref_price <= 0:
            self.log_with_clock(logging.WARNING, "No reference price available, skipping tick.")
            return []

        # --- 3. Build per-level orders ---
        orders: List[OrderCandidate] = []
        bf = self.config.balance_factor

        for level in range(self.config.order_levels):
            level_size = self.config.order_amount + self.config.order_level_amount * level

            # skew > 0 → bids heavy → boost ask side, reduce bid side
            buy_mult = max(Decimal("0"), 1 - skew * bf)
            sell_mult = max(Decimal("0"), 1 + skew * bf)

            buy_amount = level_size * buy_mult
            sell_amount = level_size * sell_mult

            bid_dist = self.config.bid_spread + self.config.order_level_spread * level
            ask_dist = self.config.ask_spread + self.config.order_level_spread * level

            buy_price = ref_price * (1 - bid_dist)
            sell_price = ref_price * (1 + ask_dist)

            if buy_amount > 0:
                orders.append(OrderCandidate(
                    trading_pair=self.config.trading_pair,
                    is_maker=True,
                    order_type=OrderType.LIMIT,
                    order_side=TradeType.BUY,
                    amount=buy_amount,
                    price=buy_price,
                ))
            if sell_amount > 0:
                orders.append(OrderCandidate(
                    trading_pair=self.config.trading_pair,
                    is_maker=True,
                    order_type=OrderType.LIMIT,
                    order_side=TradeType.SELL,
                    amount=sell_amount,
                    price=sell_price,
                ))

        bid_pct = float(bid_vol / total_vol * 100) if total_vol > 0 else 50
        self.log_with_clock(
            logging.INFO,
            f"OB imbalance — bids {bid_vol:.2f} ({bid_pct:.1f}%) / asks {ask_vol:.2f} ({100 - bid_pct:.1f}%) | "
            f"skew={float(skew):.3f} | ref={ref_price:.4f} | "
            f"buy_mult={float(buy_mult):.2f} sell_mult={float(sell_mult):.2f}"
        )

        return orders

    def _get_ref_price(self, connector) -> Decimal:
        """Return mid-price, falling back to configured ref_price."""
        try:
            bid = connector.get_price(self.config.trading_pair, False)
            ask = connector.get_price(self.config.trading_pair, True)
            if bid and ask and not bid.is_nan() and not ask.is_nan() and bid > 0 and ask > 0:
                return (bid + ask) / 2
        except Exception:
            pass
        return self.config.ref_price

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def adjust_proposal_to_budget(self, proposal: List[OrderCandidate]) -> List[OrderCandidate]:
        return self.connectors[self.config.exchange].budget_checker.adjust_candidates(
            proposal, all_or_none=False
        )

    def place_orders(self, proposal: List[OrderCandidate]) -> None:
        for order in proposal:
            if order.amount <= 0:
                continue
            if order.order_side == TradeType.BUY:
                self.buy(
                    connector_name=self.config.exchange,
                    trading_pair=order.trading_pair,
                    amount=order.amount,
                    order_type=order.order_type,
                    price=order.price,
                )
            else:
                self.sell(
                    connector_name=self.config.exchange,
                    trading_pair=order.trading_pair,
                    amount=order.amount,
                    order_type=order.order_type,
                    price=order.price,
                )

    def cancel_all_orders(self):
        for order in self.get_active_orders(connector_name=self.config.exchange):
            self.cancel(self.config.exchange, order.trading_pair, order.client_order_id)

    def did_fill_order(self, event: OrderFilledEvent):
        msg = (
            f"{event.trade_type.name} {round(event.amount, 4)} {event.trading_pair} "
            f"@ {round(event.price, 4)}"
        )
        self.log_with_clock(logging.INFO, msg)
        self.notify_hb_app_with_timestamp(msg)
