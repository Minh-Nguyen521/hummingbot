import logging
from collections import defaultdict
from decimal import Decimal
from itertools import chain
from typing import Dict, List, Optional

import pandas as pd

from hummingbot.connector.derivative.position import Position
from hummingbot.connector.derivative_base import DerivativeBase
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.common import (
    OrderType,
    PositionAction,
    PositionMode,
    TradeType,
)
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.order_candidate import PerpetualOrderCandidate
from hummingbot.core.event.events import BuyOrderCompletedEvent, PositionModeChangeEvent, SellOrderCompletedEvent
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.perpetual_market_making.data_types import PriceSize, Proposal
from hummingbot.strategy.perpetual_market_making.perpetual_market_making_order_tracker import (
    PerpetualMarketMakingOrderTracker,
)
from hummingbot.strategy.strategy_py_base import StrategyPyBase
from hummingbot.strategy.hedge_mm.data_types import LegState

NaN = float("nan")
s_decimal_zero = Decimal(0)
s_decimal_neg_one = Decimal(-1)


class HedgeMMStrategy(StrategyPyBase):
    """Dual-subaccount perpetual market-making strategy with inventory rebalancing.

    One subaccount (long leg) only opens long positions via buy limit orders.
    Another subaccount (short leg) only opens short positions via sell limit orders.
    Each leg independently manages its own profit-taking and stop-loss close orders.

    When ``|long_qty - short_qty| > rebalance_size_diff_threshold`` and the heavier
    side has reached ``rebalance_target_pnl_pct`` unrealized PnL, a reduce-only close
    order is emitted on the heavy leg ahead of normal profit-taking schedule.  While
    imbalanced, entry order sizes are scaled by ``rebalance_heavy_side_size_mult`` and
    ``rebalance_light_side_size_mult`` respectively so that MM flow naturally pushes
    toward balance.
    """

    OPTION_LOG_CREATE_ORDER = 1 << 3
    OPTION_LOG_MAKER_ORDER_FILLED = 1 << 4
    OPTION_LOG_STATUS_REPORT = 1 << 5
    OPTION_LOG_ALL = 0x7FFFFFFFFFFFFFFF
    _logger = None

    @classmethod
    def logger(cls):
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def init_params(
        self,
        market_info: MarketTradingPairTuple,
        bid_spread: Decimal,
        ask_spread: Decimal,
        minimum_spread: Decimal,
        order_amount: Decimal,
        order_levels: int,
        order_level_mode: str,
        order_level_spread: Decimal,
        order_level_price_step: Decimal,
        order_level_amount: Decimal,
        top_order_size_multiplier: Decimal,
        order_refresh_time: float,
        order_refresh_tolerance_pct: Decimal,
        filled_order_delay: float,
        long_profit_taking_spread: Decimal,
        short_profit_taking_spread: Decimal,
        stop_loss_spread: Decimal,
        stop_loss_slippage_buffer: Decimal,
        time_between_stop_loss_orders: float,
        leverage: int,
        rebalance_enabled: bool = True,
        rebalance_size_diff_threshold: Decimal = Decimal("0.5"),
        rebalance_target_pnl_pct: Decimal = Decimal("0.005"),
        rebalance_heavy_side_size_mult: Decimal = Decimal("0.5"),
        rebalance_light_side_size_mult: Decimal = Decimal("1.5"),
        news_pre_window_seconds: float = 2.0,
        news_price_shift_pct: Decimal = Decimal("0.001"),
        news_flow_window_seconds: float = 30.0,
        news_flow_skew_mult: Decimal = Decimal("2.0"),
        logging_options: int = OPTION_LOG_ALL,
        status_report_interval: float = 900,
        hb_app_notification: bool = False,
    ):
        self._sb_order_tracker = PerpetualMarketMakingOrderTracker()

        self._long_leg = LegState(market_info, is_long=True, account_role="long")
        self._short_leg = LegState(market_info, is_long=False, account_role="short")

        self._bid_spread = bid_spread
        self._ask_spread = ask_spread
        self._minimum_spread = minimum_spread
        self._order_amount = order_amount
        self._order_levels = order_levels
        self._order_level_mode = order_level_mode
        self._order_level_spread = order_level_spread
        self._order_level_price_step = order_level_price_step
        self._order_level_amount = order_level_amount
        self._top_order_size_multiplier = top_order_size_multiplier
        self._order_refresh_time = order_refresh_time
        self._order_refresh_tolerance_pct = order_refresh_tolerance_pct
        self._filled_order_delay = filled_order_delay
        self._long_profit_taking_spread = long_profit_taking_spread
        self._short_profit_taking_spread = short_profit_taking_spread
        self._stop_loss_spread = stop_loss_spread
        self._stop_loss_slippage_buffer = stop_loss_slippage_buffer
        self._time_between_stop_loss_orders = time_between_stop_loss_orders
        self._leverage = leverage
        self._rebalance_enabled = rebalance_enabled
        self._rebalance_size_diff_threshold = rebalance_size_diff_threshold
        self._rebalance_target_pnl_pct = rebalance_target_pnl_pct
        self._rebalance_heavy_side_size_mult = rebalance_heavy_side_size_mult
        self._rebalance_light_side_size_mult = rebalance_light_side_size_mult
        self._news_pre_window_seconds = news_pre_window_seconds
        self._news_price_shift_pct = news_price_shift_pct
        self._news_flow_window_seconds = news_flow_window_seconds
        self._news_flow_skew_mult = news_flow_skew_mult
        self._news_price_shift = Decimal("0")
        self._news_active_event_id: Optional[str] = None
        self._news_event_timestamp: float = 0.0
        self._news_flow_buy_qty = Decimal("0")
        self._news_flow_sell_qty = Decimal("0")
        self._news_long_skew = Decimal("1")
        self._news_short_skew = Decimal("1")
        self._logging_options = logging_options
        self._status_report_interval = status_report_interval
        self._hb_app_notification = hb_app_notification

        self._all_markets_ready = False
        self._last_timestamp = 0.0
        self._position_mode_ready = False
        self._position_mode_not_ready_counter = 0
        self._position_mode_success_count = 0

        self.add_markets([market_info.market])

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def trading_pair(self) -> str:
        return self._long_leg.market_info.trading_pair

    def _active_orders(self, leg: LegState) -> List[LimitOrder]:
        all_orders = self._sb_order_tracker.market_pair_to_active_orders.get(
            leg.market_info, []
        )
        known_ids = set(leg.entry_orders) | set(leg.exit_orders)
        return [o for o in all_orders if o.client_order_id in known_ids]

    def _get_position(self, leg: LegState) -> Optional[Position]:
        tp = leg.market_info.trading_pair
        connector = leg.market_info.market
        if hasattr(connector, "get_positions_for_role"):
            pos = connector.get_positions_for_role(leg.account_role).get(tp)
        else:
            pos = None
            for p in connector.account_positions.values():
                if p.trading_pair == tp:
                    pos = p
                    break
        if pos is None:
            return None
        # Ensure each leg only sees its own side to prevent cross-leg interference
        # when both legs share a single connector account.
        if leg.is_long and pos.amount <= 0:
            return None
        if not leg.is_long and pos.amount >= 0:
            return None
        return pos

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, clock: Clock, timestamp: float):
        market: DerivativeBase = self._long_leg.market_info.market
        tp = self._long_leg.market_info.trading_pair
        market.set_leverage(tp, self._leverage)
        market.set_position_mode(PositionMode.ONEWAY)
        if hasattr(market, "subscribe_to_news"):
            market.subscribe_to_news(self._on_news_event)

    def tick(self, timestamp: float):
        if not self._all_markets_ready:
            self._all_markets_ready = all(
                m.ready for m in self.active_markets
            )
            if not self._all_markets_ready:
                return

        if not self._position_mode_ready:
            self._position_mode_not_ready_counter += 1
            if self._position_mode_not_ready_counter >= 10:
                self._position_mode_success_count = 0
                market: DerivativeBase = self._long_leg.market_info.market
                tp = self._long_leg.market_info.trading_pair
                market.set_leverage(tp, self._leverage)
                market.set_position_mode(PositionMode.ONEWAY)
                self._position_mode_not_ready_counter = 0
            return
        self._position_mode_not_ready_counter = 0

        current_tick = timestamp // self._status_report_interval
        last_tick = self._last_timestamp // self._status_report_interval
        should_warn = (current_tick > last_tick) and (
            self._logging_options & self.OPTION_LOG_STATUS_REPORT
        )
        if should_warn:
            if not all(
                m.network_status is NetworkStatus.CONNECTED
                for m in self.active_markets
            ):
                self.logger().warning(
                    "WARNING: Some markets are not connected. Market making may be dangerous."
                )

        self._update_news_state(timestamp)

        long_pos = self._get_position(self._long_leg)
        short_pos = self._get_position(self._short_leg)

        long_qty = long_pos.amount if long_pos and long_pos.amount > 0 else Decimal("0")
        short_qty = abs(short_pos.amount) if short_pos and short_pos.amount < 0 else Decimal("0")
        diff = long_qty - short_qty  # positive = long is heavier

        self._process_leg(self._long_leg, long_pos, diff)
        self._process_leg(self._short_leg, short_pos, diff)

        self._last_timestamp = timestamp

    # ------------------------------------------------------------------
    # Per-leg processing
    # ------------------------------------------------------------------

    def _process_leg(self, leg: LegState, position: Optional[Position], diff: Decimal):
        active_orders = self._active_orders(leg)
        self._prune_order_tracking(leg, active_orders)
        active_entry_orders = [o for o in active_orders if o.client_order_id in leg.entry_orders]

        # Manage exits whenever a position is open (independent of entry order logic).
        if position is not None and not abs(position.amount).is_zero():
            self._manage_leg_exit(leg, position, diff)
        else:
            # No position — clear stale exit-order records.
            leg.exit_orders = {}

        # Entry order refresh — runs regardless of whether a position is open.
        if leg.cancel_ts > self.current_timestamp:
            return

        proposal = self._create_entry_proposal(leg)
        if proposal is None:
            return

        self._apply_rebalance_size_skew(proposal, leg, diff)
        self._apply_news_flow_skew(proposal, leg)
        self._apply_min_spread_filter(proposal, leg)

        defer = self._should_defer_cancel(leg, active_entry_orders, proposal)
        if defer:
            leg.create_ts = self.current_timestamp + self._order_refresh_time
            leg.cancel_ts = self.current_timestamp + self._order_refresh_time
        else:
            orders_to_cancel, proposal = self._reconcile_entry_orders(active_entry_orders, proposal)
            for o in orders_to_cancel:
                self.cancel_order(leg.market_info, o.client_order_id)
                leg.entry_orders.pop(o.client_order_id, None)

            if leg.create_ts <= self.current_timestamp:
                self._apply_budget_constraint(proposal, leg)
                self._filter_out_takers(proposal, leg)
                if proposal.buys or proposal.sells:
                    self._execute_entry_orders(proposal, leg)

    def _prune_order_tracking(self, leg: LegState, active_orders: List[LimitOrder]):
        active_ids = {o.client_order_id for o in active_orders}
        leg.entry_orders = {
            oid: ts for oid, ts in leg.entry_orders.items() if oid in active_ids
        }
        leg.exit_orders = {
            oid: ts for oid, ts in leg.exit_orders.items() if oid in active_ids
        }

    def _reconcile_entry_orders(
        self,
        active_entry_orders: List[LimitOrder],
        proposal: Proposal,
    ) -> (List[LimitOrder], Proposal):
        remaining_active_buys = [o for o in active_entry_orders if o.is_buy]
        remaining_active_sells = [o for o in active_entry_orders if not o.is_buy]
        proposal_buys = list(proposal.buys)
        proposal_sells = list(proposal.sells)

        proposal_buys = self._remove_matched_proposals(proposal_buys, remaining_active_buys)
        proposal_sells = self._remove_matched_proposals(proposal_sells, remaining_active_sells)

        orders_to_cancel = remaining_active_buys + remaining_active_sells
        remaining_proposal = Proposal(proposal_buys, proposal_sells)
        return orders_to_cancel, remaining_proposal

    @staticmethod
    def _remove_matched_proposals(
        proposal_orders: List[PriceSize], active_orders: List[LimitOrder]
    ) -> List[PriceSize]:
        remaining = []
        unmatched_active = list(active_orders)
        for candidate in proposal_orders:
            matched_order = None
            for order in unmatched_active:
                if Decimal(str(order.price)) == candidate.price and Decimal(str(order.quantity)) == candidate.size:
                    matched_order = order
                    break
            if matched_order is not None:
                unmatched_active.remove(matched_order)
            else:
                remaining.append(candidate)
        active_orders[:] = unmatched_active
        return remaining

    def _create_entry_proposal(self, leg: LegState) -> Optional[Proposal]:
        market: DerivativeBase = leg.market_info.market
        tp = leg.market_info.trading_pair
        ref = self._get_ref_price(leg)
        if ref.is_nan() or ref <= 0:
            return None

        buys: List[PriceSize] = self._build_distinct_levels(leg, ref, is_buy=True) if leg.is_long else []
        sells: List[PriceSize] = self._build_distinct_levels(leg, ref, is_buy=False) if not leg.is_long else []

        return Proposal(buys, sells)

    def _build_distinct_levels(self, leg: LegState, ref: Decimal, is_buy: bool) -> List[PriceSize]:
        market: DerivativeBase = leg.market_info.market
        tp = leg.market_info.trading_pair
        levels: List[PriceSize] = []
        seen_prices = set()
        level_index = 0
        max_attempts = max(self._order_levels * 20, self._order_levels + 10)

        while len(levels) < self._order_levels and level_index < max_attempts:
            raw_price = self._entry_price_for_level(leg, ref, level_index)
            price = market.quantize_order_price(tp, raw_price)
            size = market.quantize_order_amount(tp, self._size_for_level(level_index))
            level_index += 1

            if size <= 0 or price.is_nan() or price <= 0:
                continue
            if price in seen_prices:
                continue

            seen_prices.add(price)
            levels.append(PriceSize(price, size))

        if len(levels) < self._order_levels:
            side = "buy" if is_buy else "sell"
            self.logger().warning(
                f"Only generated {len(levels)} distinct {side} levels out of requested {self._order_levels}. "
                f"Check tick size and order spacing."
            )

        return levels

    def _entry_price_for_level(self, leg: LegState, ref: Decimal, level: int) -> Decimal:
        level_offset = Decimal(level)
        if self._order_level_mode == "absolute":
            if leg.is_long:
                base_price = ref * (Decimal("1") - self._bid_spread)
                return base_price - level_offset * self._order_level_price_step
            base_price = ref * (Decimal("1") + self._ask_spread)
            return base_price + level_offset * self._order_level_price_step

        if leg.is_long:
            return ref * (Decimal("1") - self._bid_spread - level_offset * self._order_level_spread)
        return ref * (Decimal("1") + self._ask_spread + level_offset * self._order_level_spread)

    def _size_for_level(self, level: int) -> Decimal:
        if self._order_levels <= 1:
            return self._order_amount
        # Clamp to (0, 1] so top_order_size_multiplier can only shrink the nearest
        # order, never boost it.  This enforces near-BBO = smallest, far-BBO = largest.
        near_mult = min(self._top_order_size_multiplier, Decimal("1"))
        level_progress = Decimal(level) / Decimal(self._order_levels - 1)
        base = self._order_amount * (near_mult + (Decimal("1") - near_mult) * level_progress)
        return base + Decimal(level) * self._order_level_amount

    def _get_ref_price(self, leg: LegState) -> Decimal:
        market: DerivativeBase = leg.market_info.market
        tp = leg.market_info.trading_pair
        bid = market.get_price(tp, False)
        ask = market.get_price(tp, True)
        if not bid.is_nan() and not ask.is_nan() and bid > 0 and ask > 0:
            ref = (bid + ask) / Decimal("2")
        elif not bid.is_nan() and bid > 0:
            ref = bid
        elif not ask.is_nan() and ask > 0:
            ref = ask
        elif hasattr(market, "_fallback_reference_price"):
            ref = market._fallback_reference_price(tp)
        else:
            return Decimal("nan")
        if self._news_price_shift != Decimal("0"):
            ref = ref * (Decimal("1") + self._news_price_shift)
        return ref

    def _apply_rebalance_size_skew(self, proposal: Proposal, leg: LegState, diff: Decimal):
        if not self._rebalance_enabled or abs(diff) <= self._rebalance_size_diff_threshold:
            return
        if diff > 0:  # long is heavier
            mult = self._rebalance_heavy_side_size_mult if leg.is_long else self._rebalance_light_side_size_mult
        else:  # short is heavier
            mult = self._rebalance_light_side_size_mult if leg.is_long else self._rebalance_heavy_side_size_mult

        market: DerivativeBase = leg.market_info.market
        tp = leg.market_info.trading_pair
        for order in proposal.buys + proposal.sells:
            order.size = market.quantize_order_amount(tp, order.size * mult)

    def _apply_news_flow_skew(self, proposal: Proposal, leg: LegState) -> None:
        """Scale entry order sizes based on post-news order flow imbalance."""
        mult = self._news_long_skew if leg.is_long else self._news_short_skew
        if mult == Decimal("1"):
            return
        market: DerivativeBase = leg.market_info.market
        tp = leg.market_info.trading_pair
        for order in proposal.buys + proposal.sells:
            order.size = market.quantize_order_amount(tp, order.size * mult)

    def _apply_min_spread_filter(self, proposal: Proposal, leg: LegState):
        if self._minimum_spread <= 0:
            return
        ref = self._get_ref_price(leg)
        if ref.is_nan() or ref <= 0:
            return
        proposal.buys = [
            b for b in proposal.buys
            if abs(b.price - ref) / ref >= self._minimum_spread
        ]
        proposal.sells = [
            s for s in proposal.sells
            if abs(s.price - ref) / ref >= self._minimum_spread
        ]

    def _apply_budget_constraint(self, proposal: Proposal, leg: LegState):
        market = leg.market_info.market
        checker = market.budget_checker
        tp = leg.market_info.trading_pair
        candidates = [
            PerpetualOrderCandidate(
                tp, True, OrderType.LIMIT, TradeType.BUY,
                b.size, b.price, leverage=Decimal(self._leverage),
            )
            for b in proposal.buys
        ] + [
            PerpetualOrderCandidate(
                tp, True, OrderType.LIMIT, TradeType.SELL,
                s.size, s.price, leverage=Decimal(self._leverage),
            )
            for s in proposal.sells
        ]
        checker.reset_locked_collateral()
        locked_collateral = defaultdict(lambda: Decimal("0"))
        adjusted = []
        for candidate in candidates:
            adjusted_candidate = checker.populate_collateral_entries(candidate)
            available_balances = {}
            for token in adjusted_candidate.collateral_dict:
                if hasattr(market, "get_available_balance_for_role"):
                    balance = market.get_available_balance_for_role(leg.account_role, token)
                else:
                    balance = market.get_available_balance(token)
                available_balances[token] = balance - locked_collateral[token]
            adjusted_candidate.adjust_from_balances(available_balances)
            if adjusted_candidate.resized:
                adjusted_candidate.set_to_zero()
            else:
                for token, amount in adjusted_candidate.collateral_dict.items():
                    locked_collateral[token] += amount
            adjusted.append(adjusted_candidate)
        checker.reset_locked_collateral()
        for order in chain(proposal.buys, proposal.sells):
            adj = adjusted.pop(0)
            order.size = adj.amount
        proposal.buys = [o for o in proposal.buys if o.size > 0]
        proposal.sells = [o for o in proposal.sells if o.size > 0]

    def _filter_out_takers(self, proposal: Proposal, leg: LegState):
        market: DerivativeBase = leg.market_info.market
        tp = leg.market_info.trading_pair
        ask = market.get_price(tp, True)
        if not ask.is_nan():
            proposal.buys = [b for b in proposal.buys if b.price < ask]
        bid = market.get_price(tp, False)
        if not bid.is_nan():
            proposal.sells = [s for s in proposal.sells if s.price > bid]

    def _execute_entry_orders(self, proposal: Proposal, leg: LegState):
        if self._logging_options & self.OPTION_LOG_CREATE_ORDER:
            side = "long" if leg.is_long else "short"
            self.logger().info(
                f"Creating {len(proposal.buys)} buy / {len(proposal.sells)} sell entry orders "
                f"on {side} leg ({leg.market_info.trading_pair})."
            )
        connector = leg.market_info.market
        for buy in proposal.buys:
            oid = self.buy_with_specific_market(
                leg.market_info, buy.size,
                order_type=OrderType.LIMIT, price=buy.price,
                position_action=PositionAction.OPEN,
            )
            if hasattr(connector, "register_order_role"):
                connector.register_order_role(oid, leg.account_role)
            leg.entry_orders[oid] = self.current_timestamp
        for sell in proposal.sells:
            oid = self.sell_with_specific_market(
                leg.market_info, sell.size,
                order_type=OrderType.LIMIT, price=sell.price,
                position_action=PositionAction.OPEN,
            )
            if hasattr(connector, "register_order_role"):
                connector.register_order_role(oid, leg.account_role)
            leg.entry_orders[oid] = self.current_timestamp
        leg.create_ts = self.current_timestamp + self._order_refresh_time
        leg.cancel_ts = self.current_timestamp + self._order_refresh_time

    # ------------------------------------------------------------------
    # News state management
    # ------------------------------------------------------------------

    def _on_news_event(self, event) -> None:
        """Fired by connector when a news event becomes public.

        Resets per-event fill counters so we can observe taker flow direction
        after the news is released and skew entry sizes toward the opposite side.
        """
        tp = self._long_leg.market_info.trading_pair
        if not event.affects_symbol(tp):
            return
        self._news_active_event_id = event.id
        self._news_event_timestamp = event.timestamp
        self._news_flow_buy_qty = Decimal("0")
        self._news_flow_sell_qty = Decimal("0")
        self._news_long_skew = Decimal("1")
        self._news_short_skew = Decimal("1")
        self.logger().info(
            f"[News] Event published: '{event.title}'  "
            f"sentiment={event.sentiment}  severity={event.severity}. "
            f"Tracking fill flow for {self._news_flow_window_seconds}s."
        )

    def _update_news_state(self, timestamp: float) -> None:
        """Called every tick to update price shift and flow skew from news."""
        market = self._long_leg.market_info.market
        tp = self._long_leg.market_info.trading_pair

        # Phase 1 — pre-news price shift: 2 s before event fires, move ref price
        # in the direction the news implies so we front-run the expected move.
        if hasattr(market, "get_active_news"):
            upcoming = market.get_active_news(
                symbols=[tp],
                before_seconds=self._news_pre_window_seconds,
                after_seconds=0,
            )
            pre_events = [e for e in upcoming if e.timestamp > timestamp]
            if pre_events:
                event = max(pre_events, key=lambda e: e.severity_rank)
                shift = self._news_price_shift_pct * Decimal(str(event.severity_rank))
                if event.sentiment == "positive":
                    self._news_price_shift = shift
                elif event.sentiment == "negative":
                    self._news_price_shift = -shift
                else:
                    self._news_price_shift = Decimal("0")
            else:
                self._news_price_shift = Decimal("0")
        else:
            self._news_price_shift = Decimal("0")

        # Phase 2 — post-news flow skew: observe which side fills more and
        # increase order sizes on the opposite side (go against taker flow).
        if self._news_active_event_id is None:
            return
        if timestamp > self._news_event_timestamp + self._news_flow_window_seconds:
            self.logger().info("[News] Flow observation window expired — resetting skew.")
            self._news_active_event_id = None
            self._news_long_skew = Decimal("1")
            self._news_short_skew = Decimal("1")
            return
        total = self._news_flow_buy_qty + self._news_flow_sell_qty
        if total <= 0:
            return
        max_mult = self._news_flow_skew_mult
        if self._news_flow_sell_qty > self._news_flow_buy_qty:
            # More sell-order fills → bearish taker flow → skew long (buy more)
            imbalance = (self._news_flow_sell_qty - self._news_flow_buy_qty) / total
            self._news_long_skew = Decimal("1") + imbalance * (max_mult - Decimal("1"))
            self._news_short_skew = Decimal("1")
        elif self._news_flow_buy_qty > self._news_flow_sell_qty:
            # More buy-order fills → bullish taker flow → skew short (sell more)
            imbalance = (self._news_flow_buy_qty - self._news_flow_sell_qty) / total
            self._news_long_skew = Decimal("1")
            self._news_short_skew = Decimal("1") + imbalance * (max_mult - Decimal("1"))
        else:
            self._news_long_skew = Decimal("1")
            self._news_short_skew = Decimal("1")

    # ------------------------------------------------------------------
    # Exit management
    # ------------------------------------------------------------------

    def _find_non_mm_bid(self, leg: LegState, active_orders: List[LimitOrder]) -> Optional[Decimal]:
        """Best bid in the orderbook that is NOT one of this leg's own MM entry prices.

        This is used for long profit-taking so the close sell order fills against a real
        noise-trader buyer rather than bouncing against the MM's own entry buy orders.
        """
        mm_prices = frozenset(
            Decimal(str(o.price)) for o in active_orders
            if o.is_buy and o.client_order_id in leg.entry_orders
        )
        try:
            ob = leg.market_info.market.get_order_book(leg.market_info.trading_pair)
            for level in ob.bid_entries():
                price = Decimal(str(level.price))
                if price not in mm_prices:
                    return price
        except Exception:
            pass
        return None

    def _find_non_mm_ask(self, leg: LegState, active_orders: List[LimitOrder]) -> Optional[Decimal]:
        """Best ask in the orderbook that is NOT one of this leg's own MM entry prices.

        Used for short profit-taking so the close buy fills against a real noise-trader
        seller rather than sitting at the MM's own ask level.
        """
        mm_prices = frozenset(
            Decimal(str(o.price)) for o in active_orders
            if not o.is_buy and o.client_order_id in leg.entry_orders
        )
        try:
            ob = leg.market_info.market.get_order_book(leg.market_info.trading_pair)
            for level in ob.ask_entries():
                price = Decimal(str(level.price))
                if price not in mm_prices:
                    return price
        except Exception:
            pass
        return None

    def _manage_leg_exit(self, leg: LegState, position: Position, diff: Decimal):
        market: DerivativeBase = leg.market_info.market
        tp = leg.market_info.trading_pair
        ask = market.get_price(tp, True)
        bid = market.get_price(tp, False)
        if ask.is_nan() or bid.is_nan():
            ref = self._get_ref_price(leg)
            if ref.is_nan():
                return
            if ask.is_nan():
                ask = ref
            if bid.is_nan():
                bid = ref

        active_orders = self._active_orders(leg)
        size = market.quantize_order_amount(tp, abs(position.amount))
        entry = position.entry_price

        if leg.is_long and position.amount > 0:
            self._manage_long_exit(leg, size, entry, ask, bid, diff, active_orders)
        elif not leg.is_long and position.amount < 0:
            self._manage_short_exit(leg, size, entry, ask, bid, diff, active_orders)

    def _manage_long_exit(
        self,
        leg: LegState,
        size: Decimal,
        entry: Decimal,
        ask: Decimal,
        bid: Decimal,
        diff: Decimal,
        active_orders: List[LimitOrder],
    ):
        market: DerivativeBase = leg.market_info.market
        tp = leg.market_info.trading_pair

        # Rebalance-triggered close takes priority when long is heavy and PnL target is met.
        # The close price is derived from rebalance_target_pnl_pct (not the normal TP spread)
        # so it is executable as soon as the threshold is reached regardless of TP spread size.
        if self._rebalance_enabled and diff > self._rebalance_size_diff_threshold:
            pnl_pct = (bid - entry) / entry if entry > 0 else Decimal("0")
            if pnl_pct >= self._rebalance_target_pnl_pct:
                rebalance_price = market.quantize_order_price(tp, bid)
                self._emit_close_sell(leg, size, rebalance_price, active_orders)
                return

        # Normal profit-taking: close at the best non-MM bid so the order fills against
        # a real noise-trader buyer rather than the MM's own entry orders.
        # Falls back to the BBO bid if no non-MM price is found.
        if self._long_profit_taking_spread > 0 and entry > 0:
            non_mm_bid = self._find_non_mm_bid(leg, active_orders)
            close_bid = non_mm_bid if non_mm_bid is not None else bid
            pnl_pct = (close_bid - entry) / entry
            if pnl_pct >= self._long_profit_taking_spread:
                tp_price = market.quantize_order_price(tp, close_bid)
                self._emit_close_sell(leg, size, tp_price, active_orders)

        # Stop-loss: bid has fallen to or below the stop price.
        if self._stop_loss_spread > 0:
            sl_price = entry * (Decimal("1") - self._stop_loss_spread)
            if bid <= sl_price:
                exec_price = market.quantize_order_price(
                    tp, sl_price * (Decimal("1") - self._stop_loss_slippage_buffer)
                )
                # Cancel any existing profit-take before placing stop-loss.
                for o in active_orders:
                    if not o.is_buy and o.client_order_id in leg.exit_orders:
                        self.cancel_order(leg.market_info, o.client_order_id)
                self._emit_close_sell(leg, size, exec_price, active_orders, is_stop_loss=True)

    def _manage_short_exit(
        self,
        leg: LegState,
        size: Decimal,
        entry: Decimal,
        ask: Decimal,
        bid: Decimal,
        diff: Decimal,
        active_orders: List[LimitOrder],
    ):
        market: DerivativeBase = leg.market_info.market
        tp = leg.market_info.trading_pair

        # Rebalance-triggered close: short is heavy and PnL target is met.
        # Close price uses rebalance_target_pnl_pct so it is immediately executable
        # regardless of how short_profit_taking_spread is configured.
        if self._rebalance_enabled and -diff > self._rebalance_size_diff_threshold:
            pnl_pct = (entry - ask) / entry if entry > 0 else Decimal("0")
            if pnl_pct >= self._rebalance_target_pnl_pct:
                rebalance_price = market.quantize_order_price(tp, ask)
                self._emit_close_buy(leg, size, rebalance_price, active_orders)
                return

        # Normal profit-taking: close at the best non-MM ask so the order fills against
        # a real noise-trader seller rather than the MM's own entry orders.
        # Falls back to the BBO ask if no non-MM price is found.
        if self._short_profit_taking_spread > 0 and entry > 0:
            non_mm_ask = self._find_non_mm_ask(leg, active_orders)
            close_ask = non_mm_ask if non_mm_ask is not None else ask
            pnl_pct = (entry - close_ask) / entry
            if pnl_pct >= self._short_profit_taking_spread:
                tp_price = market.quantize_order_price(tp, close_ask)
                self._emit_close_buy(leg, size, tp_price, active_orders)

        # Stop-loss: ask has risen to or above the stop price.
        if self._stop_loss_spread > 0:
            sl_price = entry * (Decimal("1") + self._stop_loss_spread)
            if ask >= sl_price:
                exec_price = market.quantize_order_price(
                    tp, sl_price * (Decimal("1") + self._stop_loss_slippage_buffer)
                )
                for o in active_orders:
                    if o.is_buy and o.client_order_id in leg.exit_orders:
                        self.cancel_order(leg.market_info, o.client_order_id)
                self._emit_close_buy(leg, size, exec_price, active_orders, is_stop_loss=True)

    def _emit_close_sell(
        self,
        leg: LegState,
        size: Decimal,
        price: Decimal,
        active_orders: List[LimitOrder],
        is_stop_loss: bool = False,
    ):
        """Place a reduce-only sell to close a long position, rate-limited."""
        if not is_stop_loss and self.current_timestamp < leg.next_close_ts:
            return
        existing = [
            o for o in active_orders
            if not o.is_buy and o.client_order_id in leg.exit_orders
        ]
        if existing and not is_stop_loss:
            o = existing[0]
            if o.price == price and o.quantity == size:
                return  # already correct
            self.cancel_order(leg.market_info, o.client_order_id)
        if size > 0 and price > 0:
            oid = self.sell_with_specific_market(
                leg.market_info, size,
                order_type=OrderType.LIMIT, price=price,
                position_action=PositionAction.CLOSE,
            )
            connector = leg.market_info.market
            if hasattr(connector, "register_order_role"):
                connector.register_order_role(oid, leg.account_role)
            leg.exit_orders[oid] = self.current_timestamp
            leg.next_close_ts = self.current_timestamp + self._time_between_stop_loss_orders

    def _emit_close_buy(
        self,
        leg: LegState,
        size: Decimal,
        price: Decimal,
        active_orders: List[LimitOrder],
        is_stop_loss: bool = False,
    ):
        """Place a reduce-only buy to close a short position, rate-limited."""
        if not is_stop_loss and self.current_timestamp < leg.next_close_ts:
            return
        existing = [
            o for o in active_orders
            if o.is_buy and o.client_order_id in leg.exit_orders
        ]
        if existing and not is_stop_loss:
            o = existing[0]
            if o.price == price and o.quantity == size:
                return
            self.cancel_order(leg.market_info, o.client_order_id)
        if size > 0 and price > 0:
            oid = self.buy_with_specific_market(
                leg.market_info, size,
                order_type=OrderType.LIMIT, price=price,
                position_action=PositionAction.CLOSE,
            )
            connector = leg.market_info.market
            if hasattr(connector, "register_order_role"):
                connector.register_order_role(oid, leg.account_role)
            leg.exit_orders[oid] = self.current_timestamp
            leg.next_close_ts = self.current_timestamp + self._time_between_stop_loss_orders

    # ------------------------------------------------------------------
    # Cancel helpers
    # ------------------------------------------------------------------

    def _should_defer_cancel(
        self,
        leg: LegState,
        active_orders: List[LimitOrder],
        proposal: Proposal,
    ) -> bool:
        if self._order_refresh_tolerance_pct < 0 or not active_orders:
            return False
        active_buys = [Decimal(str(o.price)) for o in active_orders if o.is_buy]
        active_sells = [Decimal(str(o.price)) for o in active_orders if not o.is_buy]
        return self._within_tolerance(active_buys, [b.price for b in proposal.buys]) and \
               self._within_tolerance(active_sells, [s.price for s in proposal.sells])

    def _within_tolerance(self, current: List[Decimal], proposal: List[Decimal]) -> bool:
        if len(current) != len(proposal):
            return False
        for c, p in zip(sorted(current), sorted(proposal)):
            if abs(p - c) / c > self._order_refresh_tolerance_pct:
                return False
        return True

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    def did_change_position_mode_succeed(self, event: PositionModeChangeEvent):
        if event.position_mode is PositionMode.ONEWAY:
            self._position_mode_success_count = min(
                self._position_mode_success_count + 1,
                len(self.active_markets),
            )
            self._position_mode_ready = (
                self._position_mode_success_count >= len(self.active_markets)
            )
            self.logger().info(
                f"Position mode changed to ONEWAY successfully "
                f"({self._position_mode_success_count}/{len(self.active_markets)} acknowledgements)."
            )
        else:
            self.logger().warning(
                f"Position mode changed to {event.position_mode.name}, expected ONEWAY."
            )
            self._position_mode_ready = False
            self._position_mode_success_count = 0

    def did_change_position_mode_fail(self, event: PositionModeChangeEvent):
        self.logger().error(
            f"Failed to set position mode to ONEWAY. Reason: {event.message}. "
            "Trading is blocked until the position mode is confirmed."
        )
        self._position_mode_ready = False
        self._position_mode_success_count = 0

    def did_complete_buy_order(self, event: BuyOrderCompletedEvent):
        oid = event.order_id
        for leg in (self._long_leg, self._short_leg):
            if oid in leg.entry_orders or oid in leg.exit_orders:
                leg.create_ts = self.current_timestamp + self._filled_order_delay
                leg.cancel_ts = min(leg.cancel_ts, leg.create_ts)
                self.logger().info(
                    f"({'Long' if leg.is_long else 'Short'} leg) Buy order {oid} filled."
                )
                if oid in leg.entry_orders and self._news_active_event_id is not None:
                    self._news_flow_buy_qty += Decimal(str(event.base_asset_amount))
                break

    def did_complete_sell_order(self, event: SellOrderCompletedEvent):
        oid = event.order_id
        for leg in (self._long_leg, self._short_leg):
            if oid in leg.entry_orders or oid in leg.exit_orders:
                leg.create_ts = self.current_timestamp + self._filled_order_delay
                leg.cancel_ts = min(leg.cancel_ts, leg.create_ts)
                self.logger().info(
                    f"({'Long' if leg.is_long else 'Short'} leg) Sell order {oid} filled."
                )
                if oid in leg.entry_orders and self._news_active_event_id is not None:
                    self._news_flow_sell_qty += Decimal(str(event.base_asset_amount))
                break

    # ------------------------------------------------------------------
    # Status display
    # ------------------------------------------------------------------

    def format_status(self) -> str:
        if not self._all_markets_ready:
            market = self._long_leg.market_info.market
            sd = getattr(market, "status_dict", {})
            not_ready = [k for k, v in sd.items() if not v]
            return f"Market connectors are not ready. Waiting on: {not_ready}"
        if not self._position_mode_ready:
            return (
                f"Waiting for position mode ONEWAY confirmation "
                f"({self._position_mode_success_count}/{len(self.active_markets)}). "
                f"Retry counter: {self._position_mode_not_ready_counter}/10."
            )
        lines = []

        long_pos = self._get_position(self._long_leg)
        short_pos = self._get_position(self._short_leg)
        long_qty = long_pos.amount if long_pos else Decimal("0")
        short_qty = abs(short_pos.amount) if short_pos else Decimal("0")
        diff = long_qty - short_qty

        lines.append("")
        lines.append("  Inventory:")
        lines.append(f"    Long  qty : {long_qty}")
        lines.append(f"    Short qty : {short_qty}")
        lines.append(f"    Diff      : {diff:+.4f}  ({'BALANCED' if abs(diff) <= self._rebalance_size_diff_threshold else 'REBALANCING'})")

        if self._news_price_shift != Decimal("0") or self._news_active_event_id is not None:
            lines.append("")
            lines.append("  News state:")
            if self._news_price_shift != Decimal("0"):
                direction = "UP" if self._news_price_shift > 0 else "DOWN"
                lines.append(f"    Price shift  : {self._news_price_shift * 100:+.3f}% ({direction})")
            if self._news_active_event_id is not None:
                lines.append(f"    Active event : {self._news_active_event_id}")
                lines.append(f"    Flow buys    : {self._news_flow_buy_qty:.4f}")
                lines.append(f"    Flow sells   : {self._news_flow_sell_qty:.4f}")
                lines.append(f"    Long skew    : {self._news_long_skew:.3f}x")
                lines.append(f"    Short skew   : {self._news_short_skew:.3f}x")

        for label, leg in (("Long", self._long_leg), ("Short", self._short_leg)):
            active = self._active_orders(leg)
            lines.append("")
            lines.append(f"  {label} leg ({leg.market_info.market.name} / role={leg.account_role}):")
            if active:
                for o in sorted(active, key=lambda x: x.price, reverse=True):
                    side = "BUY" if o.is_buy else "SELL"
                    tag = " [EXIT]" if o.client_order_id in leg.exit_orders else ""
                    lines.append(f"    {side} {o.quantity} @ {o.price}{tag}")
            else:
                lines.append("    No active orders.")

            pos = self._get_position(leg)
            if pos:
                lines.append(
                    f"    Position: {pos.position_side.name}  qty={pos.amount}  "
                    f"entry={pos.entry_price}  uPnL={pos.unrealized_pnl:.4f}"
                )

        return "\n".join(lines)
