from decimal import Decimal
from typing import List, Tuple

from hummingbot.strategy.hedge_mm import HedgeMMStrategy
from hummingbot.strategy.hedge_mm.hedge_mm_config_map import hedge_mm_config_map as c_map
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple


async def start(self):
    try:
        exchange = c_map.get("derivative").value.lower()
        raw_trading_pair = c_map.get("market").value
        leverage = c_map.get("leverage").value
        bid_spread = c_map.get("bid_spread").value / Decimal("100")
        ask_spread = c_map.get("ask_spread").value / Decimal("100")
        minimum_spread = c_map.get("minimum_spread").value / Decimal("100")
        order_amount = c_map.get("order_amount").value
        order_levels = c_map.get("order_levels").value
        order_level_mode = c_map.get("order_level_mode").value.lower()
        order_level_amount = c_map.get("order_level_amount").value
        top_order_size_multiplier = c_map.get("top_order_size_multiplier").value
        order_level_spread = c_map.get("order_level_spread").value / Decimal("100")
        order_level_price_step = c_map.get("order_level_price_step").value
        order_refresh_time = c_map.get("order_refresh_time").value
        order_refresh_tolerance_pct = c_map.get("order_refresh_tolerance_pct").value / Decimal("100")
        filled_order_delay = c_map.get("filled_order_delay").value
        long_profit_taking_spread = c_map.get("long_profit_taking_spread").value / Decimal("100")
        short_profit_taking_spread = c_map.get("short_profit_taking_spread").value / Decimal("100")
        stop_loss_spread = c_map.get("stop_loss_spread").value / Decimal("100")
        stop_loss_slippage_buffer = c_map.get("stop_loss_slippage_buffer").value / Decimal("100")
        time_between_stop_loss_orders = c_map.get("time_between_stop_loss_orders").value
        rebalance_enabled = c_map.get("rebalance_enabled").value
        rebalance_size_diff_threshold = c_map.get("rebalance_size_diff_threshold").value
        rebalance_target_pnl_pct = c_map.get("rebalance_target_pnl_pct").value / Decimal("100")
        rebalance_heavy_side_size_mult = c_map.get("rebalance_heavy_side_size_mult").value
        rebalance_light_side_size_mult = c_map.get("rebalance_light_side_size_mult").value

        trading_pair: str = raw_trading_pair
        base, quote = trading_pair.split("-")

        market_names: List[Tuple[str, List[str]]] = [(exchange, [trading_pair])]
        await self.initialize_markets(market_names)

        market_info = MarketTradingPairTuple(
            self.markets[exchange], trading_pair, base, quote
        )
        self.market_trading_pair_tuples = [market_info]

        self.strategy = HedgeMMStrategy()
        self.strategy.init_params(
            market_info=market_info,
            bid_spread=bid_spread,
            ask_spread=ask_spread,
            minimum_spread=minimum_spread,
            order_amount=order_amount,
            order_levels=order_levels,
            order_level_mode=order_level_mode,
            order_level_spread=order_level_spread,
            order_level_price_step=order_level_price_step,
            order_level_amount=order_level_amount,
            top_order_size_multiplier=top_order_size_multiplier,
            order_refresh_time=order_refresh_time,
            order_refresh_tolerance_pct=order_refresh_tolerance_pct,
            filled_order_delay=filled_order_delay,
            long_profit_taking_spread=long_profit_taking_spread,
            short_profit_taking_spread=short_profit_taking_spread,
            stop_loss_spread=stop_loss_spread,
            stop_loss_slippage_buffer=stop_loss_slippage_buffer,
            time_between_stop_loss_orders=time_between_stop_loss_orders,
            leverage=leverage,
            rebalance_enabled=rebalance_enabled,
            rebalance_size_diff_threshold=rebalance_size_diff_threshold,
            rebalance_target_pnl_pct=rebalance_target_pnl_pct,
            rebalance_heavy_side_size_mult=rebalance_heavy_side_size_mult,
            rebalance_light_side_size_mult=rebalance_light_side_size_mult,
            hb_app_notification=True,
        )
    except Exception as e:
        self.notify(str(e))
        self.logger().error("Unknown error during hedge_mm initialization.", exc_info=True)
