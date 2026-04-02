from decimal import Decimal
from typing import Optional

from hummingbot.client.config.config_validators import (
    validate_bool,
    validate_decimal,
    validate_derivative,
    validate_int,
    validate_market_trading_pair,
)
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.settings import required_exchanges


def validate_derivative_trading_pair(value: str) -> Optional[str]:
    derivative = hedge_mm_config_map.get("derivative").value
    return validate_market_trading_pair(derivative, value)


def on_derivative_validated(value: str):
    required_exchanges.add(value)


def validate_price_floor_ceiling(value: str) -> Optional[str]:
    try:
        decimal_value = Decimal(value)
    except Exception:
        return f"{value} is not in decimal format."
    if not (decimal_value == Decimal("-1") or decimal_value > Decimal("0")):
        return "Value must be more than 0 or -1 to disable this feature."


def order_amount_prompt() -> str:
    trading_pair = hedge_mm_config_map.get("market").value or "BASE-QUOTE"
    base_asset, _ = trading_pair.split("-")
    return f"What is the amount of {base_asset} per order? >>> "


def validate_order_level_mode(value: str) -> Optional[str]:
    normalized = value.lower()
    if normalized not in {"percent", "absolute"}:
        return "Value must be either 'percent' or 'absolute'."
    return None


hedge_mm_config_map = {
    "strategy":
        ConfigVar(key="strategy",
                  prompt=None,
                  default="hedge_mm"),
    "derivative":
        ConfigVar(key="derivative",
                  prompt="Enter the connector name (e.g. exchange_sim) >>> ",
                  validator=validate_derivative,
                  on_validated=on_derivative_validated,
                  prompt_on_new=True),
    "market":
        ConfigVar(key="market",
                  prompt="Enter the token trading pair (e.g. BTC-USDT) >>> ",
                  validator=validate_derivative_trading_pair,
                  prompt_on_new=True),
    "leverage":
        ConfigVar(key="leverage",
                  prompt="How much leverage do you want to use? >>> ",
                  type_str="int",
                  validator=lambda v: validate_int(v, min_value=1, inclusive=True),
                  default=10,
                  prompt_on_new=True),
    "bid_spread":
        ConfigVar(key="bid_spread",
                  prompt="Bid spread from mid price (Enter 1 for 1%) >>> ",
                  type_str="decimal",
                  validator=lambda v: validate_decimal(v, 0, 100, inclusive=False),
                  prompt_on_new=True),
    "ask_spread":
        ConfigVar(key="ask_spread",
                  prompt="Ask spread from mid price (Enter 1 for 1%) >>> ",
                  type_str="decimal",
                  validator=lambda v: validate_decimal(v, 0, 100, inclusive=False),
                  prompt_on_new=True),
    "minimum_spread":
        ConfigVar(key="minimum_spread",
                  prompt="Minimum spread to keep orders (Enter 1 for 1%) >>> ",
                  required_if=lambda: False,
                  type_str="decimal",
                  default=Decimal("-100"),
                  validator=lambda v: validate_decimal(v, -100, 100, True)),
    "order_amount":
        ConfigVar(key="order_amount",
                  prompt=order_amount_prompt,
                  type_str="decimal",
                  validator=lambda v: validate_decimal(v, min_value=Decimal("0"), inclusive=False),
                  prompt_on_new=True),
    "order_levels":
        ConfigVar(key="order_levels",
                  prompt="How many order levels? >>> ",
                  type_str="int",
                  validator=lambda v: validate_int(v, min_value=1, inclusive=True),
                  default=1),
    "order_level_mode":
        ConfigVar(key="order_level_mode",
                  prompt="Order level spacing mode (percent/absolute) >>> ",
                  type_str="str",
                  validator=validate_order_level_mode,
                  default="percent"),
    "order_level_amount":
        ConfigVar(key="order_level_amount",
                  prompt="Size increment per additional level >>> ",
                  required_if=lambda: hedge_mm_config_map.get("order_levels").value > 1,
                  type_str="decimal",
                  validator=validate_decimal,
                  default=Decimal("0")),
    "top_order_size_multiplier":
        ConfigVar(key="top_order_size_multiplier",
                  prompt="Near-BBO size fraction (0 < value <= 1): 0.5 means the nearest order is half the base size, growing to full size at the farthest level >>> ",
                  required_if=lambda: hedge_mm_config_map.get("order_levels").value >= 1,
                  type_str="decimal",
                  validator=lambda v: validate_decimal(v, Decimal("0"), Decimal("1"), inclusive=False),
                  default=Decimal("0.5")),
    "order_level_spread":
        ConfigVar(key="order_level_spread",
                  prompt="Spread increment per additional level (Enter 1 for 1%) >>> ",
                  required_if=lambda: hedge_mm_config_map.get("order_levels").value > 1 and
                  hedge_mm_config_map.get("order_level_mode").value == "percent",
                  type_str="decimal",
                  validator=lambda v: validate_decimal(v, 0, 100, inclusive=False),
                  default=Decimal("1")),
    "order_level_price_step":
        ConfigVar(key="order_level_price_step",
                  prompt="Absolute price increment per additional level >>> ",
                  required_if=lambda: hedge_mm_config_map.get("order_levels").value > 1 and
                  hedge_mm_config_map.get("order_level_mode").value == "absolute",
                  type_str="decimal",
                  validator=lambda v: validate_decimal(v, min_value=Decimal("0"), inclusive=False),
                  default=Decimal("1")),
    "order_refresh_time":
        ConfigVar(key="order_refresh_time",
                  prompt="How often to refresh orders (seconds)? >>> ",
                  type_str="float",
                  validator=lambda v: validate_decimal(v, 0, inclusive=False),
                  default=30.0,
                  prompt_on_new=True),
    "order_refresh_tolerance_pct":
        ConfigVar(key="order_refresh_tolerance_pct",
                  prompt="Price change tolerance before refresh (Enter 1 for 1%) >>> ",
                  type_str="decimal",
                  default=Decimal("0"),
                  validator=lambda v: validate_decimal(v, -10, 10, inclusive=True)),
    "filled_order_delay":
        ConfigVar(key="filled_order_delay",
                  prompt="Delay before re-entering after a fill (seconds)? >>> ",
                  type_str="float",
                  validator=lambda v: validate_decimal(v, min_value=0, inclusive=False),
                  default=60.0),
    "long_profit_taking_spread":
        ConfigVar(key="long_profit_taking_spread",
                  prompt="Spread above entry for long profit-taking sell (Enter 1 for 1%) >>> ",
                  type_str="decimal",
                  default=Decimal("0"),
                  validator=lambda v: validate_decimal(v, 0, 100, True),
                  prompt_on_new=True),
    "short_profit_taking_spread":
        ConfigVar(key="short_profit_taking_spread",
                  prompt="Spread below entry for short profit-taking buy (Enter 1 for 1%) >>> ",
                  type_str="decimal",
                  default=Decimal("0"),
                  validator=lambda v: validate_decimal(v, 0, 100, True),
                  prompt_on_new=True),
    "stop_loss_spread":
        ConfigVar(key="stop_loss_spread",
                  prompt="Spread from entry for stop-loss (Enter 1 for 1%) >>> ",
                  type_str="decimal",
                  default=Decimal("0"),
                  validator=lambda v: validate_decimal(v, 0, 101, False),
                  prompt_on_new=True),
    "stop_loss_slippage_buffer":
        ConfigVar(key="stop_loss_slippage_buffer",
                  prompt="Slippage buffer for stop-loss orders (Enter 1 for 1%) >>> ",
                  type_str="decimal",
                  default=Decimal("0.5"),
                  validator=lambda v: validate_decimal(v, 0, inclusive=True)),
    "time_between_stop_loss_orders":
        ConfigVar(key="time_between_stop_loss_orders",
                  prompt="Minimum seconds between stop-loss order refreshes >>> ",
                  type_str="float",
                  default=60.0,
                  validator=lambda v: validate_decimal(v, 0, inclusive=False)),
    # Rebalance parameters
    "rebalance_enabled":
        ConfigVar(key="rebalance_enabled",
                  prompt="Enable inventory rebalancing? (Yes/No) >>> ",
                  type_str="bool",
                  default=True,
                  validator=validate_bool),
    "rebalance_size_diff_threshold":
        ConfigVar(key="rebalance_size_diff_threshold",
                  prompt="Minimum |long_qty - short_qty| to trigger rebalancing >>> ",
                  required_if=lambda: hedge_mm_config_map.get("rebalance_enabled").value,
                  type_str="decimal",
                  default=Decimal("0.5"),
                  validator=lambda v: validate_decimal(v, min_value=Decimal("0"), inclusive=False)),
    "rebalance_target_pnl_pct":
        ConfigVar(key="rebalance_target_pnl_pct",
                  prompt="Unrealized PnL% on heavy side before rebalance close (Enter 0.5 for 0.5%) >>> ",
                  required_if=lambda: hedge_mm_config_map.get("rebalance_enabled").value,
                  type_str="decimal",
                  default=Decimal("0.5"),
                  validator=lambda v: validate_decimal(v, 0, 100, inclusive=False)),
    "rebalance_heavy_side_size_mult":
        ConfigVar(key="rebalance_heavy_side_size_mult",
                  prompt="Size multiplier for heavy-side entry orders during rebalance >>> ",
                  required_if=lambda: hedge_mm_config_map.get("rebalance_enabled").value,
                  type_str="decimal",
                  default=Decimal("0.5"),
                  validator=lambda v: validate_decimal(v, 0, 10, inclusive=False)),
    "rebalance_light_side_size_mult":
        ConfigVar(key="rebalance_light_side_size_mult",
                  prompt="Size multiplier for light-side entry orders during rebalance >>> ",
                  required_if=lambda: hedge_mm_config_map.get("rebalance_enabled").value,
                  type_str="decimal",
                  default=Decimal("1.5"),
                  validator=lambda v: validate_decimal(v, 0, 10, inclusive=False)),
}
