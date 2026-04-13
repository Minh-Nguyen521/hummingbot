from decimal import Decimal
from typing import Optional

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USDT"
USE_ETHEREUM_WALLET = False
USE_ETH_GAS_LOOKUP = False

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0002"),
    taker_percent_fee_decimal=Decimal("0.0005"),
)


class ExchangeSimConfigMap(BaseConnectorConfigMap):
    connector: str = "exchange_sim"
    exchange_sim_account_id: Optional[SecretStr] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter your exchange-sim account ID (leave blank to use dual-subaccount mode)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    exchange_sim_master_account_id: Optional[str] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter the master account ID if this is a subaccount (leave blank to skip)",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": False,
        }
    )
    exchange_sim_ref_price: Optional[Decimal] = Field(
        default=Decimal("1000"),
        json_schema_extra={
            "prompt": "Enter a reference price to use when the orderbook has no data (leave blank to skip)",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": False,
        }
    )
    exchange_sim_long_account_id: Optional[str] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter the long-leg subaccount ID for dual-subaccount mode (leave blank to skip)",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": False,
        }
    )
    exchange_sim_short_account_id: Optional[str] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter the short-leg subaccount ID for dual-subaccount mode (leave blank to skip)",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": False,
        }
    )
    exchange_sim_news_enabled: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": "Enable simulation news feed integration? (y/n)",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": False,
        }
    )
    exchange_sim_news_poll_interval: int = Field(
        default=30,
        json_schema_extra={
            "prompt": "Enter the simulation news poll interval in seconds",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": False,
        }
    )
    exchange_sim_news_lookahead_window: int = Field(
        default=3600,
        json_schema_extra={
            "prompt": "Enter the simulation news lookahead window in seconds",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": False,
        }
    )
    exchange_sim_news_replay_mode: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": "Allow future scheduled news visibility in replay mode? (y/n)",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": False,
        }
    )
    exchange_sim_news_feed_path: Optional[str] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter a local JSON news feed path for simulation (leave blank to use API)",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": False,
        }
    )
    exchange_sim_news_endpoint: str = Field(
        default="/api/v1/news",
        json_schema_extra={
            "prompt": "Enter the exchange-sim news API endpoint path",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": False,
        }
    )
    exchange_sim_news_volatility_enabled: bool = Field(
        default=False,
        json_schema_extra={
            "prompt": "Enable post-news volatility simulation? (y/n)",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": False,
        }
    )
    exchange_sim_news_impact_duration: int = Field(
        default=30,
        json_schema_extra={
            "prompt": "Enter the post-news volatility duration in seconds",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": False,
        }
    )
    exchange_sim_news_impact_multiplier: Decimal = Field(
        default=Decimal("1"),
        json_schema_extra={
            "prompt": "Enter the post-news volatility impact multiplier",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": False,
        }
    )
    exchange_sim_news_spread_multiplier: Decimal = Field(
        default=Decimal("2"),
        json_schema_extra={
            "prompt": "Enter the spread widening multiplier during post-news volatility",
            "is_secure": False,
            "is_connect_key": False,
            "prompt_on_new": False,
        }
    )
    model_config = ConfigDict(title="exchange_sim")


KEYS = ExchangeSimConfigMap.model_construct()
