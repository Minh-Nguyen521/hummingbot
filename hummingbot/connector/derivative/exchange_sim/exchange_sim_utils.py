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
    exchange_sim_account_id: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your exchange-sim account ID",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
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
    model_config = ConfigDict(title="exchange_sim")


KEYS = ExchangeSimConfigMap.model_construct()
