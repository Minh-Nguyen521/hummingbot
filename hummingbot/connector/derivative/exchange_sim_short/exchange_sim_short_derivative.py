from decimal import Decimal
from typing import List, Optional

from hummingbot.connector.derivative.exchange_sim.exchange_sim_derivative import ExchangeSimDerivative
from hummingbot.core.data_type.common import PositionMode


class ExchangeSimShortDerivative(ExchangeSimDerivative):
    """Short-leg alias for ExchangeSimDerivative.

    Identical to ExchangeSimDerivative but registered under a distinct connector
    name so that two instances can coexist in the same ConnectorManager.  The
    constructor accepts ``exchange_sim_short_*`` parameter names and forwards
    them to the parent.
    """

    DEFAULT_DOMAIN = "exchange_sim_short"

    def __init__(
        self,
        exchange_sim_short_account_id: str,
        exchange_sim_short_master_account_id: Optional[str] = None,
        exchange_sim_short_ref_price: Optional[Decimal] = None,
        balance_asset_limit=None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = DEFAULT_DOMAIN,
    ):
        super().__init__(
            exchange_sim_account_id=exchange_sim_short_account_id,
            exchange_sim_master_account_id=exchange_sim_short_master_account_id,
            exchange_sim_ref_price=exchange_sim_short_ref_price,
            balance_asset_limit=balance_asset_limit,
            rate_limits_share_pct=rate_limits_share_pct,
            trading_pairs=trading_pairs,
            trading_required=trading_required,
            domain=domain,
        )

    @property
    def name(self) -> str:
        return "exchange_sim_short"

    def supported_position_modes(self):
        from hummingbot.core.data_type.common import PositionMode
        return [PositionMode.ONEWAY]
