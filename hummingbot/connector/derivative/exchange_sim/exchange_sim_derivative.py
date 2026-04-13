# ExchangeSimDerivative connector implementation
# Stub file - will be implemented with proper Hummingbot base class methods

import asyncio
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from bidict import bidict

from hummingbot.connector.derivative.exchange_sim import (
    exchange_sim_constants as CONSTANTS,
)
from hummingbot.connector.derivative.exchange_sim import (
    exchange_sim_web_utils as web_utils,
)
from hummingbot.connector.derivative.exchange_sim.exchange_sim_api_order_book_data_source import (
    ExchangeSimAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.exchange_sim.exchange_sim_api_user_stream_data_source import (
    ExchangeSimAPIUserStreamDataSource,
)
from hummingbot.connector.derivative.exchange_sim.exchange_sim_auth import (
    ExchangeSimAuth,
)
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import (
    OrderType,
    PositionAction,
    PositionMode,
    PositionSide,
    PriceType,
    TradeType,
)
from hummingbot.core.data_type.funding_info import FundingInfoUpdate
from hummingbot.core.data_type.in_flight_order import (
    InFlightOrder,
    OrderState,
    OrderUpdate,
    TradeUpdate,
)
from hummingbot.core.data_type.order_book_tracker_data_source import (
    OrderBookTrackerDataSource,
)
from hummingbot.core.data_type.trade_fee import TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import (
    UserStreamTrackerDataSource,
)
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

from .news_service import ExchangeSimNewsService
from .news_types import NewsEvent


class ExchangeSimDerivative(PerpetualDerivativePyBase):
    """ExchangeSimDerivative connector for Hummingbot perpetual futures trading.

    Supports two modes:
    - Single-account mode: supply ``exchange_sim_account_id`` (backward-compatible).
    - Dual-subaccount mode: supply both ``exchange_sim_long_account_id`` and
      ``exchange_sim_short_account_id``; each leg is routed to its own subaccount
      by registering client order IDs with ``register_order_role(oid, role)`` before
      order placement.  Use ``get_positions_for_role(role)`` to read per-leg positions.
    """

    DEFAULT_DOMAIN = "exchange_sim"
    DEFAULT_ACCOUNT_BALANCE = Decimal("100000000000")
    DEFAULT_ACCOUNT_LEVERAGE = 10
    DEFAULT_MAKER_FEE_BPS = 2
    DEFAULT_TAKER_FEE_BPS = 5
    web_utils = web_utils

    def __init__(
        self,
        exchange_sim_account_id: Optional[str] = None,
        exchange_sim_long_account_id: Optional[str] = None,
        exchange_sim_short_account_id: Optional[str] = None,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = DEFAULT_DOMAIN,
        exchange_sim_ref_price: Optional[Decimal] = None,
        exchange_sim_master_account_id: Optional[str] = None,
        exchange_sim_news_enabled: bool = False,
        exchange_sim_news_poll_interval: int = 30,
        exchange_sim_news_lookahead_window: int = 3600,
        exchange_sim_news_replay_mode: bool = False,
        exchange_sim_news_feed_path: Optional[str] = None,
        exchange_sim_news_endpoint: str = "/api/v1/news",
        exchange_sim_news_volatility_enabled: bool = False,
        exchange_sim_news_impact_duration: int = 30,
        exchange_sim_news_impact_multiplier: Decimal = Decimal("1"),
        exchange_sim_news_spread_multiplier: Decimal = Decimal("2"),
    ):
        self._is_dual_mode = (
            exchange_sim_long_account_id is not None
            and exchange_sim_short_account_id is not None
        )
        if self._is_dual_mode:
            self._role_account_ids: Dict[str, str] = {
                "long": exchange_sim_long_account_id,
                "short": exchange_sim_short_account_id,
            }
            # _account_id is used as a fallback for shared operations (e.g. order-book).
            self._account_id = exchange_sim_long_account_id
        else:
            self._account_id = exchange_sim_account_id or "default"
            self._role_account_ids = {"default": self._account_id}

        self._master_account_id = exchange_sim_master_account_id
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        self._position_mode = None
        self._account_initialized = False
        self._role_initialized: Dict[str, bool] = {}
        self._ref_price = exchange_sim_ref_price
        self._news_enabled = exchange_sim_news_enabled
        self._news_volatility_enabled = exchange_sim_news_volatility_enabled
        self._news_impact_duration = float(exchange_sim_news_impact_duration)
        self._news_impact_multiplier = Decimal(str(exchange_sim_news_impact_multiplier))
        self._news_spread_multiplier = Decimal(str(exchange_sim_news_spread_multiplier))
        # Fallback price used when one or both sides of the orderbook are empty.
        self._last_known_price: Dict[str, Decimal] = {}
        # Per-role state for dual-subaccount mode.
        self._order_role_map: Dict[str, str] = {}  # client_order_id → role
        self._role_positions: Dict[
            str, Dict[str, Position]
        ] = {}  # role → {tp → Position}
        self._role_balances: Dict[
            str, Dict[str, Decimal]
        ] = {}  # role → {asset → total}
        self._role_available_balances: Dict[
            str, Dict[str, Decimal]
        ] = {}  # role → {asset → available}
        self._news_polling_task: Optional[asyncio.Task] = None

        super().__init__(balance_asset_limit, rate_limits_share_pct)
        self._real_time_balance_update = False
        self._set_trading_pair_symbol_map(bidict())
        self._news_service = ExchangeSimNewsService(
            api_factory=self._web_assistants_factory,
            enabled=exchange_sim_news_enabled,
            news_feed_path=exchange_sim_news_feed_path,
            news_endpoint=exchange_sim_news_endpoint,
            poll_interval=float(exchange_sim_news_poll_interval),
            lookahead_window=float(exchange_sim_news_lookahead_window),
            replay_mode=exchange_sim_news_replay_mode,
            domain=domain,
        )

    # ------------------------------------------------------------------
    # Dual-account API
    # ------------------------------------------------------------------

    def register_order_role(self, order_id: str, role: str) -> None:
        """Map a client order ID to an account role before placement.

        Must be called by the strategy immediately after ``buy_with_specific_market``
        / ``sell_with_specific_market`` returns the client order ID so that
        ``_place_order`` (which runs asynchronously later) can route to the correct
        subaccount.  In single-account mode this is a no-op.
        """
        if self._is_dual_mode:
            self._order_role_map[order_id] = role

    def get_positions_for_role(self, role: str) -> Dict[str, Position]:
        """Return the position dict for the given account role.

        In single-account mode ``role`` is ignored and ``account_positions`` is
        returned directly.
        """
        if self._is_dual_mode:
            return self._role_positions.get(role, {})
        return dict(self._perpetual_trading.account_positions)

    def get_balance_for_role(self, role: str, asset: str) -> Decimal:
        if self._is_dual_mode:
            return self._role_balances.get(role, {}).get(asset, Decimal("0"))
        return self.get_balance(asset)

    def get_available_balance_for_role(self, role: str, asset: str) -> Decimal:
        if self._is_dual_mode:
            return self._role_available_balances.get(role, {}).get(asset, Decimal("0"))
        return self.get_available_balance(asset)

    def _account_id_for_order(self, order_id: str) -> str:
        if self._is_dual_mode:
            role = self._order_role_map.get(order_id, "long")
            return self._role_account_ids.get(role, self._account_id)
        return self._account_id

    # ------------------------------------------------------------------
    # Standard connector properties
    # ------------------------------------------------------------------

    @property
    def authenticator(self) -> ExchangeSimAuth:
        return ExchangeSimAuth()

    @property
    def name(self) -> str:
        return "exchange_sim"

    @property
    def status_dict(self) -> Dict[str, bool]:
        sd = super().status_dict
        sd["news_initialized"] = self._news_service.initialized
        return sd

    @property
    def rate_limits_rules(self) -> List:
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return 36

    @property
    def client_order_id_prefix(self) -> str:
        return "HBOT-"

    @property
    def trading_rules_request_path(self) -> str:
        return "/api/v1/instruments"

    @property
    def trading_pairs_request_path(self) -> str:
        return "/api/v1/instruments"

    @property
    def check_network_request_path(self) -> str:
        return "/api/v1/time"

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return 120

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]

    def supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]

    # Half-spread applied when only one side of the book exists (0.01%)
    _ONE_SIDE_HALF_SPREAD = Decimal("0.0001")
    # When the real BBO spread exceeds this fraction, clamp to mid ± _ONE_SIDE_HALF_SPREAD
    _MAX_BBO_SPREAD = Decimal("0.002")
    _NEWS_IMPACT_BY_SEVERITY = {
        "low": Decimal("0.0005"),
        "medium": Decimal("0.0015"),
        "high": Decimal("0.0030"),
        "critical": Decimal("0.0060"),
    }

    def _fallback_reference_price(self, trading_pair: str) -> Decimal:
        ref = self._last_known_price.get(trading_pair)
        if ref is not None and not ref.is_nan() and ref > 0:
            return ref
        if self._ref_price is not None and self._ref_price > 0:
            return self._ref_price
        return Decimal("nan")

    def _sentiment_direction(self, event: NewsEvent) -> Decimal:
        if event.sentiment == "positive":
            return Decimal("1")
        if event.sentiment == "negative":
            return Decimal("-1")
        return Decimal("0")

    def _active_news_impact(self, trading_pair: str) -> Tuple[Decimal, Decimal]:
        if not self._news_volatility_enabled:
            return Decimal("0"), Decimal("1")
        events = self.get_active_news(
            symbols=[trading_pair],
            before_seconds=0,
            after_seconds=self._news_impact_duration,
        )
        if len(events) == 0:
            return Decimal("0"), Decimal("1")

        directional_shift = Decimal("0")
        spread_multiplier = Decimal("1")
        for event in events:
            elapsed = max(0.0, self.current_timestamp - event.timestamp)
            if self._news_impact_duration <= 0:
                decay = Decimal("1")
            else:
                decay = Decimal(str(max(0.0, 1 - (elapsed / self._news_impact_duration))))
            severity_impact = self._NEWS_IMPACT_BY_SEVERITY.get(event.severity, Decimal("0"))
            impact = severity_impact * self._news_impact_multiplier * decay
            directional_shift += self._sentiment_direction(event) * impact
            if impact > 0:
                spread_multiplier = max(spread_multiplier, Decimal("1") + ((self._news_spread_multiplier - 1) * decay))
        return directional_shift, spread_multiplier

    def _apply_news_impact_to_price(self, trading_pair: str, price: Decimal, is_buy: bool) -> Decimal:
        if price.is_nan() or price <= 0:
            return price
        directional_shift, spread_multiplier = self._active_news_impact(trading_pair)
        adjusted_price = price * (Decimal("1") + directional_shift)
        if spread_multiplier > 1:
            spread_half = (spread_multiplier - 1) * self._ONE_SIDE_HALF_SPREAD
            adjusted_price *= Decimal("1") + spread_half if is_buy else Decimal("1") - spread_half
        return adjusted_price

    def get_price(self, trading_pair: str, is_buy: bool) -> Decimal:
        """Return best ask (is_buy=True) or best bid (is_buy=False).

        Fallback hierarchy when the requested side is empty:
        1. Real BBO price — clamped to mid ± _ONE_SIDE_HALF_SPREAD if spread > _MAX_BBO_SPREAD.
        2. Use the opposite side ± full spread (_ONE_SIDE_HALF_SPREAD * 2).
        3. Use _last_known_price ± half-spread.
        4. Configured ref_price ± half-spread.
        5. Return NaN (strategy will skip the tick).
        """
        try:
            ob = self.get_order_book(trading_pair)
            has_ask = ob.ask_entries().__next__() is not None
        except StopIteration:
            has_ask = False
        except Exception:
            has_ask = False
        try:
            ob = self.get_order_book(trading_pair)
            has_bid = ob.bid_entries().__next__() is not None
        except StopIteration:
            has_bid = False
        except Exception:
            has_bid = False

        has_side = has_ask if is_buy else has_bid

        if has_side:
            price = super().get_price(trading_pair, is_buy)
            if not price.is_nan():
                if has_ask and has_bid:
                    ask = super().get_price(trading_pair, True)
                    bid = super().get_price(trading_pair, False)
                    mid = (ask + bid) / 2
                    self._last_known_price[trading_pair] = mid
                    if mid > 0 and (ask - bid) / mid > self._MAX_BBO_SPREAD:
                        unclamped = (
                            mid * (1 + self._ONE_SIDE_HALF_SPREAD)
                            if is_buy
                            else mid * (1 - self._ONE_SIDE_HALF_SPREAD)
                        )
                        return self._apply_news_impact_to_price(trading_pair, unclamped, is_buy)
                return self._apply_news_impact_to_price(trading_pair, price, is_buy)

        has_opposite = has_bid if is_buy else has_ask
        if has_opposite:
            opposite = super().get_price(trading_pair, not is_buy)
            if not opposite.is_nan():
                self._last_known_price[trading_pair] = opposite
                if is_buy:
                    return self._apply_news_impact_to_price(
                        trading_pair, opposite * (1 + self._ONE_SIDE_HALF_SPREAD * 2), is_buy
                    )
                else:
                    return self._apply_news_impact_to_price(
                        trading_pair, opposite * (1 - self._ONE_SIDE_HALF_SPREAD * 2), is_buy
                    )

        ref = self._fallback_reference_price(trading_pair)
        if not ref.is_nan():
            if is_buy:
                return self._apply_news_impact_to_price(trading_pair, ref * (1 + self._ONE_SIDE_HALF_SPREAD), is_buy)
            else:
                return self._apply_news_impact_to_price(trading_pair, ref * (1 - self._ONE_SIDE_HALF_SPREAD), is_buy)

        return Decimal("nan")

    def get_mid_price(self, trading_pair: str) -> Decimal:
        bid = self.get_price(trading_pair, False)
        ask = self.get_price(trading_pair, True)
        if not bid.is_nan() and not ask.is_nan() and bid > 0 and ask > 0:
            return (bid + ask) / Decimal("2")
        return self._fallback_reference_price(trading_pair)

    def get_price_by_type(self, trading_pair: str, price_type: PriceType) -> Decimal:
        if price_type is PriceType.BestBid:
            return self.get_price(trading_pair, False)
        elif price_type is PriceType.BestAsk:
            return self.get_price(trading_pair, True)
        elif price_type is PriceType.MidPrice:
            return self.get_mid_price(trading_pair)
        elif price_type is PriceType.LastTrade:
            return self._fallback_reference_price(trading_pair)
        return super().get_price_by_type(trading_pair, price_type)

    def tick(self, timestamp: float):
        super().tick(timestamp)
        self._news_service.advance_time(timestamp)

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        return "USDT"

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        return "USDT"

    async def start_network(self):
        await super().start_network()
        if self._news_enabled:
            await self._news_service.refresh()
            self._news_polling_task = asyncio.create_task(self._news_polling_loop())

    async def stop_network(self):
        if self._news_polling_task is not None:
            self._news_polling_task.cancel()
            try:
                await self._news_polling_task
            except asyncio.CancelledError:
                pass
            self._news_polling_task = None
        await super().stop_network()

    def subscribe_to_news(self, callback: Callable[[NewsEvent], None]) -> None:
        self._news_service.subscribe(callback)

    def unsubscribe_from_news(self, callback: Callable[[NewsEvent], None]) -> None:
        self._news_service.unsubscribe(callback)

    def get_upcoming_news(
        self,
        symbols: Optional[Sequence[str]] = None,
        window_seconds: Optional[float] = None,
    ) -> List[NewsEvent]:
        return self._news_service.get_upcoming_events(
            current_timestamp=self.current_timestamp,
            window_seconds=window_seconds,
            symbols=symbols,
        )

    def get_active_news(
        self,
        symbols: Optional[Sequence[str]] = None,
        before_seconds: float = 0,
        after_seconds: float = 0,
        min_severity: Optional[str] = None,
    ) -> List[NewsEvent]:
        return self._news_service.get_active_events(
            current_timestamp=self.current_timestamp,
            before_seconds=before_seconds,
            after_seconds=after_seconds,
            symbols=symbols,
            min_severity=min_severity,
        )

    def get_latest_news(self, symbol: Optional[str] = None) -> Optional[NewsEvent]:
        return self._news_service.get_latest_event(symbol=symbol)

    async def _api_request(self, *args, **kwargs):
        kwargs.setdefault("limit_id", CONSTANTS.DEFAULT_LIMIT_ID)
        return await super()._api_request(*args, **kwargs)

    async def _news_polling_loop(self):
        while True:
            try:
                await self._news_service.refresh()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().warning("Unexpected error refreshing exchange_sim news feed.", exc_info=True)
            await asyncio.sleep(self._news_service.poll_interval)

    @staticmethod
    def _is_account_missing_error(exception: Exception) -> bool:
        error_text = str(exception).lower()
        return "404" in error_text or "not found" in error_text

    @staticmethod
    def _is_account_already_exists_error(exception: Exception) -> bool:
        error_text = str(exception).lower()
        return (
            "409" in error_text
            or "already exists" in error_text
            or "conflict" in error_text
        )

    async def _ensure_single_account_exists(self, account_id: str) -> None:
        account_path = f"/api/v1/accounts/{account_id}"
        try:
            await self._api_get(path_url=account_path)
            return
        except Exception as ex:
            if not self._is_account_missing_error(ex):
                raise

        create_body = {
            "id": account_id,
            "balance": str(self.DEFAULT_ACCOUNT_BALANCE),
            "leverage": self.DEFAULT_ACCOUNT_LEVERAGE,
            "maker_fee_bps": self.DEFAULT_MAKER_FEE_BPS,
            "taker_fee_bps": self.DEFAULT_TAKER_FEE_BPS,
        }
        if self._master_account_id:
            create_path = f"/api/v1/accounts/{self._master_account_id}/subaccounts"
        else:
            create_path = "/api/v1/accounts"
        try:
            await self._api_post(path_url=create_path, data=create_body)
        except Exception as ex:
            if not self._is_account_already_exists_error(ex):
                raise

    async def _ensure_single_account_target_balance(self, account_id: str) -> None:
        if self._master_account_id is None:
            return
        account_path = f"/api/v1/accounts/{account_id}"
        resp = await self._api_get(path_url=account_path)
        current_balance = Decimal(str(resp.get("balance", "0")))
        if current_balance >= self.DEFAULT_ACCOUNT_BALANCE:
            return
        top_up_amount = self.DEFAULT_ACCOUNT_BALANCE - current_balance
        await self.transfer_funds(top_up_amount, account_id)

    async def _ensure_account_exists(self) -> None:
        if self._is_dual_mode:
            for role, account_id in self._role_account_ids.items():
                if not self._role_initialized.get(role, False):
                    await self._ensure_single_account_exists(account_id)
                    await self._ensure_single_account_target_balance(account_id)
                    self._role_initialized[role] = True
        else:
            if not self._account_initialized:
                await self._ensure_single_account_exists(self._account_id)
                await self._ensure_single_account_target_balance(self._account_id)
                self._account_initialized = True

    async def _api_request_status_only(
        self,
        path_url: str,
        method: RESTMethod,
        data: Optional[Dict[str, Any]] = None,
        is_auth_required: bool = False,
    ) -> int:
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = await self._api_request_url(
            path_url=path_url, is_auth_required=is_auth_required
        )
        response = await rest_assistant.execute_request_and_get_response(
            url=url,
            method=method,
            data=data,
            is_auth_required=is_auth_required,
            throttler_limit_id=CONSTANTS.DEFAULT_LIMIT_ID,
        )
        return response.status

    async def _place_order(
        self,
        order_id: str,
        trading_pair: str,
        amount: Decimal,
        trade_type: TradeType,
        order_type: OrderType,
        price: Decimal,
        **kwargs,
    ) -> Tuple[str, float]:
        await self._ensure_account_exists()
        account_id = self._account_id_for_order(order_id)
        side = "buy" if trade_type == TradeType.BUY else "sell"
        kind = (
            "limit"
            if order_type in (OrderType.LIMIT, OrderType.LIMIT_MAKER)
            else "market"
        )
        body = {
            "side": side,
            "kind": kind,
            "tif": "post_only" if order_type == OrderType.LIMIT_MAKER else "gtc",
            "qty": str(amount),
            "client_id": order_id,
        }
        if order_type in (OrderType.LIMIT, OrderType.LIMIT_MAKER):
            body["price"] = str(price)
        if kwargs.get("position_action") == PositionAction.CLOSE:
            body["reduce_only"] = True
        path = f"/api/v1/accounts/{account_id}/orders"
        try:
            resp = await self._api_post(path_url=path, data=body)
        except Exception as e:
            err = str(e)
            if (
                "reduce-only" in err.lower()
                or "increase position" in err.lower()
                or "no open position" in err.lower()
            ):
                tp = self._trading_pairs[0] if self._trading_pairs else "BTC-USDT"
                if self._is_dual_mode:
                    role = self._order_role_map.get(order_id, "long")
                    self._role_positions.setdefault(role, {}).pop(tp, None)
                    await self._update_positions_for_role(role, account_id)
                else:
                    self._perpetual_trading.account_positions.pop(tp, None)
                    await self._update_positions()
            raise
        return str(resp["order_id"]), self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        account_id = self._account_id_for_order(order_id)
        path = f"/api/v1/accounts/{account_id}/orders/{tracked_order.exchange_order_id}"
        try:
            status = await self._api_request_status_only(
                path_url=path, method=RESTMethod.DELETE
            )
            return 200 <= status < 300
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                return True
            return False

    async def _update_balances(self) -> None:
        await self._ensure_account_exists()
        if self._is_dual_mode:
            total_balance = Decimal("0")
            total_available = Decimal("0")
            self._role_balances.clear()
            self._role_available_balances.clear()
            for role, account_id in self._role_account_ids.items():
                path = f"/api/v1/accounts/{account_id}"
                resp = await self._api_get(path_url=path)
                balance = Decimal(resp["balance"])
                available = balance - Decimal(resp.get("reserved_margin", 0))
                self._role_balances[role] = {"USDT": balance}
                self._role_available_balances[role] = {"USDT": available}
                total_balance += balance
                total_available += available
            self._account_balances["USDT"] = total_balance
            self._account_available_balances["USDT"] = total_available
        else:
            path = f"/api/v1/accounts/{self._account_id}"
            resp = await self._api_get(path_url=path)
            self._account_balances["USDT"] = Decimal(resp["balance"])
            self._account_available_balances["USDT"] = Decimal(
                resp["balance"]
            ) - Decimal(resp.get("reserved_margin", 0))

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        account_id = self._account_id_for_order(tracked_order.client_order_id)
        path = f"/api/v1/accounts/{account_id}/orders/{tracked_order.exchange_order_id}"
        try:
            resp = await self._api_get(path_url=path)
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                return OrderUpdate(
                    trading_pair=tracked_order.trading_pair,
                    update_timestamp=self.current_timestamp,
                    new_state=OrderState.CANCELED,
                    client_order_id=tracked_order.client_order_id,
                    exchange_order_id=tracked_order.exchange_order_id,
                )
            raise
        return OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=CONSTANTS.ORDER_STATE.get(resp["status"], OrderState.OPEN),
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(resp["id"]),
        )

    async def _all_trade_updates_for_order(
        self, order: InFlightOrder
    ) -> List[TradeUpdate]:
        account_id = self._account_id_for_order(order.client_order_id)
        path = f"/api/v1/accounts/{account_id}/fills"
        fills = await self._api_get(path_url=path, params={"limit": 500})
        trade_updates = []
        for fill in fills:
            if str(fill["order_id"]) == order.exchange_order_id:
                qty = Decimal(fill["qty"])
                price = Decimal(fill["price"])
                notional = qty * price
                fee_amount = Decimal(fill.get("fee", 0))
                fee_pct = (
                    fee_amount / notional if not notional.is_zero() else Decimal("0")
                )
                trade_update = TradeUpdate(
                    trade_id=str(fill["trade_id"]),
                    client_order_id=order.client_order_id,
                    exchange_order_id=order.exchange_order_id,
                    trading_pair=order.trading_pair,
                    fill_base_amount=qty,
                    fill_quote_amount=notional,
                    fill_price=price,
                    fill_timestamp=float(fill["transaction_time"]) / 1000,
                    fee=TradeFeeBase.new_perpetual_fee(
                        fee_schema=self.trade_fee_schema(),
                        position_action=getattr(
                            order, "position_action", PositionAction.NIL
                        ),
                        percent=fee_pct,
                    ),
                )
                trade_updates.append(trade_update)
        return trade_updates

    async def _update_positions_for_role(self, role: str, account_id: str) -> None:
        """Fetch and store position for a single account role."""
        path = f"/api/v1/accounts/{account_id}"
        resp = await self._api_get(path_url=path)
        pos = resp.get("position")
        trading_pair = self._trading_pairs[0] if self._trading_pairs else "BTC-USDT"
        pos_qty = Decimal(str(pos.get("qty", 0))) if pos else Decimal("0")
        role_positions = self._role_positions.setdefault(role, {})
        if pos and not pos_qty.is_zero():
            side = PositionSide.LONG if pos.get("side") == "buy" else PositionSide.SHORT
            signed_qty = pos_qty if side == PositionSide.LONG else -pos_qty
            position = Position(
                trading_pair=trading_pair,
                position_side=side,
                unrealized_pnl=Decimal(str(pos.get("unrealized_pnl", 0))),
                entry_price=Decimal(str(pos.get("avg_entry", 0))),
                amount=signed_qty,
                leverage=Decimal(str(resp.get("leverage", 10))),
            )
            role_positions[trading_pair] = position
        else:
            role_positions.pop(trading_pair, None)

    async def _update_positions(self) -> None:
        await self._ensure_account_exists()
        if self._is_dual_mode:
            for role, account_id in self._role_account_ids.items():
                await self._update_positions_for_role(role, account_id)
            # Do not expose a merged single-pair account_positions view in dual mode.
            # Consumers must use get_positions_for_role(role) to avoid one leg masking the other.
            self._perpetual_trading.account_positions.clear()
        else:
            path = f"/api/v1/accounts/{self._account_id}"
            resp = await self._api_get(path_url=path)
            pos = resp.get("position")
            trading_pair = self._trading_pairs[0] if self._trading_pairs else "BTC-USDT"
            pos_qty = Decimal(str(pos.get("qty", 0))) if pos else Decimal("0")
            if pos and not pos_qty.is_zero():
                side = (
                    PositionSide.LONG
                    if pos.get("side") == "buy"
                    else PositionSide.SHORT
                )
                signed_qty = pos_qty if side == PositionSide.LONG else -pos_qty
                position = Position(
                    trading_pair=trading_pair,
                    position_side=side,
                    unrealized_pnl=Decimal(str(pos.get("unrealized_pnl", 0))),
                    entry_price=Decimal(str(pos.get("avg_entry", 0))),
                    amount=signed_qty,
                    leverage=Decimal(str(resp.get("leverage", 10))),
                )
                self._perpetual_trading.set_position(trading_pair, position)
            else:
                self._perpetual_trading.account_positions.pop(trading_pair, None)

    async def _fetch_last_fee_payment(
        self, trading_pair: str
    ) -> Tuple[int, Decimal, Decimal]:
        # In dual mode, use the long account for funding payment reporting.
        account_id = self._role_account_ids.get("long", self._account_id)
        path = f"/api/v1/accounts/{account_id}/funding-payments"
        payments = await self._api_get(path_url=path, params={"limit": 1})
        return (
            (0, Decimal("-1"), Decimal("-1"))
            if not payments
            else (
                payments[0]["timestamp"],
                Decimal(payments[0]["rate"]),
                Decimal(payments[0]["amount"]),
            )
        )

    async def _set_trading_pair_leverage(
        self, trading_pair: str, leverage: int
    ) -> Tuple[bool, str]:
        await self._ensure_account_exists()
        account_ids = (
            list(self._role_account_ids.values())
            if self._is_dual_mode
            else [self._account_id]
        )
        for account_id in account_ids:
            path = f"/api/v1/accounts/{account_id}/leverage"
            try:
                await self._api_put(path_url=path, data={"leverage": leverage})
            except Exception as e:
                return False, str(e)
        return True, ""

    async def _trading_pair_position_mode_set(
        self, mode: PositionMode, trading_pair: str
    ) -> Tuple[bool, str]:
        if mode != PositionMode.ONEWAY:
            return False, "Exchange only supports ONEWAY mode"
        path = "/api/v1/position-mode"
        try:
            await self._api_request_status_only(
                path_url=path, method=RESTMethod.POST, data={"mode": "oneway"}
            )
            return True, ""
        except Exception as e:
            return False, str(e)

    async def _format_trading_rules(
        self, raw_trading_rules: Dict[str, Any]
    ) -> List[TradingRule]:
        trading_rules = []
        for instrument in raw_trading_rules.get("instruments", []):
            symbol = instrument["symbol"]
            trading_rules.append(
                TradingRule(
                    trading_pair=symbol,
                    min_order_size=Decimal(str(instrument["min_order_size"])),
                    max_order_size=Decimal(
                        str(instrument.get("max_order_size") or "1000000000")
                    ),
                    min_price_increment=Decimal(str(instrument["min_price_increment"])),
                    min_base_amount_increment=Decimal(
                        str(instrument["min_base_amount_increment"])
                    ),
                    min_notional_size=Decimal(
                        str(instrument.get("min_notional_size") or "0")
                    ),
                    buy_order_collateral_token=instrument["collateral_token"],
                    sell_order_collateral_token=instrument["collateral_token"],
                )
            )
        return trading_rules

    def _initialize_trading_pair_symbols_from_exchange_info(
        self, exchange_info: Dict[str, Any]
    ) -> None:
        mapping = bidict()
        for instrument in exchange_info.get("instruments", []):
            symbol = instrument["symbol"]
            mapping[symbol] = symbol
        self._set_trading_pair_symbol_map(mapping)

    async def _user_stream_event_listener(self) -> None:
        async for event_message in self._iter_user_event_queue():
            try:
                message_type = event_message.get("type")
                if message_type == "trade":
                    raw_price = event_message.get("price")
                    if raw_price is not None:
                        tp = (
                            self._trading_pairs[0]
                            if self._trading_pairs
                            else "BTC-USDT"
                        )
                        self._last_known_price[tp] = Decimal(str(raw_price))
                if message_type == "trade":
                    client_id = event_message.get("client_id")
                    if (
                        client_id
                        and client_id in self._order_tracker.all_fillable_orders
                    ):
                        tracked_order = self._order_tracker.all_fillable_orders[
                            client_id
                        ]
                        fee = TradeFeeBase.new_perpetual_fee(
                            fee_schema=self.trade_fee_schema(),
                            position_action=getattr(
                                tracked_order, "position_action", PositionAction.NIL
                            ),
                            percent_token="USDT",
                            percent=Decimal(str(event_message.get("fee_rate", 0))),
                        )
                        trade_update = TradeUpdate(
                            trade_id=str(event_message["trade_id"]),
                            client_order_id=client_id,
                            exchange_order_id=str(event_message["order_id"]),
                            trading_pair=tracked_order.trading_pair,
                            fill_base_amount=Decimal(str(event_message["qty"])),
                            fill_quote_amount=Decimal(str(event_message["qty"]))
                            * Decimal(str(event_message["price"])),
                            fill_price=Decimal(str(event_message["price"])),
                            fill_timestamp=float(
                                event_message.get(
                                    "event_time", self.current_timestamp * 1000
                                )
                            )
                            / 1000,
                            fee=fee,
                        )
                        self._order_tracker.process_trade_update(trade_update)
                        # Refresh position for the affected account only.
                        if self._is_dual_mode and client_id:
                            role = self._order_role_map.get(client_id, "long")
                            account_id = self._role_account_ids.get(
                                role, self._account_id
                            )
                            await self._update_positions_for_role(role, account_id)
                        else:
                            await self._update_positions()
                elif message_type == "order_ack":
                    client_id = event_message.get("client_id")
                    order_id = str(event_message.get("order_id", ""))
                    status_str = event_message.get("status", "open")
                    new_state = CONSTANTS.ORDER_STATE.get(status_str, OrderState.OPEN)
                    if (
                        client_id
                        and client_id in self._order_tracker.all_fillable_orders
                    ):
                        order_update = OrderUpdate(
                            trading_pair=self._order_tracker.all_fillable_orders[
                                client_id
                            ].trading_pair,
                            update_timestamp=self.current_timestamp,
                            new_state=new_state,
                            client_order_id=client_id,
                            exchange_order_id=order_id,
                        )
                        self._order_tracker.process_order_update(order_update)
                elif message_type in ("liquidation", "stop_filled"):
                    trading_pair = (
                        self._trading_pairs[0] if self._trading_pairs else "BTC-USDT"
                    )
                    is_fully_closed = (
                        message_type == "liquidation"
                        and not event_message.get("is_partial", False)
                    ) or (
                        message_type == "stop_filled"
                        and event_message.get("fully_closed", False)
                    )
                    if self._is_dual_mode:
                        # Without a client_id we can't pin the event to a role; refresh all.
                        await self._update_positions()
                    elif is_fully_closed:
                        self._perpetual_trading.account_positions.pop(
                            trading_pair, None
                        )
                    else:
                        await self._update_positions()
                elif message_type == "funding_update":
                    tp_fu = self.trading_pairs[0] if self.trading_pairs else "BTC-USDT"
                    mark = Decimal(str(event_message.get("mark_price") or 0))
                    if mark > 0 and tp_fu not in self._last_known_price:
                        self._last_known_price[tp_fu] = mark
                    funding_info = FundingInfoUpdate(
                        trading_pair=tp_fu,
                        index_price=Decimal(str(event_message.get("fair_value") or 0)),
                        mark_price=mark,
                        next_funding_utc_timestamp=int(
                            event_message.get("next_funding_ts", 0)
                        )
                        // 1000,
                        rate=Decimal(str(event_message.get("rate", 0))),
                    )
                    self._perpetual_trading.funding_info_stream.put_nowait(funding_info)
            except Exception as e:
                self.logger().exception(f"Error processing user stream event: {e}")

    def _get_fee(
        self,
        base_currency: str,
        quote_currency: str,
        order_type: Any,
        order_side: Any,
        position_action: Any = PositionAction.NIL,
        amount: Decimal = Decimal("0"),
        price: Decimal = Decimal("nan"),
        is_maker: Optional[bool] = None,
        **kwargs,
    ) -> TradeFeeBase:
        is_maker_order = (
            is_maker if is_maker is not None else (order_type == OrderType.LIMIT_MAKER)
        )
        return TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            is_maker=is_maker_order,
        )

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(auth=self.authenticator)

    def _is_request_exception_related_to_time_synchronizer(
        self, request_exception: Exception
    ) -> bool:
        return False

    def _is_order_not_found_during_status_update_error(
        self, status_update_exception: Exception
    ) -> bool:
        return (
            "404" in str(status_update_exception)
            or "not found" in str(status_update_exception).lower()
        )

    def _is_order_not_found_during_cancelation_error(
        self, cancelation_exception: Exception
    ) -> bool:
        return (
            "404" in str(cancelation_exception)
            or "not found" in str(cancelation_exception).lower()
        )

    async def list_subaccounts(self) -> List[Dict[str, Any]]:
        """List all subaccounts under the master account. Requires master_account_id to be set."""
        if not self._master_account_id:
            raise ValueError("master_account_id is required to list subaccounts")
        return await self._api_get(
            path_url=f"/api/v1/accounts/{self._master_account_id}/subaccounts"
        )

    async def transfer_funds(
        self, amount: Decimal, to_account_id: str
    ) -> Dict[str, Any]:
        """Transfer funds from master to a subaccount (or between accounts).
        Requires master_account_id to be set."""
        if not self._master_account_id:
            raise ValueError("master_account_id is required to transfer funds")
        body = {"to": to_account_id, "amount": str(amount)}
        return await self._api_post(
            path_url=f"/api/v1/accounts/{self._master_account_id}/transfer", data=body
        )

    async def get_consolidated(self) -> Dict[str, Any]:
        """Get consolidated balance and position across all subaccounts.
        Requires master_account_id to be set."""
        if not self._master_account_id:
            raise ValueError("master_account_id is required to get consolidated view")
        return await self._api_get(
            path_url=f"/api/v1/accounts/{self._master_account_id}/consolidated"
        )

    async def _update_trading_fees(self) -> None:
        pass

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return ExchangeSimAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return ExchangeSimAPIUserStreamDataSource(
            auth=self.authenticator,
            connector=self,
            api_factory=self._web_assistants_factory,
        )
