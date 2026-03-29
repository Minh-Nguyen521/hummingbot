# ExchangeSimDerivative connector implementation
# Stub file - will be implemented with proper Hummingbot base class methods

from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

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
    PriceType,
    PositionAction,
    PositionMode,
    PositionSide,
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


class ExchangeSimDerivative(PerpetualDerivativePyBase):
    """ExchangeSimDerivative connector for Hummingbot perpetual futures trading."""

    DEFAULT_DOMAIN = "exchange_sim"
    DEFAULT_ACCOUNT_BALANCE = Decimal("100000")
    DEFAULT_ACCOUNT_LEVERAGE = 10
    DEFAULT_MAKER_FEE_BPS = 2
    DEFAULT_TAKER_FEE_BPS = 5
    web_utils = web_utils

    def __init__(
        self,
        exchange_sim_account_id: str,
        balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
        rate_limits_share_pct: Decimal = Decimal("100"),
        trading_pairs: Optional[List[str]] = None,
        trading_required: bool = True,
        domain: str = DEFAULT_DOMAIN,
        exchange_sim_ref_price: Optional[Decimal] = None,
    ):
        self._account_id = exchange_sim_account_id
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        self._position_mode = None
        self._account_initialized = False
        self._ref_price = exchange_sim_ref_price
        # Fallback price used when one or both sides of the orderbook are empty.
        # Updated from every trade and BBO event so it stays fresh.
        self._last_known_price: Dict[str, Decimal] = {}
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        self._real_time_balance_update = False
        # Initialize the symbol map via the Cython setter so trading_pair_symbol_map_ready() sees it
        self._set_trading_pair_symbol_map(bidict())

    @property
    def authenticator(self) -> ExchangeSimAuth:
        return ExchangeSimAuth()

    @property
    def name(self) -> str:
        return "exchange_sim"

    @property
    def status_dict(self) -> Dict[str, bool]:
        sd = super().status_dict
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
    # instead of returning the raw wide BBO (e.g. 0.002 = 0.2% max spread)
    _MAX_BBO_SPREAD = Decimal("0.002")

    def _fallback_reference_price(self, trading_pair: str) -> Decimal:
        ref = self._last_known_price.get(trading_pair)
        if ref is not None and not ref.is_nan() and ref > 0:
            return ref
        if self._ref_price is not None and self._ref_price > 0:
            return self._ref_price
        return Decimal("nan")

    def get_price(self, trading_pair: str, is_buy: bool) -> Decimal:
        """Return best ask (is_buy=True) or best bid (is_buy=False).

        Fallback hierarchy when the requested side is empty:
        1. Real BBO price — clamped to mid ± _ONE_SIDE_HALF_SPREAD if spread > _MAX_BBO_SPREAD.
        2. Use the opposite side ± full spread (_ONE_SIDE_HALF_SPREAD * 2).
        3. Use _last_known_price ± half-spread.
        4. Configured ref_price ± half-spread.
        5. Return NaN (strategy will skip the tick).

        We probe the orderbook directly to avoid the noisy "Ask orderbook is empty"
        warning that fires on every call to super().get_price() when the side is empty.
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
                    # Clamp wide BBO: if spread > threshold, return mid ± tiny offset
                    if mid > 0 and (ask - bid) / mid > self._MAX_BBO_SPREAD:
                        return mid * (1 + self._ONE_SIDE_HALF_SPREAD) if is_buy else mid * (1 - self._ONE_SIDE_HALF_SPREAD)
                return price

        # Requested side is empty. Try the opposite side without triggering warning.
        has_opposite = has_bid if is_buy else has_ask
        if has_opposite:
            opposite = super().get_price(trading_pair, not is_buy)
            if not opposite.is_nan():
                self._last_known_price[trading_pair] = opposite
                if is_buy:
                    return opposite * (1 + self._ONE_SIDE_HALF_SPREAD * 2)
                else:
                    return opposite * (1 - self._ONE_SIDE_HALF_SPREAD * 2)

        # Both sides empty. Fall back to last known trade price.
        ref = self._fallback_reference_price(trading_pair)
        if not ref.is_nan():
            if is_buy:
                return ref * (1 + self._ONE_SIDE_HALF_SPREAD)
            else:
                return ref * (1 - self._ONE_SIDE_HALF_SPREAD)

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

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        return "USDT"

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        return "USDT"

    async def _api_request(self, *args, **kwargs):
        kwargs.setdefault("limit_id", CONSTANTS.DEFAULT_LIMIT_ID)
        return await super()._api_request(*args, **kwargs)

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

    async def _ensure_account_exists(self) -> None:
        if self._account_initialized:
            return

        account_path = f"/api/v1/accounts/{self._account_id}"
        try:
            await self._api_get(path_url=account_path)
            self._account_initialized = True
            return
        except Exception as ex:
            if not self._is_account_missing_error(ex):
                raise

        create_body = {
            "id": self._account_id,
            "balance": str(self.DEFAULT_ACCOUNT_BALANCE),
            "leverage": self.DEFAULT_ACCOUNT_LEVERAGE,
            "maker_fee_bps": self.DEFAULT_MAKER_FEE_BPS,
            "taker_fee_bps": self.DEFAULT_TAKER_FEE_BPS,
        }
        try:
            await self._api_post(path_url="/api/v1/accounts", data=create_body)
        except Exception as ex:
            if not self._is_account_already_exists_error(ex):
                raise

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
        path = f"/api/v1/accounts/{self._account_id}/orders"
        try:
            resp = await self._api_post(path_url=path, data=body)
        except Exception as e:
            err = str(e)
            if "reduce-only" in err.lower() or "increase position" in err.lower() or "no open position" in err.lower():
                # Exchange says there's no matching position to close.
                # Sync immediately so the strategy stops retrying with stale state.
                trading_pair = self._trading_pairs[0] if self._trading_pairs else "BTC-USDT"
                self._perpetual_trading.account_positions.pop(trading_pair, None)
                await self._update_positions()
            raise
        return str(resp["order_id"]), self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder) -> bool:
        path = f"/api/v1/accounts/{self._account_id}/orders/{tracked_order.exchange_order_id}"
        try:
            status = await self._api_request_status_only(
                path_url=path, method=RESTMethod.DELETE
            )
            return 200 <= status < 300
        except Exception as e:
            # 404 = order already gone from exchange, treat as cancelled successfully
            if "404" in str(e) or "not found" in str(e).lower():
                return True
            return False

    async def _update_balances(self) -> None:
        await self._ensure_account_exists()
        path = f"/api/v1/accounts/{self._account_id}"
        resp = await self._api_get(path_url=path)
        self._account_balances["USDT"] = Decimal(resp["balance"])
        self._account_available_balances["USDT"] = Decimal(resp["balance"]) - Decimal(
            resp.get("reserved_margin", 0)
        )

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        path = f"/api/v1/accounts/{self._account_id}/orders/{tracked_order.exchange_order_id}"
        try:
            resp = await self._api_get(path_url=path)
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                # Order no longer exists on the exchange (e.g. exchange restarted).
                # Mark it as cancelled so Hummingbot stops polling for it.
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
        path = f"/api/v1/accounts/{self._account_id}/fills"
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
                        position_action=getattr(order, "position_action", PositionAction.NIL),
                        percent=fee_pct,
                    ),
                )
                trade_updates.append(trade_update)
        return trade_updates

    async def _update_positions(self) -> None:
        await self._ensure_account_exists()
        path = f"/api/v1/accounts/{self._account_id}"
        resp = await self._api_get(path_url=path)
        pos = resp.get("position")
        trading_pair = self._trading_pairs[0] if self._trading_pairs else "BTC-USDT"
        pos_qty = Decimal(str(pos.get("qty", 0))) if pos else Decimal("0")
        if pos and not pos_qty.is_zero():
            side = PositionSide.LONG if pos.get("side") == "buy" else PositionSide.SHORT
            position = Position(
                trading_pair=trading_pair,
                position_side=side,
                unrealized_pnl=Decimal(str(pos.get("unrealized_pnl", 0))),
                entry_price=Decimal(str(pos.get("avg_entry", 0))),
                amount=pos_qty,
                leverage=Decimal(str(resp.get("leverage", 10))),
            )
            self._perpetual_trading.set_position(trading_pair, position)
        else:
            self._perpetual_trading.account_positions.pop(trading_pair, None)

    async def _fetch_last_fee_payment(
        self, trading_pair: str
    ) -> Tuple[int, Decimal, Decimal]:
        path = f"/api/v1/accounts/{self._account_id}/funding-payments"
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
        path = f"/api/v1/accounts/{self._account_id}/leverage"
        try:
            await self._api_put(path_url=path, data={"leverage": leverage})
            return True, ""
        except Exception as e:
            return False, str(e)

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
                # Keep _last_known_price fresh from every trade event.
                if message_type == "trade":
                    raw_price = event_message.get("price")
                    if raw_price is not None:
                        tp = self._trading_pairs[0] if self._trading_pairs else "BTC-USDT"
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
                            position_action=getattr(tracked_order, "position_action", PositionAction.NIL),
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
                        # Refresh position immediately so Hummingbot knows the true
                        # side/qty after the fill (avoids stale reduce-only failures).
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
                    # Position was force-closed or stop-filled. Sync position immediately
                    # rather than waiting for the next polling cycle — otherwise Hummingbot
                    # keeps trying to place reduce-only close orders against a flat account.
                    trading_pair = self._trading_pairs[0] if self._trading_pairs else "BTC-USDT"
                    is_fully_closed = (
                        message_type == "liquidation" and not event_message.get("is_partial", False)
                    ) or (
                        message_type == "stop_filled" and event_message.get("fully_closed", False)
                    )
                    if is_fully_closed:
                        self._perpetual_trading.account_positions.pop(trading_pair, None)
                    else:
                        # Partial — refresh from REST to get the exact remaining qty.
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
