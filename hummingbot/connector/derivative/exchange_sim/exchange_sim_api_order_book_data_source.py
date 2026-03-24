import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.connector.derivative.exchange_sim import exchange_sim_constants as CONSTANTS
from hummingbot.connector.derivative.exchange_sim import exchange_sim_web_utils as web_utils

if TYPE_CHECKING:
    from hummingbot.connector.derivative.exchange_sim.exchange_sim_derivative import ExchangeSimDerivative


class ExchangeSimAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    """Order book data source for exchange-sim."""

    def __init__(self, trading_pairs: List[str], connector: 'ExchangeSimDerivative',
                 api_factory: WebAssistantsFactory, domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WS_URL, ping_timeout=30)
        return ws

    async def _subscribe_channels(self, ws_assistant: WSAssistant):
        """Subscribe to required WebSocket channels."""
        try:
            for channel in ["l2", "trades", "funding"]:
                await ws_assistant.send(WSJSONRequest(payload={"op": "subscribe", "channel": channel}))
            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error occurred subscribing to order book data streams.")
            raise

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """Route message to appropriate queue based on type."""
        msg_type = event_message.get("type")
        type_map = {
            "l2_update": self._snapshot_messages_queue_key,
            "trade": self._trade_messages_queue_key,
            "funding_update": self._funding_info_messages_queue_key,
        }
        return type_map.get(msg_type, "")

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """Parse order book snapshot message."""
        trading_pair = self._trading_pairs[0] if self._trading_pairs else ""
        bids = [[Decimal(b["price"]), Decimal(b["qty"])] for b in raw_message.get("bids", [])]
        asks = [[Decimal(a["price"]), Decimal(a["qty"])] for a in raw_message.get("asks", [])]

        msg = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": raw_message.get("event_time"),
                "bids": bids,
                "asks": asks,
            },
            timestamp=Decimal(raw_message.get("event_time", 0)) / 1000,
        )
        await message_queue.put(msg)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """Parse trade message."""
        trading_pair = self._trading_pairs[0] if self._trading_pairs else ""
        trade_type = TradeType.BUY if raw_message.get("side") == "buy" else TradeType.SELL

        msg = OrderBookMessage(
            OrderBookMessageType.TRADE,
            {
                "trading_pair": trading_pair,
                "trade_type": trade_type,
                "trade_id": str(raw_message.get("trade_id")),
                "price": Decimal(raw_message.get("price", 0)),
                "amount": Decimal(raw_message.get("qty", 0)),
                "update_id": raw_message.get("event_time"),
            },
            timestamp=Decimal(raw_message.get("event_time", 0)) / 1000,
        )
        await message_queue.put(msg)

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        """Parse funding info update message."""
        trading_pair = self._trading_pairs[0] if self._trading_pairs else ""
        update = FundingInfoUpdate(
            trading_pair=trading_pair,
            index_price=Decimal(raw_message.get("fair_value") or 0),
            mark_price=Decimal(raw_message.get("mark_price") or 0),
            next_funding_utc_timestamp=int(raw_message.get("next_funding_ts", 0)) // 1000,
            rate=Decimal(raw_message.get("rate", 0)),
        )
        await message_queue.put(update)

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """Fetch order book snapshot via REST."""
        rest_assistant = await self._api_factory.get_rest_assistant()
        resp = await rest_assistant.execute_request(
            url=web_utils.public_rest_url("/api/v1/orderbook"),
            params={"depth": 500},
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEFAULT_LIMIT_ID,
        )
        bids = [[Decimal(b["price"]), Decimal(b["qty"])] for b in resp.get("bids", [])]
        asks = [[Decimal(a["price"]), Decimal(a["qty"])] for a in resp.get("asks", [])]
        return OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": resp.get("event_time"),
                "bids": bids,
                "asks": asks,
            },
            timestamp=Decimal(resp.get("event_time", 0)) / 1000,
        )

    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        """Get last traded prices."""
        rest_assistant = await self._api_factory.get_rest_assistant()
        resp = await rest_assistant.execute_request(
            url=web_utils.public_rest_url("/api/v1/ticker"),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEFAULT_LIMIT_ID,
        )
        prices = {}
        for pair in trading_pairs:
            last_price = resp.get("last_price")
            bid = resp.get("bid")
            ask = resp.get("ask")
            if last_price is not None:
                prices[pair] = float(last_price)
            elif bid is not None and ask is not None:
                prices[pair] = (float(bid) + float(ask)) / 2
            elif bid is not None:
                prices[pair] = float(bid)
            elif ask is not None:
                prices[pair] = float(ask)
            else:
                prices[pair] = 0.0
        return prices

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        return True

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        """Get funding info for a trading pair."""
        rest_assistant = await self._api_factory.get_rest_assistant()
        resp = await rest_assistant.execute_request(
            url=web_utils.public_rest_url("/api/v1/funding-rate"),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEFAULT_LIMIT_ID,
        )
        return FundingInfo(
            trading_pair=trading_pair,
            index_price=Decimal(resp.get("fair_value") or 0),
            mark_price=Decimal(resp.get("mark_price") or 0),
            next_funding_utc_timestamp=int(resp.get("next_funding_ts", 0)) // 1000,
            rate=Decimal(resp.get("rate", 0)),
        )
