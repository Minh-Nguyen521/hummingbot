import asyncio
from typing import TYPE_CHECKING, Any, Dict, Optional

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.connector.derivative.exchange_sim import exchange_sim_constants as CONSTANTS
from hummingbot.connector.derivative.exchange_sim.exchange_sim_auth import ExchangeSimAuth

if TYPE_CHECKING:
    from hummingbot.connector.derivative.exchange_sim.exchange_sim_derivative import ExchangeSimDerivative


class ExchangeSimAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """User stream data source for exchange-sim."""

    def __init__(self, auth: ExchangeSimAuth, connector: 'ExchangeSimDerivative',
                 api_factory: WebAssistantsFactory, domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """Connect to WebSocket and return assistant."""
        ws = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WS_URL, ping_timeout=30)
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """Subscribe to trades and funding channels."""
        try:
            for channel in ["trades", "funding", "order_updates", "liquidations", "stop_orders"]:
                await websocket_assistant.send(WSJSONRequest(payload={"op": "subscribe", "channel": channel}))
            self.logger().info("Subscribed to private user stream channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to user streams...")
            raise

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        """Process and queue event message."""
        msg_type = event_message.get("type", "")
        if msg_type in ("trade", "funding_update", "order_ack", "liquidation", "stop_filled"):
            queue.put_nowait(event_message)
