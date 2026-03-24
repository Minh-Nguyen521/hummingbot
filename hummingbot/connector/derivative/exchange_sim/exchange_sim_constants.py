from decimal import Decimal
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

REST_URL = "http://localhost:8080"
WS_URL = "ws://localhost:8080/ws"

DEFAULT_DOMAIN = "exchange_sim"

# Maps exchange order status strings → Hummingbot OrderState
ORDER_STATE = {
    "open": OrderState.OPEN,
    "pending": OrderState.OPEN,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "cancelled": OrderState.CANCELED,
    "rejected": OrderState.FAILED,
}

DEFAULT_LIMIT_ID = "DEFAULT"
RATE_LIMITS = [
    RateLimit(limit_id=DEFAULT_LIMIT_ID, limit=1000, time_interval=1),
]
