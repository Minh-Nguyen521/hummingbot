from typing import Optional

from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from . import exchange_sim_constants as CONSTANTS
from .exchange_sim_auth import ExchangeSimAuth


def public_rest_url(path: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.REST_URL + path


def private_rest_url(path: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return public_rest_url(path, domain=domain)


def build_api_factory(auth: ExchangeSimAuth = None) -> WebAssistantsFactory:
    throttler = AsyncThrottler(rate_limits=CONSTANTS.RATE_LIMITS)
    return WebAssistantsFactory(throttler=throttler, auth=auth)


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    throttler = throttler or AsyncThrottler(rate_limits=CONSTANTS.RATE_LIMITS)
    api_factory = WebAssistantsFactory(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request(
        url=public_rest_url("/api/v1/time", domain=domain),
        throttler_limit_id=CONSTANTS.DEFAULT_LIMIT_ID,
        method=RESTMethod.GET,
    )
    return float(response.get("serverTime", 0)) * 1e6  # convert ms → ns for TimeSynchronizer
