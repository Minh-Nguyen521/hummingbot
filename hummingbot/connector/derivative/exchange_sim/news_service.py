import json
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence

from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

from . import exchange_sim_constants as CONSTANTS
from . import exchange_sim_web_utils as web_utils
from .news_types import NewsEvent, normalize_severity


class ExchangeSimNewsService:
    def __init__(
        self,
        api_factory: WebAssistantsFactory,
        enabled: bool = False,
        news_feed_path: Optional[str] = None,
        news_endpoint: str = "/api/v1/news",
        poll_interval: float = 30.0,
        lookahead_window: float = 3600.0,
        replay_mode: bool = False,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        self._api_factory = api_factory
        self._enabled = enabled
        self._news_feed_path = news_feed_path
        self._news_endpoint = news_endpoint
        self._poll_interval = poll_interval
        self._lookahead_window = lookahead_window
        self._replay_mode = replay_mode
        self._domain = domain
        self._events_by_id: Dict[str, NewsEvent] = {}
        self._published_event_ids = set()
        self._latest_by_symbol: Dict[str, NewsEvent] = {}
        self._subscribers = set()
        self._initialized = not enabled

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def poll_interval(self) -> float:
        return self._poll_interval

    @property
    def initialized(self) -> bool:
        return self._initialized

    async def refresh(self) -> List[NewsEvent]:
        if not self._enabled:
            self._initialized = True
            return []
        payload = await self._load_payload()
        events = self._normalize_payload(payload)
        self._events_by_id = {event.id: event for event in events}
        self._initialized = True
        return events

    def subscribe(self, callback: Callable[[NewsEvent], None]) -> None:
        self._subscribers.add(callback)

    def unsubscribe(self, callback: Callable[[NewsEvent], None]) -> None:
        self._subscribers.discard(callback)

    def advance_time(self, current_timestamp: float) -> List[NewsEvent]:
        newly_published = []
        for event in self.get_all_events():
            if event.id in self._published_event_ids or event.timestamp > current_timestamp:
                continue
            self._published_event_ids.add(event.id)
            if len(event.symbols) == 0:
                self._latest_by_symbol["*"] = event
            else:
                for symbol in event.symbols:
                    self._latest_by_symbol[symbol] = event
            newly_published.append(event)
            for callback in list(self._subscribers):
                callback(event)
        return newly_published

    def get_all_events(self) -> List[NewsEvent]:
        return sorted(self._events_by_id.values(), key=lambda event: (event.timestamp, event.id))

    def get_upcoming_events(
        self,
        current_timestamp: float,
        window_seconds: Optional[float] = None,
        symbols: Optional[Sequence[str]] = None,
    ) -> List[NewsEvent]:
        if not self._enabled or not self._replay_mode:
            return []
        end_timestamp = current_timestamp + (window_seconds if window_seconds is not None else self._lookahead_window)
        return self._filter_events(
            minimum_timestamp=current_timestamp,
            maximum_timestamp=end_timestamp,
            symbols=symbols,
        )

    def get_active_events(
        self,
        current_timestamp: float,
        before_seconds: float = 0,
        after_seconds: float = 0,
        symbols: Optional[Sequence[str]] = None,
        min_severity: Optional[str] = None,
    ) -> List[NewsEvent]:
        severity_rank = 0 if min_severity is None else NewsEvent(
            id="severity",
            title="severity",
            timestamp=0,
            severity=normalize_severity(min_severity),
            symbols=tuple(),
            source="exchange_sim",
        ).severity_rank
        return [
            event
            for event in self._filter_events(symbols=symbols)
            if event.is_active(current_timestamp, before_seconds=before_seconds, after_seconds=after_seconds)
            and event.severity_rank >= severity_rank
        ]

    def get_latest_event(self, symbol: Optional[str] = None) -> Optional[NewsEvent]:
        if symbol is None:
            latest_events = list(self._latest_by_symbol.values())
            if not latest_events:
                return None
            return max(latest_events, key=lambda event: event.timestamp)
        normalized = symbol.strip().upper()
        return self._latest_by_symbol.get(normalized) or self._latest_by_symbol.get("*")

    async def _load_payload(self):
        if self._news_feed_path:
            path = Path(self._news_feed_path)
            with path.open() as feed:
                return json.load(feed)
        rest_assistant = await self._api_factory.get_rest_assistant()
        return await rest_assistant.execute_request(
            url=web_utils.public_rest_url(self._news_endpoint, domain=self._domain),
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEFAULT_LIMIT_ID,
        )

    def _normalize_payload(self, payload) -> List[NewsEvent]:
        events_payload = payload.get("events", payload) if isinstance(payload, dict) else payload
        normalized_events: Dict[str, NewsEvent] = {}
        for event_payload in events_payload or []:
            event = NewsEvent.from_dict(event_payload)
            existing = normalized_events.get(event.id)
            if existing is None or event.timestamp >= existing.timestamp:
                normalized_events[event.id] = event
        return sorted(normalized_events.values(), key=lambda event: (event.timestamp, event.id))

    def _filter_events(
        self,
        minimum_timestamp: Optional[float] = None,
        maximum_timestamp: Optional[float] = None,
        symbols: Optional[Sequence[str]] = None,
    ) -> List[NewsEvent]:
        normalized_symbols = None
        if symbols:
            normalized_symbols = {symbol.strip().upper() for symbol in symbols if symbol.strip()}
        events = []
        for event in self.get_all_events():
            if minimum_timestamp is not None and event.timestamp < minimum_timestamp:
                continue
            if maximum_timestamp is not None and event.timestamp > maximum_timestamp:
                continue
            if normalized_symbols and not any(event.affects_symbol(symbol) for symbol in normalized_symbols):
                continue
            events.append(event)
        return events
