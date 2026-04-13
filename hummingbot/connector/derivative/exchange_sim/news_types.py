import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple


_SEVERITY_RANK = {
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}


def _normalize_timestamp(value: Any) -> float:
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 1e12:
            timestamp /= 1000
        return timestamp
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return _normalize_timestamp(int(stripped))
        dt = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    raise ValueError(f"Unsupported news event timestamp: {value!r}")


def normalize_severity(value: Any) -> str:
    normalized = str(value or "medium").strip().lower()
    if normalized not in _SEVERITY_RANK:
        return "medium"
    return normalized


def normalize_sentiment(value: Any) -> Optional[str]:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in {"bullish", "positive", "up", "buy"}:
        return "positive"
    if normalized in {"bearish", "negative", "down", "sell"}:
        return "negative"
    if normalized in {"neutral", "flat", "mixed"}:
        return "neutral"
    return normalized or None


@dataclass(frozen=True)
class NewsEvent:
    id: str
    title: str
    timestamp: float
    severity: str
    symbols: Tuple[str, ...]
    source: str
    sentiment: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "NewsEvent":
        event_id = str(payload.get("id") or payload.get("event_id") or payload.get("news_id") or "").strip()
        if event_id == "":
            raise ValueError("News event missing id")
        title = str(payload.get("title") or payload.get("headline") or "").strip()
        if title == "":
            raise ValueError("News event missing title")
        timestamp = _normalize_timestamp(payload.get("timestamp") or payload.get("published_at") or payload.get("time"))
        symbols = payload.get("symbols") or payload.get("trading_pairs") or []
        if isinstance(symbols, str):
            symbols = [symbol.strip() for symbol in symbols.split(",") if symbol.strip()]
        normalized_symbols = tuple(sorted({str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}))
        source = str(payload.get("source") or "exchange_sim").strip() or "exchange_sim"
        sentiment = payload.get("sentiment")
        normalized_sentiment = normalize_sentiment(sentiment)
        metadata = {
            key: value
            for key, value in payload.items()
            if key not in {"id", "event_id", "news_id", "title", "headline", "timestamp", "published_at",
                           "time", "severity", "symbols", "trading_pairs", "source", "sentiment"}
        }
        return cls(
            id=event_id,
            title=title,
            timestamp=timestamp,
            severity=normalize_severity(payload.get("severity")),
            symbols=normalized_symbols,
            source=source,
            sentiment=normalized_sentiment,
            metadata=metadata,
        )

    @property
    def severity_rank(self) -> int:
        return _SEVERITY_RANK[self.severity]

    def affects_symbol(self, symbol: str) -> bool:
        normalized = symbol.strip().upper()
        return len(self.symbols) == 0 or normalized in self.symbols

    def is_active(self, current_timestamp: float, before_seconds: float = 0, after_seconds: float = 0) -> bool:
        if math.isnan(current_timestamp):
            return False
        return (self.timestamp - before_seconds) <= current_timestamp <= (self.timestamp + after_seconds)
