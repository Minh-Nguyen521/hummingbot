import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from hummingbot.connector.derivative.exchange_sim import exchange_sim_web_utils as web_utils
from hummingbot.connector.derivative.exchange_sim.news_service import ExchangeSimNewsService


class ExchangeSimNewsServiceTest(unittest.TestCase):
    def test_refresh_deduplicates_and_queries_news_feed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            feed_path = Path(temp_dir) / "news.json"
            feed_path.write_text(json.dumps({
                "events": [
                    {
                        "id": "duplicate",
                        "title": "Older duplicate",
                        "timestamp": 900,
                        "severity": "low",
                        "symbols": ["BTC-USDT"],
                        "source": "fixture",
                    },
                    {
                        "id": "duplicate",
                        "title": "Newest duplicate",
                        "timestamp": 1200,
                        "severity": "critical",
                        "symbols": ["BTC-USDT"],
                        "source": "fixture",
                    },
                    {
                        "id": "future",
                        "title": "Future event",
                        "timestamp": 1500,
                        "severity": "high",
                        "symbols": ["BTC-USDT", "ETH-USDT"],
                        "source": "fixture",
                    },
                ]
            }))
            published = []
            service = ExchangeSimNewsService(
                api_factory=web_utils.build_api_factory(),
                enabled=True,
                news_feed_path=str(feed_path),
                replay_mode=True,
                lookahead_window=400,
            )
            service.subscribe(published.append)

            asyncio.run(service.refresh())

            all_events = service.get_all_events()
            self.assertEqual(2, len(all_events))
            self.assertEqual("Newest duplicate", all_events[0].title)

            upcoming = service.get_upcoming_events(current_timestamp=1100, window_seconds=500, symbols=["BTC-USDT"])
            self.assertEqual(["duplicate", "future"], [event.id for event in upcoming])

            active = service.get_active_events(
                current_timestamp=1150,
                before_seconds=100,
                after_seconds=100,
                symbols=["BTC-USDT"],
                min_severity="critical",
            )
            self.assertEqual(["duplicate"], [event.id for event in active])

            service.advance_time(1300)
            self.assertEqual(["duplicate"], [event.id for event in published])
            self.assertEqual("duplicate", service.get_latest_event("BTC-USDT").id)

    def test_upcoming_news_hidden_when_replay_mode_disabled(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            feed_path = Path(temp_dir) / "news.json"
            feed_path.write_text(json.dumps([
                {
                    "id": "future",
                    "title": "Future event",
                    "timestamp": 2000,
                    "severity": "high",
                    "symbols": ["BTC-USDT"],
                    "source": "fixture",
                }
            ]))
            service = ExchangeSimNewsService(
                api_factory=web_utils.build_api_factory(),
                enabled=True,
                news_feed_path=str(feed_path),
                replay_mode=False,
            )
            asyncio.run(service.refresh())

            self.assertEqual([], service.get_upcoming_events(current_timestamp=1000, window_seconds=2000))
