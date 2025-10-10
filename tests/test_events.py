"""Event bus tests ensuring state subscriber wiring works without Spark."""

import unittest

from salam_ingest.events import Emitter, Event, EventCategory, EventType, StateEventSubscriber


class DummyState:
    def __init__(self):
        self.marked = []
        self.progress = []

    def mark_event(self, *args, **kwargs):  # pragma: no cover - simple storage
        self.marked.append((args, kwargs))

    def set_progress(self, schema, table, watermark, last_loaded_date):
        self.progress.append((schema, table, watermark, last_loaded_date))


class EventBusTest(unittest.TestCase):
    def test_state_subscriber_handles_mark_and_progress(self):
        bus = Emitter()
        state = DummyState()
        bus.subscribe(StateEventSubscriber(state))

        bus.emit(
            Event(
                category=EventCategory.STATE,
                type=EventType.STATE_MARK,
                payload={
                    "schema": "demo",
                    "table": "orders",
                    "load_date": "2024-01-01",
                    "mode": "full",
                    "phase": "raw",
                    "status": "started",
                },
            )
        )
        bus.emit(
            Event(
                category=EventCategory.STATE,
                type=EventType.STATE_WATERMARK,
                payload={
                    "schema": "demo",
                    "table": "orders",
                    "watermark": "2024-01-01",
                    "last_loaded_date": "2024-01-01",
                },
            )
        )

        self.assertEqual(len(state.marked), 1)
        self.assertEqual(len(state.progress), 1)
        _, kwargs = state.marked[0]
        self.assertEqual(kwargs["schema"], "demo")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
