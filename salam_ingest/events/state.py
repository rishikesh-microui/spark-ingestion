from __future__ import annotations

from typing import Dict, Iterable, Optional

from .types import Event, EventCategory, EventType, Subscriber
from ..state import BufferedState


class StateEventSubscriber(Subscriber):
    """Updates the BufferedState when receiving state events."""

    def __init__(self, state: BufferedState) -> None:
        self.state = state

    def interests(self) -> Iterable[EventCategory]:
        return (EventCategory.STATE,)

    def on_event(self, event: Event) -> None:
        payload = event.payload
        if event.type == EventType.STATE_MARK:
            self._handle_mark(payload)
        elif event.type == EventType.STATE_WATERMARK:
            self._handle_watermark(payload)
        elif event.type == EventType.STATE_ERROR:
            self._handle_error(payload)

    def _handle_mark(self, payload: Dict[str, object]) -> None:
        extra = payload.get("extra")
        self.state.mark_event(
            schema=str(payload["schema"]),
            table=str(payload["table"]),
            load_date=str(payload["load_date"]),
            mode=str(payload["mode"]),
            phase=str(payload["phase"]),
            status=str(payload["status"]),
            rows_written=payload.get("rows_written"),
            watermark=payload.get("watermark"),
            location=payload.get("location"),
            strategy=payload.get("strategy"),
            error=payload.get("error"),
            extra=extra if isinstance(extra, dict) else None,
        )

    def _handle_watermark(self, payload: Dict[str, object]) -> None:
        self.state.set_progress(
            schema=str(payload["schema"]),
            table=str(payload["table"]),
            watermark=str(payload["watermark"]),
            last_loaded_date=str(payload["last_loaded_date"]),
        )

    def _handle_error(self, payload: Dict[str, object]) -> None:
        # Errors are recorded as mark_event with status=failed
        self._handle_mark(payload)
