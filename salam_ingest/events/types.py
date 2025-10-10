from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Iterable


class EventCategory(str, Enum):
    STATE = "state"
    PROFILE = "profile"
    CATALOG = "catalog"
    PLAN = "plan"
    TOOL = "tool"
    DEBUG = "debug"


class EventType(str, Enum):
    STATE_MARK = "state.mark"
    STATE_PROGRESS = "state.progress"
    STATE_ERROR = "state.error"
    STATE_WATERMARK = "state.watermark"
    PROFILE_SUMMARY = "profile.summary"
    CATALOG_UPDATE = "catalog.update"
    PLAN_ADAPT = "plan.adapt"
    TOOL_PROGRESS = "tool.progress"
    DEBUG_INFO = "debug.info"


@dataclass
class Event:
    category: EventCategory
    type: EventType
    payload: Dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.utcnow())


class Subscriber:
    """Base subscriber; override interests and on_event."""

    def interests(self) -> Iterable[EventCategory]:
        return []

    def on_event(self, event: Event) -> None:
        raise NotImplementedError
