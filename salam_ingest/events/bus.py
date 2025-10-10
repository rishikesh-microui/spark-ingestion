from __future__ import annotations

from threading import RLock
from typing import Iterable, List

from .types import Event, EventCategory, Subscriber


class Emitter:
    """Thread-safe event emitter supporting category filtering."""

    def __init__(self) -> None:
        self._subscribers: List[Subscriber] = []
        self._lock = RLock()

    def subscribe(self, subscriber: Subscriber) -> None:
        with self._lock:
            self._subscribers.append(subscriber)

    def unsubscribe(self, subscriber: Subscriber) -> None:
        with self._lock:
            self._subscribers = [s for s in self._subscribers if s is not subscriber]

    def emit(self, event: Event) -> None:
        with self._lock:
            subscribers = list(self._subscribers)
        for sub in subscribers:
            if not sub.interests():
                sub.on_event(event)
            elif event.category in sub.interests():
                sub.on_event(event)
