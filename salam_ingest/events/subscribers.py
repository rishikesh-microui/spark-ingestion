from __future__ import annotations

import json
from dataclasses import asdict
from typing import Dict, Iterable, Optional

from salam_ingest.common import PrintLogger, RUN_ID
from salam_ingest.io.filesystem import HDFSOutbox

from .types import Event, EventCategory, EventType, Subscriber


class StructuredLogSubscriber(Subscriber):
    """Writes structured telemetry to the PrintLogger and optional outbox."""

    def __init__(
        self,
        logger: PrintLogger,
        job_name: str,
        *,
        emit_structured: bool = True,
        event_sink: str = "outbox",
        outbox: Optional[HDFSOutbox] = None,
    ) -> None:
        self.logger = logger
        self.job_name = job_name
        self.emit_structured = emit_structured
        self.event_sink = event_sink
        self.outbox = outbox

    def interests(self) -> Iterable[EventCategory]:
        return []

    def on_event(self, event: Event) -> None:
        record = {
            "ts": event.timestamp.astimezone().isoformat(timespec="milliseconds"),
            "event": event.type.value,
            "category": event.category.value,
            "job": self.job_name,
            "run_id": RUN_ID,
            "level": "INFO",
            **event.payload,
        }
        level = record["level"] or "INFO"
        record.pop("level")
        if self.emit_structured:
            self.logger.event(event.type.value, level=level, **record)
        else:
            self.logger.info(event.type.value, **record)
        if self.outbox and self.event_sink in {"outbox", "both"}:
            try:
                self.outbox.mirror_batch("events", [record])
            except Exception:
                self.logger.warn("structured_log_outbox_failed", event=event.type.value)


class NotifierSubscriber(Subscriber):
    """Forwards notable events to the notifier sink."""

    def __init__(self, notifier) -> None:
        self.notifier = notifier

    def interests(self) -> Iterable[EventCategory]:
        return (EventCategory.INGEST, EventCategory.GUARDRAIL)

    def on_event(self, event: Event) -> None:
        if event.type == EventType.INGEST_TABLE_FAILURE:
            payload = {
                "event": "table_failed",
                "table": f"{event.payload.get('schema')}.{event.payload.get('table')}",
                "error": event.payload.get("message"),
                "error_type": event.payload.get("error_type"),
            }
            self.notifier.emit("ERROR", payload)
        elif event.type == EventType.INGEST_TABLE_SUCCESS:
            payload = {
                "event": "table_success",
                "table": f"{event.payload.get('schema')}.{event.payload.get('table')}",
                "rows": event.payload.get("rows"),
            }
            self.notifier.emit("INFO", payload)
        elif event.type == EventType.GUARDRAIL_PRECISION:
            payload = {
                "event": "guardrail_precision",
                "schema": event.payload.get("schema"),
                "table": event.payload.get("table"),
                "status": event.payload.get("status"),
            }
            self.notifier.emit("WARN", payload)
