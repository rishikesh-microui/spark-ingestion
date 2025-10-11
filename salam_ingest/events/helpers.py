from __future__ import annotations

from typing import Any, Dict, Optional

from .types import Event, EventCategory, EventType


def emit_state_mark(
    context,
    state,
    *,
    schema: str,
    table: str,
    load_date: str,
    mode: str,
    phase: str,
    status: str,
    rows_written: Optional[int] = None,
    watermark: Optional[str] = None,
    location: Optional[str] = None,
    strategy: Optional[str] = None,
    error: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    if getattr(context, "emitter", None) is not None:
        context.emitter.emit(
            Event(
                category=EventCategory.STATE,
                type=EventType.STATE_MARK,
                payload={
                    "schema": schema,
                    "table": table,
                    "load_date": load_date,
                    "mode": mode,
                    "phase": phase,
                    "status": status,
                    "rows_written": rows_written,
                    "watermark": watermark,
                    "location": location,
                    "strategy": strategy,
                    "error": error,
                    "extra": extra,
                },
            )
        )
    else:
        state.mark_event(
            schema,
            table,
            load_date,
            mode,
            phase,
            status,
            rows_written=rows_written,
            watermark=watermark,
            location=location,
            strategy=strategy,
            error=error,
            extra=extra,
        )


def emit_state_watermark(
    context,
    state,
    *,
    schema: str,
    table: str,
    watermark: str,
    last_loaded_date: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    if getattr(context, "emitter", None) is not None:
        context.emitter.emit(
            Event(
                category=EventCategory.STATE,
                type=EventType.STATE_WATERMARK,
                payload={
                    "schema": schema,
                    "table": table,
                    "watermark": watermark,
                    "last_loaded_date": last_loaded_date,
                    "extra": extra,
                },
            )
        )
    else:
        state.set_progress(schema, table, watermark, last_loaded_date)


def emit_log(
    emitter,
    *,
    level: str,
    msg: str,
    logger=None,
    **payload: Any,
) -> None:
    record = {"level": level.upper(), "msg": msg, **payload}
    if emitter is not None:
        emitter.emit(Event(category=EventCategory.LOG, type=EventType.LOG, payload=record))
    elif logger is not None:
        logger.log(level.upper(), msg, **payload)
