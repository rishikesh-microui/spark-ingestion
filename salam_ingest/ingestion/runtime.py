from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import time
import traceback
from typing import Any, Dict, List, Tuple

from salam_ingest.common import PrintLogger, RUN_ID
from salam_ingest.events import Event, EventCategory, EventType, emit_log
from salam_ingest.planning import AdaptivePlanner, PlannerRequest
from salam_ingest.planning.base import REGISTRY as PLANNER_REGISTRY
from salam_ingest.strategies import STRATEGY_REGISTRY


def _ingest_one_table(
    context,
    cfg: Dict[str, Any],
    state,
    logger: PrintLogger,
    tbl: Dict[str, Any],
    pool_name: str,
    load_date: str,
) -> Dict[str, Any]:
    _ = AdaptivePlanner  # noqa: F841  (import side-effects)
    from salam_ingest.endpoints.factory import EndpointFactory  # local import to avoid circular dependency

    schema, table = tbl["schema"], tbl["table"]
    mode = tbl.get("mode", "full").lower()
    tool = context.tool
    if tool is None:
        raise RuntimeError("Execution tool is required for ingestion")
    tool.set_job_context(
        pool=pool_name,
        group_id=f"ingest::{schema}.{table}",
        description=f"Ingest {schema}.{table}",
    )
    emit_log(
        context.emitter,
        level="INFO",
        msg="table_start",
        schema=schema,
        table=table,
        mode=mode,
        pool=pool_name,
        load_date=load_date,
        logger=logger,
    )
    context.emit_event(
        EventCategory.INGEST,
        EventType.INGEST_TABLE_START,
        schema=schema,
        table=table,
        mode=mode,
        load_date=load_date,
        pool=pool_name,
    )
    if context.emitter is not None:
        context.emitter.emit(
            Event(
                category=EventCategory.TOOL,
                type=EventType.TOOL_PROGRESS,
                payload={
                    "schema": schema,
                    "table": table,
                    "mode": mode,
                    "pool": pool_name,
                    "status": "started",
                },
            )
        )
    source_ep, sink_ep = EndpointFactory.build_endpoints(
        tool,
        cfg,
        tbl,
        metadata=context.metadata_access,
        emitter=context.emitter,
    )
    try:
        planner = PLANNER_REGISTRY.get("default")
    except KeyError:
        planner = AdaptivePlanner()
        PLANNER_REGISTRY.register("default", planner)
    planner_request = PlannerRequest(
        schema=schema,
        table=table,
        load_date=load_date,
        mode=mode,
        table_cfg={
            "slicing": cfg["runtime"].get("scd1_slicing", {}),
            "runtime": cfg["runtime"],
            "table": tbl,
        },
    )
    strategy = STRATEGY_REGISTRY.get(mode)
    if strategy is None:
        raise ValueError(f"Unsupported mode: {mode}")
    try:
        start_ts = time.time()
        result = strategy.run(
            context,
            cfg,
            state,
            logger,
            source_ep,
            sink_ep,
            planner,
            planner_request,
        )
        duration = time.time() - start_ts
        rows = None
        if isinstance(result, dict):
            rows = result.get("rows") or result.get("rows_written")
        context.emit_event(
            EventCategory.INGEST,
            EventType.INGEST_TABLE_SUCCESS,
            schema=schema,
            table=table,
            mode=mode,
            load_date=load_date,
            rows=rows,
            duration_sec=duration,
        )
        if context.emitter is not None:
            context.emitter.emit(
                Event(
                    category=EventCategory.TOOL,
                    type=EventType.TOOL_PROGRESS,
                    payload={
                        "schema": schema,
                        "table": table,
                        "mode": mode,
                        "pool": pool_name,
                        "status": "success",
                        "result": result,
                    },
                )
            )
        return result
    except Exception as exc:
        stack = traceback.format_exc()
        stack_hash = hashlib.sha1(stack.encode("utf-8")).hexdigest()[:10]
        context.emit_event(
            EventCategory.INGEST,
            EventType.INGEST_TABLE_FAILURE,
            schema=schema,
            table=table,
            mode=mode,
            load_date=load_date,
            error_type=type(exc).__name__,
            message=str(exc),
            error_hash=stack_hash,
        )
        raise
    finally:
        tool.clear_job_context()


def run_ingestion(
    context,
    cfg: Dict[str, Any],
    state,
    logger: PrintLogger,
    tables: List[Dict[str, Any]],
    load_date: str,
    heartbeat,
    notifier,
) -> Tuple[List[Dict[str, Any]], List[Tuple[str, str]]]:
    max_workers = int(cfg["runtime"].get("max_parallel_tables", 4))
    results: List[Dict[str, Any]] = []
    errors: List[Tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futmap = {}
        for idx, tbl in enumerate(tables):
            pool = f"pool-{(idx % max_workers) + 1}"
            fut = executor.submit(_ingest_one_table, context, cfg, state, logger, tbl, pool, load_date)
            futmap[fut] = f"{tbl['schema']}.{tbl['table']}"
            heartbeat.update(inflight=len(futmap))
        for fut in as_completed(futmap):
            key = futmap[fut]
            try:
                res = fut.result()
                results.append(res)
                emit_log(context.emitter, level="INFO", msg="table_done", table=key, result="ok", logger=logger)
                heartbeat.update(done=len(results), inflight=len(futmap) - len(results) - len(errors))
            except Exception as exc:  # pragma: no cover - defensive logging
                stacktrace = traceback.format_exc()
                errors.append((key, str(exc)))
                heartbeat.update(failed=len(errors), inflight=len(futmap) - len(results) - len(errors))
                emit_log(
                    context.emitter,
                    level="ERROR",
                    msg="table_failed",
                    table=key,
                    error=str(exc),
                    error_type=type(exc).__name__,
                    stacktrace=stacktrace,
                    logger=logger,
                )
                if context.emitter is not None:
                    context.emitter.emit(
                        Event(
                            category=EventCategory.TOOL,
                            type=EventType.TOOL_PROGRESS,
                            payload={
                                "table": key,
                                "status": "failed",
                                "error": str(exc),
                            },
                        )
                    )
    return results, errors
