from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from typing import Any, Dict, List, Tuple

from salam_ingest.common import PrintLogger, RUN_ID
from salam_ingest.endpoints import EndpointFactory
from salam_ingest.events import Event, EventCategory, EventType
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
    logger.info(
        "table_start",
        schema=schema,
        table=table,
        mode=mode,
        pool=pool_name,
        load_date=load_date,
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
    source_ep, sink_ep = EndpointFactory.build_endpoints(tool, cfg, tbl)
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
                logger.info("table_done", table=key, result="ok")
                notifier.emit("INFO", {"event": "table_done", "table": key})
                heartbeat.update(done=len(results), inflight=len(futmap) - len(results) - len(errors))
            except Exception as exc:  # pragma: no cover - defensive logging
                stacktrace = traceback.format_exc()
                errors.append((key, str(exc)))
                heartbeat.update(failed=len(errors), inflight=len(futmap) - len(results) - len(errors))
                notifier_payload = {
                    "event": "table_failed",
                    "table": key,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "stacktrace": stacktrace,
                }
                notifier.emit("ERROR", notifier_payload)
                logger.error(
                    "table_failed",
                    table=key,
                    error=str(exc),
                    error_type=type(exc).__name__,
                    stacktrace=stacktrace,
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
    notifier.emit("INFO", {"event": "job_summary", "ok": len(results), "err": len(errors), "run_id": RUN_ID})
    return results, errors
