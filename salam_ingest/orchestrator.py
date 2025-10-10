import argparse
import json
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession

from .common import PrintLogger, RUN_ID
from .endpoints import EndpointFactory
from .events import Emitter, StateEventSubscriber, Event, EventCategory, EventType
from .notification import Notifier
from .planning import AdaptivePlanner, PlannerRequest
from .planning.base import REGISTRY as PLANNER_REGISTRY
from .strategies import ExecutionContext, STRATEGY_REGISTRY
from .orchestrator_helpers import (
    build_heartbeat,
    build_state_components,
    filter_tables,
    summarize_run,
    suggest_singlestore_ddl,
    validate_config,
)
from .state import BufferedState
from .tools.spark import SparkTool
from .staging import Staging


def ingest_one_table(
    context: ExecutionContext,
    cfg: Dict[str, Any],
    state: BufferedState,
    logger: PrintLogger,
    tbl: Dict[str, Any],
    pool_name: str,
    load_date: str,
) -> Dict[str, Any]:
    # ensure the default planner is registered
    _ = AdaptivePlanner  # noqa: F841  (import side-effects)

    schema, table = tbl["schema"], tbl["table"]
    mode = tbl.get("mode", "full").lower()
    spark = context.spark
    if spark is not None:
        sc = spark.sparkContext
        sc.setLocalProperty("spark.scheduler.pool", pool_name)
        sc.setJobGroup(f"ingest::{schema}.{table}", f"Ingest {schema}.{table}")
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
    spark_session = context.spark or getattr(context.tool, "spark", None)
    source_ep, sink_ep = EndpointFactory.build_endpoints(context.tool, spark_session, cfg, tbl)
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

def main(
    spark: SparkSession,
    cfg: Dict[str, Any],
    args: Optional[argparse.Namespace] = None,
    base_logger: Optional[PrintLogger] = None,
) -> None:
    args = args or argparse.Namespace()
    logger = base_logger or PrintLogger(job_name=cfg["runtime"].get("job_name", "spark_ingest"))
    logger.spark = spark  # help heartbeat expose spark stats if desired
    state, sink = build_state_components(spark, cfg, logger)
    emitter = Emitter()
    emitter.subscribe(StateEventSubscriber(state))
    tool = SparkTool(spark)
    context = ExecutionContext(spark, emitter, tool)
    hb = build_heartbeat(logger, cfg, sink, args)
    notifier = Notifier(
        spark,
        logger,
        cfg,
        interval_sec=int(getattr(args, "notify_interval_seconds", 300)),
    )
    notifier.start()
    tables = filter_tables(cfg["tables"], getattr(args, "only_tables", None))
    hb.update(total=len(tables))
    hb.start()
    if not tables:
        logger.warn("no_tables_to_run")
        spark.stop()
        return
    load_date = getattr(args, "load_date", None) or datetime.now().astimezone().strftime("%Y-%m-%d")
    state.preload(spark, tables, load_date)
    logger.info("job_start", tables=len(tables), load_date=load_date)
    max_workers = int(cfg["runtime"].get("max_parallel_tables", 4))
    results: List[Dict[str, Any]] = []
    errors: List[Tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futmap = {}
        for idx, tbl in enumerate(tables):
            pool = f"pool-{(idx % max_workers) + 1}"
            fut = executor.submit(ingest_one_table, context, cfg, state, logger, tbl, pool, load_date)
            futmap[fut] = f"{tbl['schema']}.{tbl['table']}"
            hb.update(inflight=len(futmap))
        for fut in as_completed(futmap):
            key = futmap[fut]
            try:
                res = fut.result()
                results.append(res)
                logger.info("table_done", table=key, result="ok")
                notifier.emit("INFO", {"event": "table_done", "table": key})
                hb.update(done=len(results), inflight=len(futmap) - len(results) - len(errors))
            except Exception as exc:
                errors.append((key, str(exc)))
                hb.update(failed=len(errors), inflight=len(futmap) - len(results) - len(errors))
                err_payload = {
                    "table": key,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "stacktrace": traceback.format_exc(),
                }
                logger.error("table_failed", **err_payload)
                notifier.emit("ERROR", {"event": "table_failed", **err_payload})
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
    state.flush()
    logger.info("job_end", ok=len(results), err=len(errors))
    summarize_run(results, errors)
    hb.stop()
    notifier.stop()
    if sink is not None:
        sink.flush()

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--only-tables", help="Comma-separated schema.table filters", default=None)
    parser.add_argument("--load-date", help="Override load_date (YYYY-MM-DD)", default=None)
    parser.add_argument("--start-date", help="Backfill start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Backfill end date (YYYY-MM-DD)")
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Reprocess from RAW only (skip source pull); rebuild intermediate/final for the date window",
    )
    parser.add_argument(
        "--wm-lag-seconds",
        type=int,
        default=0,
        help="Global lag to subtract from watermark when pulling increments",
    )
    parser.add_argument("--heartbeat-seconds", type=int, default=120)
    parser.add_argument("--notify-interval-seconds", type=int, default=300)
    return parser.parse_args(argv)


def run_cli(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    with open(args.config, "r", encoding="utf-8") as handle:
        cfg = json.load(handle)
    validate_config(cfg)
    logger = PrintLogger(job_name=cfg["runtime"].get("job_name", "spark_ingest"), file_path=cfg["runtime"].get("log_file"))
    suggest_singlestore_ddl(logger, cfg)
    builder = (
        SparkSession.builder.appName(cfg["runtime"].get("app_name", "spark_ingest_framework"))
        .config("spark.dynamicAllocation.enabled", cfg["runtime"].get("dynamicAllocation", "true"))
        .config("spark.dynamicAllocation.initialExecutors", cfg["runtime"].get("initialExecutors", "1"))
        .config("spark.dynamicAllocation.maxExecutors", cfg["runtime"].get("maxExecutors", "2"))
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.sql.parquet.int96RebaseModeInWrite", cfg["runtime"].get("int96RebaseModeInWrite", "LEGACY"))
        .config("spark.sql.decimalOperations.allowPrecisionLoss", "true")
        .config("spark.executor.cores", cfg["runtime"].get("executor.cores", "4"))
        .config("spark.executor.memory", cfg["runtime"].get("executor.memory", "8g"))
        .config("spark.driver.memory", cfg["runtime"].get("driver.memory", "6g"))
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", cfg["runtime"].get("timezone", "Asia/Kolkata"))
        .config("spark.jars", ",".join(cfg["runtime"].get("extra_jars", [])))
        .enableHiveSupport()
    )
    spark = builder.getOrCreate()
    try:
        main(spark, cfg, args=args, base_logger=logger)
        if cfg["runtime"].get("staging", {}).get("enabled", True):
            Staging.ttl_cleanup(spark, cfg, logger)
    finally:
        spark.stop()


__all__ = ["ingest_one_table", "main", "parse_args", "run_cli", "suggest_singlestore_ddl", "validate_config"]
