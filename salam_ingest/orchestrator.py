import argparse
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from .common import PrintLogger
from .events import (
    Emitter,
    StateEventSubscriber,
    StructuredLogSubscriber,
    NotifierSubscriber,
    emit_log,
)
from .notification import Notifier
from .strategies import ExecutionContext
from .orchestrator_helpers import (
    build_heartbeat,
    build_state_components,
    filter_tables,
    summarize_run,
    suggest_singlestore_ddl,
    validate_config,
)
from .state import BufferedState
from .tools.base import ExecutionTool
from .tools.spark import SparkTool
from .staging import Staging
from .metadata import collect_metadata, build_metadata_access
from .ingestion import run_ingestion

def main(
    tool: ExecutionTool,
    cfg: Dict[str, Any],
    args: Optional[argparse.Namespace] = None,
    base_logger: Optional[PrintLogger] = None,
) -> None:
    args = args or argparse.Namespace()
    logger = base_logger or PrintLogger(job_name=cfg["runtime"].get("job_name", "spark_ingest"))
    spark = getattr(tool, "spark", None)
    if spark is None:
        raise RuntimeError("Execution tool must expose a Spark session for current ingestion strategies")
    logger.spark = spark  # help heartbeat expose spark stats if desired
    state, sink, outbox = build_state_components(spark, cfg, logger)
    emitter = Emitter()
    emitter.subscribe(StateEventSubscriber(state))
    tables = filter_tables(cfg["tables"], getattr(args, "only_tables", None))
    collect_metadata(cfg, tables, tool, logger)
    metadata_access = build_metadata_access(cfg, logger)
    logging_cfg = cfg.get("runtime", {}).get("logging", {})
    structured_logger = StructuredLogSubscriber(
        logger,
        job_name=cfg.get("runtime", {}).get("job_name", "ingest"),
        emit_structured=bool(logging_cfg.get("emit_structured", True)),
        event_sink=str(logging_cfg.get("event_sink", "outbox")).lower(),
        outbox=outbox,
    )
    emitter.subscribe(structured_logger)
    context = ExecutionContext(
        spark,
        emitter,
        tool,
        metadata_access=metadata_access,
    )
    hb = build_heartbeat(logger, cfg, sink, args)
    notifier = Notifier(
        spark,
        logger,
        cfg,
        interval_sec=int(getattr(args, "notify_interval_seconds", 300)),
    )
    emitter.subscribe(NotifierSubscriber(notifier))
    notifier.start()
    hb.update(total=len(tables))
    hb.start()
    if not tables:
        emit_log(context.emitter, level="WARN", msg="no_tables_to_run", logger=logger)
        return
    load_date = getattr(args, "load_date", None) or datetime.now().astimezone().strftime("%Y-%m-%d")
    state.preload(spark, tables, load_date)
    emit_log(context.emitter, level="INFO", msg="job_start", tables=len(tables), load_date=load_date, logger=logger)
    results, errors = run_ingestion(context, cfg, state, logger, tables, load_date, hb, notifier)
    state.flush()
    emit_log(context.emitter, level="INFO", msg="job_end", ok=len(results), err=len(errors), logger=logger)
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
    tool = SparkTool.from_config(cfg)
    logger.spark = tool.spark
    try:
        main(tool, cfg, args=args, base_logger=logger)
        if cfg["runtime"].get("staging", {}).get("enabled", True):
            Staging.ttl_cleanup(tool.spark, cfg, logger)
    finally:
        tool.stop()


__all__ = ["main", "parse_args", "run_cli"]
