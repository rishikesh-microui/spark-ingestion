import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pyspark.sql import SparkSession

from .common import PrintLogger, RUN_ID
from .io import HDFSOutbox, Paths
from .notification import Heartbeat, Notifier
from .staging import Staging, TimeAwareBufferedSink
from .state import BufferedState, SingleStoreState
from .strategies import IngestionFull, IngestionSCD1


def ingest_one_table(
    spark: SparkSession,
    cfg: Dict[str, Any],
    state: BufferedState,
    logger: PrintLogger,
    tbl: Dict[str, Any],
    pool_name: str,
    load_date: str,
) -> Dict[str, Any]:
    schema, table = tbl["schema"], tbl["table"]
    mode = tbl.get("mode", "full").lower()
    rt = cfg["runtime"]
    raw_dir, final_dir, base_raw = Paths.build(rt, schema, table, load_date)
    sc = spark.sparkContext
    sc.setLocalProperty("spark.scheduler.pool", pool_name)
    sc.setJobGroup(f"ingest::{schema}.{table}", f"Ingest {schema}.{table} -> {raw_dir}")
    logger.info("table_start", schema=schema, table=table, mode=mode, pool=pool_name, load_date=load_date)
    if mode == "full":
        return IngestionFull.run(
            spark, cfg, state, logger, schema, table, load_date, raw_dir, final_dir, tbl
        )
    if mode == "scd1":
        return IngestionSCD1.run(
            spark, cfg, state, logger, schema, table, load_date, raw_dir, base_raw, tbl
        )
    raise ValueError(f"Unsupported mode: {mode}")

def validate_config(cfg: Dict[str, Any]) -> None:
    for key in ["jdbc", "runtime", "tables"]:
        if key not in cfg:
            raise ValueError(f"Missing config key: {key}")
    rt = cfg["runtime"]
    for key in ["raw_root", "final_root", "timezone"]:
        if key not in rt:
            raise ValueError(f"Missing runtime.{key}")
    if rt.get("state_backend", "singlestore") == "singlestore":
        ss = rt.get("state", {}).get("singlestore")
        if not ss:
            raise ValueError("runtime.state.singlestore required")
        required = [
            "ddlEndpoint",
            "user",
            "password",
            "database",
            "eventsTable",
            "watermarksTable",
            "sourceId",
        ]
        for key in required:
            if key not in ss:
                raise ValueError(f"Missing singlestore.{key}")

def suggest_singlestore_ddl(logger: PrintLogger, cfg: Dict[str, Any]) -> None:
    ss = cfg["runtime"]["state"]["singlestore"]
    events = ss["eventsTable"]
    wm = ss["watermarksTable"]
    db = ss["database"]
    logger.info(
        "singlestore_recommended_ddl",
        events=f"ALTER TABLE {db}.{events} ADD COLUMN IF NOT EXISTS ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP;",
        wm=(
            f"ALTER TABLE {db}.{wm} ADD COLUMN IF NOT EXISTS updated_at "
            "TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;"
        ),
    )

def _filter_tables(tables: Iterable[Dict[str, Any]], only_tables: Optional[str]) -> List[Dict[str, Any]]:
    if not only_tables:
        return list(tables)
    allow = {s.strip().lower() for s in only_tables.split(",") if s.strip()}

    def allow_tbl(table_cfg: Dict[str, Any]) -> bool:
        key = f"{table_cfg['schema']}.{table_cfg['table']}".lower()
        return key in allow

    return [tbl for tbl in tables if allow_tbl(tbl)]

def main(
    spark: SparkSession,
    cfg: Dict[str, Any],
    args: Optional[argparse.Namespace] = None,
    base_logger: Optional[PrintLogger] = None,
) -> None:
    args = args or argparse.Namespace()
    logger = base_logger or PrintLogger(job_name=cfg["runtime"].get("job_name", "spark_ingest"))
    logger.spark = spark  # help heartbeat expose spark stats if desired
    backend = cfg["runtime"].get("state_backend", "singlestore")
    sink: Optional[TimeAwareBufferedSink] = None
    if backend == "singlestore":
        base_state = SingleStoreState(spark, cfg["runtime"]["state"]["singlestore"])
        outbox = (
            HDFSOutbox(spark, cfg["runtime"]["hdfs_outbox_root"])
            if cfg["runtime"].get("hdfs_outbox_root")
            else None
        )
        if outbox:
            outbox.restore_all(base_state, logger)
        sink = TimeAwareBufferedSink(
            base_state,
            logger,
            outbox,
            flush_every=cfg["runtime"].get("state_flush_every", 50),
            flush_age_seconds=cfg["runtime"].get("state_flush_age_seconds", 120),
        )
        state = BufferedState(
            base_state=base_state,
            source_id=cfg["runtime"]["state"]["singlestore"]["sourceId"],
            flush_every=int(cfg["runtime"].get("state_flush_every", 50)),
            outbox=outbox,
            time_sink=sink,
            logger=logger,
        )
    else:
        base_dir = cfg["runtime"]["state"]["hdfs"]["dir"]
        spark.range(1).write.mode("overwrite").parquet(str(base_dir))
        raise NotImplementedError("HDFSState not included in this file.")
    hb = Heartbeat(
        logger,
        interval_sec=int(
            cfg["runtime"].get(
                "heartbeat_seconds",
                getattr(args, "heartbeat_seconds", 120),
            )
        ),
        time_sink=sink,
    )
    notifier = Notifier(
        spark,
        logger,
        cfg,
        interval_sec=int(getattr(args, "notify_interval_seconds", 300)),
    )
    notifier.start()
    tables = _filter_tables(cfg["tables"], getattr(args, "only_tables", None))
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
            fut = executor.submit(ingest_one_table, spark, cfg, state, logger, tbl, pool, load_date)
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
                logger.error("table_failed", table=key, err=str(exc))
                notifier.emit("ERROR", {"event": "table_failed", "table": key, "error": str(exc)})
    notifier.emit("INFO", {"event": "job_summary", "ok": len(results), "err": len(errors), "run_id": RUN_ID})
    state.flush()
    logger.info("job_end", ok=len(results), err=len(errors))
    print("\n=== SUMMARY ===")
    for res in results:
        print(res)
    if errors:
        print("\n=== ERRORS ===")
        for table_name, err in errors:
            print(table_name, err)
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
