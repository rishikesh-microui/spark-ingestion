from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from pyspark.sql import SparkSession

from .io.filesystem import HDFSOutbox
from .notification import Heartbeat
from .staging import TimeAwareBufferedSink
from .state import BufferedState, SingleStoreState


def validate_config(cfg: Dict[str, Any]) -> None:
    for key in ["jdbc", "runtime", "tables"]:
        if key not in cfg:
            raise ValueError(f"Missing config key: {key}")
    runtime = cfg["runtime"]
    for key in ["raw_root", "final_root", "timezone"]:
        if key not in runtime:
            raise ValueError(f"Missing runtime.{key}")
    if runtime.get("state_backend", "singlestore") == "singlestore":
        ss = runtime.get("state", {}).get("singlestore")
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


def suggest_singlestore_ddl(logger, cfg: Dict[str, Any]) -> None:
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
        metadata=f"ALTER TABLE {db}.{events} ADD COLUMN IF NOT EXISTS metadata_json TEXT;",
    )


def filter_tables(tables: Iterable[Dict[str, Any]], only_tables: Optional[str]) -> List[Dict[str, Any]]:
    if not only_tables:
        return list(tables)
    allow = {s.strip().lower() for s in only_tables.split(",") if s.strip()}

    def allow_tbl(table_cfg: Dict[str, Any]) -> bool:
        key = f"{table_cfg['schema']}.{table_cfg['table']}".lower()
        return key in allow

    return [tbl for tbl in tables if allow_tbl(tbl)]


def build_state_components(
    spark: SparkSession,
    cfg: Dict[str, Any],
    logger,
) -> Tuple[BufferedState, Optional[TimeAwareBufferedSink]]:
    runtime = cfg["runtime"]
    backend = runtime.get("state_backend", "singlestore")
    if backend != "singlestore":
        base_dir = runtime["state"]["hdfs"]["dir"]
        spark.range(1).write.mode("overwrite").parquet(str(base_dir))
        raise NotImplementedError("HDFSState not included in this file.")

    base_state = SingleStoreState(spark, runtime["state"]["singlestore"])
    outbox = HDFSOutbox(spark, runtime["hdfs_outbox_root"]) if runtime.get("hdfs_outbox_root") else None
    if outbox:
        outbox.restore_all(base_state, logger)
    sink = TimeAwareBufferedSink(
        base_state,
        logger,
        outbox,
        flush_every=runtime.get("state_flush_every", 50),
        flush_age_seconds=runtime.get("state_flush_age_seconds", 120),
    )
    state = BufferedState(
        base_state=base_state,
        source_id=runtime["state"]["singlestore"]["sourceId"],
        flush_every=int(runtime.get("state_flush_every", 50)),
        outbox=outbox,
        time_sink=sink,
        logger=logger,
    )
    return state, sink


def build_heartbeat(logger, cfg: Dict[str, Any], sink: Optional[TimeAwareBufferedSink], args) -> Heartbeat:
    return Heartbeat(
        logger,
        interval_sec=int(cfg["runtime"].get("heartbeat_seconds", getattr(args, "heartbeat_seconds", 120))),
        time_sink=sink,
    )


def summarize_run(results, errors) -> None:
    print("\n=== SUMMARY ===")
    for res in results:
        print(res)
    if errors:
        print("\n=== ERRORS ===")
        for table_name, err in errors:
            print(table_name, err)
