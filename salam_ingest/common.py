import json
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# -------------------------
# Global run identifiers
# -------------------------
RUN_ID = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
_RUN_COUNTER = 0
_RUN_LOCK = threading.Lock()


def next_event_seq() -> int:
    """Return a monotonically increasing event sequence number for the run."""
    global _RUN_COUNTER
    with _RUN_LOCK:
        _RUN_COUNTER += 1
        return _RUN_COUNTER


class PrintLogger:
    """Simple JSON-line logger that writes to stdout and optional file."""

    _lock = threading.Lock()

    def __init__(self, job_name: str, file_path: Optional[str] = None, level: str = "INFO") -> None:
        self.job = job_name
        self.file_path = file_path
        self.level = level

    def _write_line(self, line: str) -> None:
        with self._lock:
            print(line)
            if self.file_path:
                with open(self.file_path, "a", encoding="utf-8") as handle:
                    handle.write(line + "\n")

    def log(self, level: str, msg: str, **kv: Any) -> None:
        rec: Dict[str, Any] = {
            "ts": datetime.now().astimezone().isoformat(timespec="milliseconds"),
            "level": level,
            "job": self.job,
            **kv,
            "msg": msg,
            "run_id": RUN_ID,
        }
        self._write_line(json.dumps(rec, separators=(",", ":"), ensure_ascii=False))

    def debug(self, msg: str, **kv: Any) -> None:
        self.log("DEBUG", msg, **kv)

    def info(self, msg: str, **kv: Any) -> None:
        self.log("INFO", msg, **kv)

    def warn(self, msg: str, **kv: Any) -> None:
        self.log("WARN", msg, **kv)

    def error(self, msg: str, **kv: Any) -> None:
        self.log("ERROR", msg, **kv)

    def event(self, event: str, level: str = "INFO", **kv: Any) -> None:
        kv = dict(kv)
        kv.setdefault("event", event)
        self.log(level, event, **kv)


def with_ingest_cols(df: DataFrame) -> DataFrame:
    """Augment a dataframe with standard ingestion columns."""
    return df.withColumn("load_timestamp", F.current_timestamp()).withColumn("run_id", F.lit(RUN_ID))


class Utils:
    """General-purpose helpers shared across ingestion components."""

    @staticmethod
    def minus_seconds_datetime(wm_str: str, seconds: Optional[int]) -> str:
        if not seconds:
            return wm_str
        dt = datetime.strptime(wm_str[:19], "%Y-%m-%d %H:%M:%S")
        return (dt - timedelta(seconds=int(seconds))).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def minus_seconds_epoch(wm_str: str, seconds: Optional[int], millis: bool = False) -> str:
        if not seconds:
            return wm_str
        base = int(float(wm_str))
        adj = base - (int(seconds) * (1000 if millis else 1))
        return str(max(adj, 0))

    @staticmethod
    def minus_seconds(wm_str: str, seconds: Optional[int]) -> str:
        if not seconds:
            return wm_str
        dt = datetime.strptime(wm_str[:19], "%Y-%m-%d %H:%M:%S")
        dt2 = dt - timedelta(seconds=int(seconds))
        return dt2.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def mssql_literal_from_wm(wm_str: str) -> str:
        return wm_str[:19].replace(" ", "T")

    @staticmethod
    def schema_json(schema: Any) -> Dict[str, Any]:
        return json.loads(schema.json())

    @staticmethod
    def sample_spark(spark: SparkSession) -> Dict[str, Any]:
        sc = spark.sparkContext
        try:
            stage_ids = [s.stageId() for s in sc.statusTracker().getActiveStageIds()]
        except Exception:
            stage_ids = []
        return {"active_stages": stage_ids, "default_parallelism": sc.defaultParallelism}
