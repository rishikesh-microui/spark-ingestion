import json
import threading
from datetime import datetime
from typing import Any, Dict

from pyspark.sql import DataFrame
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

    def __init__(self, job_name: str, file_path: str | None = None, level: str = "INFO") -> None:
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


def with_ingest_cols(df: DataFrame) -> DataFrame:
    """Augment a dataframe with standard ingestion columns."""
    return df.withColumn("load_timestamp", F.current_timestamp()).withColumn("run_id", F.lit(RUN_ID))
