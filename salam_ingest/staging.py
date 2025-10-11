import hashlib
import json
import threading
import time
from datetime import datetime
from typing import Any, Dict, Iterable, Optional

from pyspark.sql import SparkSession

from .common import PrintLogger
from .io.filesystem import HDFSOutbox, HDFSUtil


class Staging:
    """Helpers for managing staging directories and control markers."""

    @staticmethod
    def root(cfg: Dict[str, Any]) -> str:
        return cfg["runtime"]["staging"]["root"]

    @staticmethod
    def slice_dir(cfg: Dict[str, Any], schema: str, table: str, incr_col: str, lo: str, hi: str) -> str:
        key = f"{schema}.{table}|{incr_col}|{lo}|{hi}"
        hid = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
        return f"{Staging.root(cfg)}/{schema}/{table}/inc={incr_col}/slices/{hid}"

    @staticmethod
    def exists(spark: SparkSession, path: str) -> bool:
        jsc = spark._jsc
        jvm = spark.sparkContext._jvm
        conf = jsc.hadoopConfiguration()
        p = jvm.org.apache.hadoop.fs.Path(path)
        return p.getFileSystem(conf).exists(p)

    @staticmethod
    def write_text(spark: SparkSession, path: str, content: str) -> None:
        HDFSUtil.write_text(spark, path, content)

    @staticmethod
    def mark_success(spark: SparkSession, dirpath: str) -> None:
        Staging.write_text(spark, f"{dirpath}/_SUCCESS", "{}")

    @staticmethod
    def mark_landed(spark: SparkSession, dirpath: str) -> None:
        Staging.write_text(spark, f"{dirpath}/_LANDED", "{}")

    @staticmethod
    def is_success(spark: SparkSession, dirpath: str) -> bool:
        return Staging.exists(spark, f"{dirpath}/_SUCCESS")

    @staticmethod
    def is_landed(spark: SparkSession, dirpath: str) -> bool:
        return Staging.exists(spark, f"{dirpath}/_LANDED")

    @staticmethod
    def ttl_cleanup(
        spark: SparkSession,
        cfg: Dict[str, Any],
        logger: PrintLogger,
        now_epoch_ms: Optional[int] = None,
    ) -> None:
        """Remove slice dirs older than ttl that have both _SUCCESS and _LANDED."""
        try:
            jvm = spark.sparkContext._jvm
            jsc = spark._jsc
            conf = jsc.hadoopConfiguration()
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
            Path = jvm.org.apache.hadoop.fs.Path
            root = Path(Staging.root(cfg))
            if not fs.exists(root):
                return
            ttl_ms = int(cfg["runtime"]["staging"].get("ttl_hours", 72)) * 3600 * 1000
            now_ms = now_epoch_ms or int(time.time() * 1000)

            def list_dirs(p):
                for st in fs.listStatus(p):
                    if st.isDirectory():
                        yield st.getPath()
                        yield from list_dirs(st.getPath())

            for d in list_dirs(root):
                dstr = d.toString()
                if dstr.endswith("/slices") or "/slices/" not in dstr:
                    continue
                if Staging.exists(spark, f"{dstr}/_SUCCESS") and Staging.exists(spark, f"{dstr}/_LANDED"):
                    age = now_ms - fs.getFileStatus(d).getModificationTime()
                    if age > ttl_ms:
                        logger.info("staging_ttl_delete", path=dstr)
                        fs.delete(d, True)
        except Exception as exc:
            logger.warn("staging_ttl_cleanup_failed", err=str(exc))


class TimeAwareBufferedSink:
    """
    Buffered state writer that flushes based on batch size or time.
    Delegates durable writes to the underlying base state (e.g. SingleStore).
    """

    def __init__(
        self,
        base_state: Any,
        logger: PrintLogger,
        outbox: Optional[HDFSOutbox] = None,
        flush_every: int = 50,
        flush_age_seconds: int = 120,
    ) -> None:
        self.base_state = base_state
        self.logger = logger
        self.outbox = outbox
        self.flush_every = flush_every
        self.flush_age_seconds = flush_age_seconds
        self._events_buf: list[Any] = []
        self._progress_buf: list[Any] = []
        self._lock = threading.Lock()
        self._last_events_flush_ts = time.time()
        self._last_progress_flush_ts = time.time()

    def add_event(self, row: Any) -> None:
        with self._lock:
            self._events_buf.append(row)
            if len(self._events_buf) >= self.flush_every:
                self._flush_events_nolock("size")

    def add_progress(self, row: Any) -> None:
        with self._lock:
            self._progress_buf.append(row)
            if len(self._progress_buf) >= max(1, self.flush_every // 5):
                self._flush_progress_nolock("size")

    def maybe_flush_by_age(self) -> None:
        now = time.time()
        with self._lock:
            if now - self._last_events_flush_ts >= self.flush_age_seconds:
                self._flush_events_nolock("age")
            if now - self._last_progress_flush_ts >= self.flush_age_seconds:
                self._flush_progress_nolock("age")

    def flush(self) -> None:
        with self._lock:
            self._flush_events_nolock("final")
            self._flush_progress_nolock("final")

    def _flush_events_nolock(self, reason: str) -> None:
        if not self._events_buf:
            return
        batch = list(self._events_buf)
        self._events_buf.clear()
        path = None
        try:
            if self.outbox:
                path = self.outbox.mirror_batch("events", batch)
            self.base_state._write_events(batch)
            if path:
                self.outbox.delete(path)
            self._last_events_flush_ts = time.time()
            self.logger.info("sink_flush_events_ok", reason=reason, rows=len(batch))
        except Exception as exc:
            self._events_buf[:0] = batch
            self.logger.error("sink_flush_events_failed", reason=reason, err=str(exc), rows=len(batch))

    def _flush_progress_nolock(self, reason: str) -> None:
        if not self._progress_buf:
            return
        batch = list(self._progress_buf)
        self._progress_buf.clear()
        path = None
        try:
            if self.outbox:
                path = self.outbox.mirror_batch("progress", batch)
            self.base_state._write_progress(batch)
            if path:
                self.outbox.delete(path)
            self._last_progress_flush_ts = time.time()
            self.logger.info("sink_flush_progress_ok", reason=reason, rows=len(batch))
        except Exception as exc:
            self._progress_buf[:0] = batch
            self.logger.error("sink_flush_progress_failed", reason=reason, err=str(exc), rows=len(batch))
