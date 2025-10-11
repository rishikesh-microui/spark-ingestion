from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List

from pyspark.sql import SparkSession

from ..common import RUN_ID


class HDFSUtil:
    """Utility helpers for writing small control files to HDFS-compatible stores."""

    @staticmethod
    def write_text(spark: SparkSession, path: str, content: str) -> None:
        jsc = spark._jsc
        jvm = spark.sparkContext._jvm
        conf = jsc.hadoopConfiguration()
        p = jvm.org.apache.hadoop.fs.Path(path)
        fs = p.getFileSystem(conf)
        if fs.exists(p):
            fs.delete(p, True)
        out = fs.create(p)
        try:
            out.write(bytearray(content.encode("utf-8")))
        finally:
            out.close()

    @staticmethod
    def append_text(spark: SparkSession, path: str, content: str) -> None:
        jsc = spark._jsc
        jvm = spark.sparkContext._jvm
        conf = jsc.hadoopConfiguration()
        p = jvm.org.apache.hadoop.fs.Path(path)
        fs = p.getFileSystem(conf)
        out = fs.create(p, True) if not fs.exists(p) else fs.append(p)
        try:
            out.write(bytearray(content.encode("utf-8")))
        finally:
            out.close()


class HDFSOutbox:
    """Durable outbox backed by HDFS that mirrors events and progress updates."""

    def __init__(self, spark: SparkSession, root: str) -> None:
        self.spark = spark
        self.root = root.rstrip("/")
        self.jsc = spark._jsc
        self.jvm = spark.sparkContext._jvm
        self.conf = self.jsc.hadoopConfiguration()
        self.Path = self.jvm.org.apache.hadoop.fs.Path
        self.fs = self.jvm.org.apache.hadoop.fs.FileSystem.get(self.conf)
        for sub in ("events", "progress"):
            p = self.Path(f"{self.root}/{sub}")
            if not self.fs.exists(p):
                self.fs.mkdirs(p)

    def _atomic_write(self, path: str, content_bytes: bytes) -> None:
        p_tmp = self.Path(path + ".__tmp__")
        p = self.Path(path)
        if self.fs.exists(p_tmp):
            self.fs.delete(p_tmp, True)
        out = self.fs.create(p_tmp, True)
        try:
            out.write(content_bytes)
        finally:
            out.close()
        if self.fs.exists(p):
            self.fs.delete(p, True)
        if not self.fs.rename(p_tmp, p):
            raise RuntimeError(f"Atomic rename failed for {path}")

    def _delete(self, path: str) -> None:
        p = self.Path(path)
        if self.fs.exists(p):
            self.fs.delete(p, False)

    def mirror_batch(self, kind: str, records: Iterable[Dict[str, Any]]) -> str:
        ts = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S.%f")
        name = f"{kind}_{RUN_ID}_{ts}_{uuid.uuid4().hex}.jsonl"
        path = f"{self.root}/{kind}/{name}"
        payload = "\n".join(json.dumps(r, separators=(",", ":")) for r in records)
        self._atomic_write(path, payload.encode("utf-8"))
        return path

    def append_event(self, source_id: str, run_id: str, record: Dict[str, Any]) -> str:
        _ = source_id, run_id  # currently unused but kept for parity
        return self.mirror_batch("events", [record])

    def append_progress(self, source_id: str, run_id: str, record: Dict[str, Any]) -> str:
        _ = source_id, run_id
        return self.mirror_batch("progress", [record])

    def list_pending(self, kind: str) -> List[str]:
        base = f"{self.root}/{kind}"
        it = self.fs.listStatus(self.Path(base))
        out = []
        for status in it:
            p = status.getPath().toString()
            if p.endswith(".jsonl"):
                out.append(p)
        return sorted(out)

    def read_jsonl(self, path: str) -> List[Dict[str, Any]]:
        df = self.spark.read.json(path)
        return [row.asDict(recursive=True) for row in df.collect()]

    def delete(self, path: str) -> None:
        self._delete(path)

    def restore_all(self, base_state: Any, logger: Any) -> None:
        for kind in ("events", "progress"):
            for path in self.list_pending(kind):
                try:
                    rows = self.read_jsonl(path)
                    if kind == "events":
                        base_state._write_events(rows)
                    else:
                        base_state._write_progress(rows)
                    self.delete(path)
                    logger.info("outbox_replayed", kind=kind, file=path, rows=len(rows))
                except Exception as exc:
                    logger.error("outbox_replay_failed", kind=kind, file=path, err=str(exc))
