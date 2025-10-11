from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List

from pyspark.sql import SparkSession

from ..common import RUN_ID
from ..storage.filesystem import Filesystem


class HDFSUtil:
    """Utility helpers for writing small control files to HDFS-compatible stores."""

    @staticmethod
    def write_text(spark: SparkSession, path: str, content: str) -> None:
        fs, target = Filesystem.for_path(path, spark)
        fs.write_text(target, content)

    @staticmethod
    def append_text(spark: SparkSession, path: str, content: str) -> None:
        fs, target = Filesystem.for_path(path, spark)
        fs.append_text(target, content)


class HDFSOutbox:
    """Durable outbox backed by HDFS that mirrors events and progress updates."""

    def __init__(self, spark: SparkSession, root: str) -> None:
        self.spark = spark
        self.fs, resolved_root = Filesystem.for_path(root, spark)
        self.root = resolved_root.rstrip("/")
        for sub in ("events", "progress"):
            self.fs.makedirs(self.fs.join(self.root, sub))

    def _atomic_write(self, path: str, content_bytes: bytes) -> None:
        tmp_path = f"{path}.__tmp__"
        self.fs.write_bytes(tmp_path, content_bytes)
        if self.fs.exists(path):
            self.fs.delete(path)
        self.fs.rename(tmp_path, path)

    def _delete(self, path: str) -> None:
        if self.fs.exists(path):
            self.fs.delete(path)

    def mirror_batch(self, kind: str, records: Iterable[Dict[str, Any]]) -> str:
        ts = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S.%f")
        name = f"{kind}_{RUN_ID}_{ts}_{uuid.uuid4().hex}.jsonl"
        path = self.fs.join(self.root, kind, name)
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
        base = self.fs.join(self.root, kind)
        entries = []
        for name in self.fs.listdir(base):
            if name.endswith(".jsonl"):
                entries.append(self.fs.join(base, name))
        return sorted(entries)

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
