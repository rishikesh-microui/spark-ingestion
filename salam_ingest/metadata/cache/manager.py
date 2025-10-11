from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from salam_ingest.common import PrintLogger
from salam_ingest.metadata.core import MetadataTarget
from salam_ingest.metadata.utils import to_serializable
from salam_ingest.storage.filesystem import Filesystem


@dataclass
class MetadataCacheConfig:
    cache_path: str
    ttl_hours: int
    enabled: bool
    source_id: str


class MetadataCacheManager:
    def __init__(
        self,
        cfg: MetadataCacheConfig,
        logger: PrintLogger,
        spark: Optional[SparkSession] = None,
    ) -> None:
        self.cfg = cfg
        self.logger = logger
        self._fs = Filesystem.for_root(cfg.cache_path, spark)
        self.root = self._fs.root
        self.source_root = self._fs.join(self.root, cfg.source_id)
        self.index_path = self._fs.join(self.source_root, "catalog_index.json")
        self._fs.makedirs(self.source_root)
        self._index = self._load_index()

    def _load_index(self) -> Dict[str, Any]:
        if not self._fs.exists(self.index_path):
            return {}
        try:
            data = self._fs.read_text(self.index_path)
            return json.loads(data)
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.warn("metadata_index_load_failed", path=self.index_path, error=str(exc))
            return {}

    def _save_index(self) -> None:
        tmp_path = f"{self.index_path}.tmp"
        self._fs.write_text(tmp_path, json.dumps(self._index, ensure_ascii=False, indent=2))
        self._fs.replace(tmp_path, self.index_path)

    def _key(self, target: MetadataTarget) -> str:
        return f"{target.namespace.lower()}::{target.entity.lower()}"

    def _sanitize(self, value: str) -> str:
        return value.lower().replace("/", "_")

    def artifact_dir(self, target: MetadataTarget) -> str:
        path = self._fs.join(self.source_root, self._sanitize(target.namespace), self._sanitize(target.entity))
        self._fs.makedirs(path)
        return path

    def needs_refresh(self, target: MetadataTarget) -> bool:
        key = self._key(target)
        entry = self._index.get(key)
        if not entry:
            return True
        path = self._fs.join(self.source_root, entry["path"])
        if not self._fs.exists(path):
            return True
        expires_at = entry.get("expires_at")
        if not expires_at:
            return True
        try:
            expiry = datetime.fromisoformat(expires_at)
        except ValueError:
            return True
        now = datetime.now(timezone.utc)
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        return now >= expiry

    def record_hit(self, target: MetadataTarget) -> None:
        entry = self._index.get(self._key(target))
        if not entry:
            return
        self.logger.info(
            "metadata_cache_hit",
            namespace=target.namespace,
            entity=target.entity,
            cache_path=self._fs.join(self.source_root, entry["path"]),
        )

    def persist(self, target: MetadataTarget, payload: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        version = now.strftime("%Y%m%d%H%M%S")
        expires_at = now + timedelta(hours=self.cfg.ttl_hours)
        record = {
            **payload,
            "version": version,
            "collected_at": now.isoformat(),
            "expires_at": expires_at.isoformat(),
        }
        dataset_path = self.artifact_dir(target)
        file_path = self._fs.join(dataset_path, f"{version}.json")
        self._fs.write_text(file_path, json.dumps(to_serializable(record), ensure_ascii=False, indent=2))

        key = self._key(target)
        self._index[key] = {
            "namespace": target.namespace,
            "entity": target.entity,
            "version": version,
            "collected_at": record["collected_at"],
            "expires_at": record["expires_at"],
            "path": self._fs.relpath(file_path, self.source_root),
        }
        self._save_index()
        self.logger.info(
            "metadata_cached",
            namespace=target.namespace,
            entity=target.entity,
            version=version,
            expires_at=record["expires_at"],
            path=file_path,
        )
        return record
