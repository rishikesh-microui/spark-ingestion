from __future__ import annotations

import json
import os
from typing import Iterable, Optional, Sequence

from .interfaces import MetadataQuery, MetadataRecord, MetadataRepository, MetadataTarget


class JsonFileMetadataRepository(MetadataRepository):
    """Simple repository backed by MetadataCacheManager artifacts (read-only view)."""

    def __init__(self, cache_manager) -> None:
        self.cache = cache_manager

    def store(self, record: MetadataRecord) -> MetadataRecord:  # pragma: no cover - unused
        raise NotImplementedError("JsonFileMetadataRepository is read-only")

    def bulk_store(self, records: Iterable[MetadataRecord]) -> Sequence[MetadataRecord]:  # pragma: no cover - unused
        raise NotImplementedError("JsonFileMetadataRepository is read-only")

    def latest(self, target: MetadataTarget, kind: Optional[str] = None) -> Optional[MetadataRecord]:
        record_dict = self._load_latest(target)
        if not record_dict:
            return None
        payload = record_dict
        produced_at = record_dict.get("produced_at") or record_dict.get("collected_at")
        version = record_dict.get("version") or record_dict.get("version_hint")
        return MetadataRecord(
            target=target,
            kind=kind or record_dict.get("kind", "catalog_snapshot"),
            payload=payload,
            produced_at=_parse_iso(produced_at),
            producer_id=record_dict.get("producer_id", "unknown"),
            version=version,
            quality=record_dict.get("quality_flags", {}),
            extras=record_dict.get("producer_extras", {}),
        )

    def history(self, target: MetadataTarget, kind: Optional[str] = None, limit: Optional[int] = None) -> Sequence[MetadataRecord]:
        paths = self._list_history(target)
        records = []
        for path in paths:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
            produced_at = data.get("produced_at") or data.get("collected_at")
            version = data.get("version") or data.get("version_hint")
            records.append(
                MetadataRecord(
                    target=target,
                    kind=kind or data.get("kind", "catalog_snapshot"),
                    payload=data,
                    produced_at=_parse_iso(produced_at),
                    producer_id=data.get("producer_id", "unknown"),
                    version=version,
                    quality=data.get("quality_flags", {}),
                    extras=data.get("producer_extras", {}),
                )
            )
            if limit and len(records) >= limit:
                break
        return records

    def query(self, criteria: MetadataQuery) -> Sequence[MetadataRecord]:
        target = criteria.target
        if target:
            if criteria.include_history:
                return self.history(target, criteria.kinds[0] if criteria.kinds else None, criteria.limit)
            latest = self.latest(target, criteria.kinds[0] if criteria.kinds else None)
            return [latest] if latest else []
        raise NotImplementedError("Generic query without target not implemented")

    # ------------------------------------------------------------------ helpers --
    def _load_latest(self, target: MetadataTarget):
        key = self.cache._key(target)  # type: ignore[attr-defined]
        entry = self.cache._index.get(key)  # type: ignore[attr-defined]
        if not entry:
            return None
        path = os.path.join(self.cache.source_root, entry["path"])  # type: ignore[attr-defined]
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def _list_history(self, target: MetadataTarget):
        directory = self.cache.artifact_dir(target)
        files = sorted(
            (os.path.join(directory, name) for name in os.listdir(directory)),
            reverse=True,
        )
        return files


def _parse_iso(value: Optional[str]):
    from datetime import datetime

    if not value:
        return datetime.now()
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return datetime.now()
