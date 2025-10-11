from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from salam_ingest.common import PrintLogger
from salam_ingest.endpoints.base import MetadataCapableEndpoint
from salam_ingest.metadata.cache import MetadataCacheManager
from salam_ingest.metadata.core import MetadataTarget
from salam_ingest.metadata.utils import to_serializable


@dataclass
class MetadataServiceConfig:
    endpoint_defaults: Dict[str, Any]


@dataclass
class MetadataJob:
    target: MetadataTarget
    artifact: Dict[str, Any]
    endpoint: MetadataCapableEndpoint


class MetadataCollectionService:
    """Orchestrate metadata collection using metadata subsystems."""

    def __init__(self, config: MetadataServiceConfig, cache: MetadataCacheManager, logger: PrintLogger) -> None:
        self.config = config
        self.cache = cache
        self.logger = logger

    def run(self, jobs: List[MetadataJob]) -> None:
        if not self.cache.cfg.enabled:
            self.logger.info("metadata_collection_disabled")
            return

        for job in jobs:
            target = job.target
            artifact = job.artifact
            endpoint = job.endpoint

            if not self.cache.needs_refresh(target):
                self.cache.record_hit(target)
                continue

            caps = endpoint.capabilities()
            if not getattr(caps, "supports_metadata", False) or not isinstance(endpoint, MetadataCapableEndpoint):
                self.logger.info(
                    "metadata_capability_missing",
                    namespace=target.namespace,
                    entity=target.entity,
                    dialect=getattr(endpoint, "DIALECT", None) or endpoint.describe().get("dialect"),
                )
                continue
            if not hasattr(endpoint, "metadata_subsystem"):
                self.logger.info(
                    "metadata_subsystem_missing",
                    namespace=target.namespace,
                    entity=target.entity,
                    dialect=getattr(endpoint, "DIALECT", None) or endpoint.describe().get("dialect"),
                )
                continue

            subsystem = endpoint.metadata_subsystem()
            endpoint_cfg = self._build_endpoint_config(endpoint, artifact, subsystem)
            try:
                environment = subsystem.probe_environment(config=endpoint_cfg)
            except Exception as exc:
                self.logger.warn(
                    "metadata_environment_probe_failed",
                    namespace=target.namespace,
                    entity=target.entity,
                    error=str(exc),
                )
                environment = {}

            self.logger.info(
                "metadata_collect_start",
                namespace=target.namespace,
                entity=target.entity,
            )
            try:
                snapshot = subsystem.collect_snapshot(config=endpoint_cfg, environment=environment)
            except Exception as exc:
                self.logger.warn(
                    "metadata_collect_error",
                    namespace=target.namespace,
                    entity=target.entity,
                    error=str(exc),
                )
                continue

            snapshot_dict = to_serializable(snapshot)
            endpoint_desc = endpoint.describe()
            snapshot_dict.update(
                {
                    "schema": snapshot_dict.get("schema", target.namespace),
                    "name": snapshot_dict.get("name", target.entity),
                    "namespace": target.namespace,
                    "entity": target.entity,
                    "endpoint": endpoint_desc,
                    "quality_flags": artifact.get("quality_flags", {}),
                    "artifact_config": artifact,
                    "metadata_config": endpoint_cfg,
                }
            )
            self.cache.persist(target, snapshot_dict)
            self.logger.info(
                "metadata_collect_success",
                namespace=target.namespace,
                entity=target.entity,
            )

    def _build_endpoint_config(
        self,
        endpoint: MetadataCapableEndpoint,
        artifact_cfg: Dict[str, Any],
        subsystem,
    ) -> Dict[str, Any]:
        cfg: Dict[str, Any] = {}
        defaults = self.config.endpoint_defaults.get("default")
        if isinstance(defaults, dict):
            cfg.update(defaults)
        dialect = getattr(endpoint, "DIALECT", None) or endpoint.describe().get("dialect")
        if dialect:
            dialect_cfg = self.config.endpoint_defaults.get(str(dialect).lower())
            if isinstance(dialect_cfg, dict):
                cfg.update(dialect_cfg)
        artifact_meta = artifact_cfg.get("metadata")
        if isinstance(artifact_meta, dict):
            cfg.update(artifact_meta)
        if hasattr(subsystem, "capabilities"):
            try:
                caps = subsystem.capabilities()
            except Exception:  # pragma: no cover - defensive
                caps = None
            if isinstance(caps, dict):
                cfg.setdefault("capabilities", caps)
        return cfg
