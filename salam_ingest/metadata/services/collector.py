from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from salam_ingest.common import PrintLogger
from salam_ingest.endpoints.base import MetadataCapableEndpoint
from salam_ingest.metadata.cache import MetadataCacheManager
from salam_ingest.metadata.core import MetadataProducerRunner, MetadataTarget


@dataclass
class MetadataServiceConfig:
    endpoint_defaults: Dict[str, Any]


@dataclass
class MetadataJob:
    target: MetadataTarget
    artifact: Dict[str, Any]
    endpoint: MetadataCapableEndpoint


class MetadataCollectionService:
    """Coordinate metadata collection jobs by delegating to the metadata engine."""

    def __init__(self, config: MetadataServiceConfig, cache: MetadataCacheManager, logger: PrintLogger) -> None:
        self.config = config
        self.cache = cache
        self.logger = logger
        self.runner = MetadataProducerRunner(cache, config.endpoint_defaults)

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

            if not isinstance(endpoint, MetadataCapableEndpoint):
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

            result = self.runner.execute(endpoint, artifact, target)
            producer_id = result.producer_id or getattr(endpoint, "DIALECT", None) or "unknown"

            if result.reason == "producer_unavailable":
                self.logger.info(
                    "metadata_producer_missing",
                    namespace=target.namespace,
                    entity=target.entity,
                )
                continue
            if result.reason == "unsupported_target":
                self.logger.info(
                    "metadata_target_unsupported",
                    namespace=target.namespace,
                    entity=target.entity,
                    producer=producer_id,
                )
                continue
            if result.reason == "capability_missing":
                self.logger.info(
                    "metadata_capability_missing",
                    namespace=target.namespace,
                    entity=target.entity,
                    dialect=getattr(endpoint, "DIALECT", None) or endpoint.describe().get("dialect"),
                )
                continue

            if result.started:
                self.logger.info(
                    "metadata_collect_start",
                    namespace=target.namespace,
                    entity=target.entity,
                    producer=producer_id,
                )

            if result.error:
                self.logger.warn(
                    "metadata_collect_error",
                    namespace=target.namespace,
                    entity=target.entity,
                    producer=producer_id,
                    error=result.error,
                )
                continue

            if result.probe_error:
                self.logger.warn(
                    "metadata_environment_probe_failed",
                    namespace=target.namespace,
                    entity=target.entity,
                    producer=producer_id,
                    error=result.probe_error,
                )

            if result.stored > 0:
                self.logger.info(
                    "metadata_collect_success",
                    namespace=target.namespace,
                    entity=target.entity,
                    producer=producer_id,
                    records=result.stored,
                )
                continue

            reason = result.reason or "no_records"
            self.logger.info(
                "metadata_collect_noop",
                namespace=target.namespace,
                entity=target.entity,
                producer=producer_id,
                reason=reason,
            )
