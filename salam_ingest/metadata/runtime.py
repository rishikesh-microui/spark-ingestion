from __future__ import annotations

from typing import Any, Dict, List, Optional

from salam_ingest.common import PrintLogger
from salam_ingest.endpoints import EndpointFactory
from salam_ingest.endpoints.base import MetadataCapableEndpoint
from salam_ingest.metadata.cache import MetadataCacheConfig, MetadataCacheManager
from salam_ingest.metadata.core import MetadataTarget
from salam_ingest.metadata.services import MetadataCollectionService, MetadataJob, MetadataServiceConfig
from salam_ingest.metadata.utils import safe_upper
from salam_ingest.tools.base import ExecutionTool


def _build_metadata_configs(cfg: Dict[str, Any], logger: PrintLogger) -> tuple[MetadataServiceConfig, MetadataCacheManager, str]:
    meta_cfg = cfg.get("metadata", cfg.get("catalog", {})) or {}
    runtime_cfg = cfg.get("runtime", {})
    jdbc_cfg = cfg.get("jdbc", {})
    dialect = (jdbc_cfg.get("dialect") or "default").lower()

    cache_root = meta_cfg.get("cache_path") or meta_cfg.get("root") or "cache/catalog"
    ttl_hours = int(meta_cfg.get("ttl_hours", meta_cfg.get("ttlHours", 24)))
    enabled = bool(meta_cfg.get("enabled", True))
    source_id = (
        meta_cfg.get("source_id")
        or meta_cfg.get("source")
        or runtime_cfg.get("job_name")
        or dialect
    )
    cache_cfg = MetadataCacheConfig(
        cache_path=cache_root,
        ttl_hours=ttl_hours,
        enabled=enabled,
        source_id=str(source_id).lower().replace(" ", "_"),
    )
    cache_manager = MetadataCacheManager(cache_cfg, logger)

    endpoint_defaults = meta_cfg.get("endpoint") if isinstance(meta_cfg.get("endpoint"), dict) else {}
    service_cfg = MetadataServiceConfig(endpoint_defaults=endpoint_defaults)
    return service_cfg, cache_manager, str(source_id)


def collect_metadata(
    cfg: Dict[str, Any],
    tables: List[Dict[str, Any]],
    tool: Optional[ExecutionTool],
    logger: PrintLogger,
) -> None:
    """Collect metadata snapshots for the provided tables using their endpoints."""

    service_cfg, cache_manager, default_namespace = _build_metadata_configs(cfg, logger)
    metadata_service = MetadataCollectionService(service_cfg, cache_manager, logger)

    if not cache_manager.cfg.enabled:
        logger.info("metadata_collection_disabled")
        return
    if tool is None:
        logger.warn("metadata_collection_skipped", reason="no_execution_tool")
        return

    jobs: List[MetadataJob] = []
    for tbl in tables:
        try:
            endpoint = EndpointFactory.build_source(cfg, tbl, tool)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.warn(
                "metadata_endpoint_build_failed",
                schema=tbl.get("schema"),
                dataset=tbl.get("table"),
                error=str(exc),
            )
            continue
        if not isinstance(endpoint, MetadataCapableEndpoint):
            logger.info(
                "metadata_capability_missing",
                schema=tbl.get("schema"),
                dataset=tbl.get("table"),
            )
            continue
        namespace = safe_upper(str(tbl.get("schema") or tbl.get("namespace") or default_namespace))
        entity = safe_upper(str(tbl.get("table") or tbl.get("dataset") or tbl.get("name") or tbl.get("entity") or "unknown"))
        target = MetadataTarget(namespace=namespace, entity=entity)
        jobs.append(MetadataJob(target=target, artifact=tbl, endpoint=endpoint))

    if not jobs:
        return

    try:
        metadata_service.run(jobs)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.warn("metadata_collection_failed", error=str(exc))
