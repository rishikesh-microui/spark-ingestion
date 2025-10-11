from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, Optional, TYPE_CHECKING

from salam_ingest.metadata.cache import MetadataCacheManager
from salam_ingest.metadata.core.interfaces import (
    MetadataContext,
    MetadataProducer,
    MetadataRecord,
    MetadataRequest,
    MetadataTarget,
)
from salam_ingest.metadata.utils import to_serializable

if TYPE_CHECKING:  # pragma: no cover
    from salam_ingest.endpoints.base import MetadataCapableEndpoint


@dataclass
class ProducerRunResult:
    producer_id: Optional[str] = None
    produced: int = 0
    stored: int = 0
    error: Optional[str] = None
    reason: Optional[str] = None
    probe_error: Optional[str] = None
    started: bool = False


class MetadataProducerRunner:
    """Execute metadata producers and persist their output via the repository/cache layer."""

    def __init__(
        self,
        cache: MetadataCacheManager,
        endpoint_defaults: Mapping[str, Any],
    ) -> None:
        self.cache = cache
        self.endpoint_defaults = dict(endpoint_defaults or {})
        self.source_id = cache.cfg.source_id

    def execute(
        self,
        endpoint: "MetadataCapableEndpoint",
        artifact: Mapping[str, Any],
        target: MetadataTarget,
    ) -> ProducerRunResult:
        if not self._supports_metadata(endpoint):
            return ProducerRunResult(reason="capability_missing")

        producer = self._resolve_producer(endpoint)
        if producer is None:
            return ProducerRunResult(reason="producer_unavailable")

        producer_id = producer.producer_id
        producer_caps = self._safe_capabilities(producer)
        config = self._build_endpoint_config(endpoint, artifact, producer_caps)
        context = self._build_context(target, artifact)
        request = MetadataRequest(
            target=target,
            artifact=artifact,
            context=context,
            refresh=True,
            config=config,
        )

        try:
            supports = producer.supports(request)
        except Exception as exc:  # pragma: no cover - defensive
            return ProducerRunResult(producer_id=producer_id, error=str(exc))

        if not supports:
            return ProducerRunResult(producer_id=producer_id, reason="unsupported_target")

        try:
            records = list(producer.produce(request))
        except Exception as exc:
            return ProducerRunResult(
                producer_id=producer_id,
                error=str(exc),
                started=True,
            )

        result = ProducerRunResult(
            producer_id=producer_id,
            produced=len(records),
            started=True,
        )
        if not records:
            result.reason = "no_records"
            return result

        for record in records:
            probe_error = self._extract_probe_error(record)
            if probe_error and not result.probe_error:
                result.probe_error = probe_error
            if self._persist_record(record, request, endpoint, artifact):
                result.stored += 1

        if result.stored == 0 and result.reason is None:
            result.reason = "store_skipped"
        return result

    # ------------------------------------------------------------------ helpers --
    def _supports_metadata(self, endpoint: "MetadataCapableEndpoint") -> bool:
        caps = endpoint.capabilities()
        return bool(getattr(caps, "supports_metadata", False))

    def _resolve_producer(self, endpoint: "MetadataCapableEndpoint") -> Optional[MetadataProducer]:
        subsystem = endpoint.metadata_subsystem()
        if isinstance(subsystem, MetadataProducer):
            return subsystem
        return _EndpointProducerAdapter(endpoint, subsystem)

    def _safe_capabilities(self, producer: MetadataProducer) -> Optional[Mapping[str, Any]]:
        try:
            caps = producer.capabilities()
            if isinstance(caps, Mapping):
                return caps
        except Exception:  # pragma: no cover - defensive
            return None
        return None

    def _build_endpoint_config(
        self,
        endpoint: "MetadataCapableEndpoint",
        artifact_cfg: Mapping[str, Any],
        producer_caps: Optional[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        cfg: Dict[str, Any] = {}
        defaults = self.endpoint_defaults.get("default")
        if isinstance(defaults, dict):
            cfg.update(defaults)
        dialect = getattr(endpoint, "DIALECT", None) or endpoint.describe().get("dialect")
        if dialect:
            dialect_cfg = self.endpoint_defaults.get(str(dialect).lower())
            if isinstance(dialect_cfg, dict):
                cfg.update(dialect_cfg)
        artifact_meta = artifact_cfg.get("metadata")
        if isinstance(artifact_meta, dict):
            cfg.update(artifact_meta)
        if isinstance(producer_caps, Mapping):
            cfg.setdefault("capabilities", dict(producer_caps))
        return cfg

    def _build_context(self, target: MetadataTarget, artifact_cfg: Mapping[str, Any]) -> MetadataContext:
        job_id = str(artifact_cfg.get("metadata_job_id") or artifact_cfg.get("job_id") or "") or None
        run_id = str(artifact_cfg.get("run_id") or "") or None
        extras = {"artifact": dict(artifact_cfg)}
        return MetadataContext(
            source_id=self.source_id,
            job_id=job_id,
            run_id=run_id,
            namespace=target.namespace,
            extras=extras,
        )

    def _extract_probe_error(self, record: MetadataRecord) -> Optional[str]:
        extras = getattr(record, "extras", None)
        if isinstance(extras, Mapping):
            err = extras.get("environment_probe_error")
            if err:
                return str(err)
        return None

    def _persist_record(
        self,
        record: MetadataRecord,
        request: MetadataRequest,
        endpoint: "MetadataCapableEndpoint",
        artifact: Mapping[str, Any],
    ) -> bool:
        if record.kind != "catalog_snapshot":
            return False

        payload = record.payload
        if hasattr(payload, "__dict__") or not isinstance(payload, Mapping):
            payload_dict = to_serializable(payload)
        else:
            payload_dict = dict(payload)

        endpoint_desc = endpoint.describe()
        payload_dict.setdefault("schema", payload_dict.get("schema", record.target.namespace))
        payload_dict.setdefault("name", payload_dict.get("name", record.target.entity))
        payload_dict.setdefault("namespace", record.target.namespace)
        payload_dict.setdefault("entity", record.target.entity)
        payload_dict.setdefault("endpoint", endpoint_desc)
        payload_dict.setdefault("quality_flags", artifact.get("quality_flags", {}))
        payload_dict.setdefault("artifact_config", dict(artifact))
        if request.config:
            payload_dict.setdefault("metadata_config", dict(request.config))

        payload_dict.setdefault("producer_id", record.producer_id)
        produced_at = record.produced_at
        if not isinstance(produced_at, datetime):
            try:
                produced_at = datetime.fromisoformat(str(produced_at))
            except Exception:  # pragma: no cover - defensive
                produced_at = datetime.now(timezone.utc)
        if produced_at.tzinfo is None:
            produced_at = produced_at.replace(tzinfo=timezone.utc)
        payload_dict.setdefault("produced_at", produced_at.isoformat())
        if record.version:
            payload_dict.setdefault("version_hint", record.version)
        if record.extras:
            payload_dict.setdefault("producer_extras", to_serializable(record.extras))

        self.cache.persist(record.target, payload_dict)
        return True


class _EndpointProducerAdapter(MetadataProducer):
    """Adapter that exposes legacy metadata subsystems as MetadataProducer instances."""

    def __init__(self, endpoint: "MetadataCapableEndpoint", subsystem: Any) -> None:
        self.endpoint = endpoint
        self.subsystem = subsystem
        desc = self.endpoint.describe()
        dialect = desc.get("dialect") or getattr(self.endpoint, "DIALECT", None) or "unknown"
        schema = desc.get("schema") or getattr(self.endpoint, "schema", None) or "default"
        table = desc.get("table") or getattr(self.endpoint, "table", None) or "unknown"
        self._producer_id = f"{dialect}:{schema}.{table}"

    @property
    def producer_id(self) -> str:
        return self._producer_id

    def capabilities(self) -> Mapping[str, Any]:
        try:
            caps = self.subsystem.capabilities()
            if isinstance(caps, Mapping):
                return caps
        except Exception:  # pragma: no cover - defensive
            pass
        return {}

    def supports(self, request: MetadataRequest) -> bool:
        desc = self.endpoint.describe()
        schema = (desc.get("schema") or "").lower()
        table = (desc.get("table") or "").lower()
        return (
            request.target.namespace.lower() == schema
            and request.target.entity.lower() == table
        )

    def produce(self, request: MetadataRequest) -> Iterable[MetadataRecord]:
        config = dict(request.config or {})
        artifact = dict(request.artifact)
        probe_error: Optional[str] = None
        try:
            environment = self.subsystem.probe_environment(config=config)
        except Exception as exc:
            probe_error = str(exc)
            environment = {}
        snapshot = self.subsystem.collect_snapshot(config=config, environment=dict(environment or {}))
        snapshot_dict = to_serializable(snapshot)
        endpoint_desc = self.endpoint.describe()
        snapshot_dict.setdefault("environment", environment)
        snapshot_dict.update(
            {
                "schema": snapshot_dict.get("schema", request.target.namespace),
                "name": snapshot_dict.get("name", request.target.entity),
                "namespace": request.target.namespace,
                "entity": request.target.entity,
                "endpoint": endpoint_desc,
                "quality_flags": artifact.get("quality_flags", {}),
                "artifact_config": artifact,
                "metadata_config": config,
            }
        )
        produced_at = datetime.now(timezone.utc)
        extras: Dict[str, Any] = {
            "environment": environment,
            "refresh_requested": request.refresh,
        }
        if probe_error:
            extras["environment_probe_error"] = probe_error
        record = MetadataRecord(
            target=request.target,
            kind="catalog_snapshot",
            payload=snapshot_dict,
            produced_at=produced_at,
            producer_id=self.producer_id,
            version=None,
            quality={},
            extras=extras,
        )
        return [record]
