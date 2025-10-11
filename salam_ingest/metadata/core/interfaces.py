from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, Mapping, Optional, Protocol, Sequence, runtime_checkable

from .model import CatalogSnapshot, MetadataTarget


@dataclass
class MetadataContext:
    """Execution context shared across producer and consumer interactions."""

    source_id: str
    job_id: Optional[str] = None
    run_id: Optional[str] = None
    namespace: Optional[str] = None
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MetadataRequest:
    """Request issued to a metadata producer."""

    target: MetadataTarget
    artifact: Mapping[str, Any]
    context: MetadataContext
    refresh: bool = False
    config: Optional[Mapping[str, Any]] = None


@dataclass
class MetadataRecord:
    """Normalized metadata produced by a collector or subsystem."""

    target: MetadataTarget
    kind: str
    payload: CatalogSnapshot | Mapping[str, Any]
    produced_at: datetime
    producer_id: str
    version: Optional[str] = None
    quality: Dict[str, Any] = field(default_factory=dict)
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MetadataQuery:
    """Query envelope for repository lookups."""

    target: Optional[MetadataTarget] = None
    kinds: Optional[Sequence[str]] = None
    include_history: bool = False
    limit: Optional[int] = None
    filters: Dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class MetadataProducer(Protocol):
    """Entity capable of generating metadata records."""

    @property
    def producer_id(self) -> str: ...

    def capabilities(self) -> Mapping[str, Any]: ...

    def supports(self, request: MetadataRequest) -> bool: ...

    def produce(self, request: MetadataRequest) -> Iterable[MetadataRecord]: ...


@runtime_checkable
class MetadataConsumer(Protocol):
    """Entity that consumes metadata persisted in the core layer."""

    @property
    def consumer_id(self) -> str: ...

    def requirements(self) -> Mapping[str, Any]: ...

    def consume(
        self,
        *,
        records: Iterable[MetadataRecord],
        context: MetadataContext,
    ) -> Any: ...


@runtime_checkable
class MetadataRepository(Protocol):
    """Abstracts persistence and lookup of metadata records."""

    def store(self, record: MetadataRecord) -> MetadataRecord: ...

    def bulk_store(self, records: Iterable[MetadataRecord]) -> Sequence[MetadataRecord]: ...

    def latest(self, target: MetadataTarget, kind: Optional[str] = None) -> Optional[MetadataRecord]: ...

    def history(self, target: MetadataTarget, kind: Optional[str] = None, limit: Optional[int] = None) -> Sequence[MetadataRecord]: ...

    def query(self, criteria: MetadataQuery) -> Sequence[MetadataRecord]: ...


@runtime_checkable
class MetadataTransformer(Protocol):
    """Optional transformation hook run before persisting or serving metadata."""

    def applies_to(self, record: MetadataRecord) -> bool: ...

    def transform(self, record: MetadataRecord, context: MetadataContext) -> MetadataRecord: ...
