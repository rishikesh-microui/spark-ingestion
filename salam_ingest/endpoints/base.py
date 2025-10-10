from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple, TYPE_CHECKING, runtime_checkable

if TYPE_CHECKING:
    from ..tools.base import ExecutionTool


class EndpointRole(Enum):
    SOURCE = auto()
    SINK = auto()
    BIDIRECTIONAL = auto()


class EndpointType(Enum):
    JDBC = auto()
    HDFS = auto()
    ICEBERG = auto()
    UNKNOWN = auto()


@dataclass
class EndpointCapabilities:
    supports_full: bool = True
    supports_incremental: bool = False
    supports_count_probe: bool = False
    supports_write: bool = False
    supports_finalize: bool = False
    supports_publish: bool = False
    supports_watermark: bool = False
    supports_staging: bool = False
    supports_merge: bool = False
    incremental_literal: str = "timestamp"  # timestamp | epoch
    default_fetchsize: int = 10000
    event_metadata_keys: Tuple[str, ...] = ()


@runtime_checkable
class BaseEndpoint(Protocol):
    """Common methods for both source and sink endpoints."""

    tool: "ExecutionTool"

    def configure(self, table_cfg: Dict[str, Any]) -> None: ...

    def capabilities(self) -> EndpointCapabilities: ...

    def describe(self) -> Dict[str, Any]: ...


@runtime_checkable
class SourceEndpoint(BaseEndpoint, Protocol):
    """Contract for any endpoint that can provide data."""

    def read_full(self) -> Any: ...

    def read_slice(self, *, lower: str, upper: Optional[str]) -> Any: ...

    def count_between(self, *, lower: str, upper: Optional[str]) -> int: ...


@dataclass
class SinkWriteResult:
    rows: int
    path: str
    event_payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SinkFinalizeResult:
    final_path: str
    event_payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IngestionSlice:
    lower: str
    upper: Optional[str] = None


@dataclass
class SliceStageResult:
    slice: IngestionSlice
    path: str
    rows: int
    skipped: bool = False
    event_payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class IncrementalContext:
    schema: str
    table: str
    load_date: str
    incremental_column: str
    incremental_type: str
    primary_keys: List[str]
    effective_watermark: str
    last_watermark: str
    last_loaded_date: str
    planner_metadata: Dict[str, Any] = field(default_factory=dict)
    is_epoch: bool = False


@dataclass
class IncrementalCommitResult:
    rows: int
    raw_path: str
    new_watermark: str
    new_loaded_date: str
    raw_event_payload: Dict[str, Any] = field(default_factory=dict)
    intermediate_event_payload: Dict[str, Any] = field(default_factory=dict)
    additional_metadata: Dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class SinkEndpoint(BaseEndpoint, Protocol):
    """Contract for landing data and finalising outputs."""

    def write_raw(
        self,
        df: DataFrame,
        *,
        mode: str,
        load_date: str,
        schema: str,
        table: str,
    ) -> SinkWriteResult: ...

    def finalize_full(
        self,
        *,
        load_date: str,
        schema: str,
        table: str,
    ) -> SinkFinalizeResult: ...

    def stage_incremental_slice(
        self,
        df: DataFrame,
        *,
        context: IncrementalContext,
        slice_info: IngestionSlice,
    ) -> SliceStageResult: ...

    def commit_incremental(
        self,
        *,
        load_date: str,
        schema: str,
        table: str,
        context: IncrementalContext,
        staged_slices: List[SliceStageResult],
    ) -> IncrementalCommitResult: ...

    def publish_dataset(
        self,
        *,
        load_date: str,
        schema: str,
        table: str,
    ) -> Dict[str, Any]: ...

    def latest_watermark(
        self,
        *,
        schema: str,
        table: str,
    ) -> Optional[str]: ...


@runtime_checkable
class DataEndpoint(SourceEndpoint, SinkEndpoint, Protocol):
    """Endpoints that support both source and sink operations."""
    ...


class EndpointRegistry:
    """Simple registry for named endpoint factories."""

    def __init__(self) -> None:
        self._sources: Dict[str, Any] = {}
        self._sinks: Dict[str, Any] = {}

    def register_source(self, key: str, factory: Any) -> None:
        self._sources[key.lower()] = factory

    def register_sink(self, key: str, factory: Any) -> None:
        self._sinks[key.lower()] = factory

    def source(self, key: str):
        return self._sources.get(key.lower())

    def sink(self, key: str):
        return self._sinks.get(key.lower())


# global registry instance for convenience
REGISTRY = EndpointRegistry()
