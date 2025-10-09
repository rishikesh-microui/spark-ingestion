from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Dict, Iterable, Optional, Protocol, runtime_checkable

from pyspark.sql import DataFrame, SparkSession


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
    incremental_literal: str = "timestamp"  # timestamp | epoch
    default_fetchsize: int = 10000


@runtime_checkable
class SourceEndpoint(Protocol):
    """Contract for any endpoint that can provide data to Spark."""

    def configure(self, spark: SparkSession, table_cfg: Dict[str, Any]) -> None: ...

    def capabilities(self) -> EndpointCapabilities: ...

    def describe(self) -> Dict[str, Any]: ...

    def read_full(self) -> DataFrame: ...

    def read_slice(self, *, lower: str, upper: Optional[str]) -> DataFrame: ...

    def count_between(self, *, lower: str, upper: Optional[str]) -> int: ...


@runtime_checkable
class SinkEndpoint(Protocol):
    """Contract for landing data and finalising outputs."""

    def configure(self, spark: SparkSession, table_cfg: Dict[str, Any]) -> None: ...

    def capabilities(self) -> EndpointCapabilities: ...

    def write_raw(
        self,
        df: DataFrame,
        *,
        mode: str,
        load_date: str,
        schema: str,
        table: str,
    ) -> Dict[str, Any]: ...

    def finalize_full(
        self,
        *,
        load_date: str,
        schema: str,
        table: str,
    ) -> Dict[str, Any]: ...

    def append_incremental(
        self,
        df: DataFrame,
        *,
        load_date: str,
        schema: str,
        table: str,
        mode: str,
    ) -> Dict[str, Any]: ...


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
