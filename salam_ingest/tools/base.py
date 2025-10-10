from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol


@dataclass
class QueryRequest:
    format: str
    options: Dict[str, Any]
    partition_options: Optional[Dict[str, Any]] = None


@dataclass
class WriteRequest:
    dataset: Any
    path: str
    format: str
    mode: str
    options: Optional[Dict[str, Any]] = None


class ExecutionTool(Protocol):
    """Execution backend interface (Spark, Flink, Rust, etc.)."""

    def query(self, request: QueryRequest) -> Any: ...

    def query_scalar(self, request: QueryRequest) -> Any: ...

    def write_dataset(self, request: WriteRequest) -> None: ...

    def write_text(self, path: str, content: str) -> None: ...
