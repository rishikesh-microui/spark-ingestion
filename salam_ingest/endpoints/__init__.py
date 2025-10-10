"""
Endpoint factory and helpers for interacting with external systems.

The ingestion framework relies on these endpoint abstractions so that
planners and strategies can remain agnostic of dialect or storage
details when pulling or landing data.
"""

from .base import (
    EndpointCapabilities,
    EndpointRole,
    EndpointType,
    IncrementalCommitResult,
    IncrementalContext,
    IngestionSlice,
    SinkEndpoint,
    SinkFinalizeResult,
    SinkWriteResult,
    SourceEndpoint,
    SliceStageResult,
)
from .factory import EndpointFactory
from .jdbc import JdbcEndpoint
from .jdbc_mssql import MSSQLEndpoint
from .jdbc_oracle import OracleEndpoint
from .storage import HdfsParquetEndpoint

__all__ = [
    "EndpointCapabilities",
    "EndpointFactory",
    "EndpointRole",
    "EndpointType",
    "HdfsParquetEndpoint",
    "IncrementalCommitResult",
    "IncrementalContext",
    "IngestionSlice",
    "JdbcEndpoint",
    "MSSQLEndpoint",
    "OracleEndpoint",
    "SinkEndpoint",
    "SinkFinalizeResult",
    "SinkWriteResult",
    "SourceEndpoint",
    "SliceStageResult",
]
