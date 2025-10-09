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
    SourceEndpoint,
    SinkEndpoint,
)
from .factory import EndpointFactory
from .jdbc import JdbcEndpoint, OracleEndpoint, MSSQLEndpoint
from .storage import HdfsParquetEndpoint

__all__ = [
    "EndpointCapabilities",
    "EndpointFactory",
    "EndpointRole",
    "EndpointType",
    "HdfsParquetEndpoint",
    "JdbcEndpoint",
    "MSSQLEndpoint",
    "OracleEndpoint",
    "SinkEndpoint",
    "SourceEndpoint",
]
