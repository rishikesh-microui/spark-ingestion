from __future__ import annotations

from typing import Any, Dict, Tuple

from .base import REGISTRY, SinkEndpoint, SourceEndpoint
from .hdfs import HdfsParquetEndpoint
from .jdbc import JdbcEndpoint
from .jdbc_mssql import MSSQLEndpoint
from .jdbc_oracle import OracleEndpoint


class EndpointFactory:
    """Construct source/sink endpoints based on table configuration."""

    @staticmethod
    def build_source(
        cfg: Dict[str, Any],
        table_cfg: Dict[str, Any],
        tool,
    ) -> SourceEndpoint:
        if tool is None:
            raise ValueError("Execution tool required for source endpoint")
        jdbc_cfg = cfg.get("jdbc", {})
        dialect = (jdbc_cfg.get("dialect") or table_cfg.get("dialect") or "generic").lower()
        if dialect == "oracle":
            endpoint = OracleEndpoint(tool, jdbc_cfg, table_cfg)
        elif dialect in {"mssql", "sqlserver"}:
            endpoint = MSSQLEndpoint(tool, jdbc_cfg, table_cfg)
        else:
            endpoint = JdbcEndpoint(tool, jdbc_cfg, table_cfg)
        return endpoint

    @staticmethod
    def build_sink(
        tool,
        cfg: Dict[str, Any],
        table_cfg: Dict[str, Any],
    ) -> SinkEndpoint:
        spark = getattr(tool, "spark", None)
        if spark is None:
            raise ValueError("Execution tool must expose a Spark session for HDFS sinks")
        endpoint = HdfsParquetEndpoint(spark, cfg, table_cfg)
        return endpoint

    @staticmethod
    def build_endpoints(
        tool,
        cfg: Dict[str, Any],
        table_cfg: Dict[str, Any],
    ) -> Tuple[SourceEndpoint, SinkEndpoint]:
        return (
            EndpointFactory.build_source(cfg, table_cfg, tool),
            EndpointFactory.build_sink(tool, cfg, table_cfg),
        )


# Register defaults for simple lookup when needed
REGISTRY.register_source("jdbc", JdbcEndpoint)
REGISTRY.register_source("oracle", OracleEndpoint)
REGISTRY.register_source("mssql", MSSQLEndpoint)
REGISTRY.register_sink("hdfs", HdfsParquetEndpoint)
