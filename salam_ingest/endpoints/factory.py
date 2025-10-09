from __future__ import annotations

from typing import Any, Dict, Tuple

from pyspark.sql import SparkSession

from .base import REGISTRY, SinkEndpoint, SourceEndpoint
from .jdbc import JdbcEndpoint, MSSQLEndpoint, OracleEndpoint
from .storage import HdfsParquetEndpoint


class EndpointFactory:
    """Construct source/sink endpoints based on table configuration."""

    @staticmethod
    def build_source(
        spark: SparkSession,
        cfg: Dict[str, Any],
        table_cfg: Dict[str, Any],
    ) -> SourceEndpoint:
        jdbc_cfg = cfg.get("jdbc", {})
        dialect = (jdbc_cfg.get("dialect") or table_cfg.get("dialect") or "generic").lower()
        if dialect == "oracle":
            endpoint = OracleEndpoint(spark, jdbc_cfg, table_cfg)
        elif dialect in {"mssql", "sqlserver"}:
            endpoint = MSSQLEndpoint(spark, jdbc_cfg, table_cfg)
        else:
            endpoint = JdbcEndpoint(spark, jdbc_cfg, table_cfg)
        return endpoint

    @staticmethod
    def build_sink(
        spark: SparkSession,
        cfg: Dict[str, Any],
        table_cfg: Dict[str, Any],
    ) -> SinkEndpoint:
        runtime = cfg["runtime"]
        endpoint = HdfsParquetEndpoint(spark, runtime, table_cfg)
        return endpoint

    @staticmethod
    def build_endpoints(
        spark: SparkSession,
        cfg: Dict[str, Any],
        table_cfg: Dict[str, Any],
    ) -> Tuple[SourceEndpoint, SinkEndpoint]:
        return (
            EndpointFactory.build_source(spark, cfg, table_cfg),
            EndpointFactory.build_sink(spark, cfg, table_cfg),
        )


# Register defaults for simple lookup when needed
REGISTRY.register_source("jdbc", JdbcEndpoint)
REGISTRY.register_source("oracle", OracleEndpoint)
REGISTRY.register_source("mssql", MSSQLEndpoint)
REGISTRY.register_sink("hdfs", HdfsParquetEndpoint)
