from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession

from .base import EndpointCapabilities, SourceEndpoint


class JdbcEndpoint(SourceEndpoint):
    """Generic JDBC endpoint with dialect-specific subclasses."""

    DIALECT = "generic"

    def __init__(self, spark: SparkSession, jdbc_cfg: Dict[str, Any], table_cfg: Dict[str, Any]) -> None:
        self.spark = spark
        self.jdbc_cfg = dict(jdbc_cfg)
        self.table_cfg = dict(table_cfg)
        self.schema = table_cfg["schema"]
        self.table = table_cfg["table"]
        self.incremental_column = table_cfg.get("incremental_column")
        self.base_from_sql = self._build_from_sql()
        self._caps = EndpointCapabilities(
            supports_full=True,
            supports_incremental=bool(self.incremental_column),
            supports_count_probe=True,
            incremental_literal=(table_cfg.get("incr_col_type") or "timestamp").lower(),
            default_fetchsize=int(jdbc_cfg.get("fetchsize", 10000)),
        )

    # --- SourceEndpoint protocol -------------------------------------------------
    def configure(self, spark: SparkSession, table_cfg: Dict[str, Any]) -> None:  # pragma: no cover - already configured
        self.spark = spark
        self.table_cfg.update(table_cfg)

    def capabilities(self) -> EndpointCapabilities:
        return self._caps

    def describe(self) -> Dict[str, Any]:
        return {
            "dialect": self.jdbc_cfg.get("dialect", self.DIALECT),
            "schema": self.schema,
            "table": self.table,
            "incremental_column": self.incremental_column,
            "caps": self._caps,
        }

    def read_full(self) -> DataFrame:
        reader = self._reader_builder(dbtable=f"{self.schema}.{self.table}")
        reader = self._apply_partitioning(reader)
        return reader.load()

    def read_slice(self, *, lower: str, upper: Optional[str]) -> DataFrame:
        dbtable = self._dbtable_for_range(lower, upper)
        reader = self._reader_builder(dbtable=dbtable)
        reader = self._apply_partitioning(reader)
        return reader.load()

    def count_between(self, *, lower: str, upper: Optional[str]) -> int:
        query = self._count_query(lower, upper)
        df = (
            self._reader_builder(dbtable=query)
            .option("fetchsize", 1)
            .load()
        )
        return int(df.collect()[0][0])

    # --- Helpers -----------------------------------------------------------------
    def _reader_builder(self, *, dbtable: str):
        cfg = self.jdbc_cfg
        reader = (
            self.spark.read.format("jdbc")
            .option("url", cfg["url"])
            .option("dbtable", dbtable)
            .option("user", cfg["user"])
            .option("password", cfg["password"])
            .option("driver", cfg["driver"])
            .option("fetchsize", int(cfg.get("fetchsize", self._caps.default_fetchsize)))
        )
        if cfg.get("trustServerCertificate"):
            reader = reader.option("trustServerCertificate", cfg["trustServerCertificate"])
        return reader

    def _apply_partitioning(self, reader):
        partition_cfg = self.table_cfg.get("partition_read")
        if not partition_cfg:
            return reader
        return (
            reader.option("partitionColumn", partition_cfg["partitionColumn"])
            .option("lowerBound", str(partition_cfg["lowerBound"]))
            .option("upperBound", str(partition_cfg["upperBound"]))
            .option("numPartitions", str(partition_cfg.get("numPartitions", self.jdbc_cfg.get("default_num_partitions", 8))))
        )

    def _build_from_sql(self) -> str:
        query = self.table_cfg.get("query_sql")
        if query and query.strip():
            return f"({query}) q"
        return f"{self.schema}.{self.table}"

    def _count_query(self, lower: str, upper: Optional[str]) -> str:
        col = self.incremental_column
        base = self.base_from_sql
        predicate = f"{col} > {self._literal(lower)}"
        if upper is not None:
            predicate += f" AND {col} <= {self._literal(upper)}"
        return f"(SELECT COUNT(1) AS CNT FROM {base} WHERE {predicate}) c"

    def _dbtable_for_range(self, lower: str, upper: Optional[str]) -> str:
        col = self.incremental_column
        cols = self.table_cfg.get("cols", "*")
        if isinstance(cols, list):
            cols = ", ".join(cols)
        predicate = f"{col} > {self._literal(lower)}"
        if upper is not None:
            predicate += f" AND {col} <= {self._literal(upper)}"
        return f"(SELECT {cols} FROM {self.base_from_sql} WHERE {predicate}) t"

    def _literal(self, value: str) -> str:
        return f"'{value}'"


class OracleEndpoint(JdbcEndpoint):
    DIALECT = "oracle"

    def _literal(self, value: str) -> str:
        incr_type = (self.table_cfg.get("incr_col_type") or "").lower()
        if incr_type in {"epoch_seconds", "epoch_millis", "int", "integer", "bigint"}:
            return str(int(float(value)))
        return f"TO_TIMESTAMP('{value}','YYYY-MM-DD HH24:MI:SS')"

    def _count_query(self, lower: str, upper: Optional[str]) -> str:
        col = self.incremental_column
        base = self.base_from_sql
        predicate = f"{col} > {self._literal(lower)}"
        if upper is not None:
            predicate += f" AND {col} <= {self._literal(upper)}"
        return f"(SELECT COUNT(1) AS CNT FROM {base} WHERE {predicate}) c"


class MSSQLEndpoint(JdbcEndpoint):
    DIALECT = "mssql"

    def _literal(self, value: str) -> str:
        incr_type = (self.table_cfg.get("incr_col_type") or "").lower()
        if incr_type in {"epoch_seconds", "epoch_millis", "int", "integer", "bigint"}:
            return str(int(float(value)))
        safe = value.replace(" ", "T")
        return f"CONVERT(DATETIME2,'{safe}',126)"

    def _build_from_sql(self) -> str:
        query = self.table_cfg.get("query_sql")
        if query and query.strip():
            return f"({query}) q"
        return f"[{self.schema}].[{self.table}]"

    def _count_query(self, lower: str, upper: Optional[str]) -> str:
        col = self.incremental_column
        base = self.base_from_sql
        predicate = f"{self._column(col)} > {self._literal(lower)}"
        if upper is not None:
            predicate += f" AND {self._column(col)} <= {self._literal(upper)}"
        return f"(SELECT COUNT_BIG(1) AS CNT FROM {base} WHERE {predicate}) c"

    def _dbtable_for_range(self, lower: str, upper: Optional[str]) -> str:
        col = self.incremental_column
        cols = self.table_cfg.get("cols", "*")
        if isinstance(cols, list):
            cols = ", ".join(cols)
        predicate = f"{self._column(col)} > {self._literal(lower)}"
        if upper is not None:
            predicate += f" AND {self._column(col)} <= {self._literal(upper)}"
        return f"(SELECT {cols} FROM {self.base_from_sql} WHERE {predicate}) t"

    @staticmethod
    def _column(name: str) -> str:
        return f"[{name}]"
