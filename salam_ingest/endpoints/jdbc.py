from __future__ import annotations

from typing import Any, Dict, Optional

from ..tools.base import ExecutionTool, QueryRequest
from .base import EndpointCapabilities, SourceEndpoint


class JdbcEndpoint(SourceEndpoint):
    """Generic JDBC endpoint with dialect-specific subclasses."""

    DIALECT = "generic"

    def __init__(self, tool: ExecutionTool, jdbc_cfg: Dict[str, Any], table_cfg: Dict[str, Any]) -> None:
        self.tool = tool
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
    def configure(self, table_cfg: Dict[str, Any]) -> None:  # pragma: no cover
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

    def read_full(self) -> Any:
        options = self._jdbc_options(dbtable=f"{self.schema}.{self.table}")
        partition = self._partition_options()
        request = QueryRequest(format="jdbc", options=options, partition_options=partition)
        return self.tool.query(request)

    def read_slice(self, *, lower: str, upper: Optional[str]) -> Any:
        dbtable = self._dbtable_for_range(lower, upper)
        options = self._jdbc_options(dbtable=dbtable)
        partition = self._partition_options()
        request = QueryRequest(format="jdbc", options=options, partition_options=partition)
        return self.tool.query(request)

    def count_between(self, *, lower: str, upper: Optional[str]) -> int:
        query = self._count_query(lower, upper)
        options = self._jdbc_options(dbtable=query)
        options["fetchsize"] = 1
        request = QueryRequest(format="jdbc", options=options, partition_options=None)
        return int(self.tool.query_scalar(request))

    # --- Helpers -----------------------------------------------------------------
    def _jdbc_options(self, *, dbtable: str) -> Dict[str, Any]:
        cfg = self.jdbc_cfg
        options = {
            "url": cfg["url"],
            "dbtable": dbtable,
            "user": cfg["user"],
            "password": cfg["password"],
            "driver": cfg["driver"],
            "fetchsize": int(cfg.get("fetchsize", self._caps.default_fetchsize)),
        }
        if cfg.get("trustServerCertificate"):
            options["trustServerCertificate"] = cfg["trustServerCertificate"]
        return options

    def _partition_options(self) -> Optional[Dict[str, Any]]:
        partition_cfg = self.table_cfg.get("partition_read")
        if not partition_cfg:
            return None
        return {
            "partitionColumn": partition_cfg["partitionColumn"],
            "lowerBound": str(partition_cfg["lowerBound"]),
            "upperBound": str(partition_cfg["upperBound"]),
            "numPartitions": str(partition_cfg.get("numPartitions", self.jdbc_cfg.get("default_num_partitions", 8))),
        }


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
