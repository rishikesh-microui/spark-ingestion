from __future__ import annotations

from typing import Optional

from .jdbc import JdbcEndpoint


class MSSQLEndpoint(JdbcEndpoint):
    """Microsoft SQL Server JDBC source."""

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
        col_identifier = self._column_identifier(col) if col else None
        if not col_identifier:
            raise ValueError("incremental column required for count query")
        predicate = f"{col_identifier} > {self._literal(lower)}"
        if upper is not None:
            predicate += f" AND {col_identifier} <= {self._literal(upper)}"
        return f"(SELECT COUNT_BIG(1) AS CNT FROM {base} WHERE {predicate}) c"

    def _column_identifier(self, name: str) -> str:
        return self._column(name)

    def _column_alias(self, column: str) -> str:
        return self._column(column)

    @staticmethod
    def _column(name: str) -> str:
        return f"[{name}]"
