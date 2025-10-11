from __future__ import annotations

from typing import Any, Dict, Optional

from ..metadata.adapters.oracle import OracleMetadataSubsystem
from .base import MetadataSubsystem
from .jdbc import JdbcEndpoint


class OracleEndpoint(JdbcEndpoint):
    """Oracle-specific JDBC source."""

    DIALECT = "oracle"

    def __init__(self, tool, jdbc_cfg: Dict[str, Any], table_cfg: Dict[str, Any]) -> None:
        super().__init__(tool, jdbc_cfg, table_cfg)
        self._caps.supports_metadata = True
        self._metadata = OracleMetadataSubsystem(self)

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

    def metadata_subsystem(self) -> MetadataSubsystem:
        return self._metadata

    # --- Metadata collection ----------------------------------------------------
