from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, List, Optional

from ..tools.base import ExecutionTool, QueryRequest
from .base import EndpointCapabilities, SourceEndpoint
from ..metadata.core import MetadataTarget


class JdbcEndpoint(SourceEndpoint):
    """Generic JDBC endpoint with dialect-specific subclasses."""

    DIALECT = "generic"

    def __init__(
        self,
        tool: ExecutionTool,
        jdbc_cfg: Dict[str, Any],
        table_cfg: Dict[str, Any],
        metadata_access=None,
        emitter=None,
    ) -> None:
        self.tool = tool
        self.jdbc_cfg = dict(jdbc_cfg)
        self.table_cfg = dict(table_cfg)
        self.schema = table_cfg["schema"]
        self.table = table_cfg["table"]
        self.incremental_column = table_cfg.get("incremental_column")
        self.base_from_sql = self._build_from_sql()
        self.metadata_access = metadata_access
        self.emitter = emitter
        guardrail_cfg = (self.table_cfg.get("metadata_guardrails") or {}).get("precision")
        if not guardrail_cfg and metadata_access is not None:
            defaults = getattr(metadata_access, "guardrail_defaults", {}) or {}
            guardrail_cfg = defaults.get("precision")
        guardrail_cfg = guardrail_cfg or {}
        self._precision_guardrail_enabled = bool(guardrail_cfg.get("enabled", True))
        max_precision_cfg = guardrail_cfg.get("max_precision")
        try:
            self._precision_guardrail_max = int(max_precision_cfg) if max_precision_cfg is not None else None
        except (TypeError, ValueError):
            self._precision_guardrail_max = None
        violation_action = str(guardrail_cfg.get("violation_action", "downcast")).lower()
        self._precision_guardrail_violation_action = violation_action if violation_action in {"downcast", "fail"} else "downcast"
        open_action = str(guardrail_cfg.get("open_precision_action", self._precision_guardrail_violation_action)).lower()
        self._precision_guardrail_open_action = open_action if open_action in {"downcast", "fail"} else self._precision_guardrail_violation_action
        fallback_scale_cfg = guardrail_cfg.get("fallback_scale")
        try:
            self._precision_guardrail_fallback_scale = int(fallback_scale_cfg) if fallback_scale_cfg is not None else None
        except (TypeError, ValueError):
            self._precision_guardrail_fallback_scale = None
        fallback_precision_cfg = guardrail_cfg.get("fallback_precision")
        try:
            self._precision_guardrail_fallback_precision = int(fallback_precision_cfg) if fallback_precision_cfg is not None else None
        except (TypeError, ValueError):
            self._precision_guardrail_fallback_precision = None
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
        projection = self._projection_sql()
        if projection.strip() == "*":
            dbtable = f"{self.schema}.{self.table}"
        else:
            dbtable = f"(SELECT {projection} FROM {self.base_from_sql}) t"
        options = self._jdbc_options(dbtable=dbtable)
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
        col_identifier = self._column_identifier(col) if col else None
        if not col_identifier:
            raise ValueError("incremental column required for count query")
        predicate = f"{col_identifier} > {self._literal(lower)}"
        if upper is not None:
            predicate += f" AND {col_identifier} <= {self._literal(upper)}"
        return f"(SELECT COUNT(1) AS CNT FROM {base} WHERE {predicate}) c"

    def _dbtable_for_range(self, lower: str, upper: Optional[str]) -> str:
        col = self.incremental_column
        col_identifier = self._column_identifier(col) if col else None
        if not col_identifier:
            raise ValueError("incremental column required for range query")
        predicate = f"{col_identifier} > {self._literal(lower)}"
        if upper is not None:
            predicate += f" AND {col_identifier} <= {self._literal(upper)}"
        projection = self._projection_sql()
        return f"(SELECT {projection} FROM {self.base_from_sql} WHERE {predicate}) t"

    def _projection_sql(self) -> str:
        guardrail_projection = self._precision_guardrail_projection()
        if guardrail_projection:
            return guardrail_projection
        return self._default_projection()

    def _default_projection(self) -> str:
        cols = self.table_cfg.get("cols", "*")
        if isinstance(cols, list):
            return ", ".join(cols)
        if isinstance(cols, str):
            return cols
        return "*"

    def _precision_guardrail_projection(self) -> Optional[str]:
        if not self._precision_guardrail_enabled:
            return None
        access = getattr(self, "metadata_access", None)
        guardrail = getattr(access, "precision_guardrail", None) if access else None
        if guardrail is None:
            return None
        target = MetadataTarget(namespace=self.schema.upper(), entity=self.table.upper())
        result = guardrail.evaluate(
            target,
            max_precision=self._precision_guardrail_max,
            violation_action=self._precision_guardrail_violation_action,
            open_precision_action=self._precision_guardrail_open_action,
            fallback_precision=self._precision_guardrail_fallback_precision,
            fallback_scale=self._precision_guardrail_fallback_scale,
        )
        if result.status in {"metadata_missing", "metadata_unusable"}:
            return None
        if self.emitter and result.status in {"adjusted", "fatal"}:
            from ..events.types import Event, EventCategory, EventType

            details = {
                "schema": self.schema,
                "table": self.table,
                "status": result.status,
                "issues": [asdict(issue) for issue in result.issues],
                "cast_specs": {name: asdict(spec) for name, spec in result.cast_specs.items()},
                "config": {
                    "max_precision": self._precision_guardrail_max,
                    "violation_action": self._precision_guardrail_violation_action,
                    "open_precision_action": self._precision_guardrail_open_action,
                },
            }
            self.emitter.emit(Event(category=EventCategory.GUARDRAIL, type=EventType.GUARDRAIL_PRECISION, payload=details))
        if result.status == "fatal":
            issues = "; ".join(f"{issue.column}: {issue.reason}" for issue in result.issues if not issue.handled)
            raise ValueError(
                f"precision_guardrail_violations for {self.schema}.{self.table}: {issues}"
            )
        cast_specs = result.cast_specs
        if not cast_specs:
            return None
        columns = self._guardrail_column_list(result)
        if not columns:
            return None
        rendered = [self._render_guardrail_column(col, cast_specs) for col in columns]
        if not rendered:
            return None
        return ", ".join(rendered)

    def _guardrail_column_list(self, result) -> Optional[List[str]]:
        cols_cfg = self.table_cfg.get("cols", "*")
        if isinstance(cols_cfg, list) and cols_cfg:
            return cols_cfg
        if isinstance(cols_cfg, str) and cols_cfg.strip() and cols_cfg.strip() != "*":
            return [name.strip() for name in cols_cfg.split(",") if name.strip()]
        snapshot = result.snapshot or {}
        fields = snapshot.get("schema_fields") if isinstance(snapshot, dict) else None
        if not isinstance(fields, list):
            return None
        names = []
        for field in fields:
            if isinstance(field, dict):
                name = field.get("name")
            else:
                name = getattr(field, "name", None)
            if name:
                names.append(name)
        return names or None

    def _render_guardrail_column(self, column: str, cast_specs: Dict[str, Any]) -> str:
        key = column.upper()
        spec = cast_specs.get(key)
        identifier = self._column_identifier(column)
        if spec:
            target_type = getattr(spec, "target_type", None)
            if target_type is None and isinstance(spec, dict):
                target_type = spec.get("target_type")
            if target_type and str(target_type).lower() == "string":
                return self._cast_to_string(identifier, column, spec)
            precision_value = getattr(spec, "precision", None)
            if precision_value is None and isinstance(spec, dict):
                precision_value = spec.get("precision")
            if precision_value is not None:
                return self._cast_expression(identifier, column, spec)
        return identifier

    def _column_identifier(self, column: str) -> str:
        return column

    def _column_alias(self, column: str) -> str:
        return self._column_identifier(column)

    def _cast_expression(self, identifier: str, column: str, spec) -> str:
        precision = getattr(spec, "precision", None)
        scale = getattr(spec, "scale", None)
        if precision is None and isinstance(spec, dict):
            precision = spec.get("precision")
        if scale is None and isinstance(spec, dict):
            scale = spec.get("scale")
        if precision is None:
            return identifier
        type_keyword = self._cast_type_keyword()
        scale_value = scale if scale is not None else 0
        alias = self._column_alias(column)
        return f"CAST({identifier} AS {type_keyword}({precision},{scale_value})) AS {alias}"

    def _cast_type_keyword(self) -> str:
        return "DECIMAL"

    def _cast_to_string(self, identifier: str, column: str, spec) -> str:
        alias = self._column_alias(column)
        return f"CAST({identifier} AS {self._string_cast_type()}) AS {alias}"

    def _string_cast_type(self) -> str:
        return "VARCHAR(4000)"

    def _literal(self, value: str) -> str:
        return f"'{value}'"
