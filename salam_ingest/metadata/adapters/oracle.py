from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from pyspark.sql.utils import AnalysisException

from salam_ingest.endpoints.base import MetadataSubsystem
from salam_ingest.metadata.core import CatalogSnapshot
from salam_ingest.metadata.normalizers import OracleMetadataNormalizer
from salam_ingest.metadata.utils import collect_rows, escape_literal, safe_upper
from salam_ingest.tools.base import QueryRequest

if TYPE_CHECKING:  # pragma: no cover
    from salam_ingest.endpoints.jdbc_oracle import OracleEndpoint


class OracleMetadataSubsystem(MetadataSubsystem):
    """Metadata subsystem for Oracle sources."""

    DEFAULT_ENVIRONMENT_PROBES: List[Dict[str, str]] = [
        {
            "name": "instance",
            "sql": """
                SELECT INSTANCE_NAME, VERSION, HOST_NAME, STARTUP_TIME, STATUS
                FROM V$INSTANCE
            """,
        },
        {
            "name": "version_banner",
            "sql": "SELECT BANNER FROM V$VERSION ORDER BY BANNER",
        },
        {
            "name": "component_versions",
            "sql": """
                SELECT PRODUCT, VERSION, STATUS
                FROM PRODUCT_COMPONENT_VERSION
                ORDER BY PRODUCT
            """,
        },
    ]

    _ENVIRONMENT_CACHE: Dict[str, Dict[str, Any]] = {}

    def __init__(self, endpoint: "OracleEndpoint") -> None:
        self.endpoint = endpoint
        self._normalizer = OracleMetadataNormalizer()

    # ------------------------------------------------------------------ protocol --
    def capabilities(self) -> Dict[str, Any]:
        return {
            "sections": [
                "environment",
                "schema_fields",
                "statistics",
                "schema_field_statistics",
                "comments",
                "constraints",
            ],
            "supports_query_overrides": True,
            "supports_version_sensitive_queries": True,
        }

    def probe_environment(self, *, config: Dict[str, Any]) -> Dict[str, Any]:
        config = dict(config or {})
        probes = self._environment_probe_definitions(config)
        key = repr(
            (
                self.endpoint.jdbc_cfg.get("url"),
                self.endpoint.jdbc_cfg.get("user"),
                tuple((probe.get("name"), probe.get("sql")) for probe in probes),
                json.dumps(config, sort_keys=True, default=str),
            )
        )
        cached = self._ENVIRONMENT_CACHE.get(key)
        if cached is not None:
            return cached
        info: Dict[str, Any] = {
            "dialect": self.endpoint.jdbc_cfg.get("dialect", self.endpoint.DIALECT),
            "driver": self.endpoint.jdbc_cfg.get("driver"),
        }
        executed: List[str] = []
        for probe in probes:
            name = probe.get("name") or "probe"
            sql = probe.get("sql")
            if not sql:
                continue
            rows = self._run_metadata_query(sql)
            if not rows:
                continue
            executed.append(name)
            if name == "instance":
                info["instance"] = rows[0]
                info["database_version"] = rows[0].get("version")
            elif name == "version_banner":
                info["banners"] = [row.get("banner") for row in rows if isinstance(row, dict)]
            elif name == "component_versions":
                info["components"] = rows
                if "database_version" not in info:
                    info["database_version"] = rows[0].get("version")
            else:
                info.setdefault("additional_probes", {})[name] = rows
        info["probe_sequence"] = executed
        self._ENVIRONMENT_CACHE[key] = info
        return info

    def collect_snapshot(
        self,
        *,
        config: Dict[str, Any],
        environment: Dict[str, Any],
    ) -> CatalogSnapshot:
        owner = safe_upper(self.endpoint.schema)
        rel = safe_upper(self.endpoint.table)
        config = dict(config or {})
        environment = dict(environment or {})

        raw: Dict[str, Any] = {
            "source": self.endpoint.jdbc_cfg.get("dialect", self.endpoint.DIALECT),
            "schema": owner,
            "table": rel,
            "environment": environment,
        }
        queries_used: Dict[str, str] = {}

        if self._section_enabled("schema_fields", config, aliases=["columns"]):
            sql = self._resolve_query("columns", self._columns_sql(owner, rel), config, environment)
            raw["columns"] = self._run_metadata_query(sql)
            queries_used["columns"] = sql
        if self._section_enabled("dataset_statistics", config, aliases=["statistics"]):
            sql = self._resolve_query("statistics", self._table_stats_sql(owner, rel), config, environment)
            rows = self._run_metadata_query(sql)
            raw["statistics"] = rows[0] if rows else {}
            queries_used["statistics"] = sql
        if self._section_enabled("schema_field_statistics", config, aliases=["column_statistics"]):
            sql = self._resolve_query("column_statistics", self._column_stats_sql(owner, rel), config, environment)
            raw["column_statistics"] = self._run_metadata_query(sql)
            queries_used["column_statistics"] = sql
        if self._section_enabled("comments", config):
            table_sql, column_sql = self._comments_sql(owner, rel)
            table_sql = self._resolve_query("table_comments", table_sql, config, environment)
            column_sql = self._resolve_query("column_comments", column_sql, config, environment)
            table_comments = self._run_metadata_query(table_sql)
            column_comments = self._run_metadata_query(column_sql)
            raw["comments"] = {
                "table": table_comments[0] if table_comments else {},
                "columns": {row.get("column_name"): row for row in column_comments if isinstance(row, dict)},
            }
            queries_used["table_comments"] = table_sql
            queries_used["column_comments"] = column_sql
        if self._section_enabled("constraints", config):
            sql = self._resolve_query("constraints", self._constraints_sql(owner, rel), config, environment)
            raw["constraints"] = self._fetch_constraints_from_query(sql)
            queries_used["constraints"] = sql

        if queries_used:
            raw.setdefault("debug", {})["queries"] = queries_used

        snapshot = self._normalizer.normalize(
            raw=raw,
            environment=environment,
            config=config,
            endpoint_descriptor=self.endpoint.describe(),
        )
        snapshot.debug.setdefault("metadata_capabilities", self.capabilities())
        return snapshot

    # ------------------------------------------------------------------ helpers --
    def _environment_probe_definitions(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        use_defaults = not config.get("skip_default_probes", False)
        probes: Dict[str, Dict[str, Any]] = {}
        if use_defaults:
            for entry in self.DEFAULT_ENVIRONMENT_PROBES:
                probes[entry["name"]] = dict(entry)
        extra = config.get("environment_probes") or config.get("additional_probes") or []
        if isinstance(extra, dict):
            extra = [extra]
        for entry in extra:
            name = entry.get("name")
            sql = entry.get("sql")
            if not name or not sql:
                continue
            probes[name] = dict(entry)
        ordered_names = config.get("probe_order")
        if isinstance(ordered_names, (list, tuple)):
            ordered = [probes[name] for name in ordered_names if name in probes]
            for name, entry in probes.items():
                if name not in ordered_names:
                    ordered.append(entry)
            return ordered
        return list(probes.values())

    def _section_enabled(self, section: str, config: Dict[str, Any], aliases: Optional[List[str]] = None) -> bool:
        names = [section] + list(aliases or [])
        include = config.get("include_sections")
        if isinstance(include, (list, tuple, set)):
            include_set = {str(item).lower() for item in include}
            if any(name.lower() in include_set for name in names):
                return True
            return False
        skip = config.get("skip_sections") or config.get("exclude_sections")
        if isinstance(skip, (list, tuple, set)):
            skip_set = {str(item).lower() for item in skip}
            if any(name.lower() in skip_set for name in names):
                return False
        for name in names:
            flag = config.get(f"collect_{name}")
            if flag is not None:
                return bool(flag)
        return True

    def _resolve_query(
        self,
        name: str,
        default_sql: str,
        config: Dict[str, Any],
        environment: Dict[str, Any],
    ) -> str:
        queries = config.get("queries")
        if not isinstance(queries, dict):
            return default_sql
        override = queries.get(name)
        if override is None:
            return default_sql
        if isinstance(override, str):
            return override
        if isinstance(override, dict):
            version = str(environment.get("database_version") or "").lower()
            versions = override.get("versions")
            if isinstance(versions, dict):
                for key, sql in versions.items():
                    if not isinstance(sql, str):
                        continue
                    key_norm = str(key).lower()
                    if version.startswith(key_norm):
                        return sql
            pattern = override.get("match_version")
            if isinstance(pattern, (list, tuple)):
                for key in pattern:
                    if version.startswith(str(key).lower()):
                        sql = override.get("sql") or override.get("value")
                        if isinstance(sql, str):
                            return sql
            default_override = override.get("default") or override.get("sql") or override.get("value")
            if isinstance(default_override, str):
                return default_override
        return default_sql

    def _run_metadata_query(self, sql: str) -> List[Dict[str, Any]]:
        options = self.endpoint._jdbc_options(dbtable=f"({sql}) q")
        options["fetchsize"] = min(int(options.get("fetchsize", 1000)), 1000)
        request = QueryRequest(format="jdbc", options=options)
        try:
            result = self.endpoint.tool.query(request)
        except AnalysisException as exc:
            raise RuntimeError(f"Oracle metadata query failed: {exc.desc}") from exc
        except Exception as exc:  # pragma: no cover - defensive
            raise RuntimeError(f"Oracle metadata query error: {exc}") from exc
        return collect_rows(result)

    def _columns_sql(self, schema: str, table: str) -> str:
        return f"""
        SELECT
            OWNER AS schema_name,
            TABLE_NAME,
            COLUMN_NAME,
            DATA_TYPE,
            DATA_LENGTH,
            DATA_PRECISION,
            DATA_SCALE,
            NULLABLE,
            DATA_DEFAULT,
            COLUMN_ID,
            CHAR_LENGTH,
            CHAR_USED
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = '{escape_literal(schema)}'
          AND TABLE_NAME = '{escape_literal(table)}'
        ORDER BY COLUMN_ID
        """

    def _table_stats_sql(self, schema: str, table: str) -> str:
        return f"""
        SELECT
            OWNER AS schema_name,
            TABLE_NAME,
            NUM_ROWS,
            BLOCKS,
            AVG_ROW_LEN,
            SAMPLE_SIZE,
            LAST_ANALYZED,
            STALE_STATS,
            GLOBAL_STATS,
            USER_STATS,
            TEMPORARY,
            PARTITIONED
        FROM ALL_TAB_STATISTICS
        WHERE OWNER = '{escape_literal(schema)}'
          AND TABLE_NAME = '{escape_literal(table)}'
        """

    def _column_stats_sql(self, schema: str, table: str) -> str:
        return f"""
        SELECT
            OWNER AS schema_name,
            TABLE_NAME,
            COLUMN_NAME,
            NUM_DISTINCT,
            NUM_NULLS,
            DENSITY,
            AVG_COL_LEN,
            SAMPLE_SIZE,
            HISTOGRAM,
            LAST_ANALYZED
        FROM ALL_TAB_COL_STATISTICS
        WHERE OWNER = '{escape_literal(schema)}'
          AND TABLE_NAME = '{escape_literal(table)}'
        ORDER BY COLUMN_NAME
        """

    def _comments_sql(self, schema: str, table: str) -> Any:
        table_sql = f"""
        SELECT
            OWNER AS schema_name,
            TABLE_NAME,
            COMMENTS
        FROM ALL_TAB_COMMENTS
        WHERE OWNER = '{escape_literal(schema)}'
          AND TABLE_NAME = '{escape_literal(table)}'
        """
        column_sql = f"""
        SELECT
            OWNER AS schema_name,
            TABLE_NAME,
            COLUMN_NAME,
            COMMENTS
        FROM ALL_COL_COMMENTS
        WHERE OWNER = '{escape_literal(schema)}'
          AND TABLE_NAME = '{escape_literal(table)}'
        """
        return table_sql, column_sql

    def _constraints_sql(self, schema: str, table: str) -> str:
        return f"""
        SELECT
            ac.CONSTRAINT_NAME,
            ac.CONSTRAINT_TYPE,
            ac.STATUS,
            ac.DEFERRABLE,
            ac.DEFERRED,
            ac.VALIDATED,
            ac.GENERATED,
            acc.COLUMN_NAME,
            acc.POSITION,
            ac.R_CONSTRAINT_NAME,
            ac.DELETE_RULE
        FROM ALL_CONSTRAINTS ac
        JOIN ALL_CONS_COLUMNS acc
          ON ac.OWNER = acc.OWNER
         AND ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
         AND ac.TABLE_NAME = acc.TABLE_NAME
        WHERE ac.OWNER = '{escape_literal(schema)}'
          AND ac.TABLE_NAME = '{escape_literal(table)}'
          AND ac.CONSTRAINT_TYPE IN ('P', 'R', 'U')
        ORDER BY ac.CONSTRAINT_NAME, acc.POSITION
        """

    def _fetch_constraints_from_query(self, sql: str) -> Dict[str, Any]:
        rows = self._run_metadata_query(sql)
        constraints: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            name = row.get("constraint_name")
            if not name:
                continue
            entry = constraints.setdefault(
                name,
                {
                    "constraint_name": name,
                    "constraint_type": row.get("constraint_type"),
                    "status": row.get("status"),
                    "deferrable": row.get("deferrable"),
                    "deferred": row.get("deferred"),
                    "validated": row.get("validated"),
                    "generated": row.get("generated"),
                    "delete_rule": row.get("delete_rule"),
                    "referenced_constraint": row.get("r_constraint_name"),
                    "columns": [],
                },
            )
            entry["columns"].append(
                {
                    "column_name": row.get("column_name"),
                    "position": row.get("position"),
                }
            )
        return constraints

    def _run_metadata_query(self, sql: str) -> List[Dict[str, Any]]:
        options = self.endpoint._jdbc_options(dbtable=f"({sql}) q")
        options["fetchsize"] = min(int(options.get("fetchsize", 1000)), 1000)
        request = QueryRequest(format="jdbc", options=options)
        try:
            result = self.endpoint.tool.query(request)
        except AnalysisException as exc:
            raise RuntimeError(f"Oracle metadata query failed: {exc.desc}") from exc
        except Exception as exc:  # pragma: no cover - defensive
            raise RuntimeError(f"Oracle metadata query error: {exc}") from exc
        return collect_rows(result)
