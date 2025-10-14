from __future__ import annotations

from typing import Any, Dict, List, Optional

from salam_ingest.metadata.core import (
    CatalogSnapshot,
    DataSourceMetadata,
    DatasetConstraint,
    DatasetConstraintField,
    DatasetMetadata,
    DatasetStatistics,
    SchemaField,
    SchemaFieldStatistics,
)
from salam_ingest.metadata.normalizers.base import MetadataNormalizer
from salam_ingest.metadata.utils import safe_upper


class OracleMetadataNormalizer(MetadataNormalizer):
    def normalize(
        self,
        *,
        raw: Dict[str, Any],
        environment: Dict[str, Any],
        config: Dict[str, Any],
        endpoint_descriptor: Dict[str, Any],
    ) -> CatalogSnapshot:
        schema = safe_upper(raw.get("schema") or endpoint_descriptor.get("schema") or "")
        dataset_name = safe_upper(raw.get("table") or endpoint_descriptor.get("table") or "")
        source = endpoint_descriptor.get("dialect") or raw.get("source") or "oracle"

        field_stats_lookup = self._build_schema_field_stats(raw.get("column_statistics"))
        column_comments = (raw.get("comments") or {}).get("columns") or {}
        schema_fields: List[SchemaField] = []
        for col in raw.get("columns", []):
            name = col.get("column_name") or col.get("name")
            if not name:
                continue
            schema_fields.append(
                SchemaField(
                    name=name,
                    data_type=col.get("data_type"),
                    precision=_safe_int(col.get("data_precision")),
                    scale=_safe_int(col.get("data_scale")),
                    length=_safe_int(col.get("data_length")),
                    nullable=str(col.get("nullable", "Y")).upper() != "N",
                    default=col.get("data_default"),
                    comment=_extract_comment(column_comments.get(name)),
                    position=_safe_int(col.get("column_id")),
                    character_used=col.get("char_used"),
                    statistics=field_stats_lookup.get(name),
                    extras={k: v for k, v in col.items() if k not in _ORACLE_COLUMN_KEYS},
                )
            )

        dataset_statistics = self._build_dataset_statistics(raw.get("statistics"))
        dataset_constraints = self._build_constraints(raw.get("constraints"))

        debug = raw.get("debug", {})
        extras = {
            k: v
            for k, v in raw.items()
            if k
            not in {
                "columns",
                "statistics",
                "column_statistics",
                "comments",
                "constraints",
                "debug",
                "schema",
                "table",
                "source",
            }
        }

        instance_info = environment.get("instance")
        data_source = DataSourceMetadata(
            id=instance_info.get("instance_name") if isinstance(instance_info, dict) else environment.get("database_version"),
            name=instance_info.get("instance_name") if isinstance(instance_info, dict) else endpoint_descriptor.get("dialect"),
            type="oracle",
            system=instance_info.get("host_name") if isinstance(instance_info, dict) else None,
            environment=instance_info.get("status") if isinstance(instance_info, dict) else None,
            version=environment.get("database_version"),
            properties={"probe_sequence": environment.get("probe_sequence")} if environment.get("probe_sequence") else {},
            extras={
                k: v
                for k, v in environment.items()
                if k not in {"instance", "database_version", "probe_sequence"}
            },
        )

        dataset_comment = None
        table_comment = (raw.get("comments") or {}).get("table")
        if isinstance(table_comment, dict):
            dataset_comment = table_comment.get("comments") or table_comment.get("comment")

        dataset_descriptor = DatasetMetadata(
            id=f"{schema}.{dataset_name}" if schema and dataset_name else dataset_name,
            name=f"{schema}.{dataset_name}" if schema and dataset_name else dataset_name,
            physical_name=f"{schema}.{dataset_name}" if schema and dataset_name else dataset_name,
            type="table",
            source_id=data_source.id,
            location=f"{schema}.{dataset_name}" if schema and dataset_name else dataset_name,
            description=dataset_comment,
            properties={
                "stale_stats": raw.get("statistics", {}).get("stale_stats"),
                "sample_size": raw.get("statistics", {}).get("sample_size"),
            }
            if raw.get("statistics")
            else {},
        )

        schema_fields.sort(key=lambda f: (f.position if f.position is not None else 1_000_000, f.name))

        return CatalogSnapshot(
            source=source,
            schema=schema,
            name=dataset_name,
            data_source=data_source,
            dataset=dataset_descriptor,
            environment=environment,
            schema_fields=schema_fields,
            statistics=dataset_statistics,
            constraints=dataset_constraints,
            raw_vendor=raw,
            debug=debug,
            extras=extras,
        )

    def _build_schema_field_stats(self, rows: Optional[List[Dict[str, Any]]]) -> Dict[str, SchemaFieldStatistics]:
        lookup: Dict[str, SchemaFieldStatistics] = {}
        if not rows:
            return lookup
        for row in rows:
            name = row.get("column_name")
            if not name:
                continue
            lookup[name] = SchemaFieldStatistics(
                distinct_count=_safe_int(row.get("num_distinct")),
                null_count=_safe_int(row.get("num_nulls")),
                density=_safe_float(row.get("density")),
                average_length=_safe_float(row.get("avg_col_len")),
                histogram=self._histogram(row),
                last_analyzed=_safe_ts(row.get("last_analyzed")),
                min_value=row.get("low_value"),
                max_value=row.get("high_value"),
                min_value_length=_safe_int(row.get("low_value_length")),
                max_value_length=_safe_int(row.get("high_value_length")),
                extras={k: v for k, v in row.items() if k not in _ORACLE_COLUMN_STAT_KEYS},
            )
        return lookup

    def _histogram(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        hist_type = row.get("histogram")
        if not hist_type or str(hist_type).upper() == "NONE":
            return None
        extras = {k: v for k, v in row.items() if k not in _ORACLE_COLUMN_STAT_KEYS}
        return {"type": hist_type, "extras": extras}

    def _build_dataset_statistics(self, row: Optional[Dict[str, Any]]) -> Optional[DatasetStatistics]:
        if not row:
            return None
        extras = {k: v for k, v in row.items() if k not in _ORACLE_DATASET_STAT_KEYS}
        return DatasetStatistics(
            record_count=_safe_int(row.get("num_rows")),
            storage_blocks=_safe_int(row.get("blocks")),
            average_record_size=_safe_int(row.get("avg_row_len")),
            sample_size=_safe_int(row.get("sample_size")),
            last_profiled_at=_safe_ts(row.get("last_analyzed")),
            stale=row.get("stale_stats"),
            global_stats=row.get("global_stats"),
            user_stats=row.get("user_stats"),
            temporary=row.get("temporary"),
            partitioned=row.get("partitioned"),
            extras=extras,
        )

    def _build_constraints(self, raw_constraints: Optional[Dict[str, Any]]) -> List[DatasetConstraint]:
        if not raw_constraints:
            return []
        constraints: List[DatasetConstraint] = []
        for data in raw_constraints.values():
            fields = [
                DatasetConstraintField(
                    field=col.get("column_name"),
                    position=_safe_int(col.get("position")),
                )
                for col in data.get("columns", [])
                if col.get("column_name")
            ]
            constraints.append(
                DatasetConstraint(
                    name=data.get("constraint_name"),
                    constraint_type=data.get("constraint_type"),
                    status=data.get("status"),
                    deferrable=data.get("deferrable"),
                    deferred=data.get("deferred"),
                    validated=data.get("validated"),
                    generated=data.get("generated"),
                    delete_rule=data.get("delete_rule"),
                    referenced_constraint=data.get("referenced_constraint"),
                    fields=sorted(fields, key=lambda f: f.position if f.position is not None else 1_000_000),
                    extras={k: v for k, v in data.items() if k not in _ORACLE_CONSTRAINT_KEYS},
                )
            )
        return constraints


def _safe_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_ts(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def _extract_comment(entry: Any) -> Optional[str]:
    if entry is None:
        return None
    if isinstance(entry, dict):
        return entry.get("comments") or entry.get("comment")
    return str(entry)


_ORACLE_COLUMN_KEYS = {
    "schema_name",
    "table_name",
    "column_name",
    "data_type",
    "data_length",
    "data_precision",
    "data_scale",
    "nullable",
    "data_default",
    "column_id",
    "char_length",
    "char_used",
}

_ORACLE_COLUMN_STAT_KEYS = {
    "schema_name",
    "table_name",
    "column_name",
    "num_distinct",
    "num_nulls",
    "density",
    "avg_col_len",
    "sample_size",
    "histogram",
    "last_analyzed",
    "low_value",
    "high_value",
    "low_value_length",
    "high_value_length",
}

_ORACLE_DATASET_STAT_KEYS = {
    "schema_name",
    "table_name",
    "num_rows",
    "blocks",
    "avg_row_len",
    "sample_size",
    "last_analyzed",
    "stale_stats",
    "global_stats",
    "user_stats",
    "temporary",
    "partitioned",
}

_ORACLE_CONSTRAINT_KEYS = {
    "constraint_name",
    "constraint_type",
    "status",
    "deferrable",
    "deferred",
    "validated",
    "generated",
    "column_name",
    "position",
    "r_constraint_name",
    "delete_rule",
}
