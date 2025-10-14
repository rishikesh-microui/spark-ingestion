from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class MetadataTarget:
    """Identifies a metadata artifact within a source."""

    namespace: str
    entity: str


@dataclass
class DataSourceMetadata:
    """Describes a physical or logical data source/system."""

    id: Optional[str] = None
    name: Optional[str] = None
    type: Optional[str] = None  # e.g., oracle, snowflake, s3
    system: Optional[str] = None  # hostname, account, cluster
    environment: Optional[str] = None  # prod, staging, region
    version: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DatasetMetadata:
    """Represents a dataset/table/view exposed to consumers."""

    id: Optional[str] = None
    name: str = ""
    physical_name: Optional[str] = None
    type: str = "table"  # table, view, stream, file, topic
    source_id: Optional[str] = None
    location: Optional[str] = None  # path, database.schema, bucket/key
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SchemaFieldStatistics:
    distinct_count: Optional[int] = None
    null_count: Optional[int] = None
    density: Optional[float] = None
    average_length: Optional[float] = None
    histogram: Optional[Dict[str, Any]] = None
    last_analyzed: Optional[str] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    min_value_length: Optional[int] = None
    max_value_length: Optional[int] = None
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SchemaField:
    """Schema field/column level metadata."""

    name: str
    data_type: str
    precision: Optional[int] = None
    scale: Optional[int] = None
    length: Optional[int] = None
    nullable: bool = True
    default: Optional[str] = None
    comment: Optional[str] = None
    position: Optional[int] = None
    character_used: Optional[str] = None
    statistics: Optional[SchemaFieldStatistics] = None
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DatasetConstraintField:
    field: str
    position: Optional[int] = None


@dataclass
class DatasetConstraint:
    name: str
    constraint_type: str
    status: Optional[str] = None
    deferrable: Optional[str] = None
    deferred: Optional[str] = None
    validated: Optional[str] = None
    generated: Optional[str] = None
    delete_rule: Optional[str] = None
    referenced_constraint: Optional[str] = None
    fields: List[DatasetConstraintField] = field(default_factory=list)
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DatasetStatistics:
    record_count: Optional[int] = None
    storage_blocks: Optional[int] = None
    average_record_size: Optional[int] = None
    sample_size: Optional[int] = None
    last_profiled_at: Optional[str] = None
    stale: Optional[str] = None
    global_stats: Optional[str] = None
    user_stats: Optional[str] = None
    temporary: Optional[str] = None
    partitioned: Optional[str] = None
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataProcessMetadata:
    """Metadata about downstream ETL/processing jobs."""

    id: Optional[str] = None
    name: str = ""
    process_type: Optional[str] = None  # batch, streaming, dbt, airflow
    system: Optional[str] = None  # orchestrator identifier
    schedule: Optional[str] = None
    inputs: List[str] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)
    owners: List[str] = field(default_factory=list)
    extras: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OwnershipAssignment:
    """Ownership/contact information."""

    owner_id: str = ""
    owner_type: str = ""  # person, team, service
    role: Optional[str] = None
    contact: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CatalogSnapshot:
    source: str
    schema: str
    name: str
    data_source: Optional[DataSourceMetadata] = None
    dataset: Optional[DatasetMetadata] = None
    environment: Dict[str, Any] = field(default_factory=dict)
    schema_fields: List[SchemaField] = field(default_factory=list)
    statistics: Optional[DatasetStatistics] = None
    constraints: List[DatasetConstraint] = field(default_factory=list)
    processes: List[DataProcessMetadata] = field(default_factory=list)
    ownerships: List[OwnershipAssignment] = field(default_factory=list)
    relationships: Dict[str, Any] = field(default_factory=dict)
    raw_vendor: Dict[str, Any] = field(default_factory=dict)
    debug: Dict[str, Any] = field(default_factory=dict)
    extras: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        legacy_columns = getattr(self, "columns", None)
        if not self.schema_fields and legacy_columns:
            self.schema_fields = legacy_columns
        elif self.schema_fields and legacy_columns is None:
            setattr(self, "columns", list(self.schema_fields))

        dataset_id = None
        if self.dataset:
            dataset_id = self.dataset.id or self.dataset.name or self.name
        source_id = None
        if self.data_source:
            source_id = self.data_source.id or self.data_source.name

        if dataset_id and source_id:
            self.relationships.setdefault("dataset_to_source", []).append(
                {"dataset": dataset_id, "data_source": source_id}
            )

        if dataset_id and self.schema_fields:
            field_names = [field.name for field in self.schema_fields if field.name]
            if field_names:
                self.relationships.setdefault("dataset_fields", []).append(
                    {"dataset": dataset_id, "fields": field_names}
                )
