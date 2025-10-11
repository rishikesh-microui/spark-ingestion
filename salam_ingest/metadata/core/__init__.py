"""Core metadata models and utilities."""

from .model import (
    MetadataTarget,
    CatalogSnapshot,
    DataProcessMetadata,
    DataSourceMetadata,
    DatasetConstraint,
    DatasetConstraintField,
    DatasetMetadata,
    DatasetStatistics,
    OwnershipAssignment,
    SchemaField,
    SchemaFieldStatistics,
)
from .interfaces import (
    MetadataContext,
    MetadataConsumer,
    MetadataProducer,
    MetadataQuery,
    MetadataRecord,
    MetadataRepository,
    MetadataRequest,
    MetadataTransformer,
)
from .engine import MetadataProducerRunner, ProducerRunResult
from .repository import JsonFileMetadataRepository

__all__ = [
    "MetadataTarget",
    "CatalogSnapshot",
    "DataProcessMetadata",
    "DataSourceMetadata",
    "DatasetConstraint",
    "DatasetConstraintField",
    "DatasetMetadata",
    "DatasetStatistics",
    "OwnershipAssignment",
    "SchemaField",
    "SchemaFieldStatistics",
    "MetadataContext",
    "MetadataConsumer",
    "MetadataProducer",
    "MetadataQuery",
    "MetadataRecord",
    "MetadataRepository",
    "MetadataRequest",
    "MetadataTransformer",
    "MetadataProducerRunner",
    "ProducerRunResult",
    "JsonFileMetadataRepository",
]
