"""Metadata collection orchestration utilities."""

from .core import (
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
from .normalizers import MetadataNormalizer, OracleMetadataNormalizer
from .adapters import OracleMetadataSubsystem
from .services import MetadataCollectionService, MetadataServiceConfig, MetadataJob
from .consumers import PrecisionGuardrailEvaluator, PrecisionGuardrailResult, PrecisionIssue, PrecisionSpec
from .runtime import collect_metadata, build_metadata_access, MetadataAccess
from .cache import MetadataCacheConfig, MetadataCacheManager

__all__ = [
    "MetadataCollectionService",
    "MetadataServiceConfig",
    "MetadataCacheConfig",
    "MetadataCacheManager",
    "MetadataNormalizer",
    "OracleMetadataNormalizer",
    "OracleMetadataSubsystem",
    "MetadataJob",
    "MetadataTarget",
    "collect_metadata",
    "build_metadata_access",
    "MetadataAccess",
    "PrecisionGuardrailEvaluator",
    "PrecisionGuardrailResult",
    "PrecisionIssue",
    "PrecisionSpec",
    "CatalogSnapshot",
    "SchemaField",
    "SchemaFieldStatistics",
    "DataSourceMetadata",
    "DatasetMetadata",
    "DatasetStatistics",
    "DatasetConstraint",
    "DatasetConstraintField",
    "DataProcessMetadata",
    "OwnershipAssignment",
]
