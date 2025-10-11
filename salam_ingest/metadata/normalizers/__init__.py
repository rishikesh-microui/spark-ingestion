"""Metadata normalizers transform vendor catalog output into the neutral model."""

from .base import MetadataNormalizer
from .oracle import OracleMetadataNormalizer

__all__ = ["MetadataNormalizer", "OracleMetadataNormalizer"]
