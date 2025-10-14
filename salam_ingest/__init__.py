"""
High-level helpers for the Spark ingestion framework.

This package exposes the public entry points that the old monolithic
`ingestion.py` script offered, while keeping the implementation split across
focused modules.
"""

from .common import RUN_ID, next_event_seq, PrintLogger, with_ingest_cols
from .orchestrator import main, run_cli, validate_config, suggest_singlestore_ddl

__all__ = [
    "RUN_ID",
    "PrintLogger",
    "main",
    "next_event_seq",
    "run_cli",
    "suggest_singlestore_ddl",
    "validate_config",
    "with_ingest_cols",
]
