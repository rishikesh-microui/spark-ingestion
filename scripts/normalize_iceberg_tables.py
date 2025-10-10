#!/usr/bin/env python3
"""
Normalize Iceberg Hadoop-catalog tables to lower-case identifiers.

Background
----------
Salam derives Iceberg table names by concatenating source schema and table
(``schema__table``). When the Hadoop catalog is case-insensitive, Spark maps the
identifier to a lower-case warehouse path. If the original schema contains
upper-case characters (e.g. ``PIN01``) the canonical warehouse location becomes
``.../pin01__table`` while the ingestion job historically wrote to
``.../PIN01__table``. The metadata and data files still exist, but Spark fails
to resolve the table because it looks for the lower-case location.

This utility copies each affected table into a new lower-case identifier so the
catalog and warehouse paths align going forward. It operates entirely by using
the existing metadata/data path as a source and writing the content to the new
identifier; the original table is left untouched for verification.

Usage
-----
    spark-submit scripts/normalize_iceberg_tables.py conf/brm.json \
        --tables PIN01.account_t PIN01.bal_grp_t \
        --partition-col load_date

Steps per table
---------------
1. Locate the legacy warehouse path (``warehouse/db/SCHEMA__table``).
2. Load the Iceberg table directly from that path (no catalog lookup needed).
3. Create a new Iceberg table using the lower-case identifier
   (``schema__table``). The Hadoop catalog automatically places it under the
   lower-case warehouse directory.
4. Append all rows from the legacy table into the new table.

Once validated, downstream jobs can switch to the lower-case identifier and the
legacy table can be dropped manually.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, List, Tuple

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.utils import AnalysisException


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy Iceberg Hadoop-catalog tables from mixed-case to lower-case identifiers."
    )
    parser.add_argument("config", type=Path, help="Path to Salam runtime JSON (e.g. conf/brm.json)")
    parser.add_argument(
        "--tables",
        nargs="*",
        metavar="SCHEMA.TABLE",
        help="Optional list of schema.table pairs to normalize (defaults to all entries in the config file).",
    )
    parser.add_argument(
        "--partition-col",
        default="load_date",
        help="Partition column to use when creating the normalized table (default: load_date).",
    )
    parser.add_argument("--catalog", help="Override runtime.intermediate.catalog.")
    parser.add_argument("--namespace", help="Override runtime.intermediate.db.")
    parser.add_argument("--warehouse", help="Override runtime.intermediate.warehouse.")
    return parser.parse_args()


def load_runtime_config(cfg_path: Path) -> Tuple[str, str, str, List[Tuple[str, str]]]:
    cfg = json.loads(cfg_path.read_text())
    runtime = cfg["runtime"]
    inter = runtime["intermediate"]
    catalog = inter["catalog"]
    namespace = inter["db"]
    warehouse = inter["warehouse"]
    tables = [(tbl["schema"], tbl["table"]) for tbl in cfg.get("tables", [])]
    return catalog, namespace, warehouse, tables


def init_spark(catalog: str, warehouse: str) -> SparkSession:
    spark = SparkSession.builder.appName("normalize-iceberg-case").getOrCreate()
    spark.conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"spark.sql.catalog.{catalog}.type", "hadoop")
    spark.conf.set(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
    # Enabling case sensitivity keeps behaviour consistent if the session supports it.
    spark.conf.set(f"spark.sql.catalog.{catalog}.case-sensitive", "true")
    return spark


def ensure_namespace(spark: SparkSession, catalog: str, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def normalize_identifier(schema: str, table: str) -> Tuple[str, str]:
    legacy = f"{schema}__{table}"
    normalized = f"{schema.lower()}__{table}"
    return legacy, normalized


def build_table_path(warehouse: str, namespace: str, identifier: str) -> str:
    base = warehouse.rstrip("/")
    return f"{base}/{namespace}/{identifier}"


def path_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark.sparkContext._jvm
    jsc = spark._jsc
    conf = jsc.hadoopConfiguration()
    h_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = h_path.getFileSystem(conf)
    return fs.exists(h_path)


def table_exists(spark: SparkSession, identifier: str) -> bool:
    try:
        spark.table(identifier).limit(1)
        return True
    except AnalysisException:
        return False


def create_target_table(
    df_sample,
    target_identifier: str,
    partition_col: str,
) -> None:
    writer = df_sample.limit(0).writeTo(target_identifier).using("iceberg")
    if partition_col in df_sample.columns:
        writer = writer.partitionedBy(F.col(partition_col))
    else:
        print(f"[WARN] Partition column '{partition_col}' not present. Creating unpartitioned table.")
    writer.create()


def copy_data(df_src, target_identifier: str) -> None:
    df_src.writeTo(target_identifier).append()


def run() -> int:
    args = parse_args()
    catalog, namespace, warehouse, config_tables = load_runtime_config(args.config)
    if args.catalog:
        catalog = args.catalog
    if args.namespace:
        namespace = args.namespace
    if args.warehouse:
        warehouse = args.warehouse

    requested: Iterable[Tuple[str, str]]
    if args.tables:
        tmp: List[Tuple[str, str]] = []
        for item in args.tables:
            if "." not in item:
                raise ValueError(f"Invalid table spec '{item}'. Use SCHEMA.TABLE")
            schema, table = item.split(".", 1)
            tmp.append((schema, table))
        requested = tmp
    else:
        requested = config_tables

    spark = init_spark(catalog, warehouse)
    ensure_namespace(spark, catalog, namespace)

    for schema, table in requested:
        legacy_identifier, target_identifier = normalize_identifier(schema, table)
        legacy_path = build_table_path(warehouse, namespace, legacy_identifier)
        metadata_path = f"{legacy_path}/metadata"

        if schema.islower():
            print(f"[SKIP] {schema}.{table}: schema already lower-case.")
            continue
        if not path_exists(spark, metadata_path):
            print(f"[WARN] Metadata not found at {metadata_path}. Skipping.")
            continue

        target_ref = f"{catalog}.{namespace}.{target_identifier}"
        target_exists = table_exists(spark, target_ref)
        if target_exists:
            print(f"[SKIP] {target_ref} already exists.")
            continue

        try:
            df_src = spark.read.format("iceberg").load(legacy_path)
        except Exception as exc:  # noqa: BLE001
            print(f"[ERROR] Unable to load legacy table at {legacy_path}: {exc}")
            continue

        print(f"[INFO] Normalizing {legacy_path} -> {target_ref}")
        try:
            create_target_table(df_src, target_ref, args.partition_col)
        except AnalysisException as exc:
            print(f"[ERROR] Failed to create {target_ref}: {exc}")
            continue

        copy_data(df_src, target_ref)
        print(f"[OK] Copied {schema}.{table} into {target_ref}. Verify before removing legacy table.")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(run())
