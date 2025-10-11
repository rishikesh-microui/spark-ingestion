from __future__ import annotations

import time
import uuid
from typing import Any, Dict, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException, ParseException
from pyspark.sql.window import Window as W


class HiveHelper:
    """Helpers for managing Hive-compatible external tables."""

    _HIVE_TYPE_MAP = {
        "ByteType": "TINYINT",
        "ShortType": "SMALLINT",
        "IntegerType": "INT",
        "LongType": "BIGINT",
        "FloatType": "FLOAT",
        "DoubleType": "DOUBLE",
        "BinaryType": "BINARY",
        "BooleanType": "BOOLEAN",
        "StringType": "STRING",
        "DateType": "DATE",
        "TimestampType": "TIMESTAMP",
    }

    @staticmethod
    def table_exists(spark: SparkSession, db: str, table_name: str) -> bool:
        return spark._jsparkSession.catalog().tableExists(db, table_name)

    @staticmethod
    def _spark_type_to_hive(dt) -> str:
        s = dt.simpleString()
        if s.startswith("decimal("):
            return "DECIMAL" + s[len("decimal") :]
        if s.startswith("array<") or s.startswith("map<") or s.startswith("struct<"):
            return "STRING"
        return HiveHelper._HIVE_TYPE_MAP.get(dt.__class__.__name__, "STRING")

    @staticmethod
    def _cols_ddl_from_df_schema(df_schema, partition_col: str = "load_date") -> str:
        cols = []
        for field in df_schema.fields:
            if field.name == partition_col:
                continue
            cols.append(f"`{field.name}` {HiveHelper._spark_type_to_hive(field.dataType)}")
        return ", ".join(cols) if cols else "`_dummy` STRING"

    @staticmethod
    def create_external_if_absent(
        spark: SparkSession,
        db: str,
        table_name: str,
        location: str,
        partitioned: bool = False,
        partition_col: str = "load_date",
    ) -> None:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        if partitioned:
            spark.sql(
                f"""
              CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{table_name}` (_dummy STRING)
              PARTITIONED BY ({partition_col} STRING)
              STORED AS PARQUET
              LOCATION '{location}'
            """
            )
            try:
                spark.sql(
                    f"ALTER TABLE `{db}`.`{table_name}` REPLACE COLUMNS ({partition_col} STRING)"
                )
            except Exception:
                pass
        else:
            spark.sql(
                f"""
              CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{table_name}`
              STORED AS PARQUET
              LOCATION '{location}'
              AS SELECT * FROM (SELECT 1 as __bootstrap) t WHERE 1=0
            """
            )

    @staticmethod
    def add_partition_or_msck(
        spark: SparkSession,
        db: str,
        table_name: str,
        partition_col: str,
        load_date: str,
        base_location: str,
        use_msck: bool = False,
    ) -> None:
        part_loc = f"{base_location}/{partition_col}={load_date}"
        if use_msck:
            spark.sql(f"MSCK REPAIR TABLE `{db}`.`{table_name}`")
        else:
            spark.sql(
                f"""
              ALTER TABLE `{db}`.`{table_name}`
              ADD IF NOT EXISTS PARTITION ({partition_col}='{load_date}')
              LOCATION '{part_loc}'
            """
            )

    @staticmethod
    def set_location(spark: SparkSession, db: str, schema: str, table: str, new_location: str) -> str:
        hive_table = f"{db}.{schema}__{table}"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        spark.sql(
            f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {hive_table}
            STORED AS PARQUET
            LOCATION '{new_location}'
            AS SELECT * FROM (SELECT 1 AS _bootstrap) t LIMIT 0
        """
        )
        spark.sql(f"ALTER TABLE {hive_table} SET LOCATION '{new_location}'")
        return hive_table

    @staticmethod
    def create_partitioned_parquet_if_absent_with_schema(
        spark: SparkSession,
        db: str,
        schema: str,
        table: str,
        base_location: str,
        sample_partition_path: str,
        partition_col: str = "load_date",
    ) -> None:
        tbl = f"{schema}__{table}"
        if HiveHelper.table_exists(spark, db, tbl):
            return
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        sample_df = spark.read.parquet(sample_partition_path).limit(1)
        cols_ddl = HiveHelper._cols_ddl_from_df_schema(sample_df.schema, partition_col=partition_col)
        spark.sql(
            f"""
          CREATE EXTERNAL TABLE `{db}`.`{tbl}` (
            {cols_ddl}
          )
          PARTITIONED BY (`{partition_col}` STRING)
          STORED AS PARQUET
          LOCATION '{base_location}'
        """
        )

    @staticmethod
    def create_unpartitioned_parquet_if_absent_with_schema(
        spark: SparkSession,
        db: str,
        schema: str,
        table: str,
        base_location: str,
    ) -> None:
        tbl = f"{schema}__{table}"
        if HiveHelper.table_exists(spark, db, tbl):
            return
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        df = spark.read.parquet(base_location).limit(1)
        cols_ddl = HiveHelper._cols_ddl_from_df_schema(df.schema, partition_col="load_date")
        spark.sql(
            f"""
          CREATE EXTERNAL TABLE `{db}`.`{tbl}` (
            {cols_ddl}
          )
          STORED AS PARQUET
          LOCATION '{base_location}'
        """
        )


class IcebergHelper:
    """Helper routines for interacting with Apache Iceberg sinks."""

    @staticmethod
    def _identifier(schema: str, table: str) -> str:
        return f"{schema}__{table}"

    @staticmethod
    def setup_catalog(spark: SparkSession, cfg: Dict[str, Any]) -> None:
        inter = cfg["runtime"]["intermediate"]
        cat = inter["catalog"]
        if spark.conf.get(f"spark.sql.catalog.{cat}", None):
            return
        wh = inter["warehouse"]
        spark.conf.set(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set(f"spark.sql.catalog.{cat}.type", "hadoop")
        spark.conf.set(f"spark.sql.catalog.{cat}.warehouse", wh)
        spark.conf.set(f"spark.sql.catalog.{cat}.case-sensitive", "true")

    @staticmethod
    def ensure_table(
        spark: SparkSession,
        cfg: Dict[str, Any],
        schema: str,
        table: str,
        df: DataFrame,
        partition_col: str,
    ) -> str:
        inter = cfg["runtime"]["intermediate"]
        cat, db = inter["catalog"], inter["db"]
        tbl_ident = IcebergHelper._identifier(schema, table)
        full = f"{cat}.{db}.{tbl_ident}"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{db}")
        cols = ", ".join([f"`{c}` {df.schema[c].dataType.simpleString()}" for c in df.columns])
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {full} ({cols}) USING iceberg PARTITIONED BY ({partition_col})"
        )
        return full

    @staticmethod
    def dedup(df: DataFrame, pk_cols: Sequence[str], order_cols_desc: Sequence[str]) -> DataFrame:
        order_exprs = [F.col(c).desc_nulls_last() for c in order_cols_desc]
        if "_ingest_tiebreak" not in df.columns:
            df = df.withColumn("_ingest_tiebreak", F.monotonically_increasing_id())
        order_exprs.append(F.col("_ingest_tiebreak").desc())
        w = W.partitionBy(*[F.col(c) for c in pk_cols]).orderBy(*order_exprs)
        return (
            df.withColumn("_rn", F.row_number().over(w))
            .where(F.col("_rn") == 1)
            .drop("_rn", "_ingest_tiebreak")
        )

    @staticmethod
    def merge_upsert(
        spark: SparkSession,
        cfg: Dict[str, Any],
        schema: str,
        table: str,
        df_src: DataFrame,
        pk_cols: Sequence[str],
        load_date: str,
        partition_col: str = "load_date",
        incr_col: Optional[str] = None,
    ) -> str:
        inter = cfg["runtime"]["intermediate"]
        cat, db = inter["catalog"], inter["db"]
        tbl_ident = IcebergHelper._identifier(schema, table)
        tgt = f"{cat}.{db}.{tbl_ident}"
        IcebergHelper.setup_catalog(spark, cfg)
        if partition_col not in df_src.columns:
            df_src = df_src.withColumn(partition_col, F.to_date(F.lit(load_date), "yyyy-MM-dd"))
        order_cols = [incr_col] if incr_col else []
        order_cols.append(partition_col)
        df_src = IcebergHelper.dedup(df_src, pk_cols, order_cols)
        IcebergHelper.ensure_table(spark, cfg, schema, table, df_src, partition_col)
        tgt_schema: Optional[StructType] = None
        last_exc: Optional[Exception] = None
        for attempt in range(5):
            try:
                tgt_schema = spark.table(tgt).schema
                break
            except (AnalysisException, ParseException) as exc:
                last_exc = exc
                IcebergHelper.ensure_table(spark, cfg, schema, table, df_src, partition_col)
                if attempt < 4:
                    time.sleep(0.5)
        if tgt_schema is None:
            if last_exc:
                raise last_exc
            raise AnalysisException(f"Unable to read schema for table {tgt}")
        tgt_cols = [f.name for f in tgt_schema.fields]
        for c in tgt_cols:
            if c not in df_src.columns:
                df_src = df_src.withColumn(c, F.lit(None).cast("string"))
        df_src = df_src.select(
            [F.col(c) for c in tgt_cols if c in df_src.columns]
            + [F.col(c) for c in df_src.columns if c not in tgt_cols]
        )
        tv = f"src_{schema}_{table}_{uuid.uuid4().hex[:8]}"
        df_src.createOrReplaceTempView(tv)
        src_cols = df_src.columns
        non_pk_cols = [c for c in src_cols if c not in pk_cols and c != partition_col]
        on_clause = " AND ".join([f"t.`{c}` = s.`{c}`" for c in pk_cols])
        merge_sql = f"MERGE INTO {tgt} t USING {tv} s ON {on_clause} "
        if non_pk_cols:
            set_clause = ", ".join([f"t.`{c}` = s.`{c}`" for c in non_pk_cols])
            merge_sql += f"WHEN MATCHED THEN UPDATE SET {set_clause} "
        insert_cols = ", ".join([f"`{c}`" for c in src_cols])
        insert_vals = ", ".join([f"s.`{c}`" for c in src_cols])
        merge_sql += f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        spark.sql(merge_sql)
        return tgt

    @staticmethod
    def mirror_to_parquet_for_date(
        spark: SparkSession,
        cfg: Dict[str, Any],
        schema: str,
        table: str,
        load_date: str,
    ) -> Dict[str, str]:
        final_cfg = cfg["runtime"]["final_parquet_mirror"]
        hive_cfg = cfg["runtime"]["hive_reg"]
        inter = cfg["runtime"]["intermediate"]
        cat, db = inter["catalog"], inter["db"]
        part_col = final_cfg.get("partition_col", "load_date")
        tbl_ident = IcebergHelper._identifier(schema, table)
        ice_tbl = f"{cat}.{db}.{tbl_ident}"
        snap_df = spark.table(ice_tbl).where(f"`{part_col}` = '{load_date}'")
        append_schema = cfg["runtime"].get("append_table_schema", False)
        base = (
            f"{final_cfg['root']}/{schema}/{table}"
            if append_schema
            else f"{final_cfg['root']}/{table}"
        )
        part_path = f"{base}/{part_col}={load_date}"
        (
            snap_df.write.format(cfg["runtime"].get("write_format", "parquet"))
            .mode("overwrite")
            .option("compression", cfg["runtime"].get("compression", "snappy"))
            .save(part_path)
        )
        if hive_cfg.get("enabled", False):
            hive_db = hive_cfg["db"]
            hive_tbl = f"{schema}__{table}"
            HiveHelper.create_partitioned_parquet_if_absent_with_schema(
                spark,
                hive_db,
                schema,
                table,
                base_location=base,
                sample_partition_path=part_path,
                partition_col=part_col,
            )
            HiveHelper.add_partition_or_msck(
                spark,
                hive_db,
                hive_tbl,
                part_col,
                load_date,
                base,
                use_msck=hive_cfg.get("use_msck", False),
            )
        return {"final_parquet_path": part_path}

    @staticmethod
    def _compute_wm_ld(df: DataFrame, incr_col: str, is_int_epoch: bool = False) -> Tuple[str, str]:
        if is_int_epoch:
            agg = df.select(
                F.max(F.col(incr_col).cast("long")).alias("wm"),
                F.max(F.col("load_date")).alias("ld"),
            ).collect()[0]
        else:
            agg = df.select(
                F.max(F.col(incr_col)).alias("wm"),
                F.max(F.col("load_date")).alias("ld"),
            ).collect()[0]
        return str(agg["wm"]), str(agg["ld"])
