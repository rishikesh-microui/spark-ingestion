


from pyspark.sql import functions as F, Window
import uuid
from datetime import date
from pyspark.sql.functions import to_date, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DateType
)
from pyspark.sql import Row
import traceback
import json
from datetime import datetime
from pathlib import PurePosixPath
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, max as spark_max, desc as spark_desc
from pyspark.sql.functions import to_timestamp, max as spark_max
class StateStore:
    def get_day_status(self, schema, table, load_date):
        """Return dict like {"raw_done": bool, "finalized_done": bool} for the given date."""
        raise NotImplementedError
    def mark_event(self, schema, table, load_date, mode, phase, status,
                   rows=None, watermark=None, location=None, strategy=None, error=None):
        """Append an event row (started/success/failed/skipped)."""
        raise NotImplementedError
    def get_last_watermark(self, schema, table, default_value):
        """Return last known watermark (str)."""
        raise NotImplementedError
    def set_watermark(self, schema, table, value):
        """Append a new watermark row."""
        raise NotImplementedError

class SingleStoreState(StateStore):
    def __init__(self, spark, cfg):
        self.spark = spark
        self.sourceId = cfg["sourceId"]
        self.db = cfg["database"]
        self.events = cfg["eventsTable"]
        self.wm = cfg["watermarksTable"]
        self.base_opts = {
            "ddlEndpoint": cfg["ddlEndpoint"],
            "user":        cfg["user"],
            "password":    cfg["password"],
            "database":    cfg["database"]
        }
        # Explicit schemas (align these to your SingleStore DDL)
        # If your SingleStore events.load_date is DATE, keep DateType here.
        # If it's VARCHAR, change to StringType below and remove the to_date() cast.
        self._events_schema = StructType([
            StructField("sourceId",  StringType(), True),
            StructField("schema_name",  StringType(), True),
            StructField("table_name",   StringType(), True),
            StructField("load_date",    DateType(),   True),   # <-- DATE in DB
            StructField("mode",         StringType(), True),
            StructField("phase",        StringType(), True),
            StructField("status",       StringType(), True),
            StructField("rows_written", LongType(),   True),
            StructField("watermark",    StringType(), True),
            StructField("location",     StringType(), True),
            StructField("strategy",     StringType(), True),
            StructField("error",        StringType(), True),
        ])
        self._wm_schema = StructType([
            StructField("sourceId",  StringType(), True),
            StructField("schema_name", StringType(), True),
            StructField("table_name",  StringType(), True),
            StructField("watermark",   StringType(), True),
            StructField("last_loaded_date",   StringType(), True),
            # updated_at exists in DB with DEFAULT CURRENT_TIMESTAMP, so we omit it here
        ])
    def get_day_status(self, schema, table, load_date):
        df = (self._read_events_df()
            .where((col("schema_name") == schema) & (col("table_name") == table) & (col("load_date") == lit(load_date))))
        def _phase_done(phase):
            latest = (df.where(col("phase") == phase)
                        .orderBy(desc("ts"))
                        .limit(1)
                        .collect())
            return bool(latest and latest[0]["status"] == "success")
        return {
            "raw_done": _phase_done("raw"),
            "finalized_done": _phase_done("finalize"),      # used by FULL
            # NEW: used by SCD1 resume
            "intermediate_done": _phase_done("intermediate")
        }
    # def get_day_status(self, schema, table, load_date):
    #     df = (self._read_events_df()
    #           .where((col("schema_name") == schema) & (col("table_name") == table) & (col("load_date") == lit(load_date))))
    #     # latest per phase
    #     raw_done = False
    #     fin_done = False
    #     latest_raw = (df.where(col("phase") == "raw")
    #                     .orderBy(desc("ts"))
    #                     .limit(1)
    #                     .collect())
    #     if latest_raw and latest_raw[0]["status"] == "success":
    #         raw_done = True
    #     latest_fin = (df.where(col("phase") == "finalize")
    #                     .orderBy(desc("ts"))
    #                     .limit(1)
    #                     .collect())
    #     if latest_fin and latest_fin[0]["status"] == "success":
    #         fin_done = True
    #     return {"raw_done": raw_done, "finalized_done": fin_done}
    def _coerce_date(self, v):
        if v is None:
            return None
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            s = v.strip()
            # accept 'YYYY-MM-DD' (or tolerate time part by slicing)
            s = s[:10].replace('/', '-')  # make 'YYYY/MM/DD' work too
            return datetime.strptime(v[:10], "%Y-%m-%d").date()
        raise TypeError(
            "Unsupported load_date type: {type(v)} -> {v!r}".format(v=v))
    def _read_events_df(self):
        return (self.spark.read.format("singlestore").options(dbTable=self.events, **self.base_opts).load())
    def _read_wm_df(self):
        return (self.spark.read.format("singlestore").options(dbTable=self.wm, **self.base_opts).load())
    def _write_events(self, rows):
        """
        rows: iterable of dicts or Rows with keys matching _events_schema field names.
        Accepts load_date as 'YYYY-MM-DD' string or date; casts as needed.
        """
        # Normalize input rows into the schema field order
        def _norm(r):
            EventRow = Row(
                "sourceId", "schema_name", "table_name", "load_date", "mode", "phase",
                "status", "rows_written", "watermark", "location", "strategy", "error"
            )
            # allow dict or Row; provide defaults as None when missing
            get = (r.asDict() if isinstance(r, Row) else r)
            return EventRow(
                self.sourceId,
                get.get("schema_name"),
                get.get("table_name"),
                # may be string 'YYYY-MM-DD' or date
                self._coerce_date(get.get("load_date")),
                get.get("mode"),
                get.get("phase"),
                get.get("status"),
                get.get("rows_written", 0),
                get.get("watermark"),
                get.get("location"),
                get.get("strategy"),
                get.get("error"),
            )
        data = list(map(_norm, rows))
        df = self.spark.createDataFrame(data, schema=self._events_schema)
        # If callers passed load_date as string, enforce cast to DATE
        # (If your DB column is VARCHAR, comment the next line and make schema StringType)
        df = df.withColumn("load_date", to_date("load_date"))
        (df.coalesce(1)  # tiny writes; keep it single-batch
            .write.format("singlestore")
            .options(dbTable=self.events, **self.base_opts)
            .mode("append")
            .save())
    def get_progress(self, schema, table, default_wm, default_date):
        """
        Returns (watermark_str, last_loaded_date_str).
        """
        try:
            df = (self._read_wm_df()
                .where((col("schema_name") == schema) & (col("table_name") == table)))
            if df.rdd.isEmpty():
                return default_wm, default_date
            last = (df.select("watermark", "last_loaded_date")
                    .orderBy(spark_desc("updated_at"))
                    .limit(1).collect())
            if last:
                wm = last[0]["watermark"] or default_wm
                ldd = last[0]["last_loaded_date"] or default_date
                return wm, ldd
        except Exception as e:
            print("[WARN] get_progress:", e)
        return default_wm, default_date
    def set_progress(self, schema, table, watermark, last_loaded_date):
        """
        Upsert-like append: write a new row carrying both WM and last_loaded_date.
        """
        self._write_watermarks([{
            "sourceId":   self.sourceId,
            "schema_name": schema,
            "table_name":  table,
            "watermark":   watermark,
            # you added this column in DDL; include it by extending _wm_schema:
            # StructField("last_loaded_date", StringType(), True),
            # and pass it here:
            "last_loaded_date": last_loaded_date
        }])
    def _write_watermarks(self, rows):
        """
        rows: iterable of dicts or Rows with keys: schema_name, table_name, watermark
        """
        def _norm(r):
            EventRow = Row(
                "sourceId", "schema_name", "table_name", "watermark", "last_loaded_date"
            )
            get = (r.asDict() if isinstance(r, Row) else r)
            return EventRow(
                self.sourceId,
                get.get("schema_name"),
                get.get("table_name"),
                get.get("watermark"),
                get.get("last_loaded_date")
            )
        data = list(map(_norm, rows))
        df = self.spark.createDataFrame(data, schema=self._wm_schema)
        (df.coalesce(1)
           .write.format("singlestore")
           .options(dbTable=self.wm, **self.base_opts)
           .mode("append")
           .save())
    # ---- State API (uses the safe writers) ----
    # def _write_rows(self, rows, table):
    #     # Route to the right typed writer
    #     if table == self.events:
    #         self._write_events(rows)
    #     elif table == self.wm:
    #         self._write_watermarks(rows)
    #     else:
    #         # fallback: try to infer, but with explicit cast to strings to avoid None inference issues
    #         df = self.spark.createDataFrame(rows)
    #         for c in df.columns:
    #             df = df.withColumn(c, df[c].cast(StringType()))
    #         (df.coalesce(1)
    #            .write.format("singlestore")
    #            .options(dbTable=table, **self.base_opts)
    #            .mode("append")
    #            .save())
    def mark_event(self, schema, table, load_date, mode, phase, status,
                   rows_written=None, watermark=None, location=None, strategy=None, error=None):
        # Accept load_date as 'YYYY-MM-DD' string; typed writer will cast correctly
        self._write_events([{
            "schema_name":  schema,
            "table_name":   table,
            "load_date":    load_date,
            "mode":         mode,
            "phase":        phase,
            "status":       status,
            "rows_written": rows_written,
            "watermark":    watermark,
            "location":     location,
            "strategy":     strategy,
            "error":        error
        }])
        # if watermark is not None:
        #     self.set_watermark(schema, table, watermark)
    # def set_watermark(self, schema, table, value):
    #     self._write_watermarks([{
    #         "schema_name": schema,
    #         "table_name":  table,
    #         "watermark":   value
    #     }])
    def get_last_watermark(self, schema, table, default_value):
        """
        Return last known watermark string for (schema,table).
        default_value: value to return if no prior watermark exists.
        """
        try:
            df = self._read_wm_df().where(
                (col("schema_name") == schema) &
                (col("table_name") == table)
            )
            # pick max watermark if the column is sortable lexicographically (typical for timestamps)
            if df.rdd.isEmpty():
                return default_value
            last = df.select("watermark").orderBy(
                desc("watermark")).limit(1).collect()
            if last:
                return last[0]["watermark"]
            return default_value
        except Exception as e:
            # if anything fails (table empty etc), return default
            print(
                f"[WARN] get_last_watermark failed for {schema}.{table}: {e}")
            return default_value

# ========= ICEBERG (Spark 3) =========
def _setup_iceberg_catalog(spark, cfg):
    inter = cfg["runtime"]["intermediate"]
    cat = inter["catalog"]
    wh  = inter["warehouse"]
    # Spark 3 Iceberg catalog config (Hadoop catalog)
    spark.conf.set(f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"spark.sql.catalog.{cat}.type", "hadoop")
    spark.conf.set(f"spark.sql.catalog.{cat}.warehouse", wh)
    # helpful default for MERGE write distribution
    spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.internal.CatalogImpl")

def _ensure_iceberg_table(spark, cfg, schema, table, df, partition_col):
    inter = cfg["runtime"]["intermediate"]
    cat, db = inter["catalog"], inter["db"]
    full = f"{cat}.{db}.{schema}__{table}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{db}")
    # Create empty table if missing (partitioned by load_date)
    cols = ", ".join([f"`{c}` {df.schema[c].dataType.simpleString()}" for c in df.columns])
    # If table may not exist, use CREATE TABLE ... PARTITIONED BY
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {full} ({cols})
      USING iceberg
      PARTITIONED BY ({partition_col})
    """)
    return full

def _dedup_on_keys(df, pk_cols, order_cols_desc):
    """
    Keep exactly one row per PK using a deterministic ordering:
    order_cols_desc: list of column names to sort DESC by (e.g. ["LAST_UPD", "load_date"])
    Adds a final tie-breaker using input_file_name() if available.
    """
    # Build ordering: user-provided cols desc, then a stable tiebreaker
    order_exprs = [F.col(c).desc_nulls_last() for c in order_cols_desc]
    # Add a deterministic tiebreaker (monotonic id) if needed
    if "_ingest_tiebreak" not in df.columns:
        df = df.withColumn("_ingest_tiebreak", F.monotonically_increasing_id())
    order_exprs.append(F.col("_ingest_tiebreak").desc())
    w = Window.partitionBy(*[F.col(c) for c in pk_cols]).orderBy(*order_exprs)
    return (df
            .withColumn("_rn", F.row_number().over(w))
            .where(F.col("_rn") == 1)
            .drop("_rn", "_ingest_tiebreak"))

def _iceberg_merge_upsert(spark, cfg, schema, table, df_src, pk_cols, load_date, partition_col="load_date", incr_col=None):
    """
    Safe MERGE:
      - ensure partition_col exists on df_src
      - de-duplicate by PK using desc order on [incr_col, partition_col]
      - align to target columns if table exists
      - don't update the partition column in SET clause
    """
    inter = cfg["runtime"]["intermediate"]
    cat, db = inter["catalog"], inter["db"]
    tgt = f"{cat}.{db}.{schema}__{table}"
    # 0) Ensure catalog wired
    _setup_iceberg_catalog(spark, cfg)
    # 1) Ensure partition column exists (constant for this batch)
    if partition_col not in df_src.columns:
        df_src = df_src.withColumn(partition_col, F.lit(load_date))
    # 2) Deduplicate per PK (latest wins)
    #    If incr_col not provided, we still dedup deterministically using load_date + tie-breaker.
    order_cols = []
    if incr_col:
        order_cols.append(incr_col)
    order_cols.append(partition_col)
    df_src = _dedup_on_keys(df_src, pk_cols, order_cols_desc=order_cols)
    # 3) Create namespace and table if needed (based on current df_src schema)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{db}")
    _ensure_iceberg_table(spark, cfg, schema, table, df_src, partition_col)
    tgt_cols = [f.name for f in spark.table(tgt).schema.fields]
    for c in tgt_cols:
        if c not in df_src.columns:
            df_src = df_src.withColumn(c, F.lit(None).cast("string"))
    # Also keep any extra cols in df_src (Iceberg supports add columns on write),
    # but the MERGE will only reference tgt_cols to be safe.
    df_src = df_src.select([F.col(c) for c in tgt_cols if c in df_src.columns] +
                            [F.col(c) for c in df_src.columns if c not in tgt_cols])
    # if not spark._jsparkSession.catalog().tableExists(f"{cat}.{db}", f"{schema}__{table}"):
    #     # Build CREATE TABLE columns from df schema (basic types)
    #     cols = ", ".join(
    #         [f"`{f.name}` {f.dataType.simpleString()}" for f in df_src.schema.fields])
    #     spark.sql(f"""
    #       CREATE TABLE IF NOT EXISTS {tgt} ({cols})
    #       USING iceberg
    #       PARTITIONED BY ({partition_col})
    #     """)
    # else:
    #     # Align df columns to target schema order; add missing columns with nulls
    #     tgt_cols = [f.name for f in spark.table(tgt).schema.fields]
    #     for c in tgt_cols:
    #         if c not in df_src.columns:
    #             df_src = df_src.withColumn(c, F.lit(None).cast("string"))
    #     # Also keep any extra cols in df_src (Iceberg supports add columns on write),
    #     # but the MERGE will only reference tgt_cols to be safe.
    #     df_src = df_src.select([F.col(c) for c in tgt_cols if c in df_src.columns] +
    #                            [F.col(c) for c in df_src.columns if c not in tgt_cols])
    # 4) Register source view (unique name to avoid collisions)
    tv = f"src_{schema}_{table}_{uuid.uuid4().hex[:8]}"
    df_src.createOrReplaceTempView(tv)
    # 5) Build MERGE (exclude partition_col from UPDATE to avoid “moving” rows across partitions)
    src_cols = df_src.columns
    non_pk_cols = [
        c for c in src_cols if c not in pk_cols and c != partition_col]
    # If non_pk_cols is empty, MERGE would be insert-only; that's still valid.
    on_clause = " AND ".join([f"t.`{c}` = s.`{c}`" for c in pk_cols])
    set_clause = ", ".join(
        [f"t.`{c}` = s.`{c}`" for c in non_pk_cols]) if non_pk_cols else None
    insert_cols = ", ".join([f"`{c}`" for c in src_cols])
    insert_vals = ", ".join([f"s.`{c}`" for c in src_cols])
    merge_sql = f"MERGE INTO {tgt} t USING {tv} s ON {on_clause} "
    if set_clause:
        merge_sql += f"WHEN MATCHED THEN UPDATE SET {set_clause} "
    # If there are no updatable columns, skip UPDATE branch entirely
    merge_sql += f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    spark.sql(merge_sql)
    return tgt

# ========= PARQUET MIRROR FROM ICEBERG =========
def _mirror_iceberg_to_parquet_partition(spark, cfg, schema, table, load_date):
    final_cfg = cfg["runtime"]["final_parquet_mirror"]
    hive_cfg  = cfg["runtime"]["hive_reg"]
    inter     = cfg["runtime"]["intermediate"]
    cat, db = inter["catalog"], inter["db"]
    part_col = final_cfg.get("partition_col", "load_date")
    # Read the merged slice for this load_date from Iceberg
    ice_tbl = f"{cat}.{db}.{schema}__{table}"
    snap_df = spark.table(ice_tbl).where(f"`{part_col}` = '{load_date}'")
    # Write to partitioned Parquet final
    append_schema = cfg["runtime"].get("append_table_schema", False)
    base = f"{final_cfg['root']}/{schema}/{table}" if append_schema else f"{final_cfg['root']}/{table}"
    part_path = f"{base}/{part_col}={load_date}"
    (snap_df.write
       .format(cfg["runtime"].get("write_format", "parquet"))
       .mode("overwrite")                                  # idempotent per-day
       .option("compression", cfg["runtime"].get("compression", "snappy"))
       .save(part_path))
    # Hive external + partition registration (optional)
    if hive_cfg.get("enabled", False):
        hive_db = hive_cfg["db"]
        hive_tbl = f"{schema}__{table}"
        hive_create_external_if_absent(spark, hive_db, hive_tbl, base, partitioned=True, partition_col=part_col)
        hive_add_partition_or_msck(
            spark, hive_db, hive_tbl, part_col, load_date, base, use_msck=hive_cfg.get("use_msck", False)
        )
    return {"final_parquet_path": part_path}

# ---------- Intermediary writers ----------
def write_to_intermediate_iceberg(spark, cfg, schema, table, load_date, df):
    """
    Uses a Hadoop catalog Iceberg table at: {catalog}.{db}.{schema}__{table}
    Creates the table on first write. Partitions by load_date (string) if present.
    """
    cat = cfg["runtime"]["intermediate"]["catalog"]
    db = cfg["runtime"]["intermediate"]["db"]
    warehouse = cfg["runtime"]["intermediate"]["warehouse"]

    # Minimal catalog wiring (works when iceberg runtime jar is on classpath)
    spark.conf.set("spark.sql.catalog.{c}".format(
        c=cat), "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.{c}.type".format(c=cat), "hadoop")
    spark.conf.set("spark.sql.catalog.{c}.warehouse".format(c=cat), warehouse)

    ice_tbl = "{cat}.{db}.{name}".format(
        cat=cat, db=db, name="{s}__{t}".format(s=schema, t=table)
    )
    # Ensure load_date column exists for partitioning
    part_col = cfg["runtime"]["hive_reg"].get("partition_col", "load_date")
    if part_col not in df.columns:
        df = df.withColumn(part_col, lit(load_date))
    # Create table if missing with partition spec on load_date
    spark.sql(
        "CREATE NAMESPACE IF NOT EXISTS {cat}.{db}".format(cat=cat, db=db))
    spark.sql("""
      CREATE TABLE IF NOT EXISTS {tbl}
      USING iceberg
      PARTITIONED BY ({pc})
      AS SELECT * FROM (SELECT 1 as __bootstrap) t WHERE 1=0
    """.format(tbl=ice_tbl, pc=part_col))
    # Append batch (Iceberg handles schema evolution)
    df.write.format("iceberg").mode("append").saveAsTable(ice_tbl)
    return ice_tbl

def write_to_intermediate_parquet(spark, cfg, schema, table, load_date, df):
    """
    Writes to: {parquet_root}/{schema}/{table}/load_date=YYYY-MM-DD
    Returns the partition path.
    """
    root = cfg["runtime"]["intermediate"]["parquet_root"]
    append_schema = cfg["runtime"].get("append_table_schema", False)
    base = "{root}/{schema}/{table}".format(root=root, schema=schema, table=table) if append_schema \
           else "{root}/{table}".format(root=root, table=table)
    part_path = "{base}/load_date={ld}".format(base=base, ld=load_date)
    # ensure partition column exists
    part_col = cfg["runtime"]["hive_reg"].get("partition_col", "load_date")
    if part_col not in df.columns:
        df = df.withColumn(part_col, lit(load_date))
    (df.write
       .format(cfg["runtime"].get("write_format", "parquet"))
       .mode("append")
       .option("compression", cfg["runtime"].get("compression", "snappy"))
       .save(part_path))
    return part_path

# ---------- Hive registration helpers ----------
def hive_create_external_if_absent(spark, db, table_name, location, partitioned=False, partition_col="load_date"):
    spark.sql("CREATE DATABASE IF NOT EXISTS `{db}`".format(db=db))
    if partitioned:
        # Create an empty external table with partition column; schema-on-read from files
        spark.sql("""
          CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{tbl}` (
            _dummy STRING
          )
          PARTITIONED BY ({pc} STRING)
          STORED AS PARQUET
          LOCATION '{loc}'
        """.format(db=db, tbl=table_name, pc=partition_col, loc=location))
        # Drop the dummy column from metastore view (optional, harmless to leave)
        try:
            spark.sql("ALTER TABLE `{db}`.`{tbl}` REPLACE COLUMNS ({pc} STRING)".format(
                db=db, tbl=table_name, pc=partition_col))
        except Exception:
            pass
    else:
        spark.sql("""
          CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{tbl}`
          STORED AS PARQUET
          LOCATION '{loc}'
          AS SELECT * FROM (SELECT 1 as __bootstrap) t WHERE 1=0
        """.format(db=db, tbl=table_name, loc=location))

def hive_add_partition_or_msck(spark, db, table_name, partition_col, load_date, base_location, use_msck=False):
    part_loc = "{base}/{pc}={ld}".format(base=base_location,
                                         pc=partition_col, ld=load_date)
    if use_msck:
        spark.sql("MSCK REPAIR TABLE `{db}`.`{tbl}`".format(db=db, tbl=table_name))
    else:
        spark.sql("""
          ALTER TABLE `{db}`.`{tbl}`
          ADD IF NOT EXISTS PARTITION ({pc}='{ld}')
          LOCATION '{loc}'
        """.format(db=db, tbl=table_name, pc=partition_col, ld=load_date, loc=part_loc))

def hive_set_location(spark, db, schema, table, new_location):
    hive_table = "{db}.{schema}__{table}".format(db=db, schema=schema, table=table)
    spark.sql("CREATE DATABASE IF NOT EXISTS {db}".format(db=db))
    # bootstrap table if missing (empty external Parquet)
    spark.sql("""
        CREATE EXTERNAL TABLE IF NOT EXISTS {hive_table}
        STORED AS PARQUET
        LOCATION '{new_location}'
        AS SELECT * FROM (SELECT 1 AS _bootstrap) t LIMIT 0
    """.format(hive_table=hive_table, new_location=new_location))
    spark.sql("ALTER TABLE {hive_table} SET LOCATION '{new_location}'".format(hive_table=hive_table, new_location=new_location))
    return hive_table

def copy_and_swap_final(spark, src_dir, final_dir):
    jsc = spark._jsc; 
    #jvm = jsc.sc()._jvm
    jvm = spark.sparkContext._jvm
    conf = jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    p = jvm.org.apache.hadoop.fs.Path
    src = p(src_dir)    
    dst = p(final_dir)
    tmp = p(final_dir + ".__tmp_swap__")
    if fs.exists(tmp):
        fs.delete(tmp, True)
    ok = jvm.org.apache.hadoop.fs.FileUtil.copy(fs, src, fs, tmp, False, conf)
    if not ok:
        raise RuntimeError("Copy {src_dir} -> {final_dir}/.__tmp_swap__ failed".format(src_dir=src_dir, final_dir=final_dir))
    if fs.exists(dst):
        fs.delete(dst, True)
    if not fs.rename(tmp, dst):
        raise RuntimeError("Atomic rename failed for {final_dir}".format(final_dir=final_dir))

def ingest_one_table(spark, cfg, state, tbl, pool_name):
    schema = tbl["schema"]; table = tbl["table"]
    mode = tbl.get("mode", "full").lower()
    incr_col = tbl.get("incremental_column")
    initial_wm = tbl.get("initial_watermark", "1900-01-01 00:00:00")
    tz = cfg["runtime"].get("timezone", "Asia/Kolkata")
    load_date = datetime.now().astimezone().strftime("%Y-%m-%d")  # date label only (business date)
    rt = cfg["runtime"]; jdbc = cfg["jdbc"]
    append_table_schema = rt.get("append_table_schema", False)
    raw_dir = "{raw_root}/{schema}/{table}/load_date={load_date}".format(
        raw_root=rt["raw_root"], schema=schema, table=table, load_date=load_date) if append_table_schema else "{raw_root}/{table}/load_date={load_date}".format(
        raw_root=rt["raw_root"], table=table, load_date=load_date)
    #raw_dir = str(PurePosixPath(rt["raw_root"]) / schema / table / "load_date={load_date}".format(load_date=load_date))
    final_dir = "{final_root}/{schema}/{table}".format(
        final_root=rt["final_root"], schema=schema, table=table) if append_table_schema else "{final_root}/{table}".format(
        final_root=rt["final_root"], table=table)
    #final_dir = str(PurePosixPath(rt["final_root"]) / schema / table)
    sc = spark.sparkContext
    sc.setLocalProperty("spark.scheduler.pool", pool_name)
    sc.setJobGroup("ingest::{schema}.{table}".format(schema=schema,table=table), "Ingest {schema}.{table} -> {raw_dir}".format(schema=schema,table=table,raw_dir=raw_dir))
    # --- Skip/resume checks (idempotency) ---
    status = state.get_day_status(schema, table, load_date)
    if mode == "full" and status.get("finalized_done"):
        state.mark_event(schema, table, load_date, mode, phase="raw", status="skipped", location=raw_dir)
        state.mark_event(schema, table, load_date, mode, phase="finalize", status="skipped", location=final_dir)
        return {"table": "{schema}.{table}".format(schema=schema,table=table), "skipped": True, "reason": "already finalized today"}
    # JDBC Reader
    reader = (spark.read.format("jdbc")
              .option("url", jdbc["url"])
              .option("dbtable", "{schema}.{table}".format(schema=schema,table=table))
              .option("user", jdbc["user"])
              .option("password", jdbc["password"])
              .option("trustServerCertificate", "true")
              .option("driver", jdbc["driver"])
              .option("fetchsize", int(jdbc.get("fetchsize", 10000))))
    if tbl.get("partition_read"):
        pr = tbl["partition_read"]
        reader = (reader
                  .option("partitionColumn", pr["partitionColumn"])
                  .option("lowerBound", str(pr["lowerBound"]))
                  .option("upperBound", str(pr["upperBound"]))
                  .option("numPartitions", str(pr.get("numPartitions", jdbc.get("default_num_partitions", 8)))))
    if mode == "scd1":
        last_wm, last_ld = state.get_progress(
            schema, table, default_wm=initial_wm, default_date="1900-01-01")
        print(f"Loading Progress: {schema}.{table}: last_wm: {last_wm}, last_ld: {last_ld}")
        src_df = reader.load()
        inc_filter_df = src_df.filter(to_timestamp(
            col(incr_col)) > to_timestamp(lit(last_wm)))
        pulled = inc_filter_df.count()
        print(f"Pulled {pulled} rows for {schema}.{table}")
        if pulled > 0:
            state.mark_event(schema, table, load_date, mode,
                            "raw", "started", location=raw_dir)
            (inc_filter_df.write.format(rt.get("write_format", "parquet"))
                .mode("append")
                .option("compression", rt.get("compression", "snappy"))
                .save(raw_dir))
            state.mark_event(schema, table, load_date, mode, "raw", "success",
                            rows_written=pulled, location=raw_dir)
        else:
            state.mark_event(schema, table, load_date, mode, "raw", "success",
                            rows_written=0, location=raw_dir)
        # -------- build RAW dataframe to MERGE from: dates >= last_ld AND incr_col > last_wm ----------
        append_schema = rt.get("append_table_schema", False)
        base_raw = f"{rt['raw_root']}/{schema}/{table}" if append_schema else f"{rt['raw_root']}/{table}"
        # Read all partitions; Spark will add load_date column from dir names
        all_raw = (spark.read
                .format(rt.get("write_format", "parquet"))
                .load(base_raw))
        raw_to_merge = (all_raw
                        .where(col("load_date") >= lit(last_ld))
                        .where(to_timestamp(col(incr_col)) > to_timestamp(lit(last_wm))))
        to_merge_count = raw_to_merge.count()
        print(f"Raw to merge count: {to_merge_count}, filter used: {last_ld}, {last_wm}")
        if to_merge_count == 0:
            # Nothing to merge (maybe all merged previously). Do NOT advance progress.
            return {"table": f"{schema}.{table}", "mode": "scd1", "rows": 0, "raw": raw_dir, "wm": last_wm, "last_loaded_date": last_ld}
        # ---------- Intermediary upsert ----------
        interm_ok = False
        intermediate_info = {}
        inter_cfg = rt.get("intermediate", {"enabled": False})
        part_col = rt.get("hive_reg", {}).get("partition_col", "load_date")
        pk_cols = tbl.get("primary_keys", [])
        try:
            if inter_cfg.get("enabled", False):
                state.mark_event(schema, table, load_date,
                                 mode, "intermediate", "started")
                if inter_cfg.get("type", "iceberg") == "iceberg" and pk_cols:
                    _setup_iceberg_catalog(spark, cfg)
                    ice_tbl = _iceberg_merge_upsert(
                        spark, cfg, schema, table, raw_to_merge, pk_cols, load_date,
                        partition_col=part_col,
                        incr_col=incr_col
                    )
                    intermediate_info["iceberg_table"] = ice_tbl
                    # Optional Parquet mirror
                    final_mirror = rt.get(
                        "final_parquet_mirror", {"enabled": False})
                    if final_mirror.get("enabled", False):
                        mirror_info = _mirror_iceberg_to_parquet_partition(
                            spark, cfg, schema, table, load_date)
                        intermediate_info.update(mirror_info)
                elif inter_cfg.get("type") == "parquet":
                    part_path = write_to_intermediate_parquet(
                        spark, cfg, schema, table, load_date, df)
                    intermediate_info["parquet_path"] = part_path
                    if rt.get("hive_reg", {}).get("enabled", False):
                        hive_db = rt["hive_reg"]["db"]
                        append_schema = rt.get("append_table_schema", False)
                        base = cfg["runtime"]["intermediate"]["parquet_root"]
                        base = f"{base}/{schema}/{table}" if append_schema else f"{base}/{table}"
                        hive_tbl_name = f"{schema}__{table}"
                        hive_create_external_if_absent(
                            spark, hive_db, hive_tbl_name, base, partitioned=True, partition_col=part_col)
                        hive_add_partition_or_msck(
                            spark, hive_db, hive_tbl_name, part_col, load_date, base, use_msck=rt["hive_reg"].get("use_msck", False))
                # if we got here without exception, intermediate is OK
                interm_ok = True
                state.mark_event(schema, table, load_date,
                                 mode, "intermediate", "success")
        except Exception as e:
            intermediate_info["intermediate_error"] = str(e)
            state.mark_event(schema, table, load_date, mode,
                             "intermediate", "failed", error=str(e))
        # ---------- Watermark update ONLY on success ----------
        if interm_ok or not inter_cfg.get("enabled", False):
            # compute new watermark and last_loaded_date FROM THE MERGED RAW
            agg = raw_to_merge.select(
                spark_max(col(incr_col)).alias("wm"),
                spark_max(col("load_date")).alias("ld")
            ).collect()[0]
            new_wm = str(agg["wm"])
            new_ld = str(agg["ld"])
            state.mark_event(schema, table, load_date, mode,
                             "watermark", "success", watermark=new_wm)
            state.set_progress(schema, table, watermark=new_wm,
                            last_loaded_date=new_ld)
            out = {"table": f"{schema}.{table}", "mode": "scd1", "rows": to_merge_count,
                "raw": raw_dir, "wm": new_wm, "last_loaded_date": new_ld}
            out.update(intermediate_info)
            return out
        else:
            return {"table": f"{schema}.{table}", "mode": "scd1", "rows": to_merge_count,
                    "raw": raw_dir, "wm": last_wm, "last_loaded_date": last_ld, **intermediate_info}
    else:
        # FULL — If raw already done but not finalized, skip extract and only finalize
        if status.get("raw_done") and not status.get("finalized_done"):
            # finalize only
            result = {"table": "{schema}.{table}".format(schema=schema,table=table), "mode": "full", "raw": raw_dir}
        else:
            # fresh extract → RAW (overwrite today's slice only; never touch previous days)
            df = reader.load()
            rows = df.count()
            state.mark_event(schema, table, load_date, mode, "raw", "started", location=raw_dir)
            (df.write.format(rt.get("write_format", "parquet"))
                  .mode("overwrite")
                  .option("compression", rt.get("compression", "snappy"))
                  .save(raw_dir))
            state.mark_event(schema, table, load_date, mode, "raw",
                             "success", rows_written=rows, location=raw_dir)
            result = {"table": "{schema}.{table}".format(schema=schema, table=table), "mode": "full", "rows": rows, "raw": raw_dir}
        # finalize (optional)
        finalized = False; hive_table = None
        if rt.get("finalize_full_refresh", True):
            state.mark_event(schema, table, load_date, mode, "finalize", "started", location=final_dir,
                             strategy=rt.get("finalize_strategy"))
            if rt.get("finalize_strategy") == "hive_set_location" and rt.get("hive", {}).get("enabled", False):
                hive_table = hive_set_location(spark, rt["hive"]["db"], schema, table, raw_dir)
                finalized = True
            else:
                copy_and_swap_final(spark, src_dir=raw_dir, final_dir=final_dir)
                finalized = True
            state.mark_event(schema, table, load_date, mode, "finalize", "success",
                             location=final_dir, strategy=rt.get("finalize_strategy"))
        result.update({"final": final_dir, "finalized": finalized, "hive_table": hive_table})
        return result

def main(spark, cfg):
    # init state backend
    backend = cfg["runtime"].get("state_backend", "hdfs")
    if backend == "singlestore":
        state = SingleStoreState(spark, cfg["runtime"]["state"]["singlestore"])
    else:
        base = cfg["runtime"]["state"]["hdfs"]["dir"]
        # best-effort create
        spark.range(1).write.mode("overwrite").parquet(str(PurePosixPath(base) / "_init_marker"))
        state = HDFSState(spark, base)
    max_workers = int(cfg["runtime"].get("max_parallel_tables", 4))
    tables = cfg["tables"]
    results, errors = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futmap = {}
        for i, tbl in enumerate(tables):
            pool = "pool-{pool_id}".format(pool_id=(i % max_workers) + 1)
            fut = ex.submit(ingest_one_table, spark, cfg, state, tbl, pool)
            futmap[fut] = "{schema}.{table}".format(schema=tbl["schema"], table=tbl["table"])
        for fut in as_completed(futmap):
            key = futmap[fut]
            try:
                res = fut.result()
                results.append(res)
                print("[OK] {key}: {res}".format(key=key, res=res))
            except Exception as e:
                traceback.print_exc()
                errors.append((key, str(e)))
                print("[ERR] {key}: {e}".format(key=key, e=e))
    print("\n=== SUMMARY ===")
    for r in results:
        print(r)
    if errors:
        print("\n=== ERRORS ===")
        for k, e in errors:
            print(k, e)
            print("[ERR] {key}: {e}".format(key=key, e=e))
