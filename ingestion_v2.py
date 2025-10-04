# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, Row, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, to_date, to_timestamp, max as spark_max, desc as spark_desc
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType
from datetime import datetime, date
from pathlib import PurePosixPath
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
import uuid
import json

# =========================
# State interfaces & impls
# =========================
class StateStore:
    def get_day_status(self, schema, table, load_date):
        raise NotImplementedError

    def mark_event(self, schema, table, load_date, mode, phase, status,
                   rows_written=None, watermark=None, location=None, strategy=None, error=None):
        raise NotImplementedError

    def get_last_watermark(self, schema, table, default_value):
        raise NotImplementedError

    def get_progress(self, schema, table, default_wm, default_date):
        raise NotImplementedError

    def set_progress(self, schema, table, watermark, last_loaded_date):
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
        self._events_schema = StructType([
            StructField("sourceId",        StringType(), True),
            StructField("schema_name",     StringType(), True),
            StructField("table_name",      StringType(), True),
            StructField("load_date",       DateType(),   True),
            StructField("mode",            StringType(), True),
            StructField("phase",           StringType(), True),
            StructField("status",          StringType(), True),
            StructField("rows_written",    LongType(),   True),
            StructField("watermark",       StringType(), True),
            StructField("location",        StringType(), True),
            StructField("strategy",        StringType(), True),
            StructField("error",           StringType(), True),
        ])
        self._wm_schema = StructType([
            StructField("sourceId",         StringType(), True),
            StructField("schema_name",      StringType(), True),
            StructField("table_name",       StringType(), True),
            StructField("watermark",        StringType(), True),
            StructField("last_loaded_date", StringType(), True),
        ])

    # ----- readers/writers -----
    def _read_events_df(self):
        return (self.spark.read.format("singlestore")
                .options(dbTable=self.events, **self.base_opts).load())

    def _read_wm_df(self):
        return (self.spark.read.format("singlestore")
                .options(dbTable=self.wm, **self.base_opts).load())

    def _coerce_date(self, v):
        if v is None:
            return None
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            s = v.strip()[:10].replace("/", "-")
            return datetime.strptime(s, "%Y-%m-%d").date()
        raise TypeError("Unsupported load_date type: %r" % (v,))

    def _write_events(self, rows):
        def _norm(r):
            EventRow = Row("sourceId", "schema_name", "table_name", "load_date", "mode", "phase",
                           "status", "rows_written", "watermark", "location", "strategy", "error")
            get = (r.asDict() if isinstance(r, Row) else r)
            return EventRow(self.sourceId,
                            get.get("schema_name"), get.get("table_name"),
                            self._coerce_date(get.get("load_date")),
                            get.get("mode"), get.get(
                                "phase"), get.get("status"),
                            get.get("rows_written", 0), get.get("watermark"),
                            get.get("location"), get.get("strategy"), get.get("error"))
        df = self.spark.createDataFrame(
            list(map(_norm, rows)), schema=self._events_schema)
        df = df.withColumn("load_date", to_date("load_date"))
        (df.coalesce(1).write.format("singlestore")
           .options(dbTable=self.events, **self.base_opts).mode("append").save())

    def _write_progress(self, rows):
        def _norm(r):
            EventRow = Row("sourceId", "schema_name",
                           "table_name", "watermark", "last_loaded_date")
            get = (r.asDict() if isinstance(r, Row) else r)
            return EventRow(self.sourceId, get.get("schema_name"), get.get("table_name"),
                            get.get("watermark"), get.get("last_loaded_date"))
        df = self.spark.createDataFrame(
            list(map(_norm, rows)), schema=self._wm_schema)
        (df.coalesce(1).write.format("singlestore")
           .options(dbTable=self.wm, **self.base_opts).mode("append").save())

    # ----- API -----
    def get_day_status(self, schema, table, load_date):
        df = (self._read_events_df()
              .where((col("schema_name") == schema) & (col("table_name") == table) & (col("load_date") == lit(load_date))))

        def _done(phase):
            latest = (df.where(col("phase") == phase).orderBy(
                spark_desc("ts")).limit(1).collect())
            return bool(latest and latest[0]["status"] == "success")
        return {"raw_done": _done("raw"), "finalized_done": _done("finalize"), "intermediate_done": _done("intermediate")}

    def mark_event(self, schema, table, load_date, mode, phase, status,
                   rows_written=None, watermark=None, location=None, strategy=None, error=None):
        self._write_events([{
            "schema_name": schema, "table_name": table, "load_date": load_date, "mode": mode,
            "phase": phase, "status": status, "rows_written": rows_written, "watermark": watermark,
            "location": location, "strategy": strategy, "error": error
        }])

    def get_last_watermark(self, schema, table, default_value):
        try:
            df = self._read_wm_df().where((col("schema_name") == schema)
                                          & (col("table_name") == table))
            if df.rdd.isEmpty():
                return default_value
            last = df.select("watermark").orderBy(
                spark_desc("updated_at")).limit(1).collect()
            return last[0]["watermark"] if last else default_value
        except Exception as e:
            print(
                f"[WARN] get_last_watermark failed for {schema}.{table}: {e}")
            return default_value

    def get_progress(self, schema, table, default_wm, default_date):
        try:
            df = self._read_wm_df().where((col("schema_name") == schema)
                                          & (col("table_name") == table))
            if df.rdd.isEmpty():
                return default_wm, default_date
            last = (df.select("watermark", "last_loaded_date")
                      .orderBy(spark_desc("updated_at")).limit(1).collect())
            if last:
                return last[0]["watermark"] or default_wm, last[0]["last_loaded_date"] or default_date
        except Exception as e:
            print("[WARN] get_progress:", e)
        return default_wm, default_date

    def set_progress(self, schema, table, watermark, last_loaded_date):
        self._write_progress([{
            "schema_name": schema, "table_name": table,
            "watermark": watermark, "last_loaded_date": last_loaded_date
        }])

# =========================
# Helpers as small classes
# =========================
class Paths:
    @staticmethod
    def build(rt, schema, table, load_date):
        append_schema = rt.get("append_table_schema", False)
        raw_dir = f"{rt['raw_root']}/{schema}/{table}/load_date={load_date}" if append_schema else f"{rt['raw_root']}/{table}/load_date={load_date}"
        final_dir = f"{rt['final_root']}/{schema}/{table}" if append_schema else f"{rt['final_root']}/{table}"
        base_raw = f"{rt['raw_root']}/{schema}/{table}" if append_schema else f"{rt['raw_root']}/{table}"
        return raw_dir, final_dir, base_raw

class JDBC:
    @staticmethod
    def reader(spark, jdbc, schema, table, tbl_cfg):
        r = (spark.read.format("jdbc")
             .option("url", jdbc["url"])
             .option("dbtable", f"{schema}.{table}")
             .option("user", jdbc["user"])
             .option("password", jdbc["password"])
             .option("trustServerCertificate", "true")
             .option("driver", jdbc["driver"])
             .option("fetchsize", int(jdbc.get("fetchsize", 10000))))
        pr = tbl_cfg.get("partition_read")
        if pr:
            r = (r.option("partitionColumn", pr["partitionColumn"])
                 .option("lowerBound", str(pr["lowerBound"]))
                 .option("upperBound", str(pr["upperBound"]))
                 .option("numPartitions", str(pr.get("numPartitions", jdbc.get("default_num_partitions", 8)))))
        return r

class RawIO:
    @staticmethod
    def copy_and_swap_final(df, rt, raw_dir, state, schema, table, load_date, mode):
        state.mark_event(schema, table, load_date, mode,
                         "raw", "started", location=raw_dir)
        (df.write.format(rt.get("write_format", "parquet"))
           .mode("append")
           .option("compression", rt.get("compression", "snappy"))
           .save(raw_dir))
        state.mark_event(schema, table, load_date, mode, "raw",
                         "success", rows_written=df.count(), location=raw_dir)

    @staticmethod
    def raw_increment_df(spark, rt, schema, table, last_ld, last_wm, incr_col):
        _, __, base_raw = Paths.build(rt, schema, table, '')
        all_raw = (spark.read.format(
            rt.get("write_format", "parquet")).load(base_raw))
        return (all_raw.where(col("load_date") >= lit(last_ld))
                       .where(to_timestamp(col(incr_col)) > to_timestamp(lit(last_wm))))

class HiveHelper:
    @staticmethod
    def create_external_if_absent(spark, db, table_name, location, partitioned=False, partition_col="load_date"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        if partitioned:
            spark.sql(f"""
              CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{table_name}` (_dummy STRING)
              PARTITIONED BY ({partition_col} STRING)
              STORED AS PARQUET
              LOCATION '{location}'
            """)
            try:
                spark.sql(
                    f"ALTER TABLE `{db}`.`{table_name}` REPLACE COLUMNS ({partition_col} STRING)")
            except Exception:
                pass
        else:
            spark.sql(f"""
              CREATE EXTERNAL TABLE IF NOT EXISTS `{db}`.`{table_name}`
              STORED AS PARQUET
              LOCATION '{location}'
              AS SELECT * FROM (SELECT 1 as __bootstrap) t WHERE 1=0
            """)

    @staticmethod
    def add_partition_or_msck(spark, db, table_name, partition_col, load_date, base_location, use_msck=False):
        part_loc = f"{base_location}/{partition_col}={load_date}"
        if use_msck:
            spark.sql(f"MSCK REPAIR TABLE `{db}`.`{table_name}`")
        else:
            spark.sql(f"""
              ALTER TABLE `{db}`.`{table_name}`
              ADD IF NOT EXISTS PARTITION ({partition_col}='{load_date}')
              LOCATION '{part_loc}'
            """)

    @staticmethod
    def set_location(spark, db, schema, table, new_location):
        hive_table = f"{db}.{schema}__{table}"
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        spark.sql(f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {hive_table}
            STORED AS PARQUET
            LOCATION '{new_location}'
            AS SELECT * FROM (SELECT 1 AS _bootstrap) t LIMIT 0
        """)
        spark.sql(f"ALTER TABLE {hive_table} SET LOCATION '{new_location}'")
        return hive_table

class IcebergHelper:
    @staticmethod
    def setup_catalog(spark, cfg):
        inter = cfg["runtime"]["intermediate"]
        cat, wh = inter["catalog"], inter["warehouse"]
        spark.conf.set(
            f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set(f"spark.sql.catalog.{cat}.type", "hadoop")
        spark.conf.set(f"spark.sql.catalog.{cat}.warehouse", wh)
        spark.conf.set("spark.sql.catalog.spark_catalog",
                       "org.apache.spark.sql.internal.CatalogImpl")

    @staticmethod
    def ensure_table(spark, cfg, schema, table, df, partition_col):
        inter = cfg["runtime"]["intermediate"]
        cat, db = inter["catalog"], inter["db"]
        full = f"{cat}.{db}.{schema}__{table}"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cat}.{db}")
        cols = ", ".join(
            [f"`{c}` {df.schema[c].dataType.simpleString()}" for c in df.columns])
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {full} ({cols}) USING iceberg PARTITIONED BY ({partition_col})")
        return full

    @staticmethod
    def dedup(df, pk_cols, order_cols_desc):
        order_exprs = [F.col(c).desc_nulls_last() for c in order_cols_desc]
        if "_ingest_tiebreak" not in df.columns:
            df = df.withColumn("_ingest_tiebreak",
                               F.monotonically_increasing_id())
        order_exprs.append(F.col("_ingest_tiebreak").desc())
        w = Window.partitionBy(*[F.col(c)
                               for c in pk_cols]).orderBy(*order_exprs)
        return (df.withColumn("_rn", F.row_number().over(w))
                  .where(F.col("_rn") == 1).drop("_rn", "_ingest_tiebreak"))

    @staticmethod
    def merge_upsert(spark, cfg, schema, table, df_src, pk_cols, load_date, partition_col="load_date", incr_col=None):
        inter = cfg["runtime"]["intermediate"]
        cat, db = inter["catalog"], inter["db"]
        tgt = f"{cat}.{db}.{schema}__{table}"

        IcebergHelper.setup_catalog(spark, cfg)

        if partition_col not in df_src.columns:
            df_src = df_src.withColumn(partition_col, F.lit(load_date))

        order_cols = [incr_col] if incr_col else []
        order_cols.append(partition_col)
        df_src = IcebergHelper.dedup(df_src, pk_cols, order_cols)

        IcebergHelper.ensure_table(
            spark, cfg, schema, table, df_src, partition_col)
        tgt_cols = [f.name for f in spark.table(tgt).schema.fields]
        for c in tgt_cols:
            if c not in df_src.columns:
                df_src = df_src.withColumn(c, F.lit(None).cast("string"))
        df_src = df_src.select([F.col(c) for c in tgt_cols if c in df_src.columns] +
                               [F.col(c) for c in df_src.columns if c not in tgt_cols])

        tv = f"src_{schema}_{table}_{uuid.uuid4().hex[:8]}"
        df_src.createOrReplaceTempView(tv)

        src_cols = df_src.columns
        non_pk_cols = [
            c for c in src_cols if c not in pk_cols and c != partition_col]
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
    def mirror_to_parquet_for_date(spark, cfg, schema, table, load_date):
        final_cfg = cfg["runtime"]["final_parquet_mirror"]
        hive_cfg = cfg["runtime"]["hive_reg"]
        inter = cfg["runtime"]["intermediate"]
        cat, db = inter["catalog"], inter["db"]
        part_col = final_cfg.get("partition_col", "load_date")
        ice_tbl = f"{cat}.{db}.{schema}__{table}"
        snap_df = spark.table(ice_tbl).where(f"`{part_col}` = '{load_date}'")

        append_schema = cfg["runtime"].get("append_table_schema", False)
        base = f"{final_cfg['root']}/{schema}/{table}" if append_schema else f"{final_cfg['root']}/{table}"
        part_path = f"{base}/{part_col}={load_date}"

        (snap_df.write.format(cfg["runtime"].get("write_format", "parquet"))
         .mode("overwrite").option("compression", cfg["runtime"].get("compression", "snappy"))
         .save(part_path))

        if hive_cfg.get("enabled", False):
            hive_db = hive_cfg["db"]
            hive_tbl = f"{schema}__{table}"
            HiveHelper.create_external_if_absent(
                spark, hive_db, hive_tbl, base, partitioned=True, partition_col=part_col)
            HiveHelper.add_partition_or_msck(
                spark, hive_db, hive_tbl, part_col, load_date, base, use_msck=hive_cfg.get("use_msck", False))
        return {"final_parquet_path": part_path}

# =========================
# Ingestion strategies
# =========================
class IngestionFull:
    @staticmethod
    def run(spark, cfg, state, schema, table, load_date, raw_dir, final_dir, tbl_cfg):
        rt, jdbc = cfg["runtime"], cfg["jdbc"]
        status = state.get_day_status(schema, table, load_date)

        if status.get("finalized_done"):
            state.mark_event(schema, table, load_date, "full",
                             "raw", "skipped", location=raw_dir)
            state.mark_event(schema, table, load_date, "full",
                             "finalize", "skipped", location=final_dir)
            return {"table": f"{schema}.{table}", "skipped": True, "reason": "already finalized today"}

        reader = JDBC.reader(spark, jdbc, schema, table, tbl_cfg)
        if status.get("raw_done") and not status.get("finalized_done"):
            result = {"table": f"{schema}.{table}",
                      "mode": "full", "raw": raw_dir}
        else:
            df = reader.load()
            rows = df.count()
            state.mark_event(schema, table, load_date, "full",
                             "raw", "started", location=raw_dir)
            (df.write.format(rt.get("write_format", "parquet"))
               .mode("overwrite").option("compression", rt.get("compression", "snappy"))
               .save(raw_dir))
            state.mark_event(schema, table, load_date, "full", "raw",
                             "success", rows_written=rows, location=raw_dir)
            result = {"table": f"{schema}.{table}",
                      "mode": "full", "rows": rows, "raw": raw_dir}

        finalized = False
        hive_table = None
        if rt.get("finalize_full_refresh", True):
            state.mark_event(schema, table, load_date, "full", "finalize", "started", location=final_dir,
                             strategy=rt.get("finalize_strategy"))
            if rt.get("finalize_strategy") == "hive_set_location" and rt.get("hive", {}).get("enabled", False):
                hive_table = HiveHelper.set_location(
                    spark, rt["hive"]["db"], schema, table, raw_dir)
            else:
                # HDFS copy+swap
                jsc = spark._jsc
                jvm = spark.sparkContext._jvm
                conf = jsc.hadoopConfiguration()
                fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
                Path = jvm.org.apache.hadoop.fs.Path
                src = Path(raw_dir)
                dst = Path(final_dir)
                tmp = Path(final_dir + ".__tmp_swap__")
                if fs.exists(tmp):
                    fs.delete(tmp, True)
                ok = jvm.org.apache.hadoop.fs.FileUtil.copy(
                    fs, src, fs, tmp, False, conf)
                if not ok:
                    raise RuntimeError(
                        f"Copy {raw_dir} -> {final_dir}/.__tmp_swap__ failed")
                if fs.exists(dst):
                    fs.delete(dst, True)
                if not fs.rename(tmp, dst):
                    raise RuntimeError(f"Atomic rename failed for {final_dir}")
            finalized = True
            state.mark_event(schema, table, load_date, "full", "finalize", "success",
                             location=final_dir, strategy=rt.get("finalize_strategy"))

        result.update(
            {"final": final_dir, "finalized": finalized, "hive_table": hive_table})
        return result

class IngestionSCD1:
    @staticmethod
    def run(spark, cfg, state, schema, table, load_date, raw_dir, base_raw, tbl_cfg):
        rt, jdbc = cfg["runtime"], cfg["jdbc"]
        incr_col = tbl_cfg["incremental_column"]
        initial_wm = tbl_cfg.get("initial_watermark", "1900-01-01 00:00:00")
        pk_cols = tbl_cfg.get("primary_keys", [])

        # 1) read progress
        last_wm, last_ld = state.get_progress(
            schema, table, default_wm=initial_wm, default_date="1900-01-01")
        print(f"[{schema}.{table}] progress wm={last_wm} last_ld={last_ld}")

        # 2) pull increment and land RAW for today
        src_df = JDBC.reader(spark, jdbc, schema, table, tbl_cfg).load()
        inc_df = src_df.filter(to_timestamp(col(incr_col))
                               > to_timestamp(lit(last_wm)))
        pulled = inc_df.count()
        print(f"[{schema}.{table}] pulled={pulled}")
        if pulled > 0:
            RawIO.land_append(inc_df, rt, raw_dir, state,
                              schema, table, load_date, "scd1")
        else:
            state.mark_event(schema, table, load_date, "scd1",
                             "raw", "success", rows_written=0, location=raw_dir)

        # 3) build RAW window to merge (>= last_ld & > last_wm)
        raw_to_merge = RawIO.raw_increment_df(
            spark, rt, schema, table, last_ld, last_wm, incr_col)
        to_merge_count = raw_to_merge.count()
        print(f"[{schema}.{table}] raw_to_merge={to_merge_count}")
        if to_merge_count == 0:
            return {"table": f"{schema}.{table}", "mode": "scd1", "rows": 0, "raw": raw_dir,
                    "wm": last_wm, "last_loaded_date": last_ld}

        # 4) intermediate merge (Iceberg / Parquet)
        inter_cfg = rt.get("intermediate", {"enabled": False})
        state.mark_event(schema, table, load_date, "scd1",
                         "intermediate", "started")
        info, ok = {}, True
        if inter_cfg.get("enabled", False):
            try:
                if inter_cfg.get("type", "iceberg") == "iceberg" and pk_cols:
                    tgt = IcebergHelper.merge_upsert(
                        spark, cfg, schema, table, raw_to_merge, pk_cols,
                        load_date, partition_col=rt.get("hive_reg", {}).get(
                            "partition_col", "load_date"),
                        incr_col=incr_col
                    )
                    info["iceberg_table"] = tgt
                    # optional mirror
                    if rt.get("final_parquet_mirror", {"enabled": False}).get("enabled", False):
                        info.update(IcebergHelper.mirror_to_parquet_for_date(
                            spark, cfg, schema, table, load_date))
                elif inter_cfg.get("type") == "parquet":
                    # from_path = write_to_intermediate_parquet(
                    #     spark, cfg, schema, table, load_date, raw_to_merge)  # reuse your existing writer
                    # info["parquet_path"] = from_path
                    raise NotImplementedError("Parquet intermediary is not supported currently.")
                state.mark_event(schema, table, load_date,
                                 "scd1", "intermediate", "success")
            except Exception as e:
                ok = False
                info["intermediate_error"] = str(e)
                state.mark_event(schema, table, load_date, "scd1",
                                 "intermediate", "failed", error=str(e))

        # 5) progress only on success (or intermediate disabled)
        if ok:
            new_wm, new_ld = IngestionSCD1._compute_wm_ld(
                raw_to_merge, incr_col)
            state.mark_event(schema, table, load_date, "scd1",
                             "watermark", "success", watermark=new_wm)
            state.set_progress(schema, table, watermark=new_wm,
                               last_loaded_date=new_ld)
            res = {"table": f"{schema}.{table}", "mode": "scd1", "rows": to_merge_count,
                   "raw": raw_dir, "wm": new_wm, "last_loaded_date": new_ld}
            res.update(info)
            return res
        else:
            return {"table": f"{schema}.{table}", "mode": "scd1", "rows": to_merge_count,
                    "raw": raw_dir, "wm": last_wm, "last_loaded_date": last_ld, **info}

    @staticmethod
    def _compute_wm_ld(df, incr_col):
        agg = df.select(spark_max(col(incr_col)).alias("wm"),
                        spark_max(col("load_date")).alias("ld")).collect()[0]
        return str(agg["wm"]), str(agg["ld"])

# =========================
# Orchestration
# =========================
def ingest_one_table(spark, cfg, state, tbl, pool_name):
    schema, table = tbl["schema"], tbl["table"]
    mode = tbl.get("mode", "full").lower()
    load_date = datetime.now().astimezone().strftime("%Y-%m-%d")

    rt = cfg["runtime"]
    raw_dir, final_dir, base_raw = Paths.build(rt, schema, table, load_date)

    # scheduler labels
    sc = spark.sparkContext
    sc.setLocalProperty("spark.scheduler.pool", pool_name)
    sc.setJobGroup(f"ingest::{schema}.{table}",
                   f"Ingest {schema}.{table} -> {raw_dir}")

    if mode == "full":
        return IngestionFull.run(spark, cfg, state, schema, table, load_date, raw_dir, final_dir, tbl)
    elif mode == "scd1":
        return IngestionSCD1.run(spark, cfg, state, schema, table, load_date, raw_dir, base_raw, tbl)
    else:
        raise ValueError(f"Unsupported mode: {mode}")

# =========================
# Main
# =========================
def main(spark, cfg):
    backend = cfg["runtime"].get("state_backend", "hdfs")
    if backend == "singlestore":
        state = SingleStoreState(spark, cfg["runtime"]["state"]["singlestore"])
    else:
        base = cfg["runtime"]["state"]["hdfs"]["dir"]
        spark.range(1).write.mode("overwrite").parquet(
            str(PurePosixPath(base) / "_init_marker"))
        raise NotImplementedError("HDFSState not included in this snippet.")

    max_workers = int(cfg["runtime"].get("max_parallel_tables", 4))
    tables = cfg["tables"]
    results, errors = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futmap = {}
        for i, tbl in enumerate(tables):
            pool = f"pool-{(i % max_workers) + 1}"
            fut = ex.submit(ingest_one_table, spark, cfg, state, tbl, pool)
            futmap[fut] = f"{tbl['schema']}.{tbl['table']}"
        for fut in as_completed(futmap):
            key = futmap[fut]
            try:
                res = fut.result()
                results.append(res)
                print("[OK]", key, res)
            except Exception as e:
                traceback.print_exc()
                errors.append((key, str(e)))
                print("[ERR]", key, e)

    print("\n=== SUMMARY ===")
    for r in results:
        print(r)
    if errors:
        print("\n=== ERRORS ===")
        for k, e in errors:
            print(k, e)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    with open(args.config, "r") as f:
        cfg = json.load(f)

    spark = (
        SparkSession.builder
        .appName("oracle_ingest_to_raw_resilient")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.initialExecutors", "1")
        .config("spark.dynamicAllocation.maxExecutors", "2")
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
        .config("spark.executor.cores", "4")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "6g")
        .config("spark.jars", ",".join([
            "/home/informaticaadmin/jars/singlestore-spark-connector_2.11-3.1.2-spark-2.4.7.jar",
            "/home/informaticaadmin/jars/mariadb-java-client-2.7.11.jar",
            "/home/informaticaadmin/jars/ojdbc8-12.2.0.1.jar",
            # Add your Iceberg runtime for Spark 3.x/Scala 2.12:
            # "/path/to/iceberg-spark-runtime-3.3_2.12-<ver>.jar"
        ]))
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", cfg["runtime"].get("timezone", "Asia/Kolkata"))
        .enableHiveSupport()
        .getOrCreate()
    )
    main(spark, cfg)
    spark.stop()
