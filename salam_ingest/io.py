import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException, ParseException
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_timestamp
from pyspark.sql.window import Window as W

from .common import RUN_ID, with_ingest_cols


class SourceProbe:
    def _count_query(dialect: str, base_from_sql: str, incr_col: str, is_int_epoch: bool, wm_lit: str, wm_hi_lit: Optional[str] = None) -> str:
        if is_int_epoch:
            pred = f"{incr_col} > {wm_lit}"
            if wm_hi_lit:
                pred += f" AND {incr_col} <= {wm_hi_lit}"
            return f"(SELECT COUNT(1) AS CNT FROM {base_from_sql} WHERE {pred}) c"
        if dialect == "oracle":
            pred = f"{incr_col} > TO_TIMESTAMP('{wm_lit}', 'YYYY-MM-DD HH24:MI:SS')"
            if wm_hi_lit:
                pred += f" AND {incr_col} <= TO_TIMESTAMP('{wm_hi_lit}', 'YYYY-MM-DD HH24:MI:SS')"
            return f"(SELECT COUNT(1) AS CNT FROM {base_from_sql} WHERE {pred}) c"
        if dialect == "mssql":
            lo = wm_lit.replace(" ", "T")
            pred = f"[{incr_col}] > CONVERT(DATETIME2, '{lo}', 126)"
            if wm_hi_lit:
                hi = wm_hi_lit.replace(" ", "T")
                pred += f" AND [{incr_col}] <= CONVERT(DATETIME2, '{hi}', 126)"
            return f"(SELECT COUNT_BIG(1) AS CNT FROM {base_from_sql} WHERE {pred}) c"
        pred = f"{incr_col} > '{wm_lit}'"
        if wm_hi_lit:
            pred += f" AND {incr_col} <= '{wm_hi_lit}'"
        return f"(SELECT COUNT(1) AS CNT FROM {base_from_sql} WHERE {pred}) c"

    @staticmethod
    def estimate_count(
        spark: SparkSession,
        jdbc: Dict[str, Any],
        base_from_sql: str,
        incr_col: str,
        is_int_epoch: bool,
        wm_lit: str,
        wm_hi_lit: Optional[str] = None,
    ) -> int:
        q = SourceProbe._count_query(
            jdbc.get("dialect", "oracle").lower(),
            base_from_sql,
            incr_col,
            is_int_epoch,
            wm_lit,
            wm_hi_lit,
        )
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc["url"])
            .option("dbtable", q)
            .option("user", jdbc["user"])
            .option("password", jdbc["password"])
            .option("driver", jdbc["driver"])
            .option("fetchsize", 1000)
            .load()
        )
        return int(df.collect()[0][0])


class AdaptiveSlicePlanner:
    @staticmethod
    def _time_lits(wm_str: str, now_str: str, parts: int) -> List[Tuple[str, str]]:
        fmt = "%Y-%m-%d %H:%M:%S"
        t0 = datetime.strptime(wm_str[:19], fmt)
        t1 = datetime.strptime(now_str[:19], fmt)
        step = (t1 - t0) / parts
        bounds = [t0 + i * step for i in range(parts)] + [t1]
        lits = [b.strftime(fmt) for b in bounds]
        return [(lits[i], lits[i + 1]) for i in range(parts)]

    @staticmethod
    def _int_lits(lo_int: int, hi_int: int, parts: int) -> List[Tuple[str, str]]:
        step = max(1, (hi_int - lo_int) // parts)
        bounds = [lo_int + i * step for i in range(parts)] + [hi_int]
        return [(str(bounds[i]), str(bounds[i + 1])) for i in range(parts)]

    @staticmethod
    def plan(
        spark: SparkSession,
        logger: Any,
        jdbc: Dict[str, Any],
        tbl_cfg: Dict[str, Any],
        base_from_sql: str,
        incr_col: str,
        last_wm: str,
        now_lit: str,
        is_int_epoch: bool,
        slicing_cfg: Dict[str, Any],
    ) -> List[Tuple[str, str]]:
        max_dur_h = int(slicing_cfg.get("max_duration_hours", 168))
        max_count = int(slicing_cfg.get("max_count", 10_000_000))
        target = int(slicing_cfg.get("target_rows_per_slice", 1_000_000))
        max_parts = int(slicing_cfg.get("max_partitions", 24))
        duration_ok = True
        if not is_int_epoch:
            fmt = "%Y-%m-%d %H:%M:%S"
            t0 = datetime.strptime(last_wm[:19], fmt)
            t1 = datetime.strptime(now_lit[:19], fmt)
            duration_ok = ((t1 - t0).total_seconds() / 3600.0) <= max_dur_h
        total = SourceProbe.estimate_count(
            spark, jdbc, base_from_sql, incr_col, is_int_epoch, last_wm, now_lit
        )
        logger.info("slice_probe", total=total, duration_ok=duration_ok)
        if duration_ok and total <= max_count:
            return [(last_wm, now_lit)]
        parts = min(max_parts, max(2, (total + target - 1) // target))
        if is_int_epoch:
            lo = int(last_wm)
            hi = int(now_lit)
            return AdaptiveSlicePlanner._int_lits(lo, hi, parts)
        return AdaptiveSlicePlanner._time_lits(last_wm, now_lit, parts)


class DateUtil:
    @staticmethod
    def build_time_windows(start_iso: str, end_iso: str, step_hours: int = 24) -> List[Dict[str, str]]:
        dt = datetime.fromisoformat(start_iso.replace("Z", "")).replace(tzinfo=timezone.utc)
        end = datetime.fromisoformat(end_iso.replace("Z", "")).replace(tzinfo=timezone.utc)
        out: List[Dict[str, str]] = []
        step = timedelta(hours=step_hours)
        while dt < end:
            nxt = min(dt + step, end)
            out.append(
                {
                    "iso_left": dt.strftime("%Y-%m-%dT%H:%M:%S"),
                    "iso_right": nxt.strftime("%Y-%m-%dT%H:%M:%S"),
                    "ts_left": dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "ts_right": nxt.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            dt = nxt
        return out

    @staticmethod
    def build_predicates(dialect: str, incr_col: str, windows: Sequence[Dict[str, str]]) -> List[str]:
        preds = []
        d = dialect.lower()
        for window in windows:
            if d == "mssql":
                preds.append(
                    f"[{incr_col}] >= '{window['iso_left']}' AND [{incr_col}] < '{window['iso_right']}'"
                )
            elif d == "oracle":
                preds.append(
                    f"{incr_col} >= TIMESTAMP '{window['ts_left']}' AND {incr_col} < TIMESTAMP '{window['ts_right']}'"
                )
            else:
                preds.append(
                    f"{incr_col} >= '{window['ts_left']}' AND {incr_col} < '{window['ts_right']}'"
                )
        return preds


class HDFSUtil:
    @staticmethod
    def write_text(spark: SparkSession, path: str, content: str) -> None:
        jsc = spark._jsc
        jvm = spark.sparkContext._jvm
        conf = jsc.hadoopConfiguration()
        p = jvm.org.apache.hadoop.fs.Path(path)
        fs = p.getFileSystem(conf)
        if fs.exists(p):
            fs.delete(p, True)
        out = fs.create(p)
        try:
            out.write(bytearray(content.encode("utf-8")))
        finally:
            out.close()

    @staticmethod
    def append_text(spark: SparkSession, path: str, content: str) -> None:
        jsc = spark._jsc
        jvm = spark.sparkContext._jvm
        conf = jsc.hadoopConfiguration()
        p = jvm.org.apache.hadoop.fs.Path(path)
        fs = p.getFileSystem(conf)
        out = fs.create(p, True) if not fs.exists(p) else fs.append(p)
        try:
            out.write(bytearray(content.encode("utf-8")))
        finally:
            out.close()


class Paths:
    @staticmethod
    def build(rt: Dict[str, Any], schema: str, table: str, load_date: str) -> Tuple[str, str, str]:
        append_schema = rt.get("append_table_schema", False)
        raw_dir = (
            f"{rt['raw_root']}/{schema}/{table}/load_date={load_date}"
            if append_schema
            else f"{rt['raw_root']}/{table}/load_date={load_date}"
        )
        final_dir = (
            f"{rt['final_root']}/{schema}/{table}"
            if append_schema
            else f"{rt['final_root']}/{table}"
        )
        base_raw = (
            f"{rt['raw_root']}/{schema}/{table}"
            if append_schema
            else f"{rt['raw_root']}/{table}"
        )
        return raw_dir, final_dir, base_raw


class Utils:
    @staticmethod
    def minus_seconds_datetime(wm_str: str, seconds: Optional[int]) -> str:
        if not seconds:
            return wm_str
        dt = datetime.strptime(wm_str[:19], "%Y-%m-%d %H:%M:%S")
        return (dt - timedelta(seconds=int(seconds))).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def minus_seconds_epoch(wm_str: str, seconds: Optional[int], millis: bool = False) -> str:
        if not seconds:
            return wm_str
        base = int(float(wm_str))
        adj = base - (int(seconds) * (1000 if millis else 1))
        return str(max(adj, 0))

    @staticmethod
    def minus_seconds(wm_str: str, seconds: Optional[int]) -> str:
        if not seconds:
            return wm_str
        dt = datetime.strptime(wm_str[:19], "%Y-%m-%d %H:%M:%S")
        dt2 = dt - timedelta(seconds=int(seconds))
        return dt2.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def mssql_literal_from_wm(wm_str: str) -> str:
        return wm_str[:19].replace(" ", "T")

    @staticmethod
    def schema_json(schema: Any) -> Dict[str, Any]:
        return json.loads(schema.json())

    @staticmethod
    def sample_spark(spark: SparkSession) -> Dict[str, Any]:
        sc = spark.sparkContext
        try:
            stage_ids = [s.stageId() for s in sc.statusTracker().getActiveStageIds()]
        except Exception:
            stage_ids = []
        return {"active_stages": stage_ids, "default_parallelism": sc.defaultParallelism}


class JDBC:
    @staticmethod
    def build_from_sql(jdbc: Dict[str, Any], schema: str, table: str, tbl_cfg: Dict[str, Any]) -> str:
        if "query_sql" in tbl_cfg and tbl_cfg["query_sql"].strip():
            return f"({tbl_cfg['query_sql']}) q"
        if jdbc.get("dialect", "oracle").lower() == "mssql":
            return f"[{schema}].[{table}]"
        return f"{schema}.{table}"

    @staticmethod
    def dbtable_for_range(
        jdbc: Dict[str, Any],
        base_from_sql: str,
        incr_col: str,
        tbl_cfg: Dict[str, Any],
        lo_lit: str,
        hi_lit: Optional[str] = None,
    ) -> str:
        dialect = jdbc.get("dialect", "oracle").lower()
        incr_type = (tbl_cfg.get("incr_col_type") or "").lower()
        cols = tbl_cfg.get("cols", "*")
        if isinstance(cols, list):
            cols = ", ".join(cols)
        if dialect == "oracle":
            if incr_type in ("epoch_seconds", "epoch_millis"):
                lo = int(float(lo_lit))
                pred = f" {incr_col} > {lo}"
                if hi_lit:
                    hi = int(float(hi_lit))
                    pred += f" AND {incr_col} <= {hi}"
            else:
                pred = f"{incr_col} > TO_TIMESTAMP('{lo_lit}','YYYY-MM-DD HH24:MI:SS')"
                if hi_lit:
                    pred += f" AND {incr_col} <= TO_TIMESTAMP('{hi_lit}','YYYY-MM-DD HH24:MI:SS')"
        elif dialect == "mssql":
            if incr_type in ("epoch_seconds", "epoch_millis"):
                lo = int(float(lo_lit))
                pred = f" [{incr_col}] > {lo}"
                if hi_lit:
                    hi = int(float(hi_lit))
                    pred += f" AND [{incr_col}] <= {hi}"
            else:
                lo = lo_lit.replace(" ", "T")
                pred = f"[{incr_col}] > CONVERT(DATETIME2,'{lo}',126)"
                if hi_lit:
                    hi = hi_lit.replace(" ", "T")
                    pred += f" AND [{incr_col}] <= CONVERT(DATETIME2,'{hi}',126)"
        else:
            pred = f"{incr_col} > '{lo_lit}'" + (f" AND {incr_col} <= '{hi_lit}'" if hi_lit else "")
        return f"(SELECT {cols} FROM {base_from_sql} WHERE {pred}) t"

    @staticmethod
    def reader(spark: SparkSession, jdbc: Dict[str, Any], schema: str, table: str, tbl_cfg: Dict[str, Any]):
        reader = (
            spark.read.format("jdbc")
            .option("url", jdbc["url"])
            .option("dbtable", f"{schema}.{table}")
            .option("user", jdbc["user"])
            .option("password", jdbc["password"])
            .option("trustServerCertificate", "true")
            .option("driver", jdbc["driver"])
            .option("fetchsize", int(jdbc.get("fetchsize", 10000)))
        )
        pr = tbl_cfg.get("partition_read")
        if pr:
            reader = (
                reader.option("partitionColumn", pr["partitionColumn"])
                .option("lowerBound", str(pr["lowerBound"]))
                .option("upperBound", str(pr["upperBound"]))
                .option("numPartitions", str(pr.get("numPartitions", jdbc.get("default_num_partitions", 8))))
            )
        return reader

    @staticmethod
    def reader_increment(
        spark: SparkSession,
        jdbc: Dict[str, Any],
        schema: str,
        table: str,
        tbl_cfg: Dict[str, Any],
        incr_col: str,
        last_wm: str,
    ):
        dbtable = JDBC.dbtable_for_range(jdbc, schema, table, incr_col, tbl_cfg, last_wm, None)
        reader = (
            spark.read.format("jdbc")
            .option("url", jdbc["url"])
            .option("dbtable", dbtable)
            .option("user", jdbc["user"])
            .option("password", jdbc["password"])
            .option("driver", jdbc["driver"])
            .option("trustServerCertificate", "true")
            .option("fetchsize", int(jdbc.get("fetchsize", 10000)))
        )
        pr = tbl_cfg.get("partition_read")
        if pr:
            reader = (
                reader.option("partitionColumn", pr["partitionColumn"])
                .option("lowerBound", str(pr["lowerBound"]))
                .option("upperBound", str(pr["upperBound"]))
                .option("numPartitions", str(pr.get("numPartitions", jdbc.get("default_num_partitions", 8))))
            )
        return reader


class RawIO:
    @staticmethod
    def land_append(
        spark: SparkSession,
        logger: Any,
        df: DataFrame,
        rt: Dict[str, Any],
        raw_dir: str,
        state: Any,
        schema: str,
        table: str,
        load_date: str,
        mode: str,
    ) -> None:
        rows = df.count()
        state.mark_event(schema, table, load_date, mode, "raw", "started", location=raw_dir, rows_written=rows)
        (
            df.write.format(rt.get("write_format", "parquet"))
            .mode("append")
            .option("compression", rt.get("compression", "snappy"))
            .save(raw_dir)
        )
        state.mark_event(schema, table, load_date, mode, "raw", "success", rows_written=rows, location=raw_dir)
        metrics = {
            "schema": schema,
            "table": table,
            "run_id": RUN_ID,
            "rows_written": rows,
            "status": "success",
            "phase": "raw",
            "load_date": load_date,
            "ts": datetime.now().astimezone().isoformat(),
        }
        try:
            HDFSUtil.write_text(spark, f"{raw_dir}/_METRICS.json", json.dumps(metrics))
        except Exception as exc:
            logger.error("error writing metrics", err=str(exc))

    @staticmethod
    def raw_increment_df(
        spark: SparkSession,
        rt: Dict[str, Any],
        schema: str,
        table: str,
        last_ld: str,
        last_wm: str,
        incr_col: str,
        incr_type: str = "",
    ) -> DataFrame:
        _, _, base_raw = Paths.build(rt, schema, table, "")
        all_raw = spark.read.format(rt.get("write_format", "parquet")).load(base_raw)
        if incr_col.lower() not in [c.lower() for c in all_raw.columns]:
            return all_raw.limit(0)
        df = all_raw.where(col("load_date") >= lit(last_ld))
        incr_type = (incr_type or "").lower()
        if incr_type.startswith("epoch") or incr_type in ("bigint", "int", "numeric", "decimal"):
            df = df.where(col(incr_col).cast("long") > F.lit(int(str(last_wm))))
        else:
            df = df.where(to_timestamp(col(incr_col)) > to_timestamp(lit(last_wm)))
        return df


class HDFSOutbox:
    def __init__(self, spark: SparkSession, root: str) -> None:
        self.spark = spark
        self.root = root.rstrip("/")
        self.jsc = spark._jsc
        self.jvm = spark.sparkContext._jvm
        self.conf = self.jsc.hadoopConfiguration()
        self.Path = self.jvm.org.apache.hadoop.fs.Path
        self.fs = self.jvm.org.apache.hadoop.fs.FileSystem.get(self.conf)
        for sub in ("events", "progress"):
            p = self.Path(f"{self.root}/{sub}")
            if not self.fs.exists(p):
                self.fs.mkdirs(p)

    def _atomic_write(self, path: str, content_bytes: bytes) -> None:
        p_tmp = self.Path(path + ".__tmp__")
        p = self.Path(path)
        if self.fs.exists(p_tmp):
            self.fs.delete(p_tmp, True)
        out = self.fs.create(p_tmp, True)
        try:
            out.write(content_bytes)
        finally:
            out.close()
        if self.fs.exists(p):
            self.fs.delete(p, True)
        if not self.fs.rename(p_tmp, p):
            raise RuntimeError(f"Atomic rename failed for {path}")

    def _delete(self, path: str) -> None:
        p = self.Path(path)
        if self.fs.exists(p):
            self.fs.delete(p, False)

    def mirror_batch(self, kind: str, records: Iterable[Dict[str, Any]]) -> str:
        ts = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S.%f")
        name = f"{kind}_{RUN_ID}_{ts}_{uuid.uuid4().hex}.jsonl"
        path = f"{self.root}/{kind}/{name}"
        payload = "\n".join(json.dumps(r, separators=(",", ":")) for r in records)
        self._atomic_write(path, payload.encode("utf-8"))
        return path

    def append_event(self, source_id: str, run_id: str, record: Dict[str, Any]) -> str:
        _ = source_id, run_id  # currently unused but kept for parity
        return self.mirror_batch("events", [record])

    def append_progress(self, source_id: str, run_id: str, record: Dict[str, Any]) -> str:
        _ = source_id, run_id
        return self.mirror_batch("progress", [record])

    def list_pending(self, kind: str) -> List[str]:
        base = f"{self.root}/{kind}"
        it = self.fs.listStatus(self.Path(base))
        out = []
        for status in it:
            p = status.getPath().toString()
            if p.endswith(".jsonl"):
                out.append(p)
        return sorted(out)

    def read_jsonl(self, path: str) -> List[Dict[str, Any]]:
        df = self.spark.read.json(path)
        return [row.asDict(recursive=True) for row in df.collect()]

    def delete(self, path: str) -> None:
        self._delete(path)

    def restore_all(self, base_state: Any, logger: Any) -> None:
        for kind in ("events", "progress"):
            for path in self.list_pending(kind):
                try:
                    rows = self.read_jsonl(path)
                    if kind == "events":
                        base_state._write_events(rows)
                    else:
                        base_state._write_progress(rows)
                    self.delete(path)
                    logger.info("outbox_replayed", kind=kind, file=path, rows=len(rows))
                except Exception as exc:
                    logger.error("outbox_replay_failed", kind=kind, file=path, err=str(exc))


class HiveHelper:
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
        final_location: str,
    ) -> None:
        tbl = f"{schema}__{table}"
        if HiveHelper.table_exists(spark, db, tbl):
            return
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        df = spark.read.parquet(final_location).limit(1)
        cols_ddl = HiveHelper._cols_ddl_from_df_schema(df.schema, partition_col="load_date")
        spark.sql(
            f"""
          CREATE EXTERNAL TABLE `{db}`.`{tbl}` (
            {cols_ddl}
          )
          STORED AS PARQUET
          LOCATION '{final_location}'
        """
        )


class IcebergHelper:
    @staticmethod
    def _identifier(schema: str, table: str) -> str:
        return f"{schema.lower()}__{table.lower()}"

    @staticmethod
    def setup_catalog(spark: SparkSession, cfg: Dict[str, Any]) -> None:
        inter = cfg["runtime"]["intermediate"]
        cat, wh = inter["catalog"], inter["warehouse"]
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
                F.max(col(incr_col).cast("long")).alias("wm"),
                F.max(col("load_date")).alias("ld"),
            ).collect()[0]
        else:
            agg = df.select(
                F.max(col(incr_col)).alias("wm"),
                F.max(col("load_date")).alias("ld"),
            ).collect()[0]
        return str(agg["wm"]), str(agg["ld"])
