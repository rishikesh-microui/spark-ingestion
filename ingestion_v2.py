# -*- coding: utf-8 -*-
"""
Generic Spark Ingestion Framework
- Full refresh + SCD1 (incremental) with Iceberg merge
- RAW (parquet) -> Intermediary (Iceberg) -> Optional final parquet mirror
- SingleStore state (events + progress) with preload + buffered writes
- JSON logs + per-partition _METRICS.json
"""
import hashlib
from argparse import Namespace
from pyspark.sql import SparkSession, Row, Window as W
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, to_date, to_timestamp, max as spark_max, desc as spark_desc
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType
from datetime import datetime, date, timedelta
from pathlib import PurePosixPath
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
import uuid
import json
import threading
import argparse
import sys
import time
import itertools
# -------------------------
# Global run identifiers
# -------------------------
RUN_ID = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")

RUN_COUNTER = 0
_RUN_LOCK = threading.Lock()

def _next_event_seq():
    global RUN_COUNTER
    with _RUN_LOCK:
        RUN_COUNTER += 1
        return RUN_COUNTER
# =========================
# Minimal JSON-line logger
# =========================
class PrintLogger:
    _lock = threading.Lock()
    def __init__(self, job_name, file_path=None, level="INFO"):
        self.job = job_name
        self.file_path = file_path
        self.level = level
    def _write_line(self, line: str):
        with self._lock:
            print(line)
            if self.file_path:
                with open(self.file_path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
    def log(self, level, msg, **kv):
        rec = {
            "ts": datetime.now().astimezone().isoformat(timespec="milliseconds"),
            "level": level,
            "job": self.job,
            **kv,
            "msg": msg,
            "run_id": RUN_ID
        }
        self._write_line(json.dumps(
            rec, separators=(",", ":"), ensure_ascii=False))
    def debug(self, msg, **kv): self.log("DEBUG", msg, **kv)
    def info(self,  msg, **kv): self.log("INFO",  msg, **kv)
    def warn(self,  msg, **kv): self.log("WARN",  msg, **kv)
    def error(self, msg, **kv): self.log("ERROR", msg, **kv)

class Notifier:
    def __init__(self, spark, logger, cfg, interval_sec=300):
        self.spark = spark
        self.logger = logger
        self.interval = max(30, int(interval_sec))
        self._stop = threading.Event()
        self._thread = None
        self._buf = []  # (level, payload)
        self._lock = threading.Lock()
        self.sink = cfg["runtime"].get("notify", {}).get("sink", "file")
        self.path = cfg["runtime"].get("notify", {}).get(
            "path", "hdfs:///tmp/ingest-notify")
        # webhook placeholders
        self.webhook = cfg["runtime"].get("notify", {}).get("webhook")
    def emit(self, level, payload):
        with self._lock:
            self._buf.append((level, payload))
    def _flush(self):
        with self._lock:
            items = list(self._buf)
            self._buf.clear()
        if not items:
            return
        # file sink: append a window batch
        ts = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
        content = "\n".join(json.dumps({"level": lvl, **pl})
                            for (lvl, pl) in items)
        try:
            HDFSUtil.write_text(
                self.spark, f"{self.path}/notify_{RUN_ID}_{ts}.jsonl", content)
        except Exception as e:
            self.logger.error("notify_flush_failed", err=str(e))
        # webhook sink (placeholder; implement curl if allowed)
        if self.webhook:
            # You can integrate a small JVM HTTP client via Py4J or use a cluster-side hook.
            pass
    def _run(self):
        while not self._stop.wait(self.interval):
            self._flush()
    def start(self):
        self._thread = threading.Thread(
            target=self._run, name="notifier", daemon=True)
        self._thread.start()
    def stop(self):
        self._stop.set()
        self._flush()
        if self._thread:
            self._thread.join(timeout=5)

class Heartbeat:
    def __init__(self, logger, interval_sec=120, time_sink=None):
        self.logger = logger
        self.interval = max(5, int(interval_sec))
        self._stop = threading.Event()
        self._thread = None
        self.time_sink = time_sink
        # live counters (updated by main)
        self.total = 0
        self.done = 0
        self.failed = 0
        self.inflight = 0
        self.active_pools = set()
    def update(self, *, total=None, done=None, failed=None, inflight=None, active_pool=None):
        if total is not None:
            self.total = total
        if done is not None:
            self.done = done
        if failed is not None:
            self.failed = failed
        if inflight is not None:
            self.inflight = inflight
        if active_pool is not None:
            if isinstance(active_pool, str):
                self.active_pools.add(active_pool)
            elif isinstance(active_pool, (list, set, tuple)):
                self.active_pools |= set(active_pool)
    def _run(self):
        while not self._stop.wait(self.interval):
            snap = Utils.sample_spark(self.logger.spark) if hasattr(
                self.logger, "spark") else {}
            self.logger.info("heartbeat",
                             total=self.total, done=self.done, failed=self.failed, inflight=self.inflight,
                             active_pools=sorted(self.active_pools), **snap)
            self.time_sink.maybe_flush_by_age()

    def start(self):
        self._thread = threading.Thread(
            target=self._run, name="heartbeat", daemon=True)
        self._thread.start()
    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)


class SourceProbe:
    @staticmethod
    def _count_query(dialect, base_from_sql, incr_col, wm_lit, wm_hi_lit=None):
        # base_from_sql is either "schema.table" or "(SELECT ... ) t"
        if dialect == "oracle":
            pred = f"{incr_col} > TO_TIMESTAMP('{wm_lit}', 'YYYY-MM-DD HH24:MI:SS')"
            if wm_hi_lit:
                pred += f" AND {incr_col} <= TO_TIMESTAMP('{wm_hi_lit}', 'YYYY-MM-DD HH24:MI:SS')"
            return f"(SELECT COUNT(1) AS CNT FROM {base_from_sql} WHERE {pred}) c"
        elif dialect == "mssql":
            lo = wm_lit.replace(" ", "T")
            pred = f"[{incr_col}] > CONVERT(DATETIME2, '{lo}', 126)"
            if wm_hi_lit:
                hi = wm_hi_lit.replace(" ", "T")
                pred += f" AND [{incr_col}] <= CONVERT(DATETIME2, '{hi}', 126)"
            return f"(SELECT COUNT_BIG(1) AS CNT FROM {base_from_sql} WHERE {pred}) c"
        else:
            pred = f"{incr_col} > '{wm_lit}'"
            if wm_hi_lit:
                pred += f" AND {incr_col} <= '{wm_hi_lit}'"
            return f"(SELECT COUNT(1) AS CNT FROM {base_from_sql} WHERE {pred}) c"

    @staticmethod
    def estimate_count(spark, jdbc, base_from_sql, incr_col, wm_lit, wm_hi_lit=None):
        q = SourceProbe._count_query(jdbc.get("dialect", "oracle").lower(),
                                     base_from_sql, incr_col, wm_lit, wm_hi_lit)
        df = (spark.read.format("jdbc")
              .option("url", jdbc["url"])
              .option("dbtable", q)
              .option("user", jdbc["user"])
              .option("password", jdbc["password"])
              .option("driver", jdbc["driver"])
              .option("fetchsize", 1000)
              .load())
        return int(df.collect()[0][0])

class AdaptiveSlicePlanner:
    """
    Returns a list of slices [(lo, hi), ...] where lo<hi (both literals as strings).
    For int/epoch columns, pass ints (stringified). For timestamps, pass 'YYYY-MM-DD HH:MM:SS'.
    """
    @staticmethod
    def _time_lits(wm_str, now_str, parts):
        # even time buckets
        from datetime import datetime, timedelta
        fmt = "%Y-%m-%d %H:%M:%S"
        t0 = datetime.strptime(wm_str[:19], fmt)
        t1 = datetime.strptime(now_str[:19], fmt)
        step = (t1 - t0) / parts
        bounds = [t0 + i*step for i in range(parts)] + [t1]
        lits = [b.strftime(fmt) for b in bounds]
        return [(lits[i], lits[i+1]) for i in range(parts)]

    @staticmethod
    def _int_lits(lo_int, hi_int, parts):
        step = max(1, (hi_int - lo_int) // parts)
        bounds = [lo_int + i*step for i in range(parts)] + [hi_int]
        return [(str(bounds[i]), str(bounds[i+1])) for i in range(parts)]

    @staticmethod
    def plan(spark, logger, jdbc, tbl_cfg, base_from_sql, incr_col,
             last_wm, now_lit, is_int_epoch,
             slicing_cfg):
        # thresholds
        max_dur_h = int(slicing_cfg.get("max_duration_hours", 168))
        max_count = int(slicing_cfg.get("max_count", 10_000_000))
        target = int(slicing_cfg.get("target_rows_per_slice", 1_000_000))
        max_parts = int(slicing_cfg.get("max_partitions", 24))

        # 1) Duration check (only for timestamps)
        duration_ok = True
        if not is_int_epoch:
            fmt = "%Y-%m-%d %H:%M:%S"
            from datetime import datetime
            t0 = datetime.strptime(last_wm[:19], fmt)
            t1 = datetime.strptime(now_lit[:19], fmt)
            duration_ok = ((t1 - t0).total_seconds()/3600.0) <= max_dur_h

        # 2) Count estimate over whole window
        total = SourceProbe.estimate_count(
            spark, jdbc, base_from_sql, incr_col, last_wm, now_lit)
        logger.info("slice_probe", total=total, duration_ok=duration_ok)

        # If small enough — single slice
        if duration_ok and total <= max_count:
            return [(last_wm, now_lit)]

        # 3) Compute partitions by target size
        parts = min(max_parts, max(2, (total + target - 1)//target))

        # 4) Build slices: time buckets or integer buckets
        if is_int_epoch:
            lo = int(last_wm)
            hi = int(now_lit)
            return AdaptiveSlicePlanner._int_lits(lo, hi, parts)
        else:
            return AdaptiveSlicePlanner._time_lits(last_wm, now_lit, parts)


class Staging:
    @staticmethod
    def root(cfg): return cfg["runtime"]["staging"]["root"]

    @staticmethod
    def slice_dir(cfg, schema, table, incr_col, lo, hi):
        key = f"{schema}.{table}|{incr_col}|{lo}|{hi}"
        hid = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
        return f"{Staging.root(cfg)}/{schema}/{table}/inc={incr_col}/slices/{hid}"

    @staticmethod
    def exists(spark, path):
        jsc = spark._jsc
        jvm = spark.sparkContext._jvm
        conf = jsc.hadoopConfiguration()
        p = jvm.org.apache.hadoop.fs.Path(path)
        return p.getFileSystem(conf).exists(p)

    @staticmethod
    def write_text(spark, path, content):
        HDFSUtil.write_text(spark, path, content)

    @staticmethod
    def mark_success(spark, dirpath):
        Staging.write_text(spark, f"{dirpath}/_SUCCESS", "{}")

    @staticmethod
    def mark_landed(spark, dirpath):
        Staging.write_text(spark, f"{dirpath}/_LANDED", "{}")

    @staticmethod
    def is_success(spark, dirpath):
        return Staging.exists(spark, f"{dirpath}/_SUCCESS")

    @staticmethod
    def is_landed(spark, dirpath):
        return Staging.exists(spark, f"{dirpath}/_LANDED")

    @staticmethod
    def ttl_cleanup(spark, cfg, logger, now_epoch_ms=None):
        """Remove slice dirs older than ttl that have both _SUCCESS and _LANDED."""
        # Simple recursive scanner to delete eligible dirs.
        try:
            jvm = spark.sparkContext._jvm
            jsc = spark._jsc
            conf = jsc.hadoopConfiguration()
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
            Path = jvm.org.apache.hadoop.fs.Path
            root = Path(Staging.root(cfg))
            if not fs.exists(root):
                return
            ttl_ms = int(cfg["runtime"]["staging"].get(
                "ttl_hours", 72)) * 3600 * 1000
            now_ms = now_epoch_ms or int(time.time() * 1000)

            def list_dirs(p):
                for st in fs.listStatus(p):
                    if st.isDirectory():
                        yield st.getPath()
                        for sub in list_dirs(st.getPath()):
                            yield sub

            for d in list_dirs(root):
                dstr = d.toString()
                if dstr.endswith("/slices") or "/slices/" not in dstr:
                    continue
                # only delete if success & landed and old enough
                if (Staging.exists(spark, f"{dstr}/_SUCCESS") and
                        Staging.exists(spark, f"{dstr}/_LANDED")):
                    age = now_ms - fs.getFileStatus(d).getModificationTime()
                    if age > ttl_ms:
                        logger.info("staging_ttl_delete", path=dstr)
                        fs.delete(d, True)
        except Exception as e:
            logger.warn("staging_ttl_cleanup_failed", err=str(e))

class TimeAwareBufferedSink:
    def __init__(self, base_state, logger, outbox=None,
                 flush_every=50, flush_age_seconds=120):
        self.base_state = base_state
        self.logger = logger
        self.outbox = outbox
        self.flush_every = flush_every
        self.flush_age_seconds = flush_age_seconds

        self._events_buf = []
        self._progress_buf = []
        self._lock = threading.Lock()
        self._last_events_flush_ts = time.time()
        self._last_progress_flush_ts = time.time()

    # ----- public API (keep your old names if you had them) -----
    # schema_name, table_name, load_date, ...
    def add_event(self, row):
        with self._lock:
            self._events_buf.append(row)
            if len(self._events_buf) >= self.flush_every:
                self._flush_events_nolock("size")

    # schema_name, table_name, watermark, last_loaded_date
    def add_progress(self, row):
        with self._lock:
            self._progress_buf.append(row)
            if len(self._progress_buf) >= max(1, self.flush_every // 5):
                self._flush_progress_nolock("size")

    def maybe_flush_by_age(self):
        now = time.time()
        with self._lock:
            if now - self._last_events_flush_ts >= self.flush_age_seconds:
                self._flush_events_nolock("age")
            if now - self._last_progress_flush_ts >= self.flush_age_seconds:
                self._flush_progress_nolock("age")

    def flush(self):
        with self._lock:
            self._flush_events_nolock("final")
            self._flush_progress_nolock("final")

    # ----- internals -----
    def _flush_events_nolock(self, reason):
        if not self._events_buf:
            return
        batch = list(self._events_buf)
        self._events_buf.clear()
        path = None
        try:
            if self.outbox:
                path = self.outbox.mirror_batch("events", batch)
            self.base_state._write_events(batch)
            if path:
                self.outbox.delete(path)
            self._last_events_flush_ts = time.time()
            self.logger.info("sink_flush_events_ok",
                             reason=reason, rows=len(batch))
        except Exception as e:
            # put back for retry
            self._events_buf[:0] = batch
            self.logger.error("sink_flush_events_failed",
                              reason=reason, err=str(e), rows=len(batch))

    def _flush_progress_nolock(self, reason):
        if not self._progress_buf:
            return
        batch = list(self._progress_buf)
        self._progress_buf.clear()
        path = None
        try:
            if self.outbox:
                path = self.outbox.mirror_batch("progress", batch)
            self.base_state._write_progress(batch)
            if path:
                self.outbox.delete(path)
            self._last_progress_flush_ts = time.time()
            self.logger.info("sink_flush_progress_ok",
                             reason=reason, rows=len(batch))
        except Exception as e:
            self._progress_buf[:0] = batch
            self.logger.error("sink_flush_progress_failed",
                              reason=reason, err=str(e), rows=len(batch))

class DateUtil:
    @staticmethod
    def build_time_windows(start_iso: str, end_iso: str, step_hours: int = 24):
        # Use UTC for safety; convert your known source timezone if needed
        dt = datetime.fromisoformat(start_iso.replace(
            "Z", "")).replace(tzinfo=timezone.utc)
        end = datetime.fromisoformat(end_iso.replace(
            "Z", "")).replace(tzinfo=timezone.utc)
        out = []
        step = timedelta(hours=step_hours)
        while dt < end:
            nxt = min(dt + step, end)
            # return both SQL literal forms
            out.append({
                "iso_left": dt.strftime("%Y-%m-%dT%H:%M:%S"),
                "iso_right": nxt.strftime("%Y-%m-%dT%H:%M:%S"),
                "ts_left": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "ts_right": nxt.strftime("%Y-%m-%d %H:%M:%S"),
            })
            dt = nxt
        return out

    @staticmethod
    def build_predicates(dialect: str, incr_col: str, windows):
        preds = []
        d = dialect.lower()
        for w in windows:
            if d == "mssql":
                preds.append(
                    f"[{incr_col}] >= '{w['iso_left']}' AND [{incr_col}] < '{w['iso_right']}'")
            elif d == "oracle":
                preds.append(
                    f"{incr_col} >= TIMESTAMP '{w['ts_left']}' AND {incr_col} < TIMESTAMP '{w['ts_right']}'")
            else:  # generic
                preds.append(
                    f"{incr_col} >= '{w['ts_left']}' AND {incr_col} < '{w['ts_right']}'")
        return preds

# =========================
# HDFS util: write small text file (e.g., _METRICS.json)
# =========================
class HDFSUtil:
    @staticmethod
    def write_text(spark, path, content):
        jsc = spark._jsc
        jvm = spark.sparkContext._jvm
        conf = jsc.hadoopConfiguration()
        p = jvm.org.apache.hadoop.fs.Path(path)
        fs = p.getFileSystem(conf)
        # overwrite
        if fs.exists(p):
            fs.delete(p, True)
        out = fs.create(p)
        try:
            out.write(bytearray(content.encode("utf-8")))
        finally:
            out.close()

    @staticmethod
    def append_text(spark, path, content):
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

# =========================
# State interfaces & impls
# =========================
class StateStore:
    def get_day_status(self, schema, table,
                       load_date): raise NotImplementedError
    def mark_event(self, schema, table, load_date, mode, phase, status,
                   rows_written=None, watermark=None, location=None, strategy=None, error=None): raise NotImplementedError
    def get_last_watermark(self, schema, table,
                           default_value): raise NotImplementedError
    def get_progress(self, schema, table, default_wm,
                     default_date): raise NotImplementedError
    def set_progress(self, schema, table, watermark,
                     last_loaded_date): raise NotImplementedError

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
            # tip: add run_id in your DDL to tie events to files (optional)
            StructField("event_at",     StringType(),
                        True),   # ISO-8601 with micros
            StructField("run_id",       StringType(), True),
            StructField("event_seq",    LongType(),   True),
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
            def _now_iso_micros():
                # ISO-8601 with microseconds, local tz
                return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            EventRow = Row("sourceId", "schema_name", "table_name", "load_date", "mode", "phase",
                           "status", "rows_written", "watermark", "location", "strategy", "error",
                           "event_at", "run_id", "event_seq")
            get = (r.asDict() if isinstance(r, Row) else r)
            return EventRow(self.sourceId,
                            get.get("schema_name"), get.get("table_name"),
                            self._coerce_date(get.get("load_date")),
                            get.get("mode"), get.get("phase"), get.get("status"),
                            get.get("rows_written", 0), get.get("watermark"),
                            get.get("location"), get.get("strategy"), get.get("error"),
                            _now_iso_micros(), RUN_ID, _next_event_seq())
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
            # uses 'ts' if present (created at) else last row order is undefined
            cols = df.columns
            if "ts" in cols:
                latest = (df.where(col("phase") == phase).orderBy(
                    spark_desc("ts")).limit(1).collect())
            else:
                latest = (df.where(col("phase") == phase).limit(1).collect())
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
            cols = df.columns
            if "updated_at" in cols:
                last = df.orderBy(spark_desc("updated_at")).select(
                    "watermark").limit(1).collect()
            else:
                last = df.orderBy(spark_desc("watermark")).select(
                    "watermark").limit(1).collect()
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
            cols = df.columns
            if "updated_at" in cols:
                last = df.orderBy(spark_desc("updated_at")).select(
                    "watermark", "last_loaded_date").limit(1).collect()
            else:
                last = (df.groupBy().agg(spark_max("watermark").alias("watermark"),
                                         F.first("last_loaded_date", ignorenulls=True).alias("last_loaded_date")).collect())
            if last:
                wm = last[0]["watermark"] or default_wm
                ld = last[0]["last_loaded_date"] or default_date
                return wm, ld
        except Exception as e:
            print("[WARN] get_progress:", e)
        return default_wm, default_date
    def set_progress(self, schema, table, watermark, last_loaded_date):
        self._write_progress([{
            "schema_name": schema, "table_name": table,
            "watermark": watermark, "last_loaded_date": last_loaded_date
        }])

# =========================
# Buffered State (preload + batch writes)
# =========================
class BufferedState(StateStore):
    def __init__(self, base_state, source_id, flush_every=50,
                 outbox=None, time_sink=None, logger=None):
        self.base = base_state
        self.source_id = source_id
        self.flush_every = flush_every
        self._events_buf = []
        self._progress_buf = []
        self._lock = threading.Lock()
        self._progress_cache = {}
        self._day_status_cache = {}
        # NEW:
        # thread-safe-ish (atomic increment in CPython)
        self._event_seq_ctr = itertools.count(1)
        self.outbox = outbox                         # HDFSOutbox or None
        self.time_sink = time_sink                   # TimeAwareBufferedSink or None
        self.logger = logger
    def preload(self, spark, tables, load_date_str):
        keys = {(t["schema"], t["table"]) for t in tables}
        schemas = sorted({s for s, _ in keys})
        tbls = sorted({t for _, t in keys})
        # -------- progress (watermark + last_loaded_date) --------
        wm_df = (self.base._read_wm_df()
                .where(col("sourceId") == lit(self.source_id))
                .where(col("schema_name").isin(schemas))
                .where(col("table_name").isin(tbls)))
        if not wm_df.rdd.isEmpty():
            cols = wm_df.columns
            if "updated_at" in cols:
                from pyspark.sql.window import Window as W
                w = W.partitionBy("schema_name", "table_name").orderBy(
                    spark_desc("updated_at"))
                latest = (wm_df.withColumn("_rn", F.row_number().over(w))
                            .where(col("_rn") == 1)
                            .select("schema_name", "table_name", "watermark", "last_loaded_date"))
            else:
                # Lexicographic max on watermark still OK if it's ISO/timestamp-like;
                # choose best-effort last_loaded_date with first(…, ignorenulls=True).
                latest = (wm_df.groupBy("schema_name", "table_name")
                        .agg(spark_max("watermark").alias("watermark"),
                    F.first("last_loaded_date", ignorenulls=True).alias("last_loaded_date")))
            for r in latest.collect():
                self._progress_cache[(r["schema_name"], r["table_name"])] = (
                    r["watermark"], r["last_loaded_date"]
                )
        # -------- day-status (raw / intermediate / finalize) for this load_date --------
        ev_df = (self.base._read_events_df()
                .where(col("sourceId") == lit(self.source_id))
                .where(col("schema_name").isin(schemas))
                .where(col("table_name").isin(tbls))
                .where(col("load_date") == to_date(lit(load_date_str)))
                .where(~col("status").isin("skipped")))
        if not ev_df.rdd.isEmpty():
            from pyspark.sql.window import Window as W
            # Build a deterministic ordering key:
            # 1) prefer 'ts' if present
            # 2) else prefer 'updated_at'
            # 3) else use a constant '1970-01-01' (so rank falls back to status_rank only)
            cols = ev_df.columns
            if "ts" in cols:
                ord_col = col("ts")
            elif "updated_at" in cols:
                ord_col = col("updated_at")
            else:
                ord_col = F.to_timestamp(F.lit("1970-01-01 00:00:00"))
            # Rank statuses for tie-break: success > failed > started > others
            status_rank = (
                F.when(col("status") == F.lit("success"), F.lit(3))
                .when(col("status") == F.lit("failed"),  F.lit(2))
                .when(col("status") == F.lit("skipped"), F.lit(1))
                .otherwise(F.lit(0))
                .alias("_status_rank")
            )
            ranked = (ev_df
                    .withColumn("_status_rank", status_rank)
                    .withColumn("_ord_ts", ord_col)
                    .withColumn(
                        "_rn",
                        F.row_number().over(
                            W.partitionBy("schema_name", "table_name", "phase")
                            .orderBy(spark_desc("_ord_ts"), spark_desc("_status_rank"))
                        )
                    )
                    .where(col("_rn") == 1)
                    .select("schema_name", "table_name", "phase", "status"))
            # Pivot into columns raw/intermediate/finalize
            phases = ["raw", "intermediate", "finalize"]
            pvt = (ranked.groupBy("schema_name", "table_name")
                        .pivot("phase", phases)
                        .agg(F.first("status", ignorenulls=True)))
            # Fill cache
            for r in pvt.collect():
                self._day_status_cache[(r["schema_name"], r["table_name"], load_date_str)] = {
                    "raw_done":          (r["raw"] == "success") if "raw" in pvt.columns else False,
                    "intermediate_done": (r["intermediate"] == "success") if "intermediate" in pvt.columns else False,
                    "finalized_done":    (r["finalize"] == "success") if "finalize" in pvt.columns else False,
                }
    # reads
    def get_day_status(self, schema, table, load_date):
        k = (schema, table, load_date if isinstance(
            load_date, str) else load_date.strftime("%Y-%m-%d"))
        if k in self._day_status_cache:
            return self._day_status_cache[k]
        return self.base.get_day_status(schema, table, load_date)
    def get_last_watermark(self, schema, table, default_value):
        k = (schema, table)
        if k in self._progress_cache:
            wm, _ = self._progress_cache[k]
            return wm or default_value
        return self.base.get_last_watermark(schema, table, default_value)
    def get_progress(self, schema, table, default_wm, default_date):
        k = (schema, table)
        if k in self._progress_cache:
            wm, ld = self._progress_cache[k]
            return (wm or default_wm, ld or default_date)
        wm, ld = self.base.get_progress(
            schema, table, default_wm, default_date)
        with self._lock:
            self._progress_cache[k] = (wm, ld)
        return wm, ld
    # buffered writes

    def mark_event(self, schema, table, load_date, mode, phase, status,
                   rows_written=None, watermark=None, location=None, strategy=None, error=None):
        # Build an enriched event
        evt = {
            "schema_name": schema,
            "table_name": table,
            "load_date": load_date,
            "mode": mode,
            "phase": phase,
            "status": status,
            "rows_written": rows_written,
            "watermark": watermark,
            "location": location,
            "strategy": strategy,
            "error": error,
            # enriched metadata (may not exist in SS yet; WAL keeps them)
            "run_id": RUN_ID,
            "event_seq": next(self._event_seq_ctr),
            "event_at": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S"),
            "sourceId": self.source_id,
        }

        # 1) WAL append (best-effort; never block ingestion)
        if self.outbox is not None:
            try:
                self.outbox.append_event(self.source_id, RUN_ID, evt)
            except Exception as e:
                if self.logger:
                    self.logger.warn("wal_append_failed", err=str(
                        e), table=f"{schema}.{table}")

        # 2) Enqueue into time-aware sink (preferred path)
        if self.time_sink is not None:
            # the sink will mirror+delegate on flush
            self.time_sink.add_event(evt)
            return

        # 3) Fallback: buffer & size-based flush directly via base_state
        with self._lock:
            self._events_buf.append(evt)
            if len(self._events_buf) >= self.flush_every:
                self._flush_events_nolock()

    def _flush_events_nolock(self):
        if not self._events_buf:
            return
        batch = list(self._events_buf)
        self._events_buf.clear()
        # Strip fields unknown to SingleStore schema (until DDL is updated)
        allowed = set([f.name for f in self.base._events_schema.fields])
        batch_ss = [{k: v for k, v in r.items() if k in allowed}
                    for r in batch]
        try:
            self.base._write_events(batch_ss)
        except Exception as e:
            # put back for retry
            self._events_buf[:0] = batch
            if self.logger:
                self.logger.error("events_flush_failed",
                                  err=str(e), rows=len(batch))

    def set_progress(self, schema, table, watermark, last_loaded_date):
        row = {
            "schema_name": schema,
            "table_name": table,
            "watermark": watermark,
            "last_loaded_date": last_loaded_date,
            "run_id": RUN_ID,
            "sourceId": self.source_id,
            "event_at": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S"),
        }
        # WAL best-effort
        if self.outbox is not None:
            try:
                self.outbox.append_progress(self.source_id, RUN_ID, row)
            except Exception as e:
                if self.logger:
                    self.logger.warn("wal_append_progress_failed", err=str(
                        e), table=f"{schema}.{table}")

        # cache
        with self._lock:
            self._progress_cache[(schema, table)] = (
                watermark, last_loaded_date)

        # sink preferred
        if self.time_sink is not None:
            self.time_sink.add_progress(row)
            return

        # fallback buffered
        with self._lock:
            self._progress_buf.append(row)
            if len(self._progress_buf) >= max(1, self.flush_every // 5):
                self._flush_progress_nolock()

    def flush(self):
        # If a sink exists, just ask it to flush. Otherwise do local flush.
        if self.time_sink is not None:
            self.time_sink.flush()
            return
        with self._lock:
            self._flush_events_nolock()
            self._flush_progress_nolock()

    def _flush_progress_nolock(self):
        if not self._progress_buf:
            return
        batch = list(self._progress_buf)
        self._progress_buf.clear()
        # Strip extras if SS schema hasn’t been altered yet
        allowed = set([f.name for f in self.base._wm_schema.fields])
        batch_ss = [{k: v for k, v in r.items() if k in allowed}
                    for r in batch]
        try:
            self.base._write_progress(batch_ss)
        except Exception as e:
            self._progress_buf[:0] = batch
            if self.logger:
                self.logger.error("progress_flush_failed",
                                  err=str(e), rows=len(batch))
    
# =========================
# Helpers
# =========================
class Paths:
    @staticmethod
    def build(rt, schema, table, load_date):
        append_schema = rt.get("append_table_schema", False)
        raw_dir = f"{rt['raw_root']}/{schema}/{table}/load_date={load_date}" if append_schema else f"{rt['raw_root']}/{table}/load_date={load_date}"
        final_dir = f"{rt['final_root']}/{schema}/{table}" if append_schema else f"{rt['final_root']}/{table}"
        base_raw = f"{rt['raw_root']}/{schema}/{table}" if append_schema else f"{rt['raw_root']}/{table}"
        return raw_dir, final_dir, base_raw

class Utils:
    @staticmethod
    def minus_seconds_datetime(wm_str, seconds):
        if not seconds:
            return wm_str
        dt = datetime.strptime(wm_str[:19], "%Y-%m-%d %H:%M:%S")
        return (dt - timedelta(seconds=int(seconds))).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def minus_seconds_epoch(wm_str, seconds, millis=False):
        if not seconds:
            return wm_str
        base = int(float(wm_str))
        adj = base - (int(seconds) * (1000 if millis else 1))
        return str(max(adj, 0))

    @staticmethod
    def minus_seconds(wm_str, seconds):
        if not seconds:
            return wm_str
        dt = datetime.strptime(wm_str[:19], "%Y-%m-%d %H:%M:%S")
        dt2 = dt - timedelta(seconds=int(seconds))
        return dt2.strftime("%Y-%m-%d %H:%M:%S")
    def mssql_literal_from_wm(wm_str):
        # ISO 8601 without TZ
        return wm_str[:19].replace(" ", "T")

    @staticmethod
    def schema_json(schema):
        # generate a Spark JSON schema string from StructType
        return json.loads(schema.json())
    
    @staticmethod
    def sample_spark(spark):
        sc = spark.sparkContext
        try:
            stage_ids = [s.stageId()
                        for s in sc.statusTracker().getActiveStageIds()]
        except:
            stage_ids = []
        return {"active_stages": stage_ids, "default_parallelism": sc.defaultParallelism}


def with_ingest_cols(df):
    return (df.withColumn("load_timestamp", F.current_timestamp())
              .withColumn("run_id", F.lit(RUN_ID)))

class JDBC:
    @staticmethod
    def build_from_sql(jdbc, schema, table, tbl_cfg):
        # Returns either "schema.table" or "(SELECT ... ) t"
        if "query_sql" in tbl_cfg and tbl_cfg["query_sql"].strip():
            return f"({tbl_cfg['query_sql']}) q"
        # MSSQL safe quoting
        if jdbc.get("dialect", "oracle").lower() == "mssql":
            return f"[{schema}].[{table}]"
        return f"{schema}.{table}"

    @staticmethod
    def dbtable_for_range(jdbc, base_from_sql, incr_col, tbl_cfg, lo_lit, hi_lit=None):
        dialect = jdbc.get("dialect", "oracle").lower()
        incr_type = (tbl_cfg.get("incr_col_type") or "").lower()
        cols = tbl_cfg.get("cols", "*")
        if isinstance(cols, list):
            cols = ", ".join(cols)
        if dialect == "oracle":
            pred = f"{incr_col} > TO_TIMESTAMP('{lo_lit}','YYYY-MM-DD HH24:MI:SS')"
            if hi_lit:
                pred += f" AND {incr_col} <= TO_TIMESTAMP('{hi_lit}','YYYY-MM-DD HH24:MI:SS')"
        elif dialect == "mssql":
            if incr_type in ("epoch_seconds", "epoch_millis"):
                # NUMERIC comparison; last_wm is stored as a string -> cast to int
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
            pred = f"{incr_col} > '{lo_lit}'" + \
                (f" AND {incr_col} <= '{hi_lit}'" if hi_lit else "")
        return f"(SELECT {cols} FROM {base_from_sql} WHERE {pred}) t"

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
    @staticmethod
    def reader_increment(spark, jdbc, schema, table, tbl_cfg, incr_col, last_wm):
        """
        Oracle-optimized pushdown. Fallback to generic if dialect not provided.
        cfg["jdbc"].get("dialect") in {"oracle","generic"}
        """
        dbtable = JDBC.dbtable_for_range(jdbc, schema, table, incr_col, tbl_cfg, last_wm, None)
        r = (spark.read.format("jdbc")
             .option("url", jdbc["url"])
             .option("dbtable", dbtable)
             .option("user", jdbc["user"])
             .option("password", jdbc["password"])
             .option("driver", jdbc["driver"])
             .option("trustServerCertificate", "true")
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
    def land_append(spark, logger, df, rt, raw_dir, state, schema, table, load_date, mode):
        # df should be persisted by caller if reused
        rows = df.count()
        state.mark_event(schema, table, load_date, mode, "raw",
                         "started", location=raw_dir, rows_written=rows)
        (df.write.format(rt.get("write_format", "parquet"))
           .mode("append")
           .option("compression", rt.get("compression", "snappy"))
           .save(raw_dir))
        state.mark_event(schema, table, load_date, mode, "raw",
                         "success", rows_written=rows, location=raw_dir)
        # Write small metrics file
        metrics = {"schema": schema, "table": table, "run_id": RUN_ID, "rows_written": rows, "status": "success",
                   "phase": "raw", "load_date": load_date, "ts": datetime.now().astimezone().isoformat()}
        try:
            HDFSUtil.write_text(
                spark=spark, path=f"{raw_dir}/_METRICS.json", content=json.dumps(metrics))
        except Exception as e:
            logger.error("error writing metrics", err=str(e))
    @staticmethod
    def raw_increment_df(spark, rt, schema, table, last_ld, last_wm, incr_col, incr_type = ""):
        _, __, base_raw = Paths.build(rt, schema, table, '')
        all_raw = (spark.read.format(
            rt.get("write_format", "parquet")).load(base_raw))
        
        if incr_col.lower() not in [col.lower() for col in all_raw.columns]:
            # schema drift—skip merge to avoid corrupt progress
            return all_raw.limit(0)

        df = all_raw.where(col("load_date") >= lit(last_ld))
        incr_type = (incr_type or "").lower()
        if incr_type.startswith("epoch") or incr_type in ("bigint", "int", "numeric", "decimal"):
            # ensure numeric comparison (avoid string ordering)
            df = df.where(col(incr_col).cast("long") > F.lit(int(str(last_wm))))
        else:
            df = df.where(to_timestamp(col(incr_col)) > to_timestamp(lit(last_wm)))
        return df


class HDFSOutbox:
    """
    Durable mirror for state writes:
    - write batch to HDFS as JSONL under outbox_root/{events|progress}/
    - after successful SingleStore write, delete the file
    """

    def __init__(self, spark, root):
        self.spark = spark
        self.root = root.rstrip("/")
        self.jsc = spark._jsc
        self.jvm = spark.sparkContext._jvm
        self.conf = self.jsc.hadoopConfiguration()
        self.Path = self.jvm.org.apache.hadoop.fs.Path
        self.fs = self.jvm.org.apache.hadoop.fs.FileSystem.get(self.conf)

        # ensure dirs exist
        for sub in ("events", "progress"):
            p = self.Path(f"{self.root}/{sub}")
            if not self.fs.exists(p):
                self.fs.mkdirs(p)

    def _atomic_write(self, path, content_bytes):
        p_tmp = self.Path(path + ".__tmp__")
        p = self.Path(path)
        if self.fs.exists(p_tmp):
            self.fs.delete(p_tmp, True)
        out = self.fs.create(p_tmp, True)
        try:
            out.write(content_bytes)
        finally:
            out.close()
        # atomic rename
        if self.fs.exists(p):
            self.fs.delete(p, True)
        if not self.fs.rename(p_tmp, p):
            raise RuntimeError(f"Atomic rename failed for {path}")

    def _delete(self, path):
        p = self.Path(path)
        if self.fs.exists(p):
            self.fs.delete(p, False)

    def mirror_batch(self, kind, records):
        """
        kind: 'events' | 'progress'
        records: list[dict]
        Returns the file path written. Caller must delete on success.
        """
        ts = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S.%f")
        name = f"{kind}_{RUN_ID}_{ts}_{uuid.uuid4().hex}.jsonl"
        path = f"{self.root}/{kind}/{name}"
        payload = "\n".join(json.dumps(r, separators=(",", ":"))
                            for r in records)
        self._atomic_write(path, payload.encode("utf-8"))
        return path

    def list_pending(self, kind):
        """Return list of jsonl file paths for a kind."""
        base = f"{self.root}/{kind}"
        it = self.fs.listStatus(self.Path(base))
        out = []
        for s in it:
            p = s.getPath().toString()
            if p.endswith(".jsonl"):
                out.append(p)
        return sorted(out)

    def read_jsonl(self, path):
        """Return list of dicts from a JSONL file path."""
        # use Spark to read small JSONL reliably in cluster
        df = self.spark.read.json(path)
        return [row.asDict(recursive=True) for row in df.collect()]

    def delete(self, path):
        self._delete(path)

    def restore_all(self, base_state, logger):
        """Replay any pending outbox files into SingleStore, then delete them."""
        for kind in ("events", "progress"):
            for path in self.list_pending(kind):
                try:
                    rows = self.read_jsonl(path)
                    if kind == "events":
                        base_state._write_events(rows)
                    else:
                        base_state._write_progress(rows)
                    self.delete(path)
                    logger.info("outbox_replayed", kind=kind,
                                file=path, rows=len(rows))
                except Exception as e:
                    logger.error("outbox_replay_failed",
                                 kind=kind, file=path, err=str(e))
                    # keep the file for next attempt


class HiveHelper:
    # ---------- small utilities ----------
    @staticmethod
    def table_exists(spark, db, table_name):
        # Works with Spark 3.x
        return spark._jsparkSession.catalog().tableExists(db, table_name)
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
    def _spark_type_to_hive(dt):
        # Handle decimals & structs/arrays simply (you can extend if needed)
        s = dt.simpleString()  # e.g. decimal(10,2), array<string>, struct<...>
        if s.startswith("decimal("):
            return "DECIMAL" + s[len("decimal"):]  # keep precision/scale
        if s.startswith("array<") or s.startswith("map<") or s.startswith("struct<"):
            # Conservative fallback – for complex types, STRING external tables are common
            return "STRING"
        # Primitive types via map
        return HiveHelper._HIVE_TYPE_MAP.get(dt.__class__.__name__, "STRING")
    @staticmethod
    def _cols_ddl_from_df_schema(df_schema, partition_col="load_date"):
        cols = []
        for f in df_schema.fields:
            if f.name == partition_col:
                continue
            cols.append(
                f"`{f.name}` {HiveHelper._spark_type_to_hive(f.dataType)}")
        # Fallback for empty (shouldn’t happen for real data)
        return ", ".join(cols) if cols else "`_dummy` STRING"
    # ---------- existing helpers (kept) ----------
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
    # ---------- new “create-once with real schema” helpers ----------
    @staticmethod
    def create_partitioned_parquet_if_absent_with_schema(
        spark, db, schema, table, base_location, sample_partition_path, partition_col="load_date"
    ):
        """
        Create `{db}.{schema}__{table}` ONCE with the actual (inferred) schema from a real
        partition file (sample_partition_path). Subsequent runs won’t recreate it.
        """
        tbl = f"{schema}__{table}"
        if HiveHelper.table_exists(spark, db, tbl):
            return  # already created correctly
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        # Infer columns from one real partition that you just wrote
        sample_df = spark.read.parquet(sample_partition_path).limit(1)
        cols_ddl = HiveHelper._cols_ddl_from_df_schema(sample_df.schema, partition_col=partition_col)
        spark.sql(f"""
          CREATE EXTERNAL TABLE `{db}`.`{tbl}` (
            {cols_ddl}
          )
          PARTITIONED BY (`{partition_col}` STRING)
          STORED AS PARQUET
          LOCATION '{base_location}'
        """)
    @staticmethod
    def create_unpartitioned_parquet_if_absent_with_schema(
        spark, db, schema, table, final_location
    ):
        """
        For FULL refresh outputs if/when you decide to register them (unpartitioned path).
        Creates once using the actual schema from files in final_location.
        """
        tbl = f"{schema}__{table}"
        if HiveHelper.table_exists(spark, db, tbl):
            return
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        df = spark.read.parquet(final_location).limit(1)
        cols_ddl = HiveHelper._cols_ddl_from_df_schema(
            df.schema, partition_col="load_date")  # ignore if not present
        spark.sql(f"""
          CREATE EXTERNAL TABLE `{db}`.`{tbl}` (
            {cols_ddl}
          )
          STORED AS PARQUET
          LOCATION '{final_location}'
        """)

class IcebergHelper:
    @staticmethod
    def setup_catalog(spark, cfg):
        inter = cfg["runtime"]["intermediate"]
        cat, wh = inter["catalog"], inter["warehouse"]
        spark.conf.set(
            f"spark.sql.catalog.{cat}", "org.apache.iceberg.spark.SparkCatalog")
        spark.conf.set(f"spark.sql.catalog.{cat}.type", "hadoop")
        spark.conf.set(f"spark.sql.catalog.{cat}.warehouse", wh)
        # spark.conf.set("spark.sql.catalog.spark_catalog",
        #                "org.apache.spark.sql.internal.CatalogImpl")
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
        w = W.partitionBy(*[F.col(c)
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
            df_src = df_src.withColumn(
                partition_col, F.to_date(F.lit(load_date), "yyyy-MM-dd"))
        # Dedup latest winner
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

            # ONE-TIME creation with real schema from the partition you just wrote
            HiveHelper.create_partitioned_parquet_if_absent_with_schema(
                spark, hive_db, schema, table,
                base_location=base,
                sample_partition_path=part_path,
                partition_col=part_col
            )

            # Then only add the partition (or MSCK if you prefer)
            HiveHelper.add_partition_or_msck(
                spark, hive_db, hive_tbl, part_col, load_date, base, use_msck=hive_cfg.get("use_msck", False)
            )
        return {"final_parquet_path": part_path}

    @staticmethod 
    def _compute_wm_ld(df, incr_col, is_int_epoch = False):
        if is_int_epoch:
            agg2 = df.select(F.max(col(incr_col).cast("long")).alias("wm"),
                                    F.max(col("load_date")).alias("ld")).collect()[0]
        else:
            agg2 = df.select(F.max(col(incr_col)).alias("wm"),
                                    F.max(col("load_date")).alias("ld")).collect()[0]
        return str(agg2["wm"]), str(agg2["ld"])
# =========================
# Ingestion strategies
# =========================
class IngestionFull:
    @staticmethod
    def run(spark, cfg, state, logger, schema, table, load_date, raw_dir, final_dir, tbl_cfg):
        rt, jdbc = cfg["runtime"], cfg["jdbc"]
        status = state.get_day_status(schema, table, load_date)
        logger.info("full_status", schema=schema, table=table,
                    **status, load_date=load_date)
        if status.get("finalized_done"):
            state.mark_event(schema, table, load_date, "full",
                             "raw", "skipped", location=raw_dir)
            state.mark_event(schema, table, load_date, "full",
                             "finalize", "skipped", location=final_dir)
            logger.info("full_skip_finalized", schema=schema,
                        table=table, load_date=load_date)
            if rt.get("hive", {}).get("enabled", False):
                hive_db = rt["hive"]["db"]
                HiveHelper.create_unpartitioned_parquet_if_absent_with_schema(
                    spark, hive_db, schema, table, final_dir
                )
            return {"table": f"{schema}.{table}", "skipped": True, "reason": "already finalized today"}
        reader = JDBC.reader(spark, jdbc, schema, table, tbl_cfg)
        if status.get("raw_done") and not status.get("finalized_done"):
            result = {"table": f"{schema}.{table}",
                      "mode": "full", "raw": raw_dir}
            logger.info("full_finalize_only", schema=schema,
                        table=table, raw_dir=raw_dir)
        else:
            df = reader.load()
            df = with_ingest_cols(df) #.persist()
            
            state.mark_event(schema, table, load_date, "full",
                             "raw", "started", location=raw_dir)
            (df.write.format(rt.get("write_format", "parquet"))
               .mode("overwrite").option("compression", rt.get("compression", "snappy"))
               .save(raw_dir))
            #df.unpersist()
            rows = spark.read.format(rt.get("write_format", "parquet")).load(raw_dir).count()
            state.mark_event(schema, table, load_date, "full", "raw",
                             "success", rows_written=rows, location=raw_dir)
            logger.info("full_raw_written", schema=schema,
                        table=table, rows=rows, raw_dir=raw_dir)
            result = {"table": f"{schema}.{table}",
                      "mode": "full", "rows": rows, "raw": raw_dir}
        finalized = False
        hive_table = None
        if rt.get("finalize_full_refresh", True):
            state.mark_event(schema, table, load_date, "full", "finalize", "started",
                             location=final_dir, strategy=rt.get("finalize_strategy"))
            try:
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
                        raise RuntimeError(
                            f"Atomic rename failed for {final_dir}")
                finalized = True
                state.mark_event(schema, table, load_date, "full", "finalize", "success",
                                 location=final_dir, strategy=rt.get("finalize_strategy"))
                logger.info("full_finalized", schema=schema, table=table,
                            final_dir=final_dir, hive_table=hive_table)
            except Exception as e:
                state.mark_event(schema, table, load_date, "full",
                                 "finalize", "failed", location=final_dir, error=str(e))
                logger.error("full_finalize_failed",
                             schema=schema, table=table, err=str(e))
                raise
            if finalized and rt.get("hive", {}).get("enabled", False):
                hive_db = rt["hive"]["db"]
                HiveHelper.create_unpartitioned_parquet_if_absent_with_schema(
                    spark, hive_db, schema, table, final_dir
                )
        result.update(
            {"final": final_dir, "finalized": finalized, "hive_table": hive_table})
        return result

class IngestionSCD1:
    @staticmethod
    def run(spark, cfg, state, logger, schema, table, load_date, raw_dir, base_raw, tbl_cfg):
        rt, jdbc = cfg["runtime"], cfg["jdbc"]
        incr_col = tbl_cfg["incremental_column"]
        initial_wm = tbl_cfg.get("initial_watermark", "1900-01-01 00:00:00")
        pk_cols = tbl_cfg.get("primary_keys", [])
        # 1) progress
        last_wm, last_ld = state.get_progress(
            schema, table, default_wm=initial_wm, default_date="1900-01-01")
        logger.info("scd1_progress", schema=schema, table=table,
                    wm=last_wm, last_ld=last_ld, load_date=load_date)
        incr_type = (tbl_cfg.get("incr_col_type") or "").lower()

        lag = int(tbl_cfg.get("lag_seconds", cfg["runtime"].get("wm_lag_seconds", 0)))

        if incr_type == "epoch_seconds":
            eff_wm = Utils.minus_seconds_epoch(last_wm, lag, millis=False)
        elif incr_type == "epoch_millis":
            eff_wm = Utils.minus_seconds_epoch(last_wm, lag, millis=True)
        else:
            eff_wm = Utils.minus_seconds_datetime(last_wm, lag)
        
        logger.info("scd1_effective_wm", schema=schema, table=table,
                    last_wm=last_wm, lag_seconds=lag, effective_wm=eff_wm)

        # utilize slicing if needed.
        # 0) Build base FROM (table or wrapped query)
        base_from = JDBC.build_from_sql(jdbc, schema, table, tbl_cfg)
        is_int_epoch = str(incr_type).lower() in (
            "int", "bigint", "epoch", "epoch_seconds", "epoch_millis")

        # 1) Decide slicing
        now_lit = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S") if not is_int_epoch else str(int(time.time()))
        slices = [(eff_wm, now_lit)]
        scfg = rt.get("scd1_slicing", {})
        if scfg.get("enabled", False):
            slices = AdaptiveSlicePlanner.plan(
                spark, logger, jdbc, tbl_cfg, base_from, incr_col, eff_wm, now_lit, is_int_epoch, scfg)

        logger.info("scd1_planned_slices", schema=schema, table=table, n=len(slices))


        # 2) Stage each slice (resume-friendly)
        staged_dirs = []
        for lo, hi in slices:
            sdir = Staging.slice_dir(cfg, schema, table, incr_col, lo, hi)
            if Staging.is_success(spark, sdir):
                logger.info("slice_skip_found_success",
                            schema=schema, table=table, lo=lo, hi=hi)
                staged_dirs.append(sdir)
                continue

            dbtable = JDBC.dbtable_for_range(jdbc, base_from, incr_col, tbl_cfg, lo, hi)
            reader = (spark.read.format("jdbc")
                    .option("url", jdbc["url"])
                    .option("dbtable", dbtable)
                    .option("user", jdbc["user"])
                    .option("password", jdbc["password"])
                    .option("driver", jdbc["driver"])
                    .option("fetchsize", int(jdbc.get("fetchsize", 10000))))
            df_slice = with_ingest_cols(reader.load())
            # write to slice dir (overwrite to be idempotent)
            (df_slice.write
                .format(rt.get("write_format", "parquet"))
                .mode("overwrite")
                .option("compression", rt.get("compression", "snappy"))
                .save(sdir))
            Staging.write_text(spark, f"{sdir}/_RANGE.json", json.dumps(
                {"lo": lo, "hi": hi, "planned_at": datetime.now().isoformat()}))
            Staging.mark_success(spark, sdir)
            logger.info("slice_staged", schema=schema,
                        table=table, lo=lo, hi=hi, path=sdir)
            staged_dirs.append(sdir)

        # 3) Union all staged slices for this window
        if not staged_dirs:
            logger.info("scd1_no_slices_staged", schema=schema, table=table)
            return {"table": f"{schema}.{table}", "mode": "scd1", "rows": 0, "raw": raw_dir, "wm": last_wm, "last_loaded_date": last_ld}

        window_df = spark.read.format(
            rt.get("write_format", "parquet")).load(staged_dirs)
        to_merge_count = window_df.count()
        logger.info("scd1_window_ready", schema=schema,
                    table=table, rows=to_merge_count)

        # 4) Intermediate merge (Iceberg)
        inter_cfg = rt.get("intermediate", {"enabled": False})
        state.mark_event(schema, table, load_date, "scd1",
                         "intermediate", "started")
        info, ok = {}, True
        if inter_cfg.get("enabled", False):
            try:
                if inter_cfg.get("type", "iceberg") == "iceberg" and pk_cols:
                    tgt = IcebergHelper.merge_upsert(
                        spark, cfg, schema, table, window_df, pk_cols,
                        load_date, partition_col=rt.get("hive_reg", {}).get(
                            "partition_col", "load_date"),
                        incr_col=incr_col
                    )
                    info["iceberg_table"] = tgt
                    logger.info("scd1_intermediate_merged",
                                schema=schema, table=table, iceberg_table=tgt)
                    if rt.get("final_parquet_mirror", {"enabled": False}).get("enabled", False):
                        info.update(IcebergHelper.mirror_to_parquet_for_date(
                            spark, cfg, schema, table, load_date))
                else:
                    raise NotImplementedError(
                        "Parquet intermediary is not supported currently.")
                state.mark_event(schema, table, load_date,
                                 "scd1", "intermediate", "success")
            except Exception as e:
                traceback.print_exc()
                ok = False
                info["intermediate_error"] = str(e)
                state.mark_event(schema, table, load_date, "scd1",
                                 "intermediate", "failed", error=str(e))
                logger.error("scd1_intermediate_failed",
                             schema=schema, table=table, err=str(e))
        
        if not ok:
            return {"table": f"{schema}.{table}", "mode": "scd1", "rows": to_merge_count,
                    "raw": raw_dir, "wm": last_wm, "last_loaded_date": last_ld, **info}

        # 5) watermark advance only if ok (or intermediate disabled)
        # 5) On success: land slices into RAW for today's load_date; mark _LANDED
        for sdir in staged_dirs:
            df = spark.read.format(rt.get("write_format", "parquet")).load(sdir)
            RawIO.land_append(spark, logger, df, rt, raw_dir, state,
                            schema, table, load_date, "scd1")
            Staging.mark_landed(spark, sdir)
        
        raw_to_merge = RawIO.raw_increment_df(
            spark, rt, schema, table, last_ld, eff_wm, incr_col, incr_type)
        new_wm, new_ld = IcebergHelper._compute_wm_ld(raw_to_merge, incr_col, is_int_epoch)
        state.mark_event(schema, table, load_date, "scd1",
                        "watermark", "success", watermark=new_wm)
        state.set_progress(schema, table, watermark=new_wm, last_loaded_date=new_ld)
        logger.info("scd1_wm_advanced", schema=schema,
                    table=table, new_wm=new_wm, new_ld=new_ld)
        return {"table": f"{schema}.{table}", "mode": "scd1", "rows": to_merge_count,
                "raw": raw_dir, "wm": new_wm, "last_loaded_date": new_ld, **info}

        

# =========================
# Orchestration
# =========================
def ingest_one_table(spark, cfg, state, logger, tbl, pool_name, load_date):
    schema, table = tbl["schema"], tbl["table"]
    mode = tbl.get("mode", "full").lower()
    rt = cfg["runtime"]
    raw_dir, final_dir, base_raw = Paths.build(rt, schema, table, load_date)
    # scheduler labels
    sc = spark.sparkContext
    sc.setLocalProperty("spark.scheduler.pool", pool_name)
    sc.setJobGroup(f"ingest::{schema}.{table}",
                   f"Ingest {schema}.{table} -> {raw_dir}")
    logger.info("table_start", schema=schema, table=table,
                mode=mode, pool=pool_name, load_date=load_date)
    if mode == "full":
        return IngestionFull.run(spark, cfg, state, logger, schema, table, load_date, raw_dir, final_dir, tbl)
    elif mode == "scd1":
        return IngestionSCD1.run(spark, cfg, state, logger, schema, table, load_date, raw_dir, base_raw, tbl)
    else:
        raise ValueError(f"Unsupported mode: {mode}")

# =========================
# Config validation (light)
# =========================
def validate_config(cfg):
    for k in ["jdbc", "runtime", "tables"]:
        if k not in cfg:
            raise ValueError(f"Missing config key: {k}")
    rt = cfg["runtime"]
    for k in ["raw_root", "final_root", "timezone"]:
        if k not in rt:
            raise ValueError(f"Missing runtime.{k}")
    if rt.get("state_backend", "singlestore") == "singlestore":
        ss = rt.get("state", {}).get("singlestore")
        if not ss:
            raise ValueError("runtime.state.singlestore required")
        for k in ["ddlEndpoint", "user", "password", "database", "eventsTable", "watermarksTable", "sourceId"]:
            if k not in ss:
                raise ValueError(f"Missing singlestore.{k}")

# =========================
# Main
# =========================
def suggest_singlestore_ddl(logger, cfg):
    ss = cfg["runtime"]["state"]["singlestore"]
    events = ss["eventsTable"]
    wm = ss["watermarksTable"]
    db = ss["database"]
    logger.info("singlestore_recommended_ddl",
                events=f"ALTER TABLE {db}.{events} ADD COLUMN IF NOT EXISTS ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP;",
                wm=f"ALTER TABLE {db}.{wm} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;")

def main(spark, cfg, args={}):
    # Logger
    log_file = cfg["runtime"].get("log_file")
    logger = PrintLogger(job_name=cfg["runtime"].get(
        "job_name", "spark_ingest"), file_path=log_file)
    
    # State
    backend = cfg["runtime"].get("state_backend", "singlestore")
    if backend == "singlestore":
        base_state = SingleStoreState(
            spark, cfg["runtime"]["state"]["singlestore"])

        outbox = HDFSOutbox(spark, cfg["runtime"]["hdfs_outbox_root"]) if cfg["runtime"].get(
            "hdfs_outbox_root") else None

        if outbox:
            outbox.restore_all(base_state, logger)
        sink = TimeAwareBufferedSink(base_state, logger, outbox,
                                    flush_every=cfg["runtime"].get(
                                        "state_flush_every", 50),
                                    flush_age_seconds=cfg["runtime"].get("state_flush_age_seconds", 120))
        
        # sink = TimeAwareBufferedSink(
        #     spark=spark,
        #     base_state=base_state,            # NOT the BufferedState wrapper
        #     outbox=outbox,
        #     source_id=base_state.sourceId,
        #     events_schema=base_state._events_schema,
        #     flush_cfg=cfg["runtime"].get("state_flush", {
        #                                 "max_events": 200, "max_age_seconds": 30, "check_interval_seconds": 5})
        # )
        state = BufferedState(
                              base_state=base_state,
                              source_id=cfg["runtime"]["state"]["singlestore"]["sourceId"],
                              flush_every=int(cfg["runtime"].get(
                                  "state_flush_every", 50)),
                              outbox=outbox,
                              time_sink=sink,
                              logger=logger
                              )
    else:
        base = cfg["runtime"]["state"]["hdfs"]["dir"]
        spark.range(1).write.mode("overwrite").parquet(
            str(PurePosixPath(base) / "_init_marker"))
        raise NotImplementedError("HDFSState not included in this file.")
    hb = Heartbeat(logger, interval_sec=int(
        cfg["runtime"].get("heartbeat_seconds", args.heartbeat_seconds)), time_sink=sink)
    notifier = Notifier(spark, logger, cfg, interval_sec=int(
        args.notify_interval_seconds))
    notifier.start()

    

    # Filters
    tables = cfg["tables"]
    hb.update(total=len(tables))
    hb.start()
    if args.only_tables:
        allow = set(s.strip().lower() for s in args.only_tables.split(","))
        def allow_tbl(t):
            key = f"{t['schema']}.{t['table']}".lower()
            return key in allow
        tables = [t for t in tables if allow_tbl(t)]
    if not tables:
        logger.warn("no_tables_to_run")
        spark.stop()
        return
    # load_date (default: today in cluster tz)
    load_date = args.load_date or datetime.now().astimezone().strftime("%Y-%m-%d")
    # preload state for today
    state.preload(spark, tables, load_date)
    logger.info("job_start", tables=len(tables), load_date=load_date)
    max_workers = int(cfg["runtime"].get("max_parallel_tables", 4))
    results, errors = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futmap = {}
        for i, tbl in enumerate(tables):
            pool = f"pool-{(i % max_workers) + 1}"
            fut = ex.submit(ingest_one_table, spark, cfg,
                            state, logger, tbl, pool, load_date)
            futmap[fut] = f"{tbl['schema']}.{tbl['table']}"
            hb.update(inflight=len(futmap))
        for fut in as_completed(futmap):
            key = futmap[fut]
            try:
                res = fut.result()
                results.append(res)
                logger.info("table_done", table=key, result="ok")
                # on success per table
                notifier.emit("INFO", {"event": "table_done", "table": key})
                hb.update(done=len(results), inflight=len(
                    futmap)-len(results)-len(errors))
            except Exception as e:
                traceback.print_exc()
                errors.append((key, str(e)))
                hb.update(failed=len(errors), inflight=len(
                    futmap)-len(results)-len(errors))
                logger.error("table_failed", table=key, err=str(e))
                # on error per table
                notifier.emit("ERROR", {"event": "table_failed",
                          "table": key, "error": str(e)})
        # on end
    notifier.emit("INFO", {"event": "job_summary", "ok": len(
        results), "err": len(errors), "run_id": RUN_ID})
    # flush buffered state
    state.flush()
    logger.info("job_end", ok=len(results), err=len(errors))
    # human summary to stdout
    print("\n=== SUMMARY ===")
    for r in results:
        print(r)
    if errors:
        print("\n=== ERRORS ===")
        for k, e in errors:
            print(k, e)
    hb.stop()
    notifier.stop()
    sink.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "--only-tables", help="Comma-separated schema.table filters", default=None)
    parser.add_argument(
        "--load-date", help="Override load_date (YYYY-MM-DD)", default=None)
    parser.add_argument(
        "--start-date", help="Backfill start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Backfill end date (YYYY-MM-DD)")
    parser.add_argument("--reload", action="store_true",
                        help="Reprocess from RAW only (skip source pull); rebuild intermediate/final for the date window")
    parser.add_argument("--wm-lag-seconds", type=int, default=0,
                        help="Global lag to subtract from watermark when pulling increments")
    parser.add_argument("--heartbeat-seconds", type=int, default=120)
    parser.add_argument("--notify-interval-seconds", type=int, default=300)
    args = parser.parse_args()
    with open(args.config, "r") as f:
        cfg = json.load(f)
    validate_config(cfg)
    log_file = cfg["runtime"].get("log_file")
    logger = PrintLogger(job_name=cfg["runtime"].get(
        "job_name", "spark_ingest"), file_path=log_file)
    suggest_singlestore_ddl(logger, cfg)
    # Spark
    spark = (
        SparkSession.builder
        .appName(cfg["runtime"].get("app_name", "spark_ingest_framework"))
        .config("spark.dynamicAllocation.enabled", cfg["runtime"].get("dynamicAllocation", "true"))
        .config("spark.dynamicAllocation.initialExecutors", cfg["runtime"].get("initialExecutors", "1"))
        .config("spark.dynamicAllocation.maxExecutors", cfg["runtime"].get("maxExecutors", "2"))
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.sql.parquet.int96RebaseModeInWrite", cfg["runtime"].get("int96RebaseModeInWrite", "LEGACY"))
        .config("spark.executor.cores", cfg["runtime"].get("executor.cores", "4"))
        .config("spark.executor.memory", cfg["runtime"].get("executor.memory", "8g"))
        .config("spark.driver.memory",   cfg["runtime"].get("driver.memory", "6g"))
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", cfg["runtime"].get("timezone", "Asia/Kolkata"))
        # Optional: fair scheduler allocation file
        # .config("spark.scheduler.allocation.file", cfg["runtime"].get("fair_allocation_file", ""))
        # JARs (if not on cluster classpath)
        .config("spark.jars", ",".join(cfg["runtime"].get("extra_jars", [])))
        .enableHiveSupport()
        .getOrCreate()
    )
    main(spark, cfg, args)
    if cfg["runtime"].get("staging", {}).get("enabled", True):
        Staging.ttl_cleanup(spark, cfg, logger)
    spark.stop()



