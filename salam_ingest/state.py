import itertools
import json
import threading
from datetime import date, datetime
from typing import Any, Dict, Iterable, Optional, Tuple

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, desc as spark_desc, lit, max as spark_max, to_date
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

from .common import RUN_ID, next_event_seq


class StateStore:
    def get_day_status(self, schema: str, table: str, load_date: str) -> Dict[str, Any]:
        raise NotImplementedError

    def mark_event(
        self,
        schema: str,
        table: str,
        load_date: str,
        mode: str,
        phase: str,
        status: str,
        rows_written: Optional[int] = None,
        watermark: Optional[str] = None,
        location: Optional[str] = None,
        strategy: Optional[str] = None,
        error: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        raise NotImplementedError

    def get_last_watermark(self, schema: str, table: str, default_value: str) -> str:
        raise NotImplementedError

    def get_progress(
        self, schema: str, table: str, default_wm: str, default_date: str
    ) -> Tuple[str, str]:
        raise NotImplementedError

    def set_progress(self, schema: str, table: str, watermark: str, last_loaded_date: str) -> None:
        raise NotImplementedError


class SingleStoreState(StateStore):
    def __init__(self, spark: SparkSession, cfg: Dict[str, Any]) -> None:
        self.spark = spark
        self.sourceId = cfg["sourceId"]
        self.db = cfg["database"]
        self.events = cfg["eventsTable"]
        self.wm = cfg["watermarksTable"]
        self.base_opts = {
            "ddlEndpoint": cfg["ddlEndpoint"],
            "user": cfg["user"],
            "password": cfg["password"],
            "database": cfg["database"],
        }
        self._events_schema = StructType(
            [
                StructField("sourceId", StringType(), True),
                StructField("schema_name", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("load_date", DateType(), True),
                StructField("mode", StringType(), True),
                StructField("phase", StringType(), True),
                StructField("status", StringType(), True),
                StructField("rows_written", LongType(), True),
                StructField("watermark", StringType(), True),
                StructField("location", StringType(), True),
                StructField("strategy", StringType(), True),
                StructField("error", StringType(), True),
                StructField("metadata_json", StringType(), True),
                StructField("event_at", StringType(), True),
                StructField("run_id", StringType(), True),
                StructField("event_seq", LongType(), True),
            ]
        )
        self._wm_schema = StructType(
            [
                StructField("sourceId", StringType(), True),
                StructField("schema_name", StringType(), True),
                StructField("table_name", StringType(), True),
                StructField("watermark", StringType(), True),
                StructField("last_loaded_date", StringType(), True),
            ]
        )

    def _read_events_df(self):
        return (
            self.spark.read.format("singlestore")
            .options(dbTable=self.events, **self.base_opts)
            .load()
        )

    def _read_wm_df(self):
        return (
            self.spark.read.format("singlestore")
            .options(dbTable=self.wm, **self.base_opts)
            .load()
        )

    @staticmethod
    def _coerce_date(value: Any) -> Optional[date]:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            s = value.strip()[:10].replace("/", "-")
            return datetime.strptime(s, "%Y-%m-%d").date()
        raise TypeError(f"Unsupported load_date type: {value!r}")

    def _write_events(self, rows: Iterable[Any]) -> None:
        def _norm(r: Any) -> Row:
            def _now_iso_micros() -> str:
                return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

            EventRow = Row(
                "sourceId",
                "schema_name",
                "table_name",
                "load_date",
                "mode",
                "phase",
                "status",
                "rows_written",
                "watermark",
                "location",
                "strategy",
                "error",
                "metadata_json",
                "event_at",
                "run_id",
                "event_seq",
            )
            data = r.asDict() if isinstance(r, Row) else dict(r)
            return EventRow(
                self.sourceId,
                data.get("schema_name"),
                data.get("table_name"),
                SingleStoreState._coerce_date(data.get("load_date")),
                data.get("mode"),
                data.get("phase"),
                data.get("status"),
                data.get("rows_written", 0),
                data.get("watermark"),
                data.get("location"),
                data.get("strategy"),
                data.get("error"),
                data.get("metadata_json"),
                _now_iso_micros(),
                RUN_ID,
                next_event_seq(),
            )

        df = self.spark.createDataFrame(list(map(_norm, rows)), schema=self._events_schema)
        df = df.withColumn("load_date", to_date("load_date"))
        (
            df.coalesce(1)
            .write.format("singlestore")
            .options(dbTable=self.events, **self.base_opts)
            .mode("append")
            .save()
        )

    def _write_progress(self, rows: Iterable[Any]) -> None:
        def _norm(r: Any) -> Row:
            EventRow = Row("sourceId", "schema_name", "table_name", "watermark", "last_loaded_date")
            data = r.asDict() if isinstance(r, Row) else dict(r)
            return EventRow(
                self.sourceId,
                data.get("schema_name"),
                data.get("table_name"),
                data.get("watermark"),
                data.get("last_loaded_date"),
            )

        df = self.spark.createDataFrame(list(map(_norm, rows)), schema=self._wm_schema)
        (
            df.coalesce(1)
            .write.format("singlestore")
            .options(dbTable=self.wm, **self.base_opts)
            .mode("append")
            .save()
        )

    def get_day_status(self, schema: str, table: str, load_date: str) -> Dict[str, bool]:
        df = self._read_events_df().where(
            (col("schema_name") == schema) & (col("table_name") == table) & (col("load_date") == lit(load_date))
        )

        def _done(phase: str) -> bool:
            cols = df.columns
            if "ts" in cols:
                latest = df.where(col("phase") == phase).orderBy(spark_desc("ts")).limit(1).collect()
            else:
                latest = df.where(col("phase") == phase).limit(1).collect()
            return bool(latest and latest[0]["status"] == "success")

        return {
            "raw_done": _done("raw"),
            "finalized_done": _done("finalize"),
            "intermediate_done": _done("intermediate"),
        }

    def mark_event(
        self,
        schema: str,
        table: str,
        load_date: str,
        mode: str,
        phase: str,
        status: str,
        rows_written: Optional[int] = None,
        watermark: Optional[str] = None,
        location: Optional[str] = None,
        strategy: Optional[str] = None,
        error: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload = {
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
            "metadata_json": None,
        }
        if extra:
            known_keys = {"rows_written", "watermark", "location", "strategy", "error"}
            metadata_extra: Dict[str, Any] = {}
            for key, value in extra.items():
                if key in known_keys:
                    payload[key] = value
                else:
                    metadata_extra[key] = value
            if metadata_extra:
                payload["metadata_json"] = json.dumps(metadata_extra, separators=(",", ":"))
        self._write_events(
            [
                payload
            ]
        )

    def get_last_watermark(self, schema: str, table: str, default_value: str) -> str:
        try:
            df = self._read_wm_df().where((col("schema_name") == schema) & (col("table_name") == table))
            if df.rdd.isEmpty():
                return default_value
            cols = df.columns
            if "updated_at" in cols:
                last = df.orderBy(spark_desc("updated_at")).select("watermark").limit(1).collect()
            else:
                last = df.orderBy(spark_desc("watermark")).select("watermark").limit(1).collect()
            return last[0]["watermark"] if last else default_value
        except Exception as exc:
            print(f"[WARN] get_last_watermark failed for {schema}.{table}: {exc}")
            return default_value

    def get_progress(
        self, schema: str, table: str, default_wm: str, default_date: str
    ) -> Tuple[str, str]:
        try:
            df = self._read_wm_df().where((col("schema_name") == schema) & (col("table_name") == table))
            if df.rdd.isEmpty():
                return default_wm, default_date
            cols = df.columns
            if "updated_at" in cols:
                last = (
                    df.orderBy(spark_desc("updated_at"))
                    .select("watermark", "last_loaded_date")
                    .limit(1)
                    .collect()
                )
            else:
                last = (
                    df.groupBy()
                    .agg(
                        spark_max("watermark").alias("watermark"),
                        F.first("last_loaded_date", ignorenulls=True).alias("last_loaded_date"),
                    )
                    .collect()
                )
            if last:
                wm = last[0]["watermark"] or default_wm
                ld = last[0]["last_loaded_date"] or default_date
                return wm, ld
        except Exception as exc:
            print(f"[WARN] get_progress: {exc}")
        return default_wm, default_date

    def set_progress(self, schema: str, table: str, watermark: str, last_loaded_date: str) -> None:
        self._write_progress(
            [
                {
                    "schema_name": schema,
                    "table_name": table,
                    "watermark": watermark,
                    "last_loaded_date": last_loaded_date,
                }
            ]
        )


class BufferedState(StateStore):
    def __init__(
        self,
        base_state: SingleStoreState,
        source_id: str,
        flush_every: int = 50,
        outbox: Optional["HDFSOutbox"] = None,
        time_sink: Optional[Any] = None,
        logger: Optional[Any] = None,
    ) -> None:
        self.base = base_state
        self.source_id = source_id
        self.flush_every = flush_every
        self._events_buf: list[Dict[str, Any]] = []
        self._progress_buf: list[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._progress_cache: Dict[Tuple[str, str], Tuple[str, str]] = {}
        self._day_status_cache: Dict[Tuple[str, str], Dict[str, bool]] = {}
        self._event_seq_ctr = itertools.count(1)
        self.outbox = outbox
        self.time_sink = time_sink
        self.logger = logger

    def preload(self, spark: SparkSession, tables: Iterable[Dict[str, Any]], load_date_str: str) -> None:
        keys = {(t["schema"], t["table"]) for t in tables}
        schemas = sorted({s for s, _ in keys})
        tbls = sorted({t for _, t in keys})
        wm_df = (
            self.base._read_wm_df()
            .where(col("sourceId") == lit(self.source_id))
            .where(col("schema_name").isin(schemas))
            .where(col("table_name").isin(tbls))
        )
        if not wm_df.rdd.isEmpty():
            cols = wm_df.columns
            if "updated_at" in cols:
                from pyspark.sql.window import Window as W

                w = W.partitionBy("schema_name", "table_name").orderBy(spark_desc("updated_at"))
                latest = (
                    wm_df.withColumn("_rn", F.row_number().over(w))
                    .where(col("_rn") == 1)
                    .select("schema_name", "table_name", "watermark", "last_loaded_date")
                )
            else:
                latest = (
                    wm_df.groupBy("schema_name", "table_name")
                    .agg(
                        spark_max("watermark").alias("watermark"),
                        F.first("last_loaded_date", ignorenulls=True).alias("last_loaded_date"),
                    )
                )
            for row in latest.collect():
                self._progress_cache[(row["schema_name"], row["table_name"])] = (
                    row["watermark"],
                    row["last_loaded_date"],
                )
        ev_df = (
            self.base._read_events_df()
            .where(col("sourceId") == lit(self.source_id))
            .where(col("schema_name").isin(schemas))
            .where(col("table_name").isin(tbls))
            .where(col("load_date") == to_date(lit(load_date_str)))
            .where(~col("status").isin("skipped"))
        )
        if not ev_df.rdd.isEmpty():
            from pyspark.sql.window import Window as W

            cols = ev_df.columns
            if "ts" in cols:
                ord_col = col("ts")
            elif "updated_at" in cols:
                ord_col = col("updated_at")
            else:
                ord_col = F.to_timestamp(F.lit("1970-01-01 00:00:00"))
            status_rank = (
                F.when(col("status") == F.lit("success"), F.lit(3))
                .when(col("status") == F.lit("failed"), F.lit(2))
                .when(col("status") == F.lit("started"), F.lit(1))
                .otherwise(F.lit(0))
            )
            w = W.partitionBy("schema_name", "table_name", "mode", "phase").orderBy(
                status_rank.desc(), ord_col.desc()
            )
            latest = (
                ev_df.withColumn("_rn", F.row_number().over(w))
                .where(col("_rn") == 1)
                .select("schema_name", "table_name", "mode", "phase", "status")
            )
            for row in latest.collect():
                key = (row["schema_name"], row["table_name"])
                status_map = self._day_status_cache.setdefault(
                    key, {"raw_done": False, "finalized_done": False, "intermediate_done": False}
                )
                phase_key = {
                    "raw": "raw_done",
                    "finalize": "finalized_done",
                    "intermediate": "intermediate_done",
                }.get(row["phase"])
                if phase_key and row["status"] == "success":
                    status_map[phase_key] = True

    def get_day_status(self, schema: str, table: str, load_date: str) -> Dict[str, bool]:
        key = (schema, table)
        if key in self._day_status_cache:
            return self._day_status_cache[key]
        result = self.base.get_day_status(schema, table, load_date)
        self._day_status_cache[key] = result
        return result

    def get_last_watermark(self, schema: str, table: str, default_value: str) -> str:
        key = (schema, table)
        if key in self._progress_cache:
            wm, _ = self._progress_cache[key]
            return wm or default_value
        return self.base.get_last_watermark(schema, table, default_value)

    def get_progress(
        self, schema: str, table: str, default_wm: str, default_date: str
    ) -> Tuple[str, str]:
        key = (schema, table)
        if key in self._progress_cache:
            wm, ld = self._progress_cache[key]
            return wm or default_wm, ld or default_date
        wm, ld = self.base.get_progress(schema, table, default_wm, default_date)
        with self._lock:
            self._progress_cache[key] = (wm, ld)
        return wm, ld

    def mark_event(
        self,
        schema: str,
        table: str,
        load_date: str,
        mode: str,
        phase: str,
        status: str,
        rows_written: Optional[int] = None,
        watermark: Optional[str] = None,
        location: Optional[str] = None,
        strategy: Optional[str] = None,
        error: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
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
            "run_id": RUN_ID,
            "event_seq": next(self._event_seq_ctr),
            "event_at": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S"),
            "sourceId": self.source_id,
            "metadata_json": None,
        }
        if extra:
            known_keys = {"rows_written", "watermark", "location", "strategy", "error"}
            metadata_extra: Dict[str, Any] = {}
            for key, value in extra.items():
                if key in known_keys:
                    evt[key] = value
                else:
                    metadata_extra[key] = value
            if metadata_extra:
                evt["metadata_json"] = json.dumps(metadata_extra, separators=(",", ":"))
        if self.outbox is not None:
            try:
                self.outbox.append_event(self.source_id, RUN_ID, evt)
            except Exception as exc:
                if self.logger:
                    self.logger.warn("wal_append_failed", err=str(exc), table=f"{schema}.{table}")
        if self.time_sink is not None:
            self.time_sink.add_event(evt)
            return
        with self._lock:
            self._events_buf.append(evt)
            if len(self._events_buf) >= self.flush_every:
                self._flush_events_nolock()

    def _flush_events_nolock(self) -> None:
        if not self._events_buf:
            return
        batch = list(self._events_buf)
        self._events_buf.clear()
        allowed = {f.name for f in self.base._events_schema.fields}
        batch_ss = [{k: v for k, v in r.items() if k in allowed} for r in batch]
        try:
            self.base._write_events(batch_ss)
        except Exception as exc:
            self._events_buf[:0] = batch
            if self.logger:
                self.logger.error("events_flush_failed", err=str(exc), rows=len(batch))

    def set_progress(self, schema: str, table: str, watermark: str, last_loaded_date: str) -> None:
        row = {
            "schema_name": schema,
            "table_name": table,
            "watermark": watermark,
            "last_loaded_date": last_loaded_date,
            "run_id": RUN_ID,
            "sourceId": self.source_id,
            "event_at": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S"),
        }
        if self.outbox is not None:
            try:
                self.outbox.append_progress(self.source_id, RUN_ID, row)
            except Exception as exc:
                if self.logger:
                    self.logger.warn("wal_append_progress_failed", err=str(exc), table=f"{schema}.{table}")
        with self._lock:
            self._progress_cache[(schema, table)] = (watermark, last_loaded_date)
        if self.time_sink is not None:
            self.time_sink.add_progress(row)
            return
        with self._lock:
            self._progress_buf.append(row)
            if len(self._progress_buf) >= max(1, self.flush_every // 5):
                self._flush_progress_nolock()

    def flush(self) -> None:
        if self.time_sink is not None:
            self.time_sink.flush()
            return
        with self._lock:
            self._flush_events_nolock()
            self._flush_progress_nolock()

    def _flush_progress_nolock(self) -> None:
        if not self._progress_buf:
            return
        batch = list(self._progress_buf)
        self._progress_buf.clear()
        allowed = {f.name for f in self.base._wm_schema.fields}
        batch_ss = [{k: v for k, v in r.items() if k in allowed} for r in batch]
        try:
            self.base._write_progress(batch_ss)
        except Exception as exc:
            self._progress_buf[:0] = batch
            if self.logger:
                self.logger.error("progress_flush_failed", err=str(exc), rows=len(batch))


from .io.filesystem import HDFSOutbox  # noqa: E402
