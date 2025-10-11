from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_timestamp

from ...common import RUN_ID
from ...io.filesystem import HDFSUtil
from ...io.paths import Paths
from ...staging import Staging
from ..base import (
    EndpointCapabilities,
    IncrementalCommitResult,
    IncrementalContext,
    IngestionSlice,
    SinkEndpoint,
    SinkFinalizeResult,
    SinkWriteResult,
    SliceStageResult,
)
from .warehouse import HiveHelper, IcebergHelper


def _load_raw_increment_df(
    spark: SparkSession,
    runtime_cfg: Dict[str, Any],
    schema: str,
    table: str,
    last_ld: str,
    last_wm: str,
    incr_col: str,
    incr_type: str = "",
) -> DataFrame:
    _, _, base_raw = Paths.build(runtime_cfg, schema, table, "")
    all_raw = spark.read.format(runtime_cfg.get("write_format", "parquet")).load(base_raw)
    if incr_col.lower() not in [c.lower() for c in all_raw.columns]:
        return all_raw.limit(0)
    df = all_raw.where(col("load_date") >= lit(last_ld))
    incr_type = (incr_type or "").lower()
    if incr_type.startswith("epoch") or incr_type in ("bigint", "int", "numeric", "decimal"):
        df = df.where(col(incr_col).cast("long") > F.lit(int(str(last_wm))))
    else:
        df = df.where(to_timestamp(col(incr_col)) > to_timestamp(lit(last_wm)))
    return df


class HdfsParquetEndpoint(SinkEndpoint):
    """Handles landing to RAW directories and optional Hive registration."""

    def __init__(self, spark: SparkSession, cfg: Dict[str, Any], table_cfg: Dict[str, Any]) -> None:
        self.spark = spark
        self.cfg = cfg
        self.runtime_cfg = cfg["runtime"]
        self.table_cfg = dict(table_cfg)
        self.schema = table_cfg["schema"]
        self.table = table_cfg["table"]
        self.load_date: Optional[str] = None
        self.base_raw = Paths.build(self.runtime_cfg, self.schema, self.table, "")[2]
        self._caps = EndpointCapabilities(
            supports_write=True,
            supports_finalize=True,
            supports_publish=True,
            supports_watermark=True,
            supports_staging=True,
            supports_merge=True,
            event_metadata_keys=("location", "hive_table"),
        )

    # ------------------------------------------------------------------ SinkEndpoint protocol
    def configure(self, table_cfg: Dict[str, Any]) -> None:
        self.table_cfg.update(table_cfg)
        self.schema = self.table_cfg["schema"]
        self.table = self.table_cfg["table"]
        self.base_raw = Paths.build(self.runtime_cfg, self.schema, self.table, "")[2]

    def capabilities(self) -> EndpointCapabilities:
        return self._caps

    def write_raw(
        self,
        df: DataFrame,
        *,
        mode: str,
        load_date: str,
        schema: str,
        table: str,
        rows: Optional[int] = None,
    ) -> SinkWriteResult:
        self.load_date = load_date
        raw_dir, _, _ = self._paths_for(load_date)
        if rows is None:
            rows = df.count()
        (
            df.write.format(self.runtime_cfg.get("write_format", "parquet"))
            .mode(mode)
            .option("compression", self.runtime_cfg.get("compression", "snappy"))
            .save(raw_dir)
        )
        metrics = {
            "schema": schema,
            "table": table,
            "run_id": RUN_ID,
            "rows_written": rows,
            "status": "success",
            "phase": "raw",
            "load_date": load_date,
        }
        HDFSUtil.write_text(
            spark=self.spark,
            path=f"{raw_dir}/_METRICS.json",
            content=json.dumps(metrics),
        )
        event_payload = {"location": raw_dir, "rows_written": rows}
        return SinkWriteResult(rows=rows, path=raw_dir, event_payload=event_payload)

    def finalize_full(
        self,
        *,
        load_date: str,
        schema: str,
        table: str,
    ) -> SinkFinalizeResult:
        raw_dir, final_dir, _ = self._paths_for(load_date)
        rt = self.runtime_cfg
        hive_table = None
        if rt.get("finalize_strategy") == "hive_set_location" and rt.get("hive", {}).get("enabled", False):
            hive_table = HiveHelper.set_location(
                self.spark,
                rt["hive"]["db"],
                schema,
                table,
                raw_dir,
            )
        else:
            jsc = self.spark._jsc
            jvm = self.spark.sparkContext._jvm
            conf = jsc.hadoopConfiguration()
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
            Path = jvm.org.apache.hadoop.fs.Path
            src = Path(raw_dir)
            dst = Path(final_dir)
            tmp = Path(final_dir + ".__tmp_swap__")
            if fs.exists(tmp):
                fs.delete(tmp, True)
            ok = jvm.org.apache.hadoop.fs.FileUtil.copy(fs, src, fs, tmp, False, conf)
            if not ok:
                raise RuntimeError(f"Copy {raw_dir} -> {final_dir}/.__tmp_swap__ failed")
            if fs.exists(dst):
                fs.delete(dst, True)
            if not fs.rename(tmp, dst):
                raise RuntimeError(f"Atomic rename failed for {final_dir}")
        if rt.get("hive", {}).get("enabled", False):
            hive_db = rt["hive"]["db"]
            HiveHelper.create_unpartitioned_parquet_if_absent_with_schema(
                self.spark,
                hive_db,
                schema,
                table,
                final_dir,
            )
        event_payload = {"location": final_dir}
        if hive_table:
            event_payload["hive_table"] = hive_table
        return SinkFinalizeResult(final_path=final_dir, event_payload=event_payload)

    def stage_incremental_slice(
        self,
        df: DataFrame,
        *,
        context: IncrementalContext,
        slice_info: IngestionSlice,
    ) -> SliceStageResult:
        incr_col = context.incremental_column
        hi_value = slice_info.upper or context.planner_metadata.get("now_literal") or context.effective_watermark
        slice_path = Staging.slice_dir(
            self.cfg,
            context.schema,
            context.table,
            incr_col,
            slice_info.lower,
            hi_value,
        )
        if Staging.is_success(self.spark, slice_path) and Staging.is_landed(self.spark, slice_path):
            return SliceStageResult(
                slice=slice_info,
                path=slice_path,
                rows=0,
                skipped=True,
                event_payload={"slice_lower": slice_info.lower, "slice_upper": slice_info.upper, "skipped": True},
            )

        rows = df.count()
        (
            df.write.format(self.runtime_cfg.get("write_format", "parquet"))
            .mode("overwrite")
            .option("compression", self.runtime_cfg.get("compression", "snappy"))
            .save(slice_path)
        )
        Staging.write_text(
            self.spark,
            f"{slice_path}/_RANGE.json",
            json.dumps({"lo": slice_info.lower, "hi": slice_info.upper, "planned_at": datetime.now().isoformat()}),
        )
        Staging.mark_success(self.spark, slice_path)
        event_payload = {
            "slice_lower": slice_info.lower,
            "slice_upper": slice_info.upper,
            "rows_written": rows,
        }
        return SliceStageResult(slice=slice_info, path=slice_path, rows=rows, event_payload=event_payload)

    def commit_incremental(
        self,
        *,
        load_date: str,
        schema: str,
        table: str,
        context: IncrementalContext,
        staged_slices: List[SliceStageResult],
    ) -> IncrementalCommitResult:
        raw_dir, _, _ = self._paths_for(load_date)
        active_slices = [s for s in staged_slices if not s.skipped]
        if not active_slices:
            return IncrementalCommitResult(
                rows=0,
                raw_path=raw_dir,
                new_watermark=context.last_watermark,
                new_loaded_date=context.last_loaded_date,
                additional_metadata={"skipped": True},
            )

        slice_paths = [s.path for s in active_slices]
        read_format = self.runtime_cfg.get("write_format", "parquet")
        window_df = self.spark.read.format(read_format).load(slice_paths)
        total_rows = sum(s.rows for s in active_slices)

        intermediate_payload: Dict[str, Any] = {}
        additional_metadata: Dict[str, Any] = {}

        inter_cfg = self.runtime_cfg.get("intermediate", {"enabled": False})
        if inter_cfg.get("enabled", False) and context.primary_keys:
            iceberg_table = IcebergHelper.merge_upsert(
                self.spark,
                {"runtime": self.runtime_cfg},
                schema,
                table,
                window_df,
                context.primary_keys,
                load_date,
                partition_col=inter_cfg.get("partition_col", "load_date"),
                incr_col=context.incremental_column,
            )
            intermediate_payload["iceberg_table"] = iceberg_table
            if self.runtime_cfg.get("final_parquet_mirror", {}).get("enabled", False):
                mirror_info = IcebergHelper.mirror_to_parquet_for_date(
                    self.spark,
                    {"runtime": self.runtime_cfg},
                    schema,
                    table,
                    load_date,
                )
                additional_metadata.update(mirror_info)

        (
            window_df.write.format(read_format)
            .mode("append")
            .option("compression", self.runtime_cfg.get("compression", "snappy"))
            .save(raw_dir)
        )

        metrics = {
            "schema": schema,
            "table": table,
            "run_id": RUN_ID,
            "rows_written": total_rows,
            "status": "success",
            "phase": "raw",
            "load_date": load_date,
            "ts": datetime.now().astimezone().isoformat(),
        }
        HDFSUtil.write_text(self.spark, f"{raw_dir}/_METRICS.json", json.dumps(metrics))

        for slice_result in active_slices:
            Staging.mark_landed(self.spark, slice_result.path)

        raw_to_merge = _load_raw_increment_df(
            self.spark,
            self.runtime_cfg,
            schema,
            table,
            context.last_loaded_date,
            context.effective_watermark,
            context.incremental_column,
            context.incremental_type,
        )
        new_wm, new_ld = IcebergHelper._compute_wm_ld(
            raw_to_merge, context.incremental_column, context.is_epoch
        )

        raw_event_payload = {"rows_written": total_rows}
        additional_metadata.setdefault("staged_slices", len(active_slices))

        return IncrementalCommitResult(
            rows=total_rows,
            raw_path=raw_dir,
            new_watermark=new_wm,
            new_loaded_date=new_ld,
            raw_event_payload=raw_event_payload,
            intermediate_event_payload=intermediate_payload,
            additional_metadata=additional_metadata,
        )

    def publish_dataset(
        self,
        *,
        load_date: str,
        schema: str,
        table: str,
    ) -> Dict[str, Any]:
        hive_cfg = self.runtime_cfg.get("hive", {})
        if not hive_cfg.get("enabled", False):
            return {"status": "disabled"}
        final_dir = self._paths_for(load_date)[1]
        hive_db = hive_cfg["db"]
        HiveHelper.create_unpartitioned_parquet_if_absent_with_schema(
            self.spark,
            hive_db,
            schema,
            table,
            final_dir,
        )
        return {"status": "published", "hive_db": hive_db, "location": final_dir}

    def latest_watermark(
        self,
        *,
        schema: str,
        table: str,
    ) -> Optional[str]:
        incr_col = self.table_cfg.get("incremental_column")
        if not incr_col:
            return None
        try:
            df = self.spark.read.format(self.runtime_cfg.get("write_format", "parquet")).load(self.base_raw)
            if incr_col not in map(str.lower, df.columns):
                return None
            max_val = df.select(F.max(F.col(incr_col))).collect()[0][0]
            return str(max_val) if max_val is not None else None
        except Exception:
            return None

    # ------------------------------------------------------------------ helpers
    def _paths_for(self, load_date: str) -> Tuple[str, str, str]:
        return Paths.build(self.runtime_cfg, self.schema, self.table, load_date)
