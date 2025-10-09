from __future__ import annotations

import json
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession

from .base import EndpointCapabilities, SinkEndpoint
from ..common import RUN_ID
from ..io import HiveHelper  # reuse existing helper
from ..staging import Staging


class HdfsParquetEndpoint(SinkEndpoint):
    """Handles landing to RAW directories and optional Hive registration."""

    def __init__(self, spark: SparkSession, runtime_cfg: Dict[str, Any], table_cfg: Dict[str, Any]) -> None:
        self.spark = spark
        self.runtime_cfg = runtime_cfg
        self.table_cfg = table_cfg
        self.schema = table_cfg["schema"]
        self.table = table_cfg["table"]
        self.load_date = None
        self.raw_dir, self.final_dir, self.base_raw = self._build_paths("")
        self._caps = EndpointCapabilities(
            supports_write=True,
            supports_finalize=True,
        )

    def configure(self, spark: SparkSession, table_cfg: Dict[str, Any]) -> None:
        self.spark = spark
        self.table_cfg.update(table_cfg)
        self.schema = self.table_cfg["schema"]
        self.table = self.table_cfg["table"]
        self.raw_dir, self.final_dir, self.base_raw = self._build_paths(self.load_date or "")

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
        rows: int | None = None,
    ) -> Dict[str, Any]:
        self.load_date = load_date
        raw_dir, _, _ = self._build_paths(load_date)
        if rows is None:
            rows = df.count()
        (df.write.format(self.runtime_cfg.get("write_format", "parquet"))
         .mode(mode)
         .option("compression", self.runtime_cfg.get("compression", "snappy"))
         .save(raw_dir))
        # write metrics
        metrics = {"schema": schema, "table": table, "run_id": RUN_ID, "rows_written": rows,
                   "status": "success", "phase": "raw", "load_date": load_date}
        from ..io import HDFSUtil  # local import to avoid cycle
        HDFSUtil.write_text(
            spark=self.spark,
            path=f"{raw_dir}/_METRICS.json",
            content=json.dumps(metrics),
        )
        return {"raw_dir": raw_dir, "rows": rows}

    def append_incremental(
        self,
        df: DataFrame,
        *,
        load_date: str,
        schema: str,
        table: str,
        mode: str,
        rows: int | None = None,
    ) -> Dict[str, Any]:
        return self.write_raw(
            df,
            mode=mode,
            load_date=load_date,
            schema=schema,
            table=table,
            rows=rows,
        )

    def finalize_full(
        self,
        *,
        load_date: str,
        schema: str,
        table: str,
    ) -> Dict[str, Any]:
        _, final_dir, _ = self._build_paths(load_date)
        raw_dir, _, _ = self._build_paths(load_date)
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
        return {"final_dir": final_dir, "hive_table": hive_table}

    # helper utilities -----------------------------------------------------------
    def _build_paths(self, load_date: str):
        append_schema = self.runtime_cfg.get("append_table_schema", False)
        raw_dir = (
            f"{self.runtime_cfg['raw_root']}/{self.schema}/{self.table}/load_date={load_date}"
            if append_schema
            else f"{self.runtime_cfg['raw_root']}/{self.table}/load_date={load_date}"
        )
        final_dir = (
            f"{self.runtime_cfg['final_root']}/{self.schema}/{self.table}"
            if append_schema
            else f"{self.runtime_cfg['final_root']}/{self.table}"
        )
        base_raw = (
            f"{self.runtime_cfg['raw_root']}/{self.schema}/{self.table}"
            if append_schema
            else f"{self.runtime_cfg['raw_root']}/{self.table}"
        )
        return raw_dir, final_dir, base_raw
