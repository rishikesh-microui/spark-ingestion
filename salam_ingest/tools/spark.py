from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import SparkSession

from ..io.filesystem import HDFSUtil
from .base import ExecutionTool, QueryRequest, WriteRequest


class SparkTool(ExecutionTool):
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self._current_pool: Optional[str] = None
        self._current_group: Optional[str] = None

    def query(self, request: QueryRequest):
        reader = self.spark.read.format(request.format)
        for key, value in request.options.items():
            reader = reader.option(key, value)
        if request.partition_options:
            for key, value in request.partition_options.items():
                reader = reader.option(key, value)
        return reader.load()

    def query_scalar(self, request: QueryRequest):
        df = self.query(request)
        row = df.collect()[0]
        return row[0] if row else None

    def write_dataset(self, request: WriteRequest) -> None:
        writer = request.dataset.write.format(request.format).mode(request.mode)
        if request.options:
            for k, v in request.options.items():
                writer = writer.option(k, v)
        writer.save(request.path)

    def write_text(self, path: str, content: str) -> None:
        HDFSUtil.write_text(self.spark, path, content)

    @classmethod
    def from_config(cls, cfg: Dict[str, Any]) -> "SparkTool":
        runtime = cfg.get("runtime", {})
        builder = SparkSession.builder.appName(runtime.get("app_name", "spark_ingest_framework"))
        builder = builder.config("spark.dynamicAllocation.enabled", str(runtime.get("dynamicAllocation", "true")))
        builder = builder.config("spark.dynamicAllocation.initialExecutors", str(runtime.get("initialExecutors", "1")))
        builder = builder.config("spark.dynamicAllocation.maxExecutors", str(runtime.get("maxExecutors", "2")))
        builder = builder.config("spark.shuffle.service.enabled", "true")
        builder = builder.config(
            "spark.sql.parquet.int96RebaseModeInWrite",
            runtime.get("int96RebaseModeInWrite", "LEGACY"),
        )
        builder = builder.config("spark.sql.decimalOperations.allowPrecisionLoss", "true")
        builder = builder.config("spark.executor.cores", str(runtime.get("executor.cores", "4")))
        builder = builder.config("spark.executor.memory", str(runtime.get("executor.memory", "8g")))
        builder = builder.config("spark.driver.memory", str(runtime.get("driver.memory", "6g")))
        builder = builder.config("spark.scheduler.mode", runtime.get("scheduler.mode", "FAIR"))
        builder = builder.config(
            "spark.sql.sources.partitionOverwriteMode",
            runtime.get("partitionOverwriteMode", "dynamic"),
        )
        builder = builder.config("spark.sql.session.timeZone", runtime.get("timezone", "Asia/Kolkata"))
        extra_jars = runtime.get("extra_jars", [])
        if extra_jars:
            builder = builder.config("spark.jars", ",".join(extra_jars))
        extra_conf: Dict[str, Any] = runtime.get("spark_conf", {})
        for key, value in extra_conf.items():
            builder = builder.config(key, value)
        if runtime.get("enable_hive_support", True):
            builder = builder.enableHiveSupport()
        spark = builder.getOrCreate()
        return cls(spark)

    def stop(self) -> None:
        if self.spark is not None:
            self.spark.stop()

    def set_job_context(self, *, pool: Optional[str], group_id: Optional[str], description: Optional[str]) -> None:
        sc = self.spark.sparkContext
        if pool:
            sc.setLocalProperty("spark.scheduler.pool", pool)
            self._current_pool = pool
        if group_id is not None or description is not None:
            sc.setJobGroup(group_id or "", description or "")
            self._current_group = group_id

    def clear_job_context(self) -> None:
        sc = self.spark.sparkContext
        sc.setLocalProperty("spark.scheduler.pool", None)
        sc.setJobGroup("", "")
        self._current_pool = None
        self._current_group = None
