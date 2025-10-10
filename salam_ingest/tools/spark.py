from __future__ import annotations

from pyspark.sql import SparkSession

from ..io import HDFSUtil
from .base import ExecutionTool, QueryRequest, WriteRequest


class SparkTool(ExecutionTool):
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

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
