"""Strategy unit tests using lightweight stubs instead of a Spark cluster."""

import sys
import types
import unittest
from unittest.mock import patch
from typing import Optional

# Minimal pyspark stubs so modules import without the real dependency.
ps = types.ModuleType("pyspark")
sql = types.ModuleType("pyspark.sql")
functions = types.ModuleType("pyspark.sql.functions")
types_module = types.ModuleType("pyspark.sql.types")
types_module.StructType = lambda fields=None: fields
types_module.StructField = lambda name, dtype, nullable=True: (name, dtype, nullable)
types_module.StringType = lambda: "string"
types_module.LongType = lambda: "long"
types_module.DateType = lambda: "date"
window_module = types.ModuleType("pyspark.sql.window")

class _StubSparkSession:
    pass

sql.SparkSession = _StubSparkSession
sql.Row = lambda *args, **kwargs: None
sql.DataFrame = object
functions.current_timestamp = lambda: None
functions.lit = lambda v: v
functions.col = lambda name: name
functions.to_timestamp = lambda value: value
functions.desc = lambda col: col
functions.max = lambda col: col
functions.to_date = lambda col: col
functions.first = lambda col, ignorenulls=None: col
functions.row_number = lambda: None
functions.monotonically_increasing_id = lambda: None


class _Window:
    @staticmethod
    def partitionBy(*args, **kwargs):
        return _Window()

    def orderBy(self, *args, **kwargs):
        return self

    def rangeBetween(self, *args, **kwargs):  # pragma: no cover
        return self


window_module.Window = _Window

ps.sql = sql
sys.modules.setdefault("pyspark", ps)
sys.modules.setdefault("pyspark.sql", sql)
sys.modules.setdefault("pyspark.sql.functions", functions)
sys.modules.setdefault("pyspark.sql.types", types_module)
sys.modules.setdefault("pyspark.sql.window", window_module)

from salam_ingest.common import PrintLogger
from salam_ingest.endpoints.base import (
    EndpointCapabilities,
    SinkEndpoint,
    SinkFinalizeResult,
    SinkWriteResult,
    SourceEndpoint,
)
from salam_ingest.planning.adaptive import AdaptivePlanner
from salam_ingest.planning.base import PlannerRequest
from salam_ingest.strategies import ExecutionContext, FullRefreshStrategy
from salam_ingest.tools.base import ExecutionTool


class MockDataFrame:
    def __init__(self, rows):
        self.rows = rows

    def count(self):
        return len(self.rows)


class MockTool(ExecutionTool):
    def __init__(self, dataset: MockDataFrame):
        self.dataset = dataset

    def query(self, request):  # type: ignore[override]
        return self.dataset

    def query_scalar(self, request):  # type: ignore[override]
        return len(self.dataset.rows)

    def write_dataset(self, request):  # type: ignore[override]
        pass

    def write_text(self, path: str, content: str) -> None:  # type: ignore[override]
        pass


class StaticSourceEndpoint(SourceEndpoint):
    def __init__(self, tool: ExecutionTool, rows):
        self.tool = tool
        self.dataframe = MockDataFrame(rows)
        self.schema = "demo"
        self.table = "orders"
        self._caps = EndpointCapabilities(supports_full=True)

    def configure(self, table_cfg):
        pass

    def capabilities(self):
        return self._caps

    def describe(self):
        return {"schema": self.schema, "table": self.table, "caps": self._caps}

    def read_full(self):
        return self.dataframe

    def read_slice(self, *, lower: str, upper: Optional[str]):  # pragma: no cover
        raise NotImplementedError

    def count_between(self, *, lower: str, upper: Optional[str]):  # pragma: no cover
        raise NotImplementedError


class CollectingSinkEndpoint(SinkEndpoint):
    def __init__(self):
        self.tool = None
        self.writes = []
        self.finalized = False
        self._caps = EndpointCapabilities(supports_write=True, supports_finalize=True)

    def configure(self, table_cfg):
        pass

    def capabilities(self):
        return self._caps

    def write_raw(self, df, *, mode, load_date, schema, table, rows=None):
        count = df.count() if rows is None else rows
        self.writes.append({"rows": count, "mode": mode, "load_date": load_date})
        return SinkWriteResult(rows=count, path=f"mock://{schema}/{table}/{load_date}")

    def finalize_full(self, *, load_date, schema, table):
        self.finalized = True
        return SinkFinalizeResult(final_path=f"mock://final/{schema}/{table}", event_payload={"finalized": True})


class MockState:
    def __init__(self):
        self.events = []
        self.progress = None

    def get_day_status(self, schema, table, load_date):
        return {"raw_done": False, "finalized_done": False}

    def mark_event(self, schema, table, load_date, mode, phase, status, **kwargs):
        self.events.append({
            "schema": schema,
            "table": table,
            "mode": mode,
            "phase": phase,
            "status": status,
        })

    def set_progress(self, schema, table, watermark, last_loaded_date):
        self.progress = (schema, table, watermark, last_loaded_date)


class FullStrategyTest(unittest.TestCase):
    """Verify the orchestration logic for full refresh without real Spark."""

    def test_full_refresh_happy_path(self):
        """Full refresh should write and finalise when nothing is cached."""
        dataset = MockDataFrame([("a", 1), ("b", 2)])
        tool = MockTool(dataset)
        source = StaticSourceEndpoint(tool, dataset.rows)
        sink = CollectingSinkEndpoint()
        state = MockState()
        logger = PrintLogger("test")
        context = ExecutionContext(spark="mock", tool=tool)
        planner = AdaptivePlanner()
        cfg = {"runtime": {"raw_root": "/tmp/raw", "final_root": "/tmp/final"}}
        request = PlannerRequest(schema="demo", table="orders", load_date="2024-01-01", mode="full")

        with patch("salam_ingest.strategies.with_ingest_cols", lambda df: df):
            result = FullRefreshStrategy().run(context, cfg, state, logger, source, sink, planner, request)

        self.assertTrue(sink.finalized)
        self.assertEqual(result["rows"], 2)

    def test_full_refresh_skips_when_already_finalized(self):
        """If the state reports the day as finalised, no work should be done."""

        class SkipState(MockState):
            def get_day_status(self, schema, table, load_date):  # pragma: no cover - simple override
                return {"raw_done": True, "finalized_done": True}

        dataset = MockDataFrame([])
        tool = MockTool(dataset)
        source = StaticSourceEndpoint(tool, dataset.rows)
        sink = CollectingSinkEndpoint()
        state = SkipState()
        logger = PrintLogger("test")
        context = ExecutionContext(spark="mock", tool=tool)
        planner = AdaptivePlanner()
        cfg = {"runtime": {"raw_root": "/tmp/raw", "final_root": "/tmp/final"}}
        request = PlannerRequest(schema="demo", table="orders", load_date="2024-01-01", mode="full")

        with patch("salam_ingest.strategies.with_ingest_cols", lambda df: df):
            result = FullRefreshStrategy().run(context, cfg, state, logger, source, sink, planner, request)

        self.assertEqual(sink.writes, [])
        self.assertFalse(sink.finalized)
        self.assertTrue(result.get("skipped"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
