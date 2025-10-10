import traceback
from typing import Any, Dict, List, Optional, Protocol

from pyspark.sql import SparkSession

from .common import with_ingest_cols
from .endpoints.base import (
    IncrementalContext,
    IngestionSlice,
    SinkEndpoint,
    SourceEndpoint,
    SliceStageResult,
)
from .planning.base import Planner, PlannerRequest
from .io import Paths, Utils
from .tools.base import ExecutionTool
from .events import emit_state_mark, emit_state_watermark


class ExecutionContext:
    """Abstraction around the execution engine (Spark today, pluggable later)."""

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        emitter=None,
        tool: Optional[ExecutionTool] = None,
    ) -> None:
        self.spark = spark
        self.emitter = emitter
        self.tool = tool

    def read_full(self, source: SourceEndpoint) -> Any:
        if self.tool is None:
            raise RuntimeError("Execution tool is required for source operations")
        return source.read_full()

    def read_slice(self, source: SourceEndpoint, slice_info: IngestionSlice) -> Any:
        if self.tool is None:
            raise RuntimeError("Execution tool is required for source operations")
        return source.read_slice(lower=slice_info.lower, upper=slice_info.upper)
class Strategy(Protocol):
    mode: str

    def run(
        self,
        context: ExecutionContext,
        cfg: Dict[str, Any],
        state: Any,
        logger: Any,
        source_endpoint: SourceEndpoint,
        sink_endpoint: SinkEndpoint,
        planner: Planner,
        request: PlannerRequest,
    ) -> Dict[str, Any]:
        ...


class FullRefreshStrategy:
    mode = "full"

    def run(
        self,
        context: ExecutionContext,
        cfg: Dict[str, Any],
        state: Any,
        logger: Any,
        source_endpoint: SourceEndpoint,
        sink_endpoint: SinkEndpoint,
        planner: Planner,
        request: PlannerRequest,
    ) -> Dict[str, Any]:
        spark = context.spark
        if spark is None:
            raise RuntimeError("Spark context required for full refresh strategy")
        schema, table, load_date = request.schema, request.table, request.load_date
        rt = cfg["runtime"]
        raw_dir, final_dir, _ = Paths.build(rt, schema, table, load_date)

        status = state.get_day_status(schema, table, load_date)
        logger.info("full_status", schema=schema, table=table, **status, load_date=load_date)
        if status.get("finalized_done"):
            emit_state_mark(
                context,
                state,
                schema=schema,
                table=table,
                load_date=load_date,
                mode="full",
                phase="raw",
                status="skipped",
                location=raw_dir,
            )
            emit_state_mark(
                context,
                state,
                schema=schema,
                table=table,
                load_date=load_date,
                mode="full",
                phase="finalize",
                status="skipped",
                location=final_dir,
            )
            logger.info("full_skip_finalized", schema=schema, table=table, load_date=load_date)
            return {"table": f"{schema}.{table}", "skipped": True, "reason": "already finalized today"}

        result: Dict[str, Any]
        if status.get("raw_done") and not status.get("finalized_done"):
            logger.info("full_finalize_only", schema=schema, table=table, raw_dir=raw_dir)
            result = {"table": f"{schema}.{table}", "mode": "full", "raw": raw_dir}
            rows = None
        else:
            df = with_ingest_cols(context.read_full(source_endpoint))
            rows = df.count()
            emit_state_mark(
                context,
                state,
                schema=schema,
                table=table,
                load_date=load_date,
                mode="full",
                phase="raw",
                status="started",
                rows_written=rows,
                location=raw_dir,
            )
            write_result = sink_endpoint.write_raw(
                df,
                mode="overwrite",
                load_date=load_date,
                schema=schema,
                table=table,
                rows=rows,
            )
            emit_state_mark(
                context,
                state,
                schema=schema,
                table=table,
                load_date=load_date,
                mode="full",
                phase="raw",
                status="success",
                rows_written=rows,
                location=write_result.path,
                extra=write_result.event_payload,
            )
            logger.info("full_raw_written", schema=schema, table=table, rows=rows, raw_dir=write_result.path)
            result = {"table": f"{schema}.{table}", "mode": "full", "rows": rows, "raw": write_result.path}
            result.update({k: v for k, v in write_result.event_payload.items() if k not in result})

        if not rt.get("finalize_full_refresh", True):
            return result

        emit_state_mark(
            context,
            state,
            schema=schema,
            table=table,
            load_date=load_date,
            mode="full",
            phase="finalize",
            status="started",
            location=final_dir,
            strategy=rt.get("finalize_strategy"),
        )
        try:
            finalize_result = sink_endpoint.finalize_full(load_date=load_date, schema=schema, table=table)
            emit_state_mark(
                context,
                state,
                schema=schema,
                table=table,
                load_date=load_date,
                mode="full",
                phase="finalize",
                status="success",
                location=final_dir,
                strategy=rt.get("finalize_strategy"),
                extra=finalize_result.event_payload,
            )
            logger.info(
                "full_finalized",
                schema=schema,
                table=table,
                final_dir=final_dir,
                details=finalize_result.event_payload,
            )
            result.update({"final": final_dir, "finalized": True, **{k: v for k, v in finalize_result.event_payload.items() if k not in result}})
        except Exception as exc:
            emit_state_mark(
                context,
                state,
                schema=schema,
                table=table,
                load_date=load_date,
                mode="full",
                phase="finalize",
                status="failed",
                location=final_dir,
                error=str(exc),
            )
            logger.error("full_finalize_failed", schema=schema, table=table, err=str(exc))
            raise
        return result


class Scd1Strategy:
    mode = "scd1"

    def run(
        self,
        context: ExecutionContext,
        cfg: Dict[str, Any],
        state: Any,
        logger: Any,
        source_endpoint: SourceEndpoint,
        sink_endpoint: SinkEndpoint,
        planner: Planner,
        request: PlannerRequest,
    ) -> Dict[str, Any]:
        spark = context.spark
        if spark is None:
            raise RuntimeError("Spark context required for SCD1 strategy")
        schema, table, load_date = request.schema, request.table, request.load_date
        tbl_cfg = request.table_cfg.get("table", {})
        rt = cfg["runtime"]
        raw_dir, _, _ = Paths.build(rt, schema, table, load_date)
        incr_col = tbl_cfg["incremental_column"]
        initial_wm = tbl_cfg.get("initial_watermark", "1900-01-01 00:00:00")
        pk_cols = list(tbl_cfg.get("primary_keys", []))

        last_wm, last_ld = state.get_progress(schema, table, default_wm=initial_wm, default_date="1900-01-01")
        logger.info("scd1_progress", schema=schema, table=table, wm=last_wm, last_ld=last_ld, load_date=load_date)

        incr_type = (tbl_cfg.get("incr_col_type") or "").lower()
        lag = int(tbl_cfg.get("lag_seconds", cfg["runtime"].get("wm_lag_seconds", 0)))
        if incr_type == "epoch_seconds":
            eff_wm = Utils.minus_seconds_epoch(last_wm, lag, millis=False)
        elif incr_type == "epoch_millis":
            eff_wm = Utils.minus_seconds_epoch(last_wm, lag, millis=True)
        else:
            eff_wm = Utils.minus_seconds_datetime(last_wm, lag)
        is_int_epoch = incr_type in ("int", "integer", "bigint", "epoch", "epoch_seconds", "epoch_millis")
        logger.info(
            "scd1_effective_wm",
            schema=schema,
            table=table,
            last_wm=last_wm,
            lag_seconds=lag,
            effective_wm=eff_wm,
        )

        request.last_watermark = eff_wm
        plan = planner.build_plan(source_endpoint, request)
        plan_slices = plan.slices or [
            {"lower": eff_wm, "upper": plan.metadata.get("now_literal")}
        ]  # type: ignore[arg-type]
        ingestion_slices = [IngestionSlice(lower=s["lower"], upper=s.get("upper")) for s in plan_slices]
        logger.info("scd1_planned_slices", schema=schema, table=table, n=len(ingestion_slices))

        incremental_ctx = IncrementalContext(
            schema=schema,
            table=table,
            load_date=load_date,
            incremental_column=incr_col,
            incremental_type=incr_type,
            primary_keys=pk_cols,
            effective_watermark=eff_wm,
            last_watermark=last_wm,
            last_loaded_date=last_ld,
            planner_metadata=plan.metadata,
            is_epoch=is_int_epoch,
        )

        staged_results: List[SliceStageResult] = []
        for slice_obj in ingestion_slices:
            df_slice = with_ingest_cols(context.read_slice(source_endpoint, slice_obj))
            stage_result = sink_endpoint.stage_incremental_slice(
                df_slice,
                context=incremental_ctx,
                slice_info=slice_obj,
            )
            if stage_result.skipped:
                logger.info(
                    "slice_skip_found_success",
                    schema=schema,
                    table=table,
                    lo=slice_obj.lower,
                    hi=slice_obj.upper,
                    path=stage_result.path,
                )
            else:
                logger.info(
                    "slice_staged",
                    schema=schema,
                    table=table,
                    lo=slice_obj.lower,
                    hi=slice_obj.upper,
                    path=stage_result.path,
                    rows=stage_result.rows,
                )
            staged_results.append(stage_result)

        emit_state_mark(
            context,
            state,
            schema=schema,
            table=table,
            load_date=load_date,
            mode="scd1",
            phase="intermediate",
            status="started",
        )
        emit_state_mark(
            context,
            state,
            schema=schema,
            table=table,
            load_date=load_date,
            mode="scd1",
            phase="raw",
            status="started",
            location=raw_dir,
        )

        try:
            commit_result = sink_endpoint.commit_incremental(
                load_date=load_date,
                schema=schema,
                table=table,
                context=incremental_ctx,
                staged_slices=staged_results,
            )
            emit_state_mark(
                context,
                state,
                schema=schema,
                table=table,
                load_date=load_date,
                mode="scd1",
                phase="intermediate",
                status="success",
                extra=commit_result.intermediate_event_payload,
            )
        except Exception as exc:
            traceback.print_exc()
            emit_state_mark(
                context,
                state,
                schema=schema,
                table=table,
                load_date=load_date,
                mode="scd1",
                phase="intermediate",
                status="failed",
                error=str(exc),
            )
            logger.error("scd1_intermediate_failed", schema=schema, table=table, err=str(exc))
            raise

        emit_state_mark(
            context,
            state,
            schema=schema,
            table=table,
            load_date=load_date,
            mode="scd1",
            phase="raw",
            status="success",
            rows_written=commit_result.rows,
            location=commit_result.raw_path,
            extra=commit_result.raw_event_payload,
        )

        emit_state_mark(
            context,
            state,
            schema=schema,
            table=table,
            load_date=load_date,
            mode="scd1",
            phase="watermark",
            status="success",
            watermark=commit_result.new_watermark,
            extra=commit_result.additional_metadata,
        )
        emit_state_watermark(
            context,
            state,
            schema=schema,
            table=table,
            watermark=commit_result.new_watermark,
            last_loaded_date=commit_result.new_loaded_date,
            extra=commit_result.additional_metadata,
        )
        logger.info(
            "scd1_wm_advanced",
            schema=schema,
            table=table,
            new_wm=commit_result.new_watermark,
            new_ld=commit_result.new_loaded_date,
        )

        result = {
            "table": f"{schema}.{table}",
            "mode": "scd1",
            "rows": commit_result.rows,
            "raw": commit_result.raw_path,
            "wm": commit_result.new_watermark,
            "last_loaded_date": commit_result.new_loaded_date,
        }
        result.update(commit_result.intermediate_event_payload)
        result.update(commit_result.additional_metadata)
        return result


# Registry of available strategies keyed by mode name
STRATEGY_REGISTRY: Dict[str, Strategy] = {
    "full": FullRefreshStrategy(),
    "scd1": Scd1Strategy(),
}

# Backwards-compatible exports
IngestionFull = STRATEGY_REGISTRY["full"]
IngestionSCD1 = STRATEGY_REGISTRY["scd1"]
