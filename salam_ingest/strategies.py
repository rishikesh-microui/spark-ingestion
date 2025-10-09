import json
import traceback
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from .common import with_ingest_cols
from .endpoints.base import SinkEndpoint, SourceEndpoint
from .planning.base import IngestionPlan, Planner, PlannerRequest
from .io import HiveHelper, IcebergHelper, Paths, RawIO, Utils
from .staging import Staging


class IngestionFull:
    @staticmethod
    def run(
        spark: SparkSession,
        cfg: Dict[str, Any],
        state: Any,
        logger: Any,
        source_endpoint: SourceEndpoint,
        sink_endpoint: SinkEndpoint,
        planner: Planner,
        request: PlannerRequest,
    ) -> Dict[str, Any]:
        schema, table, load_date = request.schema, request.table, request.load_date
        rt = cfg["runtime"]
        raw_dir, final_dir, _ = Paths.build(rt, schema, table, load_date)

        status = state.get_day_status(schema, table, load_date)
        logger.info("full_status", schema=schema, table=table, **status, load_date=load_date)
        if status.get("finalized_done"):
            state.mark_event(schema, table, load_date, "full", "raw", "skipped", location=raw_dir)
            state.mark_event(schema, table, load_date, "full", "finalize", "skipped", location=final_dir)
            logger.info("full_skip_finalized", schema=schema, table=table, load_date=load_date)
            if rt.get("hive", {}).get("enabled", False):
                hive_db = rt["hive"]["db"]
                HiveHelper.create_unpartitioned_parquet_if_absent_with_schema(spark, hive_db, schema, table, final_dir)
            return {"table": f"{schema}.{table}", "skipped": True, "reason": "already finalized today"}

        result: Dict[str, Any]
        if status.get("raw_done") and not status.get("finalized_done"):
            logger.info("full_finalize_only", schema=schema, table=table, raw_dir=raw_dir)
            result = {"table": f"{schema}.{table}", "mode": "full", "raw": raw_dir}
            rows = None
        else:
            df = with_ingest_cols(source_endpoint.read_full())
            rows = df.count()
            state.mark_event(schema, table, load_date, "full", "raw", "started", location=raw_dir, rows_written=rows)
            write_info = sink_endpoint.write_raw(
                df,
                mode="overwrite",
                load_date=load_date,
                schema=schema,
                table=table,
                rows=rows,
            )
            state.mark_event(
                schema,
                table,
                load_date,
                "full",
                "raw",
                "success",
                rows_written=rows,
                location=write_info["raw_dir"],
            )
            logger.info("full_raw_written", schema=schema, table=table, rows=rows, raw_dir=write_info["raw_dir"])
            result = {"table": f"{schema}.{table}", "mode": "full", "rows": rows, "raw": write_info["raw_dir"]}

        if not rt.get("finalize_full_refresh", True):
            return result

        state.mark_event(
            schema,
            table,
            load_date,
            "full",
            "finalize",
            "started",
            location=final_dir,
            strategy=rt.get("finalize_strategy"),
        )
        try:
            finalize_info = sink_endpoint.finalize_full(load_date=load_date, schema=schema, table=table)
            state.mark_event(
                schema,
                table,
                load_date,
                "full",
                "finalize",
                "success",
                location=final_dir,
                strategy=rt.get("finalize_strategy"),
            )
            logger.info(
                "full_finalized",
                schema=schema,
                table=table,
                final_dir=final_dir,
                hive_table=finalize_info.get("hive_table"),
            )
            if finalize_info.get("hive_table"):
                result["hive_table"] = finalize_info["hive_table"]
            result.update({"final": final_dir, "finalized": True})
        except Exception as exc:
            state.mark_event(
                schema,
                table,
                load_date,
                "full",
                "finalize",
                "failed",
                location=final_dir,
                error=str(exc),
            )
            logger.error("full_finalize_failed", schema=schema, table=table, err=str(exc))
            raise
        return result


class IngestionSCD1:
    @staticmethod
    def run(
        spark: SparkSession,
        cfg: Dict[str, Any],
        state: Any,
        logger: Any,
        source_endpoint: SourceEndpoint,
        sink_endpoint: SinkEndpoint,
        planner: Planner,
        request: PlannerRequest,
    ) -> Dict[str, Any]:
        schema, table, load_date = request.schema, request.table, request.load_date
        tbl_cfg = request.table_cfg.get("table", {})
        rt = cfg["runtime"]
        raw_dir, _, base_raw = Paths.build(rt, schema, table, load_date)
        incr_col = tbl_cfg["incremental_column"]
        initial_wm = tbl_cfg.get("initial_watermark", "1900-01-01 00:00:00")
        pk_cols = tbl_cfg.get("primary_keys", [])

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
        plan = planner.build_plan(spark, source_endpoint, request)
        slices = plan.slices or [{"lower": eff_wm, "upper": plan.metadata.get("now_literal")}]
        logger.info("scd1_planned_slices", schema=schema, table=table, n=len(slices))

        staged_dirs: List[str] = []
        for slice_info in slices:
            lo = slice_info["lower"]
            hi = slice_info.get("upper")
            sdir = Staging.slice_dir(cfg, schema, table, incr_col, lo, hi or plan.metadata.get("now_literal"))
            if Staging.is_success(spark, sdir):
                logger.info("slice_skip_found_success", schema=schema, table=table, lo=lo, hi=hi, path=sdir)
                staged_dirs.append(sdir)
                continue
            df_slice = with_ingest_cols(source_endpoint.read_slice(lower=lo, upper=hi))
            (
                df_slice.write.format(rt.get("write_format", "parquet"))
                .mode("overwrite")
                .option("compression", rt.get("compression", "snappy"))
                .save(sdir)
            )
            Staging.write_text(
                spark,
                f"{sdir}/_RANGE.json",
                json.dumps({"lo": lo, "hi": hi, "planned_at": plan.metadata.get("now_literal")}),
            )
            Staging.mark_success(spark, sdir)
            logger.info("slice_staged", schema=schema, table=table, lo=lo, hi=hi, path=sdir)
            staged_dirs.append(sdir)

        if not staged_dirs:
            logger.info("scd1_no_slices_staged", schema=schema, table=table)
            return {
                "table": f"{schema}.{table}",
                "mode": "scd1",
                "rows": 0,
                "raw": raw_dir,
                "wm": last_wm,
                "last_loaded_date": last_ld,
            }

        window_df = spark.read.format(rt.get("write_format", "parquet")).load(staged_dirs)
        to_merge_count = window_df.count()
        logger.info("scd1_window_ready", schema=schema, table=table, rows=to_merge_count)

        # Intermediate (Iceberg) merge
        inter_cfg = rt.get("intermediate", {"enabled": False})
        state.mark_event(schema, table, load_date, "scd1", "intermediate", "started")
        info: Dict[str, Any] = {}
        ok = True
        if inter_cfg.get("enabled", False):
            try:
                if inter_cfg.get("type", "iceberg") == "iceberg" and pk_cols:
                    tgt = IcebergHelper.merge_upsert(
                        spark,
                        cfg,
                        schema,
                        table,
                        window_df,
                        pk_cols,
                        load_date,
                        partition_col=rt.get("hive_reg", {}).get("partition_col", "load_date"),
                        incr_col=incr_col,
                    )
                    info["iceberg_table"] = tgt
                    logger.info("scd1_intermediate_merged", schema=schema, table=table, iceberg_table=tgt)
                    if rt.get("final_parquet_mirror", {"enabled": False}).get("enabled", False):
                        info.update(IcebergHelper.mirror_to_parquet_for_date(spark, cfg, schema, table, load_date))
                else:
                    raise NotImplementedError("Parquet intermediary is not supported currently.")
                state.mark_event(schema, table, load_date, "scd1", "intermediate", "success")
            except Exception as exc:
                traceback.print_exc()
                ok = False
                info["intermediate_error"] = str(exc)
                state.mark_event(
                    schema,
                    table,
                    load_date,
                    "scd1",
                    "intermediate",
                    "failed",
                    error=str(exc),
                )
                logger.error("scd1_intermediate_failed", schema=schema, table=table, err=str(exc))

        if not ok:
            return {
                "table": f"{schema}.{table}",
                "mode": "scd1",
                "rows": to_merge_count,
                "raw": raw_dir,
                "wm": last_wm,
                "last_loaded_date": last_ld,
                **info,
            }

        for sdir in staged_dirs:
            df_staged = spark.read.format(rt.get("write_format", "parquet")).load(sdir)
            rows_staged = df_staged.count()
            state.mark_event(
                schema,
                table,
                load_date,
                "scd1",
                "raw",
                "started",
                location=raw_dir,
                rows_written=rows_staged,
            )
            write_info = sink_endpoint.append_incremental(
                df_staged,
                load_date=load_date,
                schema=schema,
                table=table,
                mode="append",
                rows=rows_staged,
            )
            state.mark_event(
                schema,
                table,
                load_date,
                "scd1",
                "raw",
                "success",
                rows_written=write_info["rows"],
                location=write_info["raw_dir"],
            )
            Staging.mark_landed(spark, sdir)

        raw_to_merge = RawIO.raw_increment_df(
            spark,
            rt,
            schema,
            table,
            last_ld,
            eff_wm,
            incr_col,
            incr_type,
        )
        new_wm, new_ld = IcebergHelper._compute_wm_ld(raw_to_merge, incr_col, is_int_epoch)
        state.mark_event(
            schema,
            table,
            load_date,
            "scd1",
            "watermark",
            "success",
            watermark=new_wm,
        )
        state.set_progress(schema, table, watermark=new_wm, last_loaded_date=new_ld)
        logger.info("scd1_wm_advanced", schema=schema, table=table, new_wm=new_wm, new_ld=new_ld)

        return {
            "table": f"{schema}.{table}",
            "mode": "scd1",
            "rows": to_merge_count,
            "raw": raw_dir,
            "wm": new_wm,
            "last_loaded_date": new_ld,
            **info,
        }
