import json
import time
import traceback
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from pyspark.sql import SparkSession

from .common import RUN_ID, with_ingest_cols
from .io import AdaptiveSlicePlanner, HiveHelper, IcebergHelper, JDBC, RawIO, Utils
from .staging import Staging


class IngestionFull:
    @staticmethod
    def run(
        spark: SparkSession,
        cfg: Dict[str, Any],
        state: Any,
        logger: Any,
        schema: str,
        table: str,
        load_date: str,
        raw_dir: str,
        final_dir: str,
        tbl_cfg: Dict[str, Any],
    ) -> Dict[str, Any]:
        rt, jdbc = cfg["runtime"], cfg["jdbc"]
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
        reader = JDBC.reader(spark, jdbc, schema, table, tbl_cfg)
        if status.get("raw_done") and not status.get("finalized_done"):
            result = {"table": f"{schema}.{table}", "mode": "full", "raw": raw_dir}
            logger.info("full_finalize_only", schema=schema, table=table, raw_dir=raw_dir)
        else:
            df = reader.load()
            df = with_ingest_cols(df)
            state.mark_event(schema, table, load_date, "full", "raw", "started", location=raw_dir)
            (
                df.write.format(rt.get("write_format", "parquet"))
                .mode("overwrite")
                .option("compression", rt.get("compression", "snappy"))
                .save(raw_dir)
            )
            rows = spark.read.format(rt.get("write_format", "parquet")).load(raw_dir).count()
            state.mark_event(
                schema,
                table,
                load_date,
                "full",
                "raw",
                "success",
                rows_written=rows,
                location=raw_dir,
            )
            logger.info("full_raw_written", schema=schema, table=table, rows=rows, raw_dir=raw_dir)
            result = {"table": f"{schema}.{table}", "mode": "full", "rows": rows, "raw": raw_dir}
        finalized = False
        hive_table: Optional[str] = None
        if rt.get("finalize_full_refresh", True):
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
                if rt.get("finalize_strategy") == "hive_set_location" and rt.get("hive", {}).get("enabled", False):
                    hive_table = HiveHelper.set_location(spark, rt["hive"]["db"], schema, table, raw_dir)
                else:
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
                    ok = jvm.org.apache.hadoop.fs.FileUtil.copy(fs, src, fs, tmp, False, conf)
                    if not ok:
                        raise RuntimeError(f"Copy {raw_dir} -> {final_dir}/.__tmp_swap__ failed")
                    if fs.exists(dst):
                        fs.delete(dst, True)
                    if not fs.rename(tmp, dst):
                        raise RuntimeError(f"Atomic rename failed for {final_dir}")
                finalized = True
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
                    hive_table=hive_table,
                )
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
            if finalized and rt.get("hive", {}).get("enabled", False):
                hive_db = rt["hive"]["db"]
                HiveHelper.create_unpartitioned_parquet_if_absent_with_schema(spark, hive_db, schema, table, final_dir)
        result.update({"final": final_dir, "finalized": finalized, "hive_table": hive_table})
        return result


class IngestionSCD1:
    @staticmethod
    def run(
        spark: SparkSession,
        cfg: Dict[str, Any],
        state: Any,
        logger: Any,
        schema: str,
        table: str,
        load_date: str,
        raw_dir: str,
        base_raw: str,
        tbl_cfg: Dict[str, Any],
    ) -> Dict[str, Any]:
        rt, jdbc = cfg["runtime"], cfg["jdbc"]
        incr_col = tbl_cfg["incremental_column"]
        initial_wm = tbl_cfg.get("initial_watermark", "1900-01-01 00:00:00")
        pk_cols = tbl_cfg.get("primary_keys", [])
        last_wm, last_ld = state.get_progress(schema, table, default_wm=initial_wm, default_date="1900-01-01")
        logger.info(
            "scd1_progress",
            schema=schema,
            table=table,
            wm=last_wm,
            last_ld=last_ld,
            load_date=load_date,
        )
        incr_type = (tbl_cfg.get("incr_col_type") or "").lower()
        lag = int(tbl_cfg.get("lag_seconds", cfg["runtime"].get("wm_lag_seconds", 0)))
        if incr_type == "epoch_seconds":
            eff_wm = Utils.minus_seconds_epoch(last_wm, lag, millis=False)
        elif incr_type == "epoch_millis":
            eff_wm = Utils.minus_seconds_epoch(last_wm, lag, millis=True)
        else:
            eff_wm = Utils.minus_seconds_datetime(last_wm, lag)
        logger.info(
            "scd1_effective_wm",
            schema=schema,
            table=table,
            last_wm=last_wm,
            lag_seconds=lag,
            effective_wm=eff_wm,
        )
        base_from = JDBC.build_from_sql(jdbc, schema, table, tbl_cfg)
        is_int_epoch = str(incr_type).lower() in ("int", "bigint", "epoch", "epoch_seconds", "epoch_millis")
        now_lit = datetime.now().strftime("%Y-%m-%d %H:%M:%S") if not is_int_epoch else str(int(time.time()))
        slices = [(eff_wm, now_lit)]
        slicing_cfg = rt.get("scd1_slicing", {})
        if slicing_cfg.get("enabled", False):
            slices = AdaptiveSlicePlanner.plan(
                spark, logger, jdbc, tbl_cfg, base_from, incr_col, eff_wm, now_lit, is_int_epoch, slicing_cfg
            )
        logger.info("scd1_planned_slices", schema=schema, table=table, n=len(slices))
        staged_dirs = []
        for lo, hi in slices:
            sdir = Staging.slice_dir(cfg, schema, table, incr_col, lo, hi)
            if Staging.is_success(spark, sdir):
                logger.info("slice_skip_found_success", schema=schema, table=table, lo=lo, hi=hi)
                staged_dirs.append(sdir)
                continue
            dbtable = JDBC.dbtable_for_range(jdbc, base_from, incr_col, tbl_cfg, lo, hi)
            reader = (
                spark.read.format("jdbc")
                .option("url", jdbc["url"])
                .option("dbtable", dbtable)
                .option("user", jdbc["user"])
                .option("password", jdbc["password"])
                .option("driver", jdbc["driver"])
                .option("fetchsize", int(jdbc.get("fetchsize", 10000)))
            )
            df_slice = with_ingest_cols(reader.load())
            (
                df_slice.write.format(rt.get("write_format", "parquet"))
                .mode("overwrite")
                .option("compression", rt.get("compression", "snappy"))
                .save(sdir)
            )
            Staging.write_text(
                spark,
                f"{sdir}/_RANGE.json",
                json.dumps({"lo": lo, "hi": hi, "planned_at": datetime.now().isoformat()}),
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
                        info.update(
                            IcebergHelper.mirror_to_parquet_for_date(spark, cfg, schema, table, load_date)
                        )
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
            df = spark.read.format(rt.get("write_format", "parquet")).load(sdir)
            RawIO.land_append(spark, logger, df, rt, raw_dir, state, schema, table, load_date, "scd1")
            Staging.mark_landed(spark, sdir)
        raw_to_merge = RawIO.raw_increment_df(
            spark, rt, schema, table, last_ld, eff_wm, incr_col, incr_type
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
