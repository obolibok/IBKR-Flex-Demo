from __future__ import annotations

import os
import importlib
import datetime as dt
from typing import Type
import time

import duckdb
import pandas as pd

from scripts.config_helpers import cfg_load_config, cfg_ensure_dirs
from scripts.etl_meta import etl_ensure_meta
from scripts.jobs.base import FlexJob, JobContext, JobResult
from scripts.etl_manifest import write_manifest_parquet
from scripts.data_classes import PhaseLogRecord
from scripts.obfuscation import obfuscate_gold_outputs


def _load_job_class(handler: str) -> Type[FlexJob]:
    mod_name, cls_name = handler.split(":")
    mod = importlib.import_module(mod_name)
    return getattr(mod, cls_name)


def _job_result_to_phase_log(run_utc: dt.datetime, phase: str, res: JobResult) -> PhaseLogRecord:
    return PhaseLogRecord(
        run_utc=run_utc,
        phase=phase,
        job_id=res.job_id,
        query_id=res.query_id,
        status=res.status,
        reason=res.reason,
        reference_code=res.reference_code,
        report_date=res.report_date,
        rows=res.rows,
    )


def _phase_log_to_row(rec: PhaseLogRecord) -> dict:
    return {
        "run_utc": rec.run_utc,
        "phase": rec.phase,
        "job_id": rec.job_id,
        "query_id": rec.query_id,
        "status": rec.status,
        "reason": rec.reason,
        "reference_code": rec.reference_code,
        "report_date": rec.report_date,
        "rows": rec.rows,
    }


def _write_phase_log(con, rec: PhaseLogRecord) -> None:
    con.execute("""
        INSERT INTO etl.job_runs(
            run_utc,
            phase,
            job_id,
            query_id,
            status,
            reference_code,
            report_date,
            rows,
            error
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        rec.run_utc,
        rec.phase,
        rec.job_id,
        rec.query_id,
        rec.status,
        rec.reference_code,
        rec.report_date,
        rec.rows,
        rec.reason if rec.status == "error" else None,
    ])


def _update_job_state_from_bronze(con, run_utc: dt.datetime, res: JobResult) -> None:
    if res.status == "ok":
        con.execute("""
            INSERT INTO etl.job_state(
                job_id,
                last_success_utc,
                last_report_date,
                last_reference_code,
                last_rows,
                last_error
            )
            VALUES (?, ?, ?, ?, ?, NULL)
            ON CONFLICT(job_id) DO UPDATE SET
                last_success_utc = excluded.last_success_utc,
                last_report_date = excluded.last_report_date,
                last_reference_code = excluded.last_reference_code,
                last_rows = excluded.last_rows,
                last_error = NULL
        """, [res.job_id, run_utc, res.report_date, res.reference_code, res.rows])

    elif res.status == "error":
        con.execute("""
            INSERT INTO etl.job_state(job_id, last_error)
            VALUES (?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                last_error = excluded.last_error
        """, [res.job_id, res.reason])


def _export_status_gold(con, gold_root: str) -> None:
    status_path = os.path.join(gold_root, "etl", "status_latest.parquet")
    os.makedirs(os.path.dirname(status_path), exist_ok=True)

    con.execute(f"""
        COPY (
            SELECT *
            FROM etl.job_state
            ORDER BY job_id
        )
        TO '{status_path.replace("'", "''")}'
        (FORMAT parquet, COMPRESSION zstd);
    """)


def _build_jobs(cfg) -> list[FlexJob]:
    jobs: list[FlexJob] = []

    for job_cfg in cfg.flex_jobs:
        if not job_cfg.enabled:
            continue

        job_cls = _load_job_class(job_cfg.handler)
        jobs.append(job_cls(job=job_cfg, cfg=cfg))

    return jobs


def _make_phase_ok(run_utc: dt.datetime, phase: str, job: FlexJob) -> PhaseLogRecord:
    return PhaseLogRecord(
        run_utc=run_utc,
        phase=phase,
        job_id=job.job.id,
        query_id=job.job.query_id,
        status="ok",
    )


def _make_phase_error(run_utc: dt.datetime, phase: str, job: FlexJob, ex: Exception) -> PhaseLogRecord:
    return PhaseLogRecord(
        run_utc=run_utc,
        phase=phase,
        job_id=job.job.id,
        query_id=job.job.query_id,
        status="error",
        reason=f"{type(ex).__name__}: {ex}",
    )


def run_update(kit_root: str, config_path: str) -> pd.DataFrame:
    cfg = cfg_load_config(config_path)

    cfg.paths.kit_root = kit_root
    cfg.paths.duckdb_path = cfg.paths.duckdb_path.replace("{kit_root}", kit_root)
    cfg.paths.gold_root = cfg.paths.gold_root.replace("{kit_root}", kit_root)

    cfg_ensure_dirs(cfg)

    con = duckdb.connect(cfg.paths.duckdb_path)
    etl_ensure_meta(con)

    ctx = JobContext(cfg=cfg, con=con)
    run_utc = dt.datetime.utcnow()
    pause_seconds = getattr(cfg.etl, "pause_between_jobs_seconds", 3)

    jobs = _build_jobs(cfg)
    result_rows: list[dict] = []

    # -------------------------
    # PHASE 1: BRONZE
    # -------------------------
    if ctx.cfg.etl.bronze:
        for job in jobs:
            try:
                job.ensure_storage(ctx)
                bronze_res = job.run_bronze(ctx)
            except Exception as ex:
                bronze_res = JobResult(
                    job_id=job.job.id,
                    query_id=job.job.query_id,
                    status="error",
                    reason=f"{type(ex).__name__}: {ex}",
                    reference_code=None,
                    report_date=None,
                    rows=0,
                )

            rec = _job_result_to_phase_log(run_utc, "bronze", bronze_res)
            _write_phase_log(con, rec)
            _update_job_state_from_bronze(con, run_utc, bronze_res)
            result_rows.append(_phase_log_to_row(rec))

            time.sleep(pause_seconds)

    # -------------------------
    # PHASE 2: SILVER
    # -------------------------
    if ctx.cfg.etl.silver:
        for job in jobs:
            try:
                job.update_silver(ctx)
                rec = _make_phase_ok(run_utc, "silver", job)
            except Exception as ex:
                rec = _make_phase_error(run_utc, "silver", job, ex)

            _write_phase_log(con, rec)
            result_rows.append(_phase_log_to_row(rec))

    # -------------------------
    # PHASE 3: GOLD
    # -------------------------
    if ctx.cfg.etl.gold:
        for job in jobs:
            try:
                job.build_gold(ctx)
                rec = _make_phase_ok(run_utc, "gold", job)
            except Exception as ex:
                rec = _make_phase_error(run_utc, "gold", job, ex)

            _write_phase_log(con, rec)
            result_rows.append(_phase_log_to_row(rec))

        write_manifest_parquet(con, cfg.paths.gold_root)

    if ctx.cfg.etl.obfuscate:
        obf_ctx = obfuscate_gold_outputs(con)

        result_rows.append({
            "run_utc": run_utc,
            "phase": "obfuscate",
            "job_id": "_system",
            "query_id": None,
            "status": "ok",
            "reason": None,
            "reference_code": None,
            "report_date": None,
            "rows": len(obf_ctx.symbol_map),
        })

    _export_status_gold(con, cfg.paths.gold_root)

    return pd.DataFrame(result_rows)
