from __future__ import annotations

import os
import datetime as dt
from typing import Optional

import duckdb
import pandas as pd

from scripts.jobs.base import FlexJob, JobContext, JobResult
from scripts.ibkr_flex_client import flex_download_statement
from scripts.parse_positions import parse_positions
from scripts.etl_manifest import register_asset

def _get_job_state(con: duckdb.DuckDBPyConnection, job_id: str):
    row = con.execute(
        "SELECT last_success_utc, last_report_date FROM etl.job_state WHERE job_id = ?",
        [job_id],
    ).fetchone()
    if not row:
        return None, None
    return row[0], row[1]


class PositionsJob(FlexJob):
    def ensure_storage(self, ctx: JobContext) -> None:
        con = ctx.con
        con.execute("""CREATE TABLE IF NOT EXISTS bronze.positions_snapshot(
                    accountId VARCHAR,
                    currency VARCHAR,
                    fxRateToBase DOUBLE,
                    assetCategory VARCHAR,
                    subCategory VARCHAR,
                    symbol VARCHAR,
                    description VARCHAR,
                    conid BIGINT,
                    securityID VARCHAR,
                    securityIDType VARCHAR,
                    cusip VARCHAR,
                    isin VARCHAR,
                    listingExchange VARCHAR,
                    multiplier DOUBLE,
                    reportDate DATE,
                    "position" DOUBLE,
                    markPrice DOUBLE,
                    positionValue DOUBLE,
                    openPrice DOUBLE,
                    costBasisPrice DOUBLE,
                    costBasisMoney DOUBLE,
                    percentOfNAV DOUBLE,
                    fifoPnlUnrealized DOUBLE,
                    side VARCHAR,
                    sourceHash VARCHAR);""")
        super().ensure_storage(ctx)

    def run_bronze(self, ctx: JobContext) -> JobResult:
        con = ctx.con
        cfg = ctx.cfg

        now_utc = dt.datetime.utcnow()

        last_success_utc, last_report_date = _get_job_state(con, self.job.id)

        # throttling
        if last_success_utc is not None:
            age_sec = (now_utc - last_success_utc).total_seconds()
            if age_sec < cfg.etl.min_seconds_between_runs:
                return JobResult(
                    job_id=self.job.id,
                    query_id=self.job.query_id,
                    status="skipped",
                    reason="throttled",
                    reference_code=None,
                    report_date=last_report_date,
                    rows=0,
                )

        try:
            xml_bytes, ref = flex_download_statement(
                                                    cfg=cfg.ibkr,
                                                    query_id=self.job.query_id,
                                                    poll_seconds=cfg.etl.poll_seconds,
                                                    max_wait_seconds=cfg.etl.max_wait_seconds,
                                                    initial_wait_seconds=cfg.etl.initial_wait_seconds,
                                                    cycle_attempts=4,
                                                    cycle_sleep_seconds=cfg.etl.pause_between_jobs_seconds,
                                                )
            rows = parse_positions(xml_bytes)
            df = pd.DataFrame(rows)

            report_date: Optional[dt.date] = None
            if not df.empty and "reportDate" in df.columns and df["reportDate"].notna().any():
                report_date = df["reportDate"].dropna().iloc[0]

            # создаём таблицу по df на первом успешном получении
            con.register("df_in", df)

            # 1) если df содержит много дат (история) — удаляем все эти даты
            con.execute("""
                DELETE FROM bronze.positions_snapshot
                WHERE reportDate IN (
                    SELECT DISTINCT reportDate
                    FROM df_in
                    WHERE reportDate IS NOT NULL
                );
            """)

            # 2) подстраховка: дедуп по sourceHash (на случай NULL reportDate или пересечений)
            con.execute("""
                DELETE FROM bronze.positions_snapshot
                WHERE sourceHash IN (SELECT sourceHash FROM df_in);
            """)

            # 3) вставляем новый пакет
            con.execute("""INSERT INTO bronze.positions_snapshot (accountId,currency,fxRateToBase,assetCategory,subCategory,symbol,"description",conid,securityID,securityIDType,cusip,isin,listingExchange,multiplier,reportDate,"position",markPrice,positionValue,openPrice,costBasisPrice,costBasisMoney,percentOfNAV,fifoPnlUnrealized,side,sourceHash)
                        SELECT accountId,currency,fxRateToBase,assetCategory,subCategory,symbol,"description",conid,securityID,securityIDType,cusip,isin,listingExchange,multiplier,reportDate,"position",markPrice,positionValue,openPrice,costBasisPrice,costBasisMoney,percentOfNAV,fifoPnlUnrealized,side,sourceHash
                        FROM df_in;""")

            return JobResult(
                job_id=self.job.id,
                query_id=self.job.query_id,
                status="ok",
                reason="downloaded",
                reference_code=ref,
                report_date=report_date,
                rows=int(len(df)),
            )

        except Exception as e:
            return JobResult(
                job_id=self.job.id,
                query_id=self.job.query_id,
                status="error",
                reason=f"{type(e).__name__}: {e}",
                reference_code=None,
                report_date=None,
                rows=0,
            )

    def update_silver(self, ctx: JobContext) -> None:
        # нужно подтянуть символы из бронзы, чтобы потом юзать их в других джобах
        con = ctx.con
        con.execute("""
            INSERT INTO silver.symbols (map_to, symbol, conid, description, first_seen, src)
            SELECT NULL AS map_to, tr.symbol, tr.conid, tr.description, MIN(tr.reportDate) AS first_seen, 'positions' AS src
            FROM bronze.positions_snapshot tr
                LEFT JOIN silver.symbols sy ON sy.symbol = tr.symbol and sy.conid = tr.conid and sy.description = tr.description
            WHERE tr.conid IS NOT NULL AND sy.symbol IS NULL
            GROUP BY tr.symbol, tr.conid, tr.description;""")
        super().update_silver(ctx)

    def build_gold(self, ctx: JobContext) -> None:
        con = ctx.con
        gold_dir = os.path.join(ctx.cfg.paths.gold_root, "positions")
        os.makedirs(gold_dir, exist_ok=True)

        # 1) Latest snapshot (как и было)
        latest_path = os.path.join(gold_dir, "latest.parquet")
        latest_sql = latest_path.replace("\\", "/").replace("'", "''")

        con.execute(f"""
            COPY (
                SELECT currency, fxRateToBase, assetCategory, subCategory, symbol, description, multiplier, reportDate, "position", markPrice, positionValue, openPrice, costBasisPrice, costBasisMoney, percentOfNAV, fifoPnlUnrealized
                FROM bronze.positions_snapshot
                WHERE reportDate = (SELECT MAX(reportDate) FROM bronze.positions_snapshot)
            )
            TO '{latest_sql}'
            (FORMAT parquet, COMPRESSION zstd);
        """)

        # 2) Full history in ONE file (вместо тысяч партиций)
        history_path = os.path.join(gold_dir, "history.parquet")
        history_sql = history_path.replace("\\", "/").replace("'", "''")

        con.execute(f"""
            COPY (
                SELECT COALESCE(b.map_to,a.symbol) AS symbol, currency, fxRateToBase, assetCategory, subCategory, multiplier, reportDate, "position", markPrice, positionValue, openPrice, costBasisPrice, costBasisMoney, percentOfNAV, fifoPnlUnrealized
                FROM bronze.positions_snapshot AS a
                LEFT JOIN silver.symbols AS b on b.symbol=a.symbol and b.conid = a.conid and a.description = b.description
            )
            TO '{history_sql}'
            (FORMAT parquet, COMPRESSION zstd);
        """)

        # manifest
        register_asset(ctx.con, "positions_latest", latest_path, "file", None, "Latest positions snapshot")
        register_asset(ctx.con, "positions_history", history_path, "file", None, "Positions history (single parquet)")
        super().build_gold(ctx)
