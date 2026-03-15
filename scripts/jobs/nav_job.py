from __future__ import annotations

import os
import datetime as dt
from typing import Optional

import duckdb
import pandas as pd

from scripts.jobs.base import FlexJob, JobContext, JobResult
from scripts.ibkr_flex_client import flex_send_request, flex_get_statement_wait_query
from scripts.parse_nav import parse_nav
from scripts.etl_manifest import register_asset


def _get_job_state(con: duckdb.DuckDBPyConnection, job_id: str):
    row = con.execute(
        "SELECT last_success_utc, last_report_date FROM etl.job_state WHERE job_id = ?",
        [job_id],
    ).fetchone()
    if not row:
        return None, None
    return row[0], row[1]


def _dedup_nav_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # На случай, если accountId где-то пустой
    if "accountId" in df.columns:
        df["accountId"] = df["accountId"].fillna("")

    return (
        df.sort_values(["accountId", "reportDate", "sourceHash"])
        .drop_duplicates(subset=["accountId", "reportDate"], keep="last")
        .reset_index(drop=True)
    )


class NavJob(FlexJob):
    def ensure_storage(self, ctx: JobContext) -> None:
        con = ctx.con
        con.execute("""
            CREATE TABLE IF NOT EXISTS bronze.account_nav_daily (
                accountId VARCHAR,
                currency VARCHAR,
                reportDate DATE,
                cash DOUBLE,
                stock DOUBLE,
                options DOUBLE,
                funds DOUBLE,
                dividendAccruals DOUBLE,
                interestAccruals DOUBLE,
                forexCfdUnrealizedPl DOUBLE,
                cfdUnrealizedPl DOUBLE,
                crypto DOUBLE,
                total DOUBLE,
                totalLong DOUBLE,
                totalShort DOUBLE,
                sourceHash VARCHAR
            );
        """)
        super().ensure_storage(ctx)

    def run_bronze(self, ctx: JobContext) -> JobResult:
        con = ctx.con
        cfg = ctx.cfg

        now_utc = dt.datetime.utcnow()
        last_success_utc, last_report_date = _get_job_state(con, self.job.id)

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
            ref = flex_send_request(cfg.ibkr, self.job.query_id)
            xml_bytes = flex_get_statement_wait_query(
                cfg.ibkr.base_url,
                cfg.ibkr.token,
                ref,
                cfg.ibkr.version,
                cfg.etl.poll_seconds,
                cfg.etl.max_wait_seconds,
            )

            rows = parse_nav(xml_bytes)
            df = pd.DataFrame(rows)
            df = _dedup_nav_df(df)

            report_date: Optional[dt.date] = None
            if not df.empty and "reportDate" in df.columns and df["reportDate"].notna().any():
                report_date = df["reportDate"].max()

            con.register("df_in", df)

            # Удаляем пересекающиеся accountId + reportDate и вставляем свежие строки
            con.execute("""
                DELETE FROM bronze.account_nav_daily t
                USING df_in s
                WHERE COALESCE(t.accountId, '') = COALESCE(s.accountId, '')
                  AND t.reportDate = s.reportDate;
            """)

            con.execute("""
                INSERT INTO bronze.account_nav_daily (accountId,currency,reportDate,cash,stock,options,funds,dividendAccruals,interestAccruals,forexCfdUnrealizedPl,cfdUnrealizedPl,crypto,total,totalLong,totalShort,sourceHash)
                SELECT  accountId,currency,reportDate,cash,stock,options,funds,dividendAccruals,interestAccruals,forexCfdUnrealizedPl,cfdUnrealizedPl,crypto,total,totalLong,totalShort,sourceHash
                FROM df_in;
            """)

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
        # no symbol-level data, only aggregated NAV, so no silver update needed
        super().update_silver(ctx)

    def build_gold(self, ctx: JobContext) -> None:
        con = ctx.con
        gold_dir = os.path.join(ctx.cfg.paths.gold_root, "account_nav")
        os.makedirs(gold_dir, exist_ok=True)

        latest_path = os.path.join(gold_dir, "latest.parquet")
        latest_sql = latest_path.replace("\\", "/").replace("'", "''")

        history_path = os.path.join(gold_dir, "history.parquet")
        history_sql = history_path.replace("\\", "/").replace("'", "''")

        con.execute(f"""
            COPY (
                SELECT currency, reportDate, cash, stock, "options", funds, dividendAccruals, interestAccruals, forexCfdUnrealizedPl, cfdUnrealizedPl, crypto, total, totalLong, totalShort
                FROM bronze.account_nav_daily
                WHERE reportDate = (SELECT MAX(reportDate) FROM bronze.account_nav_daily)
            )
            TO '{latest_sql}'
            (FORMAT parquet, COMPRESSION zstd);
        """)

        con.execute(f"""
            COPY (
                SELECT currency, reportDate, cash, stock, "options", funds, dividendAccruals, interestAccruals, forexCfdUnrealizedPl, cfdUnrealizedPl, crypto, total, totalLong, totalShort
                FROM bronze.account_nav_daily
                ORDER BY accountId, reportDate
            )
            TO '{history_sql}'
            (FORMAT parquet, COMPRESSION zstd);
        """)

        register_asset(con, "account_nav_latest", latest_path, "file", None, "Latest account NAV snapshot")
        register_asset(con, "account_nav_history", history_path, "file", None, "Account NAV history")
        super().build_gold(ctx)