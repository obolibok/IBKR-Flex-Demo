import os
import datetime as dt
import pandas as pd

from scripts.jobs.base import FlexJob, JobContext, JobResult
from scripts.ibkr_flex_client import flex_download_statement
from scripts.parse_cash_transactions import parse_cash_transactions
from scripts.etl_manifest import register_asset

class CashTransactionsJob(FlexJob):
    def ensure_storage(self, ctx: JobContext) -> None:
        con = ctx.con
        con.execute("""CREATE TABLE IF NOT EXISTS bronze.cash_transactions(
                    accountId VARCHAR,
                    currency VARCHAR,
                    fxRateToBase DOUBLE,
                    assetCategory VARCHAR,
                    subCategory VARCHAR,
                    symbol VARCHAR,
                    description VARCHAR,
                    conid DOUBLE,
                    securityID VARCHAR,
                    securityIDType VARCHAR,
                    cusip VARCHAR,
                    isin VARCHAR,
                    figi VARCHAR,
                    listingExchange VARCHAR,
                    issuerCountryCode VARCHAR,
                    multiplier DOUBLE,
                    dateTime TIMESTAMP_NS,
                    settleDate DATE,
                    availableForTradingDate DATE,
                    reportDate DATE,
                    amount DOUBLE,
                    "type" VARCHAR,
                    transactionID VARCHAR,
                    clientReference VARCHAR,
                    actionID VARCHAR,
                    sourceHashHex VARCHAR);""")
        super().ensure_storage(ctx)

    def run_bronze(self, ctx: JobContext) -> JobResult:
        con = ctx.con
        cfg = ctx.cfg
        now_utc = dt.datetime.utcnow()

        state = con.execute(
            "SELECT last_success_utc FROM etl.job_state WHERE job_id = ?",
            [self.job.id],
        ).fetchone()

        if state and state[0] is not None:
            age_sec = (now_utc - state[0]).total_seconds()
            if age_sec < cfg.etl.min_seconds_between_runs:
                return JobResult(self.job.id, self.job.query_id, "skipped", "throttled", None, None, 0)

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
            df = pd.DataFrame(parse_cash_transactions(xml_bytes))

            if df.empty:
                return JobResult(self.job.id, self.job.query_id, "ok", "no_rows", ref, None, 0)

            con.register("df_in", df)

            con.execute("""
                DELETE FROM bronze.cash_transactions
                WHERE sourceHashHex IN (
                    SELECT sourceHashHex
                    FROM df_in
                    WHERE sourceHashHex IS NOT NULL
                );
            """)

            con.execute("""INSERT INTO bronze.cash_transactions (accountId,currency,fxRateToBase,assetCategory,subCategory,symbol,"description",conid,securityID,securityIDType,cusip,isin,figi,listingExchange,
                                                                issuerCountryCode,multiplier,"dateTime",settleDate,availableForTradingDate,reportDate,amount,"type",transactionID,clientReference,actionID,sourceHashHex)
                        SELECT accountId,currency,fxRateToBase,assetCategory,subCategory,symbol,"description",conid,securityID,securityIDType,cusip,isin,figi,listingExchange,
                                issuerCountryCode,multiplier,"dateTime",settleDate,availableForTradingDate,reportDate,amount,"type",transactionID,clientReference,actionID,sourceHashHex
                        FROM df_in;""")

            max_report_date = con.execute("SELECT MAX(reportDate) FROM df_in;").fetchone()[0]
            return JobResult(self.job.id, self.job.query_id, "ok", "downloaded", ref, max_report_date, int(len(df)))

        except Exception as e:
            return JobResult(self.job.id, self.job.query_id, "error", f"{type(e).__name__}: {e}", None, None, 0)

    def update_silver(self, ctx: JobContext) -> None:
        # нужно подтянуть символы из бронзы, чтобы потом юзать их в других джобах
        con = ctx.con
        con.execute("""
            INSERT INTO silver.symbols (map_to, symbol, conid, description, first_seen, src)
            SELECT NULL AS map_to, tr.symbol, tr.conid, tr.description, MIN(tr.reportDate) AS first_seen, 'cash_transactions' AS src
            FROM bronze.cash_transactions tr
                LEFT JOIN silver.symbols sy ON sy.symbol = tr.symbol and sy.conid = tr.conid and sy.description = tr.description
            WHERE tr.conid IS NOT NULL AND sy.symbol IS NULL
            GROUP BY tr.symbol, tr.conid, tr.description;""")
        super().update_silver(ctx)

    def build_gold(self, ctx: JobContext) -> None:
        con = ctx.con
        gold_dir = os.path.join(ctx.cfg.paths.gold_root, "cash_transactions")
        os.makedirs(gold_dir, exist_ok=True)

        history_path = os.path.join(gold_dir, "history.parquet")
        history_sql = history_path.replace("\\", "/").replace("'", "''")

        con.execute(f"""
            COPY (
                SELECT COALESCE(b.map_to,a.symbol) AS symbol, a.currency, a.fxRateToBase, a.assetCategory, a.subCategory, a.description, a.issuerCountryCode, a.multiplier, a."dateTime", a.settleDate, a.availableForTradingDate, a.reportDate, a.amount, a."type"
                FROM bronze.cash_transactions a
                LEFT JOIN silver.symbols AS b on b.symbol=a.symbol and b.conid = a.conid and a.description = b.description
            )
            TO '{history_sql}'
            (FORMAT parquet, COMPRESSION zstd);
        """)

        register_asset(con, "cash_transactions_history", history_path, "file", None, "Cash transactions history (single parquet)")
        super().build_gold(ctx)