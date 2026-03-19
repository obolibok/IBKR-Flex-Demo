import os
import datetime as dt
import pandas as pd

from scripts.jobs.base import FlexJob, JobContext, JobResult
from scripts.ibkr_flex_client import flex_download_statement
from scripts.parse_corporate_actions import parse_corporate_actions
from scripts.etl_manifest import register_asset


class CorporateActionsJob(FlexJob):
    def ensure_storage(self, ctx: JobContext) -> None:
        con = ctx.con
        con.execute("""
            CREATE TABLE IF NOT EXISTS bronze.corporate_actions(
                accountId VARCHAR,
                model VARCHAR,
                currency VARCHAR,
                fxRateToBase DOUBLE,

                assetCategory VARCHAR,
                subCategory VARCHAR,
                symbol VARCHAR,
                underlyingSymbol VARCHAR,
                conid BIGINT,
                underlyingConid BIGINT,
                listingExchange VARCHAR,
                underlyingListingExchange VARCHAR,

                transactionID VARCHAR,
                actionID BIGINT,
                type VARCHAR,
                code VARCHAR,

                reportDate DATE,
                dateTime TIMESTAMP_NS,

                description VARCHAR,
                actionDescription VARCHAR,

                quantity DOUBLE,
                amount DOUBLE,
                proceeds DOUBLE,
                value DOUBLE,
                costBasis DOUBLE,
                fifoPnlRealized DOUBLE,
                mtmPnl DOUBLE,

                securityID VARCHAR,
                securityIDType VARCHAR,
                cusip VARCHAR,
                isin VARCHAR,
                figi VARCHAR,

                underlyingSecurityID VARCHAR,

                issuer VARCHAR,
                issuerCountryCode VARCHAR,

                multiplier DOUBLE,
                strike DOUBLE,
                expiry DATE,
                putCall VARCHAR,
                principalAdjustFactor DOUBLE,

                levelOfDetail VARCHAR,
                serialNumber VARCHAR,
                deliveryType VARCHAR,
                commodityType VARCHAR,
                fineness DOUBLE,
                weight DOUBLE,

                sourceHashHex VARCHAR
            );
        """)
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
            xml_bytes, ref = flex_download_statement(cfg=cfg.ibkr, query_id=self.job.query_id, poll_seconds=cfg.etl.poll_seconds,
                                                    max_wait_seconds=cfg.etl.max_wait_seconds, initial_wait_seconds=cfg.etl.initial_wait_seconds,
                                                    cycle_attempts=4, cycle_sleep_seconds=cfg.etl.pause_between_jobs_seconds,)
            rows = parse_corporate_actions(xml_bytes)
            if not rows:
                return JobResult(self.job.id, self.job.query_id, "ok", "downloaded-empty", ref, None, 0,)
            df = pd.DataFrame(rows)
            con.register("df_in", df)

            con.execute("""
                DELETE FROM bronze.corporate_actions
                WHERE sourceHashHex IN (
                    SELECT sourceHashHex
                    FROM df_in
                    WHERE sourceHashHex IS NOT NULL
                );
            """)

            con.execute("""
                INSERT INTO bronze.corporate_actions (
                    accountId, model, currency, fxRateToBase,
                    assetCategory, subCategory, symbol, underlyingSymbol, conid, underlyingConid,
                    listingExchange, underlyingListingExchange,
                    transactionID, actionID, type, code,
                    reportDate, dateTime,
                    description, actionDescription,
                    quantity, amount, proceeds, value, costBasis, fifoPnlRealized, mtmPnl,
                    securityID, securityIDType, cusip, isin, figi,
                    underlyingSecurityID,
                    issuer, issuerCountryCode,
                    multiplier, strike, expiry, putCall, principalAdjustFactor,
                    levelOfDetail, serialNumber, deliveryType, commodityType, fineness, weight,
                    sourceHashHex
                )
                SELECT
                    accountId, model, currency, fxRateToBase,
                    assetCategory, subCategory, symbol, underlyingSymbol, conid, underlyingConid,
                    listingExchange, underlyingListingExchange,
                    transactionID, actionID, type, code,
                    reportDate, dateTime,
                    description, actionDescription,
                    quantity, amount, proceeds, value, costBasis, fifoPnlRealized, mtmPnl,
                    securityID, securityIDType, cusip, isin, figi,
                    underlyingSecurityID,
                    issuer, issuerCountryCode,
                    multiplier, strike, expiry, putCall, principalAdjustFactor,
                    levelOfDetail, serialNumber, deliveryType, commodityType, fineness, weight,
                    sourceHashHex
                FROM df_in;
            """)

            max_report_date = con.execute("SELECT MAX(reportDate) FROM df_in;").fetchone()[0]
            return JobResult(self.job.id, self.job.query_id, "ok", "downloaded", ref, max_report_date, int(len(df)))

        except Exception as e:
            return JobResult(self.job.id, self.job.query_id, "error", f"{type(e).__name__}: {e}", None, None, 0)

    def update_silver(self, ctx: JobContext) -> None:
        con = ctx.con

        con.execute("""
            INSERT INTO silver.symbols (map_to, symbol, conid, description, first_seen, src)
            SELECT
                NULL AS map_to,
                ca.symbol,
                ca.conid,
                COALESCE(ca.actionDescription, ca.description) AS description,
                MIN(ca.reportDate) AS first_seen,
                'corporate_actions' AS src
            FROM bronze.corporate_actions ca
            LEFT JOIN silver.symbols sy
                ON sy.symbol = ca.symbol
               AND sy.conid = ca.conid
               AND sy.description = COALESCE(ca.actionDescription, ca.description)
            WHERE ca.conid IS NOT NULL
              AND ca.assetCategory = 'STK'
              AND sy.symbol IS NULL
            GROUP BY ca.symbol, ca.conid, COALESCE(ca.actionDescription, ca.description);
        """)

        super().update_silver(ctx)

    def build_gold(self, ctx: JobContext) -> None:
        con = ctx.con
        gold_dir = os.path.join(ctx.cfg.paths.gold_root, "corporate_actions")
        os.makedirs(gold_dir, exist_ok=True)

        history_path = os.path.join(gold_dir, "history.parquet")
        history_sql = history_path.replace("\\", "/").replace("'", "''")

        con.execute(f"""
            COPY (
                SELECT
                    COALESCE(sy.map_to, ca.symbol) AS symbol,
                    ca.currency,
                    ca.assetCategory,
                    ca.subCategory,
                    ca.reportDate,
                    ca.dateTime,
                    ca.type,
                    ca.code,
                    ca.quantity,
                    ca.amount,
                    ca.proceeds,
                    ca.value,
                    ca.costBasis,
                    ca.fifoPnlRealized,
                    ca.mtmPnl,
                    ca.securityIDType,
                    ca.listingExchange,
                    ca.description,
                    ca.actionDescription
                FROM bronze.corporate_actions ca
                LEFT JOIN silver.symbols sy ON sy.symbol = ca.symbol AND sy.conid = ca.conid AND ca.description = sy.description
            )
            TO '{history_sql}'
            (FORMAT parquet, COMPRESSION zstd);
        """)

        register_asset(
            con,
            "corporate_actions_history",
            history_path,
            "file",
            None,
            "Corporate actions history (single parquet)"
        )

        super().build_gold(ctx)