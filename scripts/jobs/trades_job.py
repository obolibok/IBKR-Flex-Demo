import os
import datetime as dt
import pandas as pd

from scripts.jobs.base import FlexJob, JobContext, JobResult
from scripts.ibkr_flex_client import flex_download_statement
from scripts.parse_trades import parse_trades
from scripts.etl_manifest import register_asset


class TradesJob(FlexJob):
    def ensure_storage(self, ctx: JobContext) -> None:
        con = ctx.con
        con.execute("""CREATE TABLE IF NOT EXISTS bronze.trades(
                    accountId VARCHAR,
                    currency VARCHAR,
                    assetCategory VARCHAR,
                    symbol VARCHAR,
                    conid BIGINT,
                    listingExchange VARCHAR,
                    tradeID BIGINT,
                    transactionID VARCHAR,
                    ibExecID VARCHAR,
                    ibOrderID BIGINT,
                    reportDate DATE,
                    tradeDate DATE,
                    dateTime TIMESTAMP_NS,
                    orderTime TIMESTAMP_NS,
                    transactionType VARCHAR,
                    exchange VARCHAR,
                    buySell VARCHAR,
                    openCloseIndicator VARCHAR,
                    quantity DOUBLE,
                    tradePrice DOUBLE,
                    tradeMoney DOUBLE,
                    proceeds DOUBLE,
                    taxes DOUBLE,
                    ibCommission DOUBLE,
                    ibCommissionCurrency VARCHAR,
                    netCash DOUBLE,
                    closePrice DOUBLE,
                    "cost" DOUBLE,
                    fifoPnlRealized DOUBLE,
                    mtmPnl DOUBLE,
                    orderType VARCHAR,
                    orderReference VARCHAR,
                    brokerageOrderID VARCHAR,
                    isAPIOrder BOOLEAN,
                    description VARCHAR,
                    fxRateToBase DOUBLE,
                    subCategory VARCHAR,
                    multiplier DOUBLE,
                    settleDateTarget DATE,
                    sourceHashHex VARCHAR);""")
        super().ensure_storage(ctx)

    def run_bronze(self, ctx: JobContext) -> JobResult:
        con = ctx.con
        cfg = ctx.cfg
        now_utc = dt.datetime.utcnow()

        # throttling по etl.job_state, как у тебя уже сделано в общей схеме
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
                                                    cycle_sleep_seconds=cfg.etl.pause_between_jobs_seconds,)
            df = pd.DataFrame(parse_trades(xml_bytes))
            con.register("df_in", df)

            # дедуп: ежедневные 30 дней будут пересекаться — убираем то, что уже было
            con.execute("""
                DELETE FROM bronze.trades
                WHERE sourceHashHex IN (SELECT sourceHashHex FROM df_in);
            """)
            con.execute("""
                INSERT INTO bronze.trades (accountId,currency,assetCategory,symbol,conid,listingExchange,tradeID,transactionID,ibExecID,ibOrderID,reportDate,tradeDate,"dateTime",orderTime,transactionType,
                            exchange,buySell,openCloseIndicator,quantity,tradePrice,tradeMoney,proceeds,taxes,ibCommission,ibCommissionCurrency,netCash,closePrice,"cost",fifoPnlRealized,
                            mtmPnl,orderType,orderReference,brokerageOrderID,isAPIOrder,"description",fxRateToBase,subCategory,multiplier,settleDateTarget,sourceHashHex)
                    SELECT  accountId,currency,assetCategory,symbol,conid,listingExchange,tradeID,transactionID,ibExecID,ibOrderID,reportDate,tradeDate,"dateTime",orderTime,transactionType,
                            exchange,buySell,openCloseIndicator,quantity,tradePrice,tradeMoney,proceeds,taxes,ibCommission,ibCommissionCurrency,netCash,closePrice,"cost",fifoPnlRealized,
                            mtmPnl,orderType,orderReference,brokerageOrderID,isAPIOrder,"description",fxRateToBase,subCategory,multiplier,settleDateTarget,sourceHashHex
                    FROM df_in;
            """)

            max_trade_date = con.execute("SELECT MAX(tradeDate) FROM df_in;").fetchone()[0]
            return JobResult(self.job.id, self.job.query_id, "ok", "downloaded", ref, max_trade_date, int(len(df)))

        except Exception as e:
            return JobResult(self.job.id, self.job.query_id, "error", f"{type(e).__name__}: {e}", None, None, 0)

    def update_silver(self, ctx: JobContext) -> None:
        # нужно подтянуть символы из бронзы, чтобы потом юзать их в других джобах
        con = ctx.con
        con.execute("""
            INSERT INTO silver.symbols (map_to, symbol, conid, description, first_seen, src)
            SELECT NULL AS map_to, tr.symbol, tr.conid, tr.description, MIN(tr.reportDate) AS first_seen, 'trades' AS src
            FROM bronze.trades tr
                LEFT JOIN silver.symbols sy ON sy.symbol = tr.symbol and sy.conid = tr.conid and sy.description = tr.description
            WHERE tr.conid IS NOT NULL AND tr.assetCategory='STK' AND sy.symbol IS NULL
            GROUP BY tr.symbol, tr.conid, tr.description;""")
        super().update_silver(ctx)

    def build_gold(self, ctx: JobContext) -> None:
        con = ctx.con
        gold_dir = os.path.join(ctx.cfg.paths.gold_root, "trades")
        os.makedirs(gold_dir, exist_ok=True)

        history_path = os.path.join(gold_dir, "history.parquet")
        history_sql = history_path.replace("\\", "/").replace("'", "''")

        con.execute(f"""
            COPY (
            SELECT COALESCE(b.map_to,a.symbol) AS symbol, a.currency, a.assetCategory, a.reportDate, a."dateTime", a.orderTime, a.transactionType, a.buySell,
                    a.openCloseIndicator, a.quantity, a.tradePrice, a.tradeMoney, a.proceeds, a.taxes, a.ibCommission, a.ibCommissionCurrency, a.netCash, a.closePrice,
                    a."cost", a.fifoPnlRealized, a.mtmPnl, a.orderType, a.fxRateToBase, a.subCategory, a.multiplier, a.settleDateTarget
            FROM bronze.trades AS a
            LEFT JOIN silver.symbols AS b ON b.symbol=a.symbol and b.conid = a.conid and a.description = b.description
            )
            TO '{history_sql}'
            (FORMAT parquet, COMPRESSION zstd);
        """)

        register_asset(con, "trades_history", history_path, "file", None, "Trades history (single parquet)")
        super().build_gold(ctx)
