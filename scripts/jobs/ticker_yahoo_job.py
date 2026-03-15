import os
import datetime as dt
from typing import Optional

import pandas as pd
import yfinance as yf

from scripts.jobs.base import FlexJob, JobContext, JobResult
from scripts.etl_manifest import register_asset


def _normalize_history_df(df: pd.DataFrame, symbol: str, source: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "symbol",
                "reportDate",
                "open",
                "high",
                "low",
                "close",
                "adjClose",
                "volume",
                "source",
            ]
        )

    out = df.reset_index().copy()

    # yfinance обычно отдаёт Date/DatetimeIndex
    if "Date" in out.columns:
        out.rename(columns={"Date": "reportDate"}, inplace=True)
    elif "Datetime" in out.columns:
        out.rename(columns={"Datetime": "reportDate"}, inplace=True)

    rename_map = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adjClose",
        "Volume": "volume",
    }
    out.rename(columns=rename_map, inplace=True)

    # Берём только то, что нам нужно
    keep_cols = ["reportDate", "open", "high", "low", "close", "adjClose", "volume"]
    out = out[keep_cols].copy()

    # Нормализуем дату до date без времени/таймзоны
    out["reportDate"] = pd.to_datetime(out["reportDate"]).dt.date

    out["symbol"] = symbol
    out["source"] = source

    # Типы
    numeric_cols = ["open", "high", "low", "close", "adjClose", "volume"]
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    # На всякий случай уберём дубли по symbol+date
    out = (
        out.sort_values(["reportDate"])
        .drop_duplicates(subset=["symbol", "reportDate"], keep="last")
        .reset_index(drop=True)
    )

    return out[
        [
            "symbol",
            "reportDate",
            "open",
            "high",
            "low",
            "close",
            "adjClose",
            "volume",
            "source",
        ]
    ]


def _download_yahoo_daily(
    symbol: str,
    start_date: Optional[dt.date] = None,
    end_date: Optional[dt.date] = None,
) -> pd.DataFrame:
    # end в yfinance обычно exclusive, поэтому для daily лучше +1 день
    yf_end = None
    if end_date is not None:
        yf_end = end_date + dt.timedelta(days=1)

    ticker = yf.Ticker(symbol)
    df = ticker.history(
        start=start_date.isoformat() if start_date else None,
        end=yf_end.isoformat() if yf_end else None,
        interval="1d",
        auto_adjust=False,
        actions=False,
    )

    return _normalize_history_df(df, symbol=symbol, source="yahoo")


class TickerYahooJob(FlexJob):
    def ensure_storage(self, ctx: JobContext) -> None:
        con = ctx.con
        con.execute("""CREATE TABLE IF NOT EXISTS bronze.tickers_daily (
                    symbol VARCHAR NOT NULL,
                    reportDate DATE NOT NULL,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    adjClose DOUBLE,
                    volume DOUBLE,
                    source VARCHAR NOT NULL);""")
        super().ensure_storage(ctx)

    def run_bronze(self, ctx: JobContext) -> JobResult:
        con = ctx.con
        cfg = ctx.cfg
        now_utc = dt.datetime.utcnow()
        symbol = (self.job.query_id or "").strip()

        if not symbol:
            return JobResult(self.job.id, self.job.query_id, "error", "Empty ticker symbol", None, None, 0)

        state = con.execute(
            "SELECT last_success_utc FROM etl.job_state WHERE job_id = ?",
            [self.job.id],
        ).fetchone()

        if state and state[0] is not None:
            age_sec = (now_utc - state[0]).total_seconds()
            if age_sec < cfg.etl.min_seconds_between_runs:
                return JobResult(self.job.id, self.job.query_id, "skipped", "throttled", None, None, 0)

        try:
            # Тянем хвост последних 5 дней, чтобы перекрыть выходные/праздники/коррекции
            end_date = now_utc.date()
            start_date = end_date - dt.timedelta(days=5)

            df = _download_yahoo_daily(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
            )

            if df.empty:
                return JobResult(self.job.id, self.job.query_id, "ok", "no rows", None, None, 0)

            con.register("df_in", df)

            # upsert-like логика: удаляем пересекающиеся даты по symbol+source
            con.execute("""
                DELETE FROM bronze.tickers_daily t
                USING df_in s
                WHERE t.symbol = s.symbol
                  AND t.source = s.source
                  AND t.reportDate = s.reportDate;
            """)

            con.execute("""
                INSERT INTO bronze.tickers_daily (symbol, reportDate, "open", high, low, "close", adjClose, volume, "source")
                SELECT symbol, reportDate, "open", high, low, "close", adjClose, volume, "source"
                FROM df_in;
            """)

            max_report_date = con.execute("SELECT MAX(reportDate) FROM df_in;").fetchone()[0]

            return JobResult(
                self.job.id,
                self.job.query_id,
                "ok",
                f"downloaded yahoo daily for {symbol}",
                None,
                max_report_date,
                int(len(df)),
            )

        except Exception as e:
            return JobResult(
                self.job.id,
                self.job.query_id,
                "error",
                f"{type(e).__name__}: {e}",
                None,
                None,
                0,
            )

    def update_silver(self, ctx: JobContext) -> None:
        # для этого job нет silber-слоя, но метод обязателен, поэтому просто пропускаем
        pass

    def build_gold(self, ctx: JobContext) -> None:
        con = ctx.con
        gold_dir = os.path.join(ctx.cfg.paths.gold_root, "tickers_daily")
        os.makedirs(gold_dir, exist_ok=True)

        history_path = os.path.join(gold_dir, "history.parquet")
        history_sql = history_path.replace("\\", "/").replace("'", "''")

        con.execute(f"""
            COPY (
                SELECT symbol, reportDate, "open", high, low, "close", adjClose, volume, "source"
                FROM bronze.tickers_daily
            )
            TO '{history_sql}'
            (FORMAT parquet, COMPRESSION zstd);
        """)

        register_asset(con, "tickers_daily_history", history_path, "file", None, "Daily market data history")
        super().build_gold(ctx)
