import argparse
import datetime as dt

import duckdb
import pandas as pd
import yaml

from scripts.data_classes import Config, IbkrCfg, EtlCfg, PathsCfg, FlexJobCfg
from scripts.jobs.ticker_yahoo_job import _download_yahoo_daily


def load_config(path: str) -> Config:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    kit_root = raw["paths"]["kit_root"]
    duckdb_path = raw["paths"]["duckdb_path"].format(kit_root=kit_root)
    gold_root = raw["paths"]["gold_root"].format(kit_root=kit_root)

    ibkr = IbkrCfg(**raw["ibkr"])
    etl = EtlCfg(
        poll_seconds=raw["etl"]["poll_seconds"],
        max_wait_seconds=raw["etl"]["max_wait_seconds"],
        min_seconds_between_runs=raw["etl"]["min_seconds_between_runs"],
        bronze=bool(raw["etl"].get("bronze", True)),
        silver=bool(raw["etl"].get("silver", True)),
        gold=bool(raw["etl"].get("gold", True)),
        obfuscate=bool(raw["etl"].get("obfuscate", False)),
    )
    paths = PathsCfg(
        kit_root=kit_root,
        duckdb_path=duckdb_path,
        gold_root=gold_root,
    )

    flex_jobs = [FlexJobCfg(**x) for x in raw.get("flex_jobs", [])]

    return Config(
        ibkr=ibkr,
        etl=etl,
        paths=paths,
        flex_jobs=flex_jobs,
    )


def ensure_storage(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.tickers_daily (
            symbol VARCHAR NOT NULL,
            reportDate DATE NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            adjClose DOUBLE,
            volume DOUBLE,
            source VARCHAR NOT NULL
        );
    """)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", required=False, help="YYYY-MM-DD")
    args = parser.parse_args()

    cfg = load_config(args.config)
    con = duckdb.connect(cfg.paths.duckdb_path)

    ensure_storage(con)

    start_date = dt.date.fromisoformat(args.start)
    end_date = dt.date.fromisoformat(args.end) if args.end else dt.date.today()

    df = _download_yahoo_daily(
        symbol=args.symbol.strip(),
        start_date=start_date,
        end_date=end_date,
    )

    if df.empty:
        print("No rows downloaded.")
        return

    con.register("df_in", df)

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

    print(f"Imported {len(df)} rows for {args.symbol} from {start_date} to {end_date}")


if __name__ == "__main__":
    main()