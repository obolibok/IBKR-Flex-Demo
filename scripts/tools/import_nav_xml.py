import argparse
from pathlib import Path

import duckdb
import pandas as pd

from scripts.etl_meta import etl_ensure_meta
from scripts.parse_nav import parse_nav


def _dedup_nav_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    if "accountId" in df.columns:
        df["accountId"] = df["accountId"].fillna("")

    return (
        df.sort_values(["accountId", "reportDate", "sourceHash"])
        .drop_duplicates(subset=["accountId", "reportDate"], keep="last")
        .reset_index(drop=True)
    )


def import_nav_xml(con: duckdb.DuckDBPyConnection, xml_bytes: bytes, source_name: str) -> dict:
    rows = parse_nav(xml_bytes)
    df = pd.DataFrame(rows)
    df = _dedup_nav_df(df)

    if df.empty:
        return {"source": source_name, "rows": 0, "dates": 0}

    con.register("df_in", df)

    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

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

    dates = con.execute("SELECT COUNT(DISTINCT reportDate) FROM df_in;").fetchone()[0]
    return {"source": source_name, "rows": int(len(df)), "dates": int(dates)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duckdb", required=True, help="Path to warehouse.duckdb")
    ap.add_argument("--file", required=True, help="Path to Flex XML file")
    args = ap.parse_args()

    db_path = Path(args.duckdb)
    xml_path = Path(args.file)

    con = duckdb.connect(str(db_path))
    etl_ensure_meta(con)

    info = import_nav_xml(con, xml_path.read_bytes(), xml_path.name)
    print(info)


if __name__ == "__main__":
    main()