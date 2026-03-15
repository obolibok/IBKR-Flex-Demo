import argparse
from pathlib import Path
import duckdb
import pandas as pd

from scripts.etl_meta import etl_ensure_meta
from scripts.parse_positions import parse_positions


def import_positions_xml(con: duckdb.DuckDBPyConnection, xml_bytes: bytes, source_name: str) -> dict:
    rows = parse_positions(xml_bytes)
    df = pd.DataFrame(rows)

    if df.empty:
        return {"source": source_name, "rows": 0, "dates": 0}

    con.register("df_in", df)

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
                    percentOfNAV DOUBLE,
                    side VARCHAR,
                    sourceHash VARCHAR);""")

    # дедуп по sourceHash (в твоём парсере он учитывает reportDate, так что дубликаты из перекрывающихся файлов уберём)
    con.execute("""
        DELETE FROM bronze.positions_snapshot
        WHERE sourceHash IN (SELECT sourceHash FROM df_in);
    """)

    con.execute("""INSERT INTO bronze.positions_snapshot (accountId,currency,fxRateToBase,assetCategory,subCategory,symbol,"description",conid,securityID,securityIDType,cusip,isin,listingExchange,multiplier,reportDate,"position",markPrice,positionValue,percentOfNAV,side,sourceHash)
                        SELECT accountId,currency,fxRateToBase,assetCategory,subCategory,symbol,"description",conid,securityID,securityIDType,cusip,isin,listingExchange,multiplier,reportDate,"position",markPrice,positionValue,percentOfNAV,side,sourceHash
                        FROM df_in;""")

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

    info = import_positions_xml(con, xml_path.read_bytes(), xml_path.name)
    print(info)


if __name__ == "__main__":
    main()