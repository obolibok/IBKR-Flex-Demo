import argparse
from pathlib import Path
import duckdb
import pandas as pd

from scripts.etl_meta import etl_ensure_meta
from scripts.parse_cash_transactions import parse_cash_transactions


def import_cash_transactions_xml(con: duckdb.DuckDBPyConnection, xml_bytes: bytes, source_name: str) -> dict:
    rows = parse_cash_transactions(xml_bytes)
    df = pd.DataFrame(rows)

    if df.empty:
        return {"source": source_name, "rows": 0}

    con.register("df_in", df)

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

    dmin, dmax = con.execute("SELECT MIN(reportDate), MAX(reportDate) FROM df_in;").fetchone()
    return {
        "source": source_name,
        "rows": int(len(df)),
        "reportDate_min": str(dmin),
        "reportDate_max": str(dmax),
    }


def main():
    ap = argparse.ArgumentParser("Import IBKR Cash Transactions XML into DuckDB bronze.cash_transactions")
    ap.add_argument("--duckdb", required=True, help="Path to warehouse.duckdb")
    ap.add_argument("--file", required=True, help="Path to Flex XML file")
    args = ap.parse_args()

    db_path = Path(args.duckdb)
    xml_path = Path(args.file)

    con = duckdb.connect(str(db_path))
    etl_ensure_meta(con)

    info = import_cash_transactions_xml(con, xml_path.read_bytes(), xml_path.name)
    print(info)


if __name__ == "__main__":
    main()