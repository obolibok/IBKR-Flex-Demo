import argparse
from pathlib import Path
import duckdb
import pandas as pd

from scripts.etl_meta import etl_ensure_meta
from scripts.parse_trades import parse_trades  # :contentReference[oaicite:3]{index=3}


def import_trades_xml(con: duckdb.DuckDBPyConnection, xml_bytes: bytes, source_name: str) -> dict:
    rows = parse_trades(xml_bytes)
    df = pd.DataFrame(rows)

    if df.empty:
        return {"source": source_name, "rows": 0}

    con.register("df_in", df)

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
                    sourceHashHex VARCHAR)""")

    # дедуп по sourceHashHex (строковый, стабильный)
    con.execute("""
        DELETE FROM bronze.trades
        WHERE sourceHashHex IN (SELECT sourceHashHex FROM df_in WHERE sourceHashHex IS NOT NULL);
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

    dmin, dmax = con.execute("SELECT MIN(tradeDate), MAX(tradeDate) FROM df_in;").fetchone()
    return {"source": source_name, "rows": int(len(df)), "tradeDate_min": str(dmin), "tradeDate_max": str(dmax)}


def main():
    ap = argparse.ArgumentParser("Import IBKR Trades XML into DuckDB bronze.trades")
    ap.add_argument("--duckdb", required=True, help="Path to warehouse.duckdb")
    ap.add_argument("--file", required=True, help="Path to Flex XML file")
    args = ap.parse_args()

    db_path = Path(args.duckdb)
    xml_path = Path(args.file)

    con = duckdb.connect(str(db_path))
    etl_ensure_meta(con)

    info = import_trades_xml(con, xml_path.read_bytes(), xml_path.name)
    print(info)


if __name__ == "__main__":
    main()
