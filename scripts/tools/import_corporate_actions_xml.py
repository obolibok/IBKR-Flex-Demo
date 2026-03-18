import argparse
from pathlib import Path
import duckdb
import pandas as pd

from scripts.etl_meta import etl_ensure_meta
from scripts.parse_corporate_actions import parse_corporate_actions


def import_corporate_actions_xml(
    con: duckdb.DuckDBPyConnection,
    xml_bytes: bytes,
    source_name: str
) -> dict:
    rows = parse_corporate_actions(xml_bytes)
    if not rows:
        return {"source": source_name, "rows": 0}

    df = pd.DataFrame(rows)

    if df.empty:
        return {"source": source_name, "rows": 0}

    con.register("df_in", df)

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
        )
    """)

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

    dmin, dmax = con.execute("SELECT MIN(reportDate), MAX(reportDate) FROM df_in;").fetchone()
    return {
        "source": source_name,
        "rows": int(len(df)),
        "reportDate_min": str(dmin),
        "reportDate_max": str(dmax),
    }


def main():
    ap = argparse.ArgumentParser("Import IBKR Corporate Actions XML into DuckDB bronze.corporate_actions")
    ap.add_argument("--duckdb", required=True, help="Path to warehouse.duckdb")
    ap.add_argument("--file", required=True, help="Path to Flex XML file")
    args = ap.parse_args()

    db_path = Path(args.duckdb)
    xml_path = Path(args.file)

    con = duckdb.connect(str(db_path))
    etl_ensure_meta(con)

    info = import_corporate_actions_xml(con, xml_path.read_bytes(), xml_path.name)
    print(info)


if __name__ == "__main__":
    main()