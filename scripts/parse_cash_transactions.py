from typing import Dict, Any, List
from lxml import etree
from scripts.parsing_utils import *

def parse_cash_transactions(xml_bytes: bytes) -> List[Dict[str, Any]]:
    root = etree.fromstring(xml_bytes)
    items = root.findall(".//CashTransaction")

    rows: List[Dict[str, Any]] = []
    for x in items:
        a = dict(x.attrib)

        # В Flex cash часто приходят SUMMARY + DETAIL.
        # Для аналитики и дедупа лучше брать DETAIL.
        if a.get("levelOfDetail") and a.get("levelOfDetail") != "DETAIL":
            continue

        canonical = "|".join([f"{k}={a[k]}" for k in sorted(a.keys())]).encode("utf-8")
        sourceHashHex = sha256_hex(canonical)

        row = {
            "accountId": a.get("accountId"),
            "currency": a.get("currency"),
            "fxRateToBase": parse_decimal(a.get("fxRateToBase")),
            "assetCategory": a.get("assetCategory"),
            "subCategory": a.get("subCategory"),
            "symbol": a.get("symbol"),
            "description": a.get("description"),
            "conid": parse_int(a.get("conid")),
            "securityID": a.get("securityID"),
            "securityIDType": a.get("securityIDType"),
            "cusip": a.get("cusip"),
            "isin": a.get("isin"),
            "figi": a.get("figi"),
            "listingExchange": a.get("listingExchange"),
            "issuerCountryCode": a.get("issuerCountryCode"),
            "multiplier": parse_decimal(a.get("multiplier")),
            "dateTime": parse_dt(a.get("dateTime")),
            "settleDate": parse_date_yyyymmdd(a.get("settleDate")),
            "availableForTradingDate": parse_date_yyyymmdd(a.get("availableForTradingDate")),
            "reportDate": parse_date_yyyymmdd(a.get("reportDate")),
            "amount": parse_decimal(a.get("amount")),
            "type": a.get("type"),
            "transactionID": a.get("transactionID"),
            "clientReference": a.get("clientReference"),
            "actionID": a.get("actionID"),
            "sourceHashHex": sourceHashHex,
        }

        # Минимальный sanity filter
        if row["accountId"] and row["currency"] and row["reportDate"] is not None:
            rows.append(row)

    return rows