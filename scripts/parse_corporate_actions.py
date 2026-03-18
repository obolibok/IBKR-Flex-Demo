from typing import Dict, Any, List
from lxml import etree
from scripts.parsing_utils import *

def parse_corporate_actions(xml_bytes: bytes) -> List[Dict[str, Any]]:
    root = etree.fromstring(xml_bytes)
    items = root.findall(".//CorporateAction")

    rows: List[Dict[str, Any]] = []
    for x in items:
        a = dict(x.attrib)
        canonical = "|".join([f"{k}={a[k]}" for k in sorted(a.keys())]).encode("utf-8")
        sourceHashHex = sha256_hex(canonical)

        row = {
            "accountId": a.get("accountId"),
            "model": a.get("model"),
            "currency": a.get("currency"),
            "fxRateToBase": parse_decimal(a.get("fxRateToBase")),

            "assetCategory": a.get("assetCategory"),
            "subCategory": a.get("subCategory"),
            "symbol": a.get("symbol"),
            "underlyingSymbol": a.get("underlyingSymbol"),
            "conid": parse_int(a.get("conid")),
            "underlyingConid": parse_int(a.get("underlyingConid")),
            "listingExchange": a.get("listingExchange"),
            "underlyingListingExchange": a.get("underlyingListingExchange"),

            "transactionID": a.get("transactionID"),
            "actionID": parse_int(a.get("actionID")),
            "type": a.get("type"),
            "code": a.get("code"),

            "reportDate": parse_date_yyyymmdd(a.get("reportDate")),
            "dateTime": parse_dt(a.get("dateTime")),

            "description": a.get("description"),
            "actionDescription": a.get("actionDescription"),

            "quantity": parse_decimal(a.get("quantity")),
            "amount": parse_decimal(a.get("amount")),
            "proceeds": parse_decimal(a.get("proceeds")),
            "value": parse_decimal(a.get("value")),
            "costBasis": parse_decimal(a.get("costBasis")),
            "fifoPnlRealized": parse_decimal(a.get("fifoPnlRealized")),
            "mtmPnl": parse_decimal(a.get("mtmPnl")),

            "securityID": a.get("securityID"),
            "securityIDType": a.get("securityIDType"),
            "cusip": a.get("cusip"),
            "isin": a.get("isin"),
            "figi": a.get("figi"),

            "underlyingSecurityID": a.get("underlyingSecurityID"),

            "issuer": a.get("issuer"),
            "issuerCountryCode": a.get("issuerCountryCode"),

            "multiplier": parse_decimal(a.get("multiplier")),
            "strike": parse_decimal(a.get("strike")),
            "expiry": parse_date_yyyymmdd(a.get("expiry")),
            "putCall": a.get("putCall"),
            "principalAdjustFactor": parse_decimal(a.get("principalAdjustFactor")),

            "levelOfDetail": a.get("levelOfDetail"),
            "serialNumber": a.get("serialNumber"),
            "deliveryType": a.get("deliveryType"),
            "commodityType": a.get("commodityType"),
            "fineness": parse_decimal(a.get("fineness")),
            "weight": parse_decimal(a.get("weight")),

            "sourceHashHex": sourceHashHex,
        }

        if row["accountId"] and row["symbol"] and row["reportDate"]:
            rows.append(row)

    return rows