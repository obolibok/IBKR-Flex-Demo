from typing import Dict, Any, List
from lxml import etree
from scripts.parsing_utils import *

def parse_trades(xml_bytes: bytes) -> List[Dict[str, Any]]:
    root = etree.fromstring(xml_bytes)
    items = root.findall(".//Trade")

    rows: List[Dict[str, Any]] = []
    for x in items:
        a = dict(x.attrib)
        canonical = "|".join([f"{k}={a[k]}" for k in sorted(a.keys())]).encode("utf-8")
        sourceHashHex = sha256_hex(canonical)

        row = {
            "accountId": a.get("accountId"),
            "currency": a.get("currency"),
            "assetCategory": a.get("assetCategory"),
            "symbol": a.get("symbol"),
            "conid": parse_int(a.get("conid")),
            "listingExchange": a.get("listingExchange"),

            "tradeID": parse_int(a.get("tradeID")),
            "transactionID": a.get("transactionID"),
            "ibExecID": a.get("ibExecID"),
            "ibOrderID": parse_int(a.get("ibOrderID")),

            "reportDate": parse_date_yyyymmdd(a.get("reportDate")),
            "tradeDate": parse_date_yyyymmdd(a.get("tradeDate")),
            "dateTime": parse_dt(a.get("dateTime")),
            "orderTime": parse_dt(a.get("orderTime")),

            "transactionType": a.get("transactionType"),
            "exchange": a.get("exchange"),
            "buySell": a.get("buySell"),
            "openCloseIndicator": a.get("openCloseIndicator"),

            "quantity": parse_decimal(a.get("quantity")),
            "tradePrice": parse_decimal(a.get("tradePrice")),
            "tradeMoney": parse_decimal(a.get("tradeMoney")),
            "proceeds": parse_decimal(a.get("proceeds")),
            "taxes": parse_decimal(a.get("taxes")),
            "ibCommission": parse_decimal(a.get("ibCommission")),
            "ibCommissionCurrency": a.get("ibCommissionCurrency"),
            "netCash": parse_decimal(a.get("netCash")),
            "closePrice": parse_decimal(a.get("closePrice")),
            "cost": parse_decimal(a.get("cost")),
            "fifoPnlRealized": parse_decimal(a.get("fifoPnlRealized")),
            "mtmPnl": parse_decimal(a.get("mtmPnl")),

            "orderType": a.get("orderType"),
            "orderReference": a.get("orderReference"),
            "brokerageOrderID": a.get("brokerageOrderID"),
            "isAPIOrder": parse_bool_yn(a.get("isAPIOrder")),

            "description": a.get("description"),

            "fxRateToBase": parse_decimal(a.get("fxRateToBase")),
            "subCategory": a.get("subCategory"),
            "multiplier": parse_decimal(a.get("multiplier")),
            "settleDateTarget": parse_date_yyyymmdd(a.get("settleDateTarget")),
            "initialInvestment": parse_decimal(a.get("initialInvestment")),
            
            "sourceHashHex": sourceHashHex,
        }

        if row["accountId"] and row["symbol"] and row["tradeDate"]:
            rows.append(row)

    return rows