from typing import Dict, Any, List
from lxml import etree
from scripts.parsing_utils import *

def parse_nav(xml_bytes: bytes) -> List[Dict[str, Any]]:
    root = etree.fromstring(xml_bytes)
    items = root.findall(".//EquitySummaryByReportDateInBase")

    rows: List[Dict[str, Any]] = []

    for x in items:
        a = dict(x.attrib)

        canonical = "|".join([f"{k}={a[k]}" for k in sorted(a.keys())]).encode("utf-8")
        source_hash = sha256_hex(canonical)

        row = {
            "accountId": a.get("accountId"),
            "currency": a.get("currency"),
            "reportDate": parse_date_yyyymmdd(a.get("reportDate")),
            "cash": parse_decimal(a.get("cash")),
            "stock": parse_decimal(a.get("stock")),
            "options": parse_decimal(a.get("options")),
            "funds": parse_decimal(a.get("funds")),
            "dividendAccruals": parse_decimal(a.get("dividendAccruals")),
            "interestAccruals": parse_decimal(a.get("interestAccruals")),
            "forexCfdUnrealizedPl": parse_decimal(a.get("forexCfdUnrealizedPl")),
            "cfdUnrealizedPl": parse_decimal(a.get("cfdUnrealizedPl")),
            "crypto": parse_decimal(a.get("crypto")),
            "total": parse_decimal(a.get("total")),
            "totalLong": parse_decimal(a.get("totalLong")),
            "totalShort": parse_decimal(a.get("totalShort")),
            "sourceHash": source_hash,
        }

        if row["accountId"] and row["reportDate"] is not None:
            rows.append(row)

    return rows