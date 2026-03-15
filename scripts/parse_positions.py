# ----------------------------
# Flex XML parsing (positions)
# ----------------------------
from typing import Dict, Any, List
from lxml import etree
from scripts.parsing_utils import *
from scripts.ibkr_flex_client import *

def find_position_rows(root: ET.Element) -> List[ET.Element]:
    """
    Best-effort: find OpenPosition rows.
    Actual tag names depend on your Flex query sections.
    """
    # common names seen in Flex outputs:
    # <OpenPositions> <OpenPosition .../>
    rows = root.findall(".//OpenPosition")
    if rows:
        return rows

    # fallback: sometimes tag naming differs; try endswith
    fallback = []
    for el in root.iter():
        if el.tag.lower().endswith("openposition"):
            fallback.append(el)
    return fallback

def parse_positions(xml_bytes: bytes) -> List[Dict[str, Any]]:
    root = etree.fromstring(xml_bytes)
    items = root.findall(".//OpenPosition")

    rows: List[Dict[str, Any]] = []
    for x in items:
        a = dict(x.attrib)

        canonical = "|".join([f"{k}={a[k]}" for k in sorted(a.keys())]).encode("utf-8")
        source_hash = sha256_hex(canonical)

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
            "listingExchange": a.get("listingExchange"),
            "multiplier": parse_decimal(a.get("multiplier")),
            "reportDate": parse_date_yyyymmdd(a.get("reportDate")),
            "position": parse_decimal(a.get("position")),
            "markPrice": parse_decimal(a.get("markPrice")),
            "positionValue": parse_decimal(a.get("positionValue")),
            "percentOfNAV": parse_decimal(a.get("percentOfNAV")),
            "side": a.get("side"),
            "sourceHash": source_hash,
        }

        if row["accountId"] and row["symbol"]:
            rows.append(row)

    return rows
