# Utility functions for parsing IBKR XML data, shared across multiple parsers.
import hashlib
import datetime as dt
from typing import Optional

# Hashing for deduplication and change detection.
def sha256_bytes(b: bytes) -> bytes:
    return hashlib.sha256(b).digest()

def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

# Date-time parsing with multiple formats, including IBKR-specific ones.
def parse_dt(s: Optional[str]) -> Optional[dt.datetime]:
    if not s:
        return None
    s = s.strip().replace(";", " ")
    # IBKR: YYYYMMDDHHMMSS
    if s.isdigit() and len(s) == 14:
        return dt.datetime.strptime(s, "%Y%m%d%H%M%S")
    if s.isdigit() and len(s) == 8:
        return dt.datetime.strptime(s, "%Y%m%d")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return dt.datetime.strptime(s, fmt)
        except ValueError:
            pass
    try:
        return dt.fromisoformat(s)
    except Exception:
        raise ValueError(f"Unrecognized datetime format: {s}")

# Date parsing with multiple formats, including IBKR-specific ones.
def parse_date_yyyymmdd(s: Optional[str]):
    if not s:
        return None
    s = s.strip()
    if s.isdigit() and len(s) == 8:
        return dt.datetime.strptime(s, "%Y%m%d").date()
    for fmt in ("%Y-%m-%d",):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"Unrecognized date format: {s}")

# Decimal parsing that handles commas and empty strings.
def parse_decimal(s: Optional[str]):
    if s is None or s == "":
        return None
    return float(s.replace(",", ""))

# Integer parsing that returns None for empty strings or invalid integers.
def parse_int(s: Optional[str]):
    if s is None or s == "":
        return None
    try:
        return int(s)
    except ValueError:
        return None

def normalize_date(s: str) -> Optional[str]:
    s = s.strip()
    # common formats: YYYYMMDD, YYYY-MM-DD, "28 August, 2012 10:37 AM EDT" (ignore)
    try:
        if len(s) == 8 and s.isdigit():
            d = dt.datetime.strptime(s, "%Y%m%d").date()
            return d.isoformat()
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            d = dt.date.fromisoformat(s)
            return d.isoformat()
    except Exception as e:
        print(s, e)
        pass
    return None

def to_number(x: Optional[str]) -> Optional[float]:
    if x is None:
        return None
    x = x.strip()
    if x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None

def parse_bool_yn(s: Optional[str]):
    if not s:
        return None
    s = s.strip().upper()
    if s == "Y":
        return True
    if s == "N":
        return False
    return None
