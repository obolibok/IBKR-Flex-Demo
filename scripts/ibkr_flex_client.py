import requests
import xml.etree.ElementTree as ET
import time
from typing import Optional, Tuple

from scripts.data_classes import IbkrCfg

def flex_send_request(cfg: IbkrCfg, query_id: str, max_attempts: int = 6, base_sleep_sec: int = 5) -> str:
    url = f"{cfg.base_url}/SendRequest"
    params = {"t": cfg.token, "q": query_id, "v": cfg.version}

    for attempt in range(1, max_attempts + 1):
        r = requests.get(url, params=params, timeout=60)
        r.raise_for_status()

        root = ET.fromstring(r.text)
        status = (root.findtext("Status") or "").strip()

        if status == "Success":
            ref = (root.findtext("ReferenceCode") or "").strip()
            if not ref:
                raise RuntimeError("SendRequest: missing ReferenceCode")
            return ref

        code = (root.findtext("ErrorCode") or "").strip()
        msg = (root.findtext("ErrorMessage") or "").strip()

        # 1018: Too many requests => backoff and retry
        if code == "1018" and attempt < max_attempts:
            sleep_sec = max(base_sleep_sec, base_sleep_sec * attempt)
            time.sleep(sleep_sec)
            continue

        raise RuntimeError(f"SendRequest failed: {code} {msg}".strip())
    
def flex_send_request_old(cfg: IbkrCfg, query_id: int) -> str:
    url = f"{cfg.base_url}/SendRequest"
    params = {"t": cfg.token, "q": int(query_id), "v": cfg.version}
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()

    root = ET.fromstring(r.text)
    status = (root.findtext("Status") or "").strip()
    if status != "Success":
        code = (root.findtext("ErrorCode") or "").strip()
        msg = (root.findtext("ErrorMessage") or "").strip()
        raise RuntimeError(f"SendRequest failed: {code} {msg}".strip())

    ref = (root.findtext("ReferenceCode") or "").strip()
    if not ref:
        raise RuntimeError("SendRequest: missing ReferenceCode")
    return ref


def flex_try_parse_error(xml_text: str) -> Optional[Tuple[str, str, str]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return None

    if root.tag != "FlexStatementResponse":
        return None

    status = (root.findtext("Status") or "").strip()
    if status != "Fail":
        return None

    code = (root.findtext("ErrorCode") or "").strip()
    msg = (root.findtext("ErrorMessage") or "").strip()
    return (status, code, msg)


RETRYABLE_CODES = {
    "1001","1003","1004","1005","1006","1007","1008","1009",
    "1018","1019","1021",
}


def flex_get_statement_wait_query(
    base_url: str,
    token: str,
    ref_code: str,
    version: int = 3,
    poll_seconds: int = 3,
    max_wait_seconds: int = 180
) -> bytes:
    url = f"{base_url.rstrip('/')}/GetStatement"
    params = {"t": token, "q": ref_code, "v": version}
    headers = {"User-Agent": "ibkr-flex-etl/1.0"}

    deadline = time.time() + max_wait_seconds
    last_err = None

    while time.time() < deadline:
        r = requests.get(url, params=params, headers=headers, timeout=60, allow_redirects=True)
        r.raise_for_status()
        content = r.content

        if b"<FlexQueryResponse" in content:
            return content

        try:
            root = ET.fromstring(content)
        except ET.ParseError:
            last_err = "Unparseable response (not XML)"
            time.sleep(poll_seconds)
            continue

        if root.tag == "FlexStatementResponse":
            status = (root.findtext("Status") or "").strip()
            if status == "Fail":
                code = (root.findtext("ErrorCode") or "").strip()
                msg = (root.findtext("ErrorMessage") or "").strip()
                last_err = f"{code} {msg}".strip()

                if code in RETRYABLE_CODES:
                    time.sleep(max(poll_seconds, 10) if code == "1018" else poll_seconds)
                    continue

                raise RuntimeError(f"GetStatement failed: {last_err}")

        last_err = f"Unexpected root tag: {root.tag}"
        time.sleep(poll_seconds)

    raise TimeoutError(f"Report not ready in time. Last: {last_err}")