import requests
import xml.etree.ElementTree as ET
import time
from typing import Optional, Tuple

from scripts.data_classes import IbkrCfg


SEND_RETRYABLE_CODES = {
    "1001", "1003", "1004", "1005", "1006", "1007", "1008", "1009",
    "1018", "1021",
}

GET_POLL_RETRYABLE_CODES = {
    "1018",  # too many requests
    "1019",  # statement generation in progress
}

GET_RESTART_CYCLE_CODES = {
    "1001",
    "1009",
    "1017",  # reference code invalid
    "1020",  # invalid request / unable to validate request
    "1021",
}


def _parse_flex_response(xml_bytes: bytes) -> ET.Element:
    try:
        return ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        preview = xml_bytes[:500]
        raise RuntimeError(f"IBKR Flex returned non-parseable XML: {preview!r}") from e


def _extract_status_code_msg(root: ET.Element) -> Tuple[str, str, str]:
    status = (root.findtext("Status") or "").strip()
    code = (root.findtext("ErrorCode") or "").strip()
    msg = (root.findtext("ErrorMessage") or "").strip()
    return status, code, msg


def flex_send_request(
    cfg: IbkrCfg,
    query_id: str,
    max_attempts: int = 6,
    base_sleep_sec: int = 5,
) -> str:
    url = f"{cfg.base_url.rstrip('/')}/SendRequest"
    params = {
        "t": cfg.token.strip(),
        "q": str(query_id).strip(),
        "v": int(cfg.version),
    }
    headers = {"User-Agent": "ibkr-flex-etl/1.0"}

    last_err = None

    for attempt in range(1, max_attempts + 1):
        r = requests.get(url, params=params, headers=headers, timeout=60, allow_redirects=True)
        r.raise_for_status()

        root = _parse_flex_response(r.content)

        if root.tag != "FlexStatementResponse":
            raise RuntimeError(
                f"SendRequest: unexpected root tag: {root.tag}. "
                f"Body={r.content[:500]!r}"
            )

        status, code, msg = _extract_status_code_msg(root)

        if status == "Success":
            ref = (root.findtext("ReferenceCode") or "").strip()
            if not ref:
                raise RuntimeError(
                    f"SendRequest succeeded but ReferenceCode is empty. "
                    f"Body={r.content[:500]!r}"
                )
            return ref

        last_err = f"{code} {msg}".strip()

        if code in SEND_RETRYABLE_CODES and attempt < max_attempts:
            if code == "1018":
                sleep_sec = max(base_sleep_sec * attempt, 10)
            else:
                sleep_sec = max(base_sleep_sec, attempt * 2)
            time.sleep(sleep_sec)
            continue

        raise RuntimeError(f"SendRequest failed: {last_err}")

    raise RuntimeError(f"SendRequest failed after retries: {last_err}")


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


def flex_get_statement_wait_query(
    base_url: str,
    token: str,
    ref_code: str,
    version: int = 3,
    poll_seconds: int = 3,
    max_wait_seconds: int = 180,
    initial_wait_seconds: int = 5,
) -> bytes:
    url = f"{base_url.rstrip('/')}/GetStatement"
    params = {
        "t": token.strip(),
        "q": ref_code.strip(),
        "v": int(version),
    }
    headers = {"User-Agent": "ibkr-flex-etl/1.0"}

    if initial_wait_seconds > 0:
        time.sleep(initial_wait_seconds)

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
            last_err = f"Unparseable response (not XML): {content[:500]!r}"
            time.sleep(poll_seconds)
            continue

        if root.tag == "FlexStatementResponse":
            status, code, msg = _extract_status_code_msg(root)

            if status == "Fail":
                last_err = f"{code} {msg}".strip()

                if code in GET_POLL_RETRYABLE_CODES:
                    sleep_sec = max(poll_seconds, 10) if code == "1018" else poll_seconds
                    time.sleep(sleep_sec)
                    continue

                if code in GET_RESTART_CYCLE_CODES:
                    raise RuntimeError(f"GetStatement restartable failure: {last_err}")

                raise RuntimeError(
                    f"GetStatement failed: {last_err}. Body={content[:500]!r}"
                )

        last_err = f"Unexpected root tag: {root.tag}. Body={content[:500]!r}"
        time.sleep(poll_seconds)

    raise TimeoutError(f"Report not ready in time. Last: {last_err}")


def flex_download_statement(
    cfg: IbkrCfg,
    query_id: str,
    poll_seconds: int = 3,
    max_wait_seconds: int = 180,
    initial_wait_seconds: int = 5,
    cycle_attempts: int = 4,
    cycle_sleep_seconds: int = 10,
) -> tuple[bytes, str]:
    """
    Full safe cycle for IBKR Flex:
    SendRequest -> GetStatement.
    On restartable GetStatement errors (e.g. 1020 / 1017), retries the whole cycle.
    Returns: (xml_bytes, reference_code)
    """
    last_err = None

    for attempt in range(1, cycle_attempts + 1):
        ref = None
        try:
            ref = flex_send_request(cfg, query_id=query_id)
            xml_bytes = flex_get_statement_wait_query(
                base_url=cfg.base_url,
                token=cfg.token,
                ref_code=ref,
                version=cfg.version,
                poll_seconds=poll_seconds,
                max_wait_seconds=max_wait_seconds,
                initial_wait_seconds=initial_wait_seconds,
            )
            print(f"[flex] query_id={query_id} attempt={attempt} ref={ref!r}")
            return xml_bytes, ref

        except RuntimeError as e:
            msg = str(e)
            last_err = msg
            print(f"[flex] query_id={query_id} attempt={attempt} error={msg} ref={ref!r}")

            restartable = (
                "GetStatement restartable failure:" in msg
                or " 1017 " in f" {msg} "
                or " 1020 " in f" {msg} "
                or " 1021 " in f" {msg} "
                or " 1001 " in f" {msg} "
                or " 1009 " in f" {msg} "
            )

            if restartable and attempt < cycle_attempts:
                time.sleep(max(cycle_sleep_seconds, attempt * 5))
                continue

            raise RuntimeError(msg) from e

    raise RuntimeError(f"Flex download failed after full-cycle retries: {last_err}")

