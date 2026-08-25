import json
import logging
from pathlib import Path

logger = logging.getLogger("stream_proxy.loader")

STREAM_FILE = Path("streams.json")


def infer_default_headers(url: str, user_headers: dict | None) -> dict:
    headers = dict(user_headers or {})
    has_referer = any(k.lower() == "referer" for k in headers)

    if not has_referer:
        url_lower = url.lower()
        if "slast430did" in url_lower or "allmovieland" in url_lower:
            headers["Referer"] = "https://allmovieland.one/"
        elif "moviezwap" in url_lower or "moviezzwaphd" in url_lower:
            headers["Referer"] = "https://www.moviezwap.taxi/"
        elif "klcxm.com" in url_lower:
            headers["Referer"] = "https://api.hlowb.com"
        elif "mycdn-mb.xyz" in url_lower:
            headers["Referer"] = "MovieBlast"
            headers["User-Agent"] = "MovieBlast"
            headers["x-request-x"] = "com.movieblast"

    return headers


def load_streams():
    if not STREAM_FILE.exists():
        return {}

    if STREAM_FILE.stat().st_size == 0:
        return {}

    with open(STREAM_FILE, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            return {}

    sessions = {}

    for idx, stream in enumerate(data.get("streams", [])):
        url = stream["url"]
        stype = (stream.get("type") or "VIDEO").upper()
        headers = infer_default_headers(url, stream.get("headers"))

        sessions[idx] = {
            "id": idx,
            "type": stype,
            "url": url,
            "base_url": url.rsplit("/", 1)[0] + "/",
            "headers": headers,
        }
        logger.info(f"Loaded Stream {idx} [{stype}]: {url}")

    return sessions