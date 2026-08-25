import json
from pathlib import Path

STREAM_FILE = Path("streams.json")


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
        headers = stream.get("headers") or {}

        sessions[idx] = {
            "id": idx,
            "type": stype,
            "url": url,
            "base_url": url.rsplit("/", 1)[0] + "/",
            "headers": headers,
        }
        logger.info(f"Loaded Stream {idx} [{stype}]: {url}")

    return sessions