import re
import json
import logging
import urllib.parse
from pathlib import Path
from contextlib import asynccontextmanager
from urllib.parse import urljoin, quote, urlparse
import httpx
from cachetools import TTLCache
from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.responses import Response, RedirectResponse, StreamingResponse
from starlette.background import BackgroundTask

import stream_loader

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("proxy")

# ─── Constants ────────────────────────────────────────────────────────────────
CACHE_MAX_TOTAL_BYTES   = 64 * 1024 * 1024
CACHE_MAX_SEGMENT_BYTES = 1 * 1024 * 1024
CACHE_TTL_SECONDS       = 300
STREAM_CHUNK_SIZE       = 64 * 1024

# We now forward Content-Length and Content-Encoding because
# we disabled auto-decompression — bytes pass through raw.
FORWARD_HEADERS = (
    "Content-Type",
    "Content-Length",
    "Content-Range",
    "Content-Encoding",
    "Accept-Ranges",
    "ETag",
    "Last-Modified",
)

_URI_RE = re.compile(r'URI="([^"]+)"')

# ─── Cache ────────────────────────────────────────────────────────────────────

class ByteSizedTTLCache(TTLCache):
    def __init__(self, max_bytes: int, ttl: float):
        super().__init__(maxsize=max_bytes, ttl=ttl, getsizeof=self._sizeof)

    @staticmethod
    def _sizeof(value):
        if isinstance(value, dict) and "content" in value:
            return max(1, len(value["content"]))
        return 1

segment_cache = ByteSizedTTLCache(
    max_bytes=CACHE_MAX_TOTAL_BYTES,
    ttl=CACHE_TTL_SECONDS
)

# ─── HTTP Clients ─────────────────────────────────────────────────────────────

# Client for playlists / manifests — text content, decompression is fine
playlist_client = httpx.AsyncClient(
    follow_redirects=True,
    http2=False,
    timeout=httpx.Timeout(connect=10, read=30, write=10, pool=10),
    limits=httpx.Limits(
        max_connections=20,
        max_keepalive_connections=10,
        keepalive_expiry=20
    )
)

# Client for segments — RAW bytes, NO decompression
# This is critical: upstream may use Content-Encoding tricks or serve
# binary media as .js files. Auto-decompression corrupts the data.
segment_client = httpx.AsyncClient(
    follow_redirects=True,
    http2=False,
    timeout=httpx.Timeout(connect=10, read=60, write=10, pool=10),
    limits=httpx.Limits(
        max_connections=20,
        max_keepalive_connections=10,
        keepalive_expiry=20
    ),
    headers={
        "Accept-Encoding": "identity"  # tell upstream: don't compress
    }
)

# ─── App & Lifecycle ──────────────────────────────────────────────────────────

sessions: dict = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    global sessions
    sessions = stream_loader.load_streams()
    logger.warning("Startup: loaded %d streams", len(sessions))
    yield
    await playlist_client.aclose()
    await segment_client.aclose()
    logger.warning("Shutdown complete")

app = FastAPI(
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_session(stream_id: int) -> dict | None:
    return sessions.get(stream_id)


def pick_response_headers(headers: httpx.Headers) -> dict:
    return {h: headers[h] for h in FORWARD_HEADERS if h in headers}


def rewrite_m3u8(text: str, playlist_url: str, proxy_base: str) -> str:
    out = []

    for line in text.splitlines():
        stripped = line.strip()

        if not stripped:
            out.append(line)
            continue

        # Rewrite URI="..." inside supported HLS tags
        if stripped.startswith((
            "#EXT-X-MEDIA",
            "#EXT-X-I-FRAME-STREAM-INF",
            "#EXT-X-MAP",
            "#EXT-X-KEY",
        )):
            def replace_uri(match):
                absolute = urljoin(playlist_url, match.group(1))
                return f'URI="{proxy_base}{quote(absolute, safe="")}"'

            out.append(_URI_RE.sub(replace_uri, line))
            continue

        # Keep other tags unchanged
        if stripped.startswith("#"):
            out.append(line)
            continue

        # Rewrite standalone segment/sub-playlist URI
        absolute = urljoin(playlist_url, stripped)
        out.append(proxy_base + quote(absolute, safe=""))

    return "\n".join(out)


async def fetch_playlist(url: str, headers: dict) -> httpx.Response:
    """Fetch text content like playlists and manifests."""
    try:
        return await playlist_client.get(url, headers=headers)
    except httpx.TimeoutException:
        raise HTTPException(504, "Upstream timeout")
    except httpx.ConnectError:
        raise HTTPException(502, "Cannot reach upstream")
    except httpx.HTTPError as e:
        raise HTTPException(502, f"Upstream error: {e}")


async def serve_segment(
    url: str,
    headers: dict,
    cache_key: tuple,
) -> Response | StreamingResponse:
    """
    Fetch a media segment with NO decompression.
    Raw bytes from upstream pass through exactly as-is.
    """
    try:
        req = segment_client.build_request("GET", url, headers=headers)
        r   = await segment_client.send(req, stream=True)
    except httpx.TimeoutException:
        raise HTTPException(504, "Upstream segment timeout")
    except httpx.ConnectError:
        raise HTTPException(502, "Cannot reach upstream")
    except httpx.HTTPError as e:
        raise HTTPException(502, f"Upstream error: {e}")

    resp_headers = pick_response_headers(r.headers)
    content_type = r.headers.get("Content-Type", "application/octet-stream")
    status       = r.status_code

    # Content-Length is safe to forward now because we disabled decompression —
    # the raw byte count matches what we will actually send.
    declared_length = int(r.headers.get("Content-Length", 0))

    # ── Small segment → buffer + cache ────────────────────────────────────
    if 0 < declared_length <= CACHE_MAX_SEGMENT_BYTES:
        # Read raw bytes — no decompression happening
        body = b""
        async for chunk in r.aiter_raw():
            body += chunk
        await r.aclose()

        try:
            segment_cache[cache_key] = {
                "content":      body,
                "status":       status,
                "content_type": content_type,
                "headers":      resp_headers,
            }
        except ValueError:
            pass

        return Response(
            content=body,
            status_code=status,
            media_type=content_type,
            headers=resp_headers,
        )

    # ── Large or unknown → stream raw bytes ───────────────────────────────
    async def _stream():
        async for chunk in r.aiter_raw():
            yield chunk

    return StreamingResponse(
        _stream(),
        status_code=status,
        media_type=content_type,
        headers=resp_headers,
        background=BackgroundTask(r.aclose),
    )


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"ok": True, "streams": len(sessions)}


@app.get("/streams")
async def list_streams():
    global sessions
    sessions = stream_loader.load_streams()
    return {
        "count": len(sessions),
        "streams": [
            {"id": s["id"], "type": s["type"], "url": s["url"]}
            for s in sessions.values()
        ],
    }


@app.get("/play/{stream_id}")
async def play(stream_id: int, request: Request):
    session = get_session(stream_id)
    if session is None:
        raise HTTPException(404, "Stream not found")

    if session["type"] == "DASH":
        return RedirectResponse(f"{request.base_url}manifest/{stream_id}.mpd")

    if session["type"] == "M3U8":
        return RedirectResponse(f"{request.base_url}playlist/{stream_id}.m3u8")

    raise HTTPException(400, f"Unsupported type: {session['type']}")


@app.get("/playlist/{stream_id}.m3u8")
async def playlist(stream_id: int, request: Request):
    session = get_session(stream_id)
    if session is None:
        raise HTTPException(404, "Stream not found")

    r = await fetch_playlist(session["url"], session["headers"].copy())

    if r.status_code != 200:
        raise HTTPException(r.status_code, "Upstream playlist error")

    rewritten = rewrite_m3u8(
        r.text,
        session["url"],
        f"{request.base_url}hls/{stream_id}/"
    )

    return Response(rewritten, media_type="application/vnd.apple.mpegurl")


@app.get("/hls/{stream_id}/{encoded_url:path}")
async def hls(stream_id: int, encoded_url: str, request: Request):
    session = get_session(stream_id)
    if session is None:
        raise HTTPException(404, "Stream not found")

    url = urllib.parse.unquote(encoded_url)
    headers = session["headers"].copy()

    if "range" in request.headers:
        headers["Range"] = request.headers["range"]

    cache_key = (stream_id, url, headers.get("Range"))

    # cache hit
    cached = segment_cache.get(cache_key)
    if cached is not None:
        return Response(
            content=cached["content"],
            status_code=cached["status"],
            media_type=cached["content_type"],
            headers=cached["headers"],
        )

    # IMPORTANT FIX:
    # detect playlist using parsed URL path, not whole URL string
    if is_m3u8_url(url):
        r = await fetch_playlist(url, headers)
        if r.status_code != 200:
            raise HTTPException(r.status_code, "Upstream playlist error")

        rewritten = rewrite_m3u8(
            r.text,
            url,
            f"{request.base_url}hls/{stream_id}/"
        )
        return Response(
            content=rewritten,
            media_type="application/vnd.apple.mpegurl"
        )

    # otherwise treat as segment
    return await serve_segment(url, headers, cache_key)


@app.get("/manifest/{stream_id}.mpd")
async def manifest(stream_id: int, request: Request):
    session = get_session(stream_id)
    if session is None:
        raise HTTPException(404, "Stream not found")

    r = await fetch_playlist(session["url"], session["headers"].copy())

    if r.status_code != 200:
        raise HTTPException(r.status_code, "Unable to fetch MPD")

    proxy_base = f"{request.base_url}proxy/{stream_id}/"

    mpd = re.sub(
        r'(<MPD[^>]*>)',
        rf'\1\n<BaseURL>{proxy_base}</BaseURL>',
        r.text,
        count=1
    )

    return Response(mpd, media_type="application/dash+xml")


@app.get("/proxy/{stream_id}/{path:path}")
async def proxy(stream_id: int, path: str, request: Request):
    session = get_session(stream_id)
    if session is None:
        raise HTTPException(404, "Stream not found")

    base = session.get("base_url") or (session["url"].rsplit("/", 1)[0] + "/")
    url  = urljoin(base, path)

    headers = session["headers"].copy()
    if "range" in request.headers:
        headers["Range"] = request.headers["range"]

    cache_key = (stream_id, url, headers.get("Range"))

    cached = segment_cache.get(cache_key)
    if cached is not None:
        return Response(
            content=cached["content"],
            status_code=cached["status"],
            media_type=cached["content_type"],
            headers=cached["headers"],
        )

    return await serve_segment(url, headers, cache_key)


# ─── Upload ───────────────────────────────────────────────────────────────────

STREAM_FILE = Path("streams.json")

@app.post("/upload")
async def upload(
    request: Request,
    file: UploadFile | None = File(default=None),
):
    try:
        if file is not None:
            content = await file.read()
            data    = json.loads(content)
        else:
            data    = await request.json()
            content = json.dumps(data, indent=2).encode()

        if "streams" not in data:
            raise HTTPException(400, "'streams' field missing")

        STREAM_FILE.write_bytes(content)

        global sessions
        sessions = stream_loader.load_streams()

        return {
            "success":      True,
            "stream_count": len(sessions),
            "types":        sorted({s["type"] for s in sessions.values()}),
        }

    except json.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON")

def is_m3u8_url(url: str) -> bool:
    try:
        return urlparse(url).path.lower().endswith(".m3u8")
    except Exception:
        return url.lower().endswith(".m3u8")