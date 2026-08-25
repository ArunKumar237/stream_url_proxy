import asyncio
import json
import logging
import re
import urllib.parse
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote, urljoin, urlparse

import httpx
from cachetools import TTLCache
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.background import BackgroundTask
from fastapi.middleware.cors import CORSMiddleware
import stream_loader
import resolvers
import scrapers

# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("stream_proxy")
logger.setLevel(logging.INFO)

STREAM_FILE = Path("streams.json")

# Render-friendly limits
CACHE_MAX_TOTAL_BYTES = 96 * 1024 * 1024      # 96 MB total cache
CACHE_MAX_SEGMENT_BYTES = 5 * 1024 * 1024     # cache segments up to 5 MB
CACHE_TTL_SECONDS = 600                       # 10 min
STREAM_CHUNK_SIZE = 128 * 1024                # 128 KB

PREFETCH_SEGMENTS = 2                         # next 2 segments only
PREFETCH_CONCURRENCY = 2                      # keep it safe for Render

# For raw media responses we pass upstream bytes as-is
RAW_FORWARD_HEADERS = (
    "Content-Type",
    "Content-Length",
    "Content-Range",
    "Content-Encoding",
    "Accept-Ranges",
    "ETag",
    "Last-Modified",
    "Cache-Control",
    "Content-Disposition",
)

_URI_RE = re.compile(r'URI="([^"]+)"')

# ──────────────────────────────────────────────────────────────────────────────
# Cache
# ──────────────────────────────────────────────────────────────────────────────

class ByteSizedTTLCache(TTLCache):
    def __init__(self, max_bytes: int, ttl: int):
        super().__init__(maxsize=max_bytes, ttl=ttl, getsizeof=self._sizeof)

    @staticmethod
    def _sizeof(value):
        if isinstance(value, dict) and "content" in value:
            return max(1, len(value["content"]))
        return 1


segment_cache = ByteSizedTTLCache(
    max_bytes=CACHE_MAX_TOTAL_BYTES,
    ttl=CACHE_TTL_SECONDS,
)

# segment_url -> [next_segment_1, next_segment_2]
next_segments_map: dict[str, list[str]] = {}

# avoid duplicate background prefetch
prefetch_inflight: set[tuple[int, str]] = set()
prefetch_semaphore = asyncio.Semaphore(PREFETCH_CONCURRENCY)

# ──────────────────────────────────────────────────────────────────────────────
# Clients
# ──────────────────────────────────────────────────────────────────────────────

# text client for playlists/manifests
text_client = httpx.AsyncClient(
    follow_redirects=True,
    http2=False,
    timeout=httpx.Timeout(connect=10, read=20, write=10, pool=10),
    limits=httpx.Limits(
        max_connections=20,
        max_keepalive_connections=10,
        keepalive_expiry=20,
    ),
)

# raw client for media segments
raw_client = httpx.AsyncClient(
    follow_redirects=True,
    http2=False,
    timeout=httpx.Timeout(connect=10, read=60, write=10, pool=10),
    limits=httpx.Limits(
        max_connections=20,
        max_keepalive_connections=10,
        keepalive_expiry=20,
    ),
    headers={
        "Accept-Encoding": "identity",   # ask upstream for raw bytes
    },
)

# ──────────────────────────────────────────────────────────────────────────────
# App state
# ──────────────────────────────────────────────────────────────────────────────

sessions: dict = {}


def load_sessions() -> dict:
    return stream_loader.load_streams()


def reset_runtime_state():
    segment_cache.clear()
    next_segments_map.clear()
    prefetch_inflight.clear()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global sessions
    sessions = load_sessions()
    logger.warning("Loaded %d streams", len(sessions))
    yield
    await text_client.aclose()
    await raw_client.aclose()


app = FastAPI(
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def get_session(stream_id: int) -> dict | None:
    return sessions.get(stream_id)


def is_m3u8_url(url: str) -> bool:
    try:
        path = urlparse(url).path.lower()
        return path.endswith(".m3u8") or path.endswith(".m3u")
    except Exception:
        clean = url.split("?")[0].lower()
        return clean.endswith(".m3u8") or clean.endswith(".m3u")


def is_dash_url(url: str) -> bool:
    try:
        path = urlparse(url).path.lower()
        return path.endswith(".mpd")
    except Exception:
        clean = url.split("?")[0].lower()
        return clean.endswith(".mpd")


def guess_video_content_type(url: str, default: str = "video/mp4") -> str:
    try:
        clean_path = urlparse(url).path.lower()
    except Exception:
        clean_path = url.split("?")[0].lower()

    if clean_path.endswith((".mp4", ".m4v", ".m4a")):
        return "video/mp4"
    if clean_path.endswith(".mkv"):
        return "video/x-matroska"
    if clean_path.endswith(".webm"):
        return "video/webm"
    if clean_path.endswith(".avi"):
        return "video/x-msvideo"
    if clean_path.endswith(".mov"):
        return "video/quicktime"
    if clean_path.endswith(".flv"):
        return "video/x-flv"
    if clean_path.endswith(".ts"):
        return "video/mp2t"
    if clean_path.endswith(".3gp"):
        return "video/3gpp"
    return default


def unique_urls(urls: list[str]) -> list[str]:
    seen = set()
    out = []
    for url in urls:
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out


def pick_raw_headers(headers: httpx.Headers) -> dict:
    return {h: headers[h] for h in RAW_FORWARD_HEADERS if h in headers}


def make_binary_response(body: bytes, status_code: int, headers: dict) -> Response:
    final_headers = dict(headers)
    final_headers.setdefault("Content-Type", "application/octet-stream")
    return Response(
        content=body,
        status_code=status_code,
        headers=final_headers,
    )


DEFAULT_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def prepare_upstream_headers(url: str, headers: dict) -> dict:
    req_headers = dict(headers or {})
    if not any(k.lower() == "user-agent" for k in req_headers):
        req_headers["User-Agent"] = DEFAULT_BROWSER_UA
    if not any(k.lower() == "referer" for k in req_headers):
        url_lower = url.lower()
        if "slast430did" in url_lower or "allmovieland" in url_lower:
            req_headers["Referer"] = "https://allmovieland.one/"
        elif "moviezwap" in url_lower or "moviezzwaphd" in url_lower:
            req_headers["Referer"] = "https://www.moviezwap.taxi/"
        elif "klcxm.com" in url_lower:
            req_headers["Referer"] = "https://api.hlowb.com"
        elif "mycdn-mb.xyz" in url_lower:
            req_headers["Referer"] = "MovieBlast"
            req_headers["User-Agent"] = "MovieBlast"
            req_headers["x-request-x"] = "com.movieblast"
    return req_headers


async def fetch_text(url: str, headers: dict) -> httpx.Response:
    req_headers = prepare_upstream_headers(url, headers)
    try:
        r = await text_client.get(url, headers=req_headers)
        logger.info(f"[UPSTREAM_TEXT_RESP] Status: {r.status_code} | Content-Type: {r.headers.get('content-type', 'N/A')} | URL: {url}")
        return r
    except httpx.TimeoutException:
        logger.error(f"[UPSTREAM_TIMEOUT] Request timed out for: {url}")
        raise HTTPException(504, f"Upstream timeout for {url}")
    except httpx.ConnectError as e:
        logger.error(f"[UPSTREAM_CONNECT_ERR] Cannot connect to: {url} | {e}")
        raise HTTPException(502, f"Cannot reach upstream: {url}")
    except httpx.HTTPError as e:
        logger.error(f"[UPSTREAM_HTTP_ERR] HTTP error for: {url} | {e}")
        raise HTTPException(502, f"Upstream error: {e}")


async def fetch_raw_stream(url: str, headers: dict) -> httpx.Response:
    req_headers = prepare_upstream_headers(url, headers)
    try:
        req = raw_client.build_request("GET", url, headers=req_headers)
        r = await raw_client.send(req, stream=True)
        logger.info(
            f"[UPSTREAM_RAW_RESP] Status: {r.status_code} | "
            f"Content-Type: {r.headers.get('content-type', 'N/A')} | "
            f"Length: {r.headers.get('content-length', 'chunked')} | "
            f"URL: {url}"
        )
        return r
    except httpx.TimeoutException:
        logger.error(f"[UPSTREAM_TIMEOUT] Media stream request timed out for: {url}")
        raise HTTPException(504, f"Upstream timeout for {url}")
    except httpx.ConnectError as e:
        logger.error(f"[UPSTREAM_CONNECT_ERR] Cannot connect to media host: {url} | {e}")
        raise HTTPException(502, f"Cannot reach upstream: {url}")
    except httpx.HTTPError as e:
        logger.error(f"[UPSTREAM_HTTP_ERR] Media stream error for: {url} | {e}")
        raise HTTPException(502, f"Upstream error: {e}")


async def read_raw_body(response: httpx.Response) -> bytes:
    buf = bytearray()
    async for chunk in response.aiter_raw():
        buf.extend(chunk)
    return bytes(buf)


def encode_hls_url(url: str) -> str:
    import base64
    return base64.urlsafe_b64encode(url.encode("utf-8")).decode("utf-8").rstrip("=")


def decode_hls_url(encoded: str) -> str:
    import base64
    if encoded.startswith("http://") or encoded.startswith("https://"):
        return encoded
    try:
        padded = encoded + "=" * (-len(encoded) % 4)
        decoded = base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8")
        if decoded.startswith("http://") or decoded.startswith("https://"):
            return decoded
    except Exception:
        pass
    unquoted = urllib.parse.unquote(encoded)
    if unquoted.startswith("http://") or unquoted.startswith("https://"):
        return unquoted
    return encoded


def rewrite_m3u8(
    text: str,
    playlist_url: str,
    proxy_base: str,
) -> tuple[str, list[str]]:
    out = []
    media_segment_urls: list[str] = []
    initial_prefetch_urls: list[str] = []

    for line in text.splitlines():
        stripped = line.strip()

        if not stripped:
            out.append(line)
            continue

        # Rewrite URI="..." tags
        if stripped.startswith((
            "#EXT-X-MEDIA",
            "#EXT-X-I-FRAME-STREAM-INF",
            "#EXT-X-MAP",
            "#EXT-X-KEY",
        )):
            def replace_uri(match):
                absolute = urljoin(playlist_url, match.group(1))

                # prefetch init segment if present
                if stripped.startswith("#EXT-X-MAP") and not is_m3u8_url(absolute):
                    initial_prefetch_urls.append(absolute)

                return f'URI="{proxy_base}{encode_hls_url(absolute)}"'

            out.append(_URI_RE.sub(replace_uri, line))
            continue

        # Keep non-URI tags unchanged
        if stripped.startswith("#"):
            out.append(line)
            continue

        # Rewrite segment/sub-playlist line
        absolute = urljoin(playlist_url, stripped)
        out.append(proxy_base + encode_hls_url(absolute))

        # only store actual media segment URLs, not nested playlists
        if not is_m3u8_url(absolute):
            media_segment_urls.append(absolute)

    # Build next-segment map for background prefetch after each segment request
    if media_segment_urls:
        for i, seg_url in enumerate(media_segment_urls):
            next_segments_map[seg_url] = media_segment_urls[i + 1:i + 1 + PREFETCH_SEGMENTS]

    # Prefetch init + first few real segments when playlist is first loaded
    initial_prefetch_urls.extend(media_segment_urls[:PREFETCH_SEGMENTS])

    return "\n".join(out), unique_urls(initial_prefetch_urls)


async def background_prefetch(stream_id: int, urls: list[str], headers: dict):
    urls = unique_urls(urls)

    for url in urls:
        key = (stream_id, url)

        if key in prefetch_inflight:
            continue

        cache_key = (stream_id, url, None)
        if segment_cache.get(cache_key) is not None:
            continue

        prefetch_inflight.add(key)

        try:
            async with prefetch_semaphore:
                response = await fetch_raw_stream(url, headers)

                try:
                    if response.status_code != 200:
                        continue

                    declared_length = int(response.headers.get("Content-Length", "0") or "0")

                    # Render-friendly: only prefetch/cache smaller segments
                    if declared_length <= 0 or declared_length > CACHE_MAX_SEGMENT_BYTES:
                        continue

                    body = await read_raw_body(response)
                    resp_headers = pick_raw_headers(response.headers)
                    resp_headers.setdefault(
                        "Content-Type",
                        response.headers.get("Content-Type", "application/octet-stream")
                    )

                    segment_cache[cache_key] = {
                        "content": body,
                        "status": response.status_code,
                        "headers": resp_headers,
                    }
                finally:
                    await response.aclose()

        except Exception:
            pass
        finally:
            prefetch_inflight.discard(key)


def schedule_prefetch(stream_id: int, urls: list[str], headers: dict):
    if not urls:
        return

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(background_prefetch(stream_id, urls[:3], headers.copy()))
    except RuntimeError:
        pass


async def serve_raw_segment(
    stream_id: int,
    url: str,
    headers: dict,
    cache_key: tuple,
) -> Response | StreamingResponse:
    response = await fetch_raw_stream(url, headers)
    resp_headers = pick_raw_headers(response.headers)
    resp_headers.setdefault(
        "Content-Type",
        response.headers.get("Content-Type", "application/octet-stream")
    )

    status_code = response.status_code
    declared_length = int(response.headers.get("Content-Length", "0") or "0")

    # Do not cache error responses
    if status_code not in (200, 206):
        logger.warning(f"[SEGMENT_WARN] Upstream returned status {status_code} for: {url}")
        return StreamingResponse(
            response.aiter_raw(),
            status_code=status_code,
            headers=resp_headers,
            background=BackgroundTask(response.aclose),
        )

    # Buffer + cache small responses
    if 0 < declared_length <= CACHE_MAX_SEGMENT_BYTES:
        try:
            body = await read_raw_body(response)
        finally:
            await response.aclose()

        segment_cache[cache_key] = {
            "content": body,
            "status": status_code,
            "headers": resp_headers,
        }

        return make_binary_response(body, status_code, resp_headers)

    # Stream large responses
    return StreamingResponse(
        response.aiter_raw(),
        status_code=status_code,
        headers=resp_headers,
        background=BackgroundTask(response.aclose),
    )

# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "ok": True,
        "streams": len(sessions),
        "cache_entries": len(segment_cache),
        "cache_bytes": segment_cache.currsize,
        "cache_mb": round(segment_cache.currsize / 1024 / 1024, 2),
    }


@app.get("/streams")
async def list_streams():
    global sessions
    sessions = load_sessions()
    return {
        "count": len(sessions),
        "streams": [
            {
                "id": s["id"],
                "type": s["type"],
                "url": s["url"],
            }
            for s in sessions.values()
        ],
    }


@app.get("/play/{stream_id}")
@app.head("/play/{stream_id}")
async def play(stream_id: int, request: Request):
    session = get_session(stream_id)
    if session is None:
        logger.warning(f"[Stream {stream_id}] [PLAY] Stream not found (404)")
        raise HTTPException(404, "Stream not found")

    stype = session.get("type", "").upper()
    url = session.get("url", "")
    logger.info(f"[Stream {stream_id}] [PLAY] Type: {stype} | Target URL: {url}")

    if stype in ("DASH", "MPD") or is_dash_url(url):
        return RedirectResponse(f"{request.base_url}manifest/{stream_id}.mpd")

    if stype in ("M3U8", "HLS", "M3U") or is_m3u8_url(url):
        return RedirectResponse(f"{request.base_url}playlist/{stream_id}.m3u8")

    return RedirectResponse(f"{request.base_url}video/{stream_id}")


@app.get("/playlist/{stream_id}.m3u8")
async def playlist(stream_id: int, request: Request):
    session = get_session(stream_id)
    if session is None:
        raise HTTPException(404, "Stream not found")

    url = session["url"]
    logger.info(f"[Stream {stream_id}] [M3U8 Master] Fetching playlist from: {url}")
    headers = session["headers"].copy()
    response = await fetch_text(url, headers)

    if response.status_code != 200:
        logger.error(f"[Stream {stream_id}] [M3U8 Master ERROR] Upstream status {response.status_code} for URL: {url}")
        raise HTTPException(response.status_code, "Upstream playlist error")

    rewritten, prefetch_urls = rewrite_m3u8(
        response.text,
        url,
        f"{request.base_url}hls/{stream_id}/",
    )

    # prefetch init + first two segments
    schedule_prefetch(stream_id, prefetch_urls, session["headers"])

    return Response(
        content=rewritten,
        media_type="application/vnd.apple.mpegurl",
    )


@app.get("/hls/{stream_id}/{encoded_url:path}")
async def hls(stream_id: int, encoded_url: str, request: Request):
    session = get_session(stream_id)
    if session is None:
        raise HTTPException(404, "Stream not found")

    url = decode_hls_url(encoded_url)

    base_headers = session["headers"].copy()
    request_headers = base_headers.copy()

    if "range" in request.headers:
        request_headers["Range"] = request.headers["range"]

    cache_key = (stream_id, url, request_headers.get("Range"))

    # Cache hit
    cached = segment_cache.get(cache_key)
    if cached is not None:
        return make_binary_response(
            cached["content"],
            cached["status"],
            cached["headers"],
        )

    # Detect playlist correctly even with query params
    if is_m3u8_url(url):
        logger.info(f"[Stream {stream_id}] [M3U8 Variant] Fetching sub-playlist from: {url}")
        response = await fetch_text(url, request_headers)

        if response.status_code != 200:
            logger.error(f"[Stream {stream_id}] [M3U8 Variant ERROR] Upstream status {response.status_code} for: {url}")
            raise HTTPException(response.status_code, "Upstream playlist error")

        rewritten, prefetch_urls = rewrite_m3u8(
            response.text,
            url,
            f"{request.base_url}hls/{stream_id}/",
        )

        schedule_prefetch(stream_id, prefetch_urls, base_headers)

        return Response(
            content=rewritten,
            media_type="application/vnd.apple.mpegurl",
        )

    # Segment
    logger.info(f"[Stream {stream_id}] [HLS Segment] Fetching: {url} | Range: {request_headers.get('Range') or 'Full'}")
    resp = await serve_raw_segment(
        stream_id=stream_id,
        url=url,
        headers=request_headers,
        cache_key=cache_key,
    )

    # Only prefetch next full segments, not ranged requests
    if "Range" not in request_headers:
        schedule_prefetch(stream_id, next_segments_map.get(url, []), base_headers)

    return resp


@app.get("/manifest/{stream_id}.mpd")
async def manifest(stream_id: int, request: Request):
    session = get_session(stream_id)
    if session is None:
        raise HTTPException(404, "Stream not found")

    url = session["url"]
    logger.info(f"[Stream {stream_id}] [DASH MPD] Fetching manifest from: {url}")
    response = await fetch_text(url, session["headers"].copy())

    if response.status_code != 200:
        logger.error(f"[Stream {stream_id}] [DASH ERROR] Upstream status {response.status_code} for: {url}")
        raise HTTPException(response.status_code, "Unable to fetch MPD")

    proxy_base = f"{request.base_url}proxy/{stream_id}/"

    mpd = re.sub(
        r'(<MPD[^>]*>)',
        rf'\1\n<BaseURL>{proxy_base}</BaseURL>',
        response.text,
        count=1,
    )

    return Response(
        content=mpd,
        media_type="application/dash+xml",
    )


@app.get("/proxy/{stream_id}/{path:path}")
async def proxy(stream_id: int, path: str, request: Request):
    session = get_session(stream_id)
    if session is None:
        raise HTTPException(404, "Stream not found")

    base_url = session.get("base_url") or (session["url"].rsplit("/", 1)[0] + "/")
    url = urljoin(base_url, path)
    logger.info(f"[Stream {stream_id}] [DASH Segment] Fetching: {url}")

    headers = session["headers"].copy()
    if "range" in request.headers:
        headers["Range"] = request.headers["range"]

    cache_key = (stream_id, url, headers.get("Range"))

    cached = segment_cache.get(cache_key)
    if cached is not None:
        return make_binary_response(
            cached["content"],
            cached["status"],
            cached["headers"],
        )

    return await serve_raw_segment(
        stream_id=stream_id,
        url=url,
        headers=headers,
        cache_key=cache_key,
    )


@app.get("/video/{stream_id}")
@app.get("/video/{stream_id}.mp4")
@app.get("/video/{stream_id}/{filename:path}")
@app.get("/stream/{stream_id}")
@app.get("/direct/{stream_id}")
@app.head("/video/{stream_id}")
@app.head("/video/{stream_id}.mp4")
@app.head("/video/{stream_id}/{filename:path}")
@app.head("/stream/{stream_id}")
@app.head("/direct/{stream_id}")
async def video(stream_id: int, request: Request, filename: str | None = None):
    session = get_session(stream_id)
    if session is None:
        logger.warning(f"[Stream {stream_id}] [VIDEO] Stream not found (404)")
        raise HTTPException(404, "Stream not found")

    url = session.get("resolved_url") or session["url"]
    base_headers = session.get("resolved_headers") or session["headers"]
    headers = base_headers.copy()

    # Forward client Range and conditional headers to upstream
    if "range" in request.headers:
        headers["Range"] = request.headers["range"]
    if "if-range" in request.headers:
        headers["If-Range"] = request.headers["if-range"]

    logger.info(f"[Stream {stream_id}] [VIDEO] Fetching video stream: {url} | Range: {headers.get('Range') or 'Full'}")

    # Handle HEAD request
    if request.method == "HEAD":
        try:
            head_resp = await raw_client.head(url, headers=headers)
            resp_headers = pick_raw_headers(head_resp.headers)
            resp_headers.setdefault("Accept-Ranges", "bytes")
            if "Content-Type" not in resp_headers:
                resp_headers["Content-Type"] = guess_video_content_type(url)
            return Response(status_code=head_resp.status_code, headers=resp_headers)
        except Exception:
            pass

    response = await fetch_raw_stream(url, headers)
    content_type = response.headers.get("content-type", "").lower()

    # If upstream returned HTML error/redirect (e.g. expired link / IP-bound token), attempt auto-resolution
    if "text/html" in content_type or response.status_code not in (200, 206):
        logger.warning(
            f"[Stream {stream_id}] [AUTO-RESOLVE] Upstream status {response.status_code} / Content-Type '{content_type}'. "
            f"Attempting to auto-resolve fresh URL for: {session['url']}"
        )
        await response.aclose()
        resolved_url, resolved_headers = await resolvers.resolve_stream(session["url"], session["headers"])
        if resolved_url and resolved_url != url:
            logger.info(f"[Stream {stream_id}] [AUTO-RESOLVE SUCCESS] Resolved to: {resolved_url}")
            session["resolved_url"] = resolved_url
            session["resolved_headers"] = resolved_headers
            url = resolved_url
            headers = resolved_headers.copy()
            if "range" in request.headers:
                headers["Range"] = request.headers["range"]
            if "if-range" in request.headers:
                headers["If-Range"] = request.headers["if-range"]
            # Retry with fresh resolved URL
            response = await fetch_raw_stream(url, headers)
            content_type = response.headers.get("content-type", "").lower()

    if "text/html" in content_type:
        logger.error(f"[Stream {stream_id}] [VIDEO ERROR] Upstream returned HTML instead of media for: {url}")
        await response.aclose()
        raise HTTPException(
            status_code=403,
            detail="Upstream server returned an HTML webpage instead of a media stream. The video URL or security token may be expired or invalid.",
        )

    resp_headers = pick_raw_headers(response.headers)
    resp_headers.setdefault("Accept-Ranges", "bytes")
    if "Content-Type" not in resp_headers:
        resp_headers["Content-Type"] = guess_video_content_type(url)

    async def stream_raw():
        try:
            async for chunk in response.aiter_raw(chunk_size=STREAM_CHUNK_SIZE):
                yield chunk
        finally:
            await response.aclose()

    return StreamingResponse(
        stream_raw(),
        status_code=response.status_code,
        headers=resp_headers,
        background=BackgroundTask(response.aclose),
    )


STATIC_DIR = Path(__file__).parent / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def root():
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))
    return {
        "status": "online",
        "service": "MovieBox Stream URL Proxy",
        "endpoints": {
            "search": "/api/search?q={movie_name}",
            "resolve": "/api/resolve",
            "streams": "/streams",
            "upload": "/upload",
        }
    }


@app.get("/api/search")
async def api_search(q: str = ""):
    if not q.strip():
        return {"query": q, "count": 0, "results": []}
    results = await scrapers.search_all_movies(q.strip())
    return {"query": q, "count": len(results), "results": results}


@app.post("/api/resolve")
async def api_resolve(request: Request):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON body")

    movie_url = data.get("url")
    provider = data.get("provider", "Unknown")
    quality = data.get("quality", "720p")

    if not movie_url:
        raise HTTPException(400, "Missing 'url' parameter")

    logger.info(f"[API_RESOLVE] Resolving '{movie_url}' from provider '{provider}' (target={quality})...")

    resolved_url, resolved_headers = await resolvers.resolve_stream(movie_url, {}, target_quality=quality)
    if not resolved_url:
        logger.error(f"[API_RESOLVE ERROR] Could not resolve video stream from: {movie_url}")
        return {"success": False, "detail": "Could not resolve stream link from provider"}

    # Determine stream type
    url_lower = resolved_url.lower()
    if ".m3u8" in url_lower:
        stype = "M3U8"
    elif ".mpd" in url_lower:
        stype = "DASH"
    else:
        stype = "VIDEO"

    # Allocate next session ID
    global sessions
    stream_id = max(sessions.keys(), default=-1) + 1 if sessions else 0
    sessions[stream_id] = {
        "id": stream_id,
        "type": stype,
        "url": resolved_url,
        "base_url": resolved_url.rsplit("/", 1)[0] + "/",
        "headers": resolved_headers or {},
    }

    logger.info(f"[API_RESOLVE SUCCESS] Registered Stream {stream_id} [{stype}] -> {resolved_url}")

    hls_endpoint = f"/playlist/{stream_id}.m3u8" if stype == "M3U8" else None
    video_endpoint = f"/video/{stream_id}" if stype == "VIDEO" else None

    return {
        "success": True,
        "stream_id": stream_id,
        "type": stype,
        "play_url": f"/play/{stream_id}",
        "hls_url": hls_endpoint,
        "video_url": video_endpoint,
        "target_url": resolved_url,
    }


@app.post("/upload")
async def upload(
    request: Request,
    file: UploadFile | None = File(default=None),
):
    try:
        if file is not None:
            content = await file.read()
            data = json.loads(content)
        else:
            data = await request.json()
            content = json.dumps(data, indent=2).encode("utf-8")

        if "streams" not in data:
            raise HTTPException(400, "Invalid export: 'streams' field missing")

        STREAM_FILE.write_bytes(content)

        global sessions
        sessions = load_sessions()
        reset_runtime_state()

        logger.info(f"[UPLOAD] Successfully uploaded {len(sessions)} stream(s):")
        for s in sessions.values():
            logger.info(f"  -> Stream {s['id']} [{s['type']}]: {s['url']}")

        return {
            "success": True,
            "message": "Upload successful",
            "stream_count": len(sessions),
            "types": sorted({s["type"] for s in sessions.values()}),
        }

    except json.JSONDecodeError:
        logger.error("[UPLOAD ERROR] Invalid JSON provided")
        raise HTTPException(400, "Invalid JSON")