import asyncio
import json
import logging
import re
from urllib.parse import parse_qs, urljoin, urlparse
import httpx

logger = logging.getLogger("stream_proxy.resolvers")

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# Common media file extensions and MIME indicators
MEDIA_EXT_PATTERN = re.compile(
    r'\.(?:mp4|m3u8|mpd|mkv|webm|ts|m4v|avi|mov)(?:\?[^"\'\s<>]*)?$',
    re.IGNORECASE,
)

DIRECT_MEDIA_RE = re.compile(
    r'(https?://[^\s"\'<>]+\.(?:mp4|m3u8|mpd|mkv|webm|ts|m4v)(?:\?[^\s"\'<>]*)?)',
    re.IGNORECASE,
)

# Regex for common video player configurations in JS
PLAYER_SOURCES_RE = [
    re.compile(r'file\s*:\s*["\'](https?://[^"\'\s]+)["\']', re.IGNORECASE),
    re.compile(r'source\s*:\s*["\'](https?://[^"\'\s]+)["\']', re.IGNORECASE),
    re.compile(r'src\s*:\s*["\'](https?://[^"\'\s]+)["\']', re.IGNORECASE),
    re.compile(r'hls\s*:\s*["\'](https?://[^"\'\s]+)["\']', re.IGNORECASE),
    re.compile(r'url\s*:\s*["\'](https?://[^"\'\s]+\.(?:mp4|m3u8|mpd|mkv)[^"\'\s]*)["\']', re.IGNORECASE),
    re.compile(r'["\'](https?://[^"\'\s]+\.(?:m3u8|mpd|mp4)[^"\'\s]*)["\']', re.IGNORECASE),
]

# Dean Edwards Packer unpacker
PACKER_RE = re.compile(r'eval\(function\(p,a,c,k,e,d\).+?\}\s*\((.+?)\)\)', re.DOTALL)


def unpack_packer(html: str) -> str:
    """Unpacks Dean Edwards p.a.c.k.e.r javascript obfuscation."""
    unpacked_all = []
    for match in PACKER_RE.finditer(html):
        try:
            payload = match.group(1)
            # Basic unpacker implementation
            parts = payload.rsplit(",", 3)
            if len(parts) != 4:
                continue
            p, a, c, k = parts[0].strip(), int(parts[1].strip()), int(parts[2].strip()), parts[3].strip()
            # If p is quoted, unquote
            if (p.startswith("'") and p.endswith("'")) or (p.startswith('"') and p.endswith('"')):
                p = p[1:-1]
            # k is dictionary or array
            if k.startswith("[") and k.endswith("]"):
                k_list = [x.strip(" '\"") for x in k[1:-1].split(",")]
            else:
                k_list = k.split("|")

            def encode_base(num, base):
                chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
                res = ""
                while num > 0:
                    res = chars[num % base] + res
                    num //= base
                return res or "0"

            def get_word(c_idx):
                return k_list[c_idx] if c_idx < len(k_list) and k_list[c_idx] else encode_base(c_idx, a)

            res = p
            for i in range(c - 1, -1, -1):
                word = get_word(i)
                if word:
                    res = re.sub(rf'\b{encode_base(i, a)}\b', word, res)
            unpacked_all.append(res)
        except Exception:
            pass
    return "\n".join(unpacked_all)


# ──────────────────────────────────────────────────────────────────────────────
# Generic HTML Media Extractor
# ──────────────────────────────────────────────────────────────────────────────

def extract_media_links_from_html(html: str, base_url: str) -> list[str]:
    """
    Scans any HTML page for direct video links, player configs, or embedded media tags.
    """
    found = []

    # 1. Look for <video src="..."> and <source src="..."> tags
    for tag_match in re.finditer(r'<(?:video|source)[^>]+src=["\']([^"\']+)["\']', html, re.IGNORECASE):
        src = tag_match.group(1).strip()
        if src:
            found.append(urljoin(base_url, src))

    # 2. Look for JS player source patterns
    for pat in PLAYER_SOURCES_RE:
        for m in pat.finditer(html):
            link = m.group(1).strip().replace(r'\/', '/')
            if MEDIA_EXT_PATTERN.search(link) or "m3u8" in link or "mp4" in link:
                found.append(urljoin(base_url, link))

    # 3. Look for <a> tags pointing directly to media
    for a_match in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\']', html, re.IGNORECASE):
        href = a_match.group(1).strip()
        if href and MEDIA_EXT_PATTERN.search(href):
            found.append(urljoin(base_url, href))

    # 4. Unpack packer scripts and look inside
    unpacked = unpack_packer(html)
    if unpacked:
        for pat in PLAYER_SOURCES_RE:
            for m in pat.finditer(unpacked):
                link = m.group(1).strip().replace(r'\/', '/')
                if MEDIA_EXT_PATTERN.search(link) or "m3u8" in link or "mp4" in link:
                    found.append(urljoin(base_url, link))

    # Deduplicate while preserving order
    seen = set()
    unique_links = []
    for link in found:
        # Ignore obvious non-media assets
        if link.lower().endswith((".jpg", ".png", ".gif", ".css", ".js", ".html", ".php")):
            continue
        if link not in seen:
            seen.add(link)
            unique_links.append(link)

    return unique_links


def extract_intermediate_links(html: str, base_url: str) -> list[str]:
    """
    Finds intermediate download/stream buttons or next-step links in HTML.
    """
    candidates = []
    # Match href with download/stream keywords
    for a_match in re.finditer(r'<a[^>]+href=["\']([^"\']+)["\']', html, re.IGNORECASE):
        href = a_match.group(1).strip()
        if not href or href.startswith(("#", "javascript:", "whatsapp:", "tg:", "mailto:")):
            continue
        lower_href = href.lower()
        if any(kw in lower_href for kw in ("download", "dwload", "get_link", "stream", "file", "play", "watch", "server")):
            abs_url = urljoin(base_url, href)
            if abs_url not in candidates:
                candidates.append(abs_url)
    return candidates


# ──────────────────────────────────────────────────────────────────────────────
# Universal Recursive Resolver
# ──────────────────────────────────────────────────────────────────────────────

async def generic_crawl_resolve(
    url: str,
    headers: dict,
    max_depth: int = 3,
) -> tuple[str | None, dict]:
    """
    Universally crawls any webpage, follows download/embed chains, and resolves
    the true direct media stream URL.
    """
    current_url = url
    current_headers = dict(headers)
    visited = set()

    for depth in range(max_depth):
        if current_url in visited:
            break
        visited.add(current_url)

        try:
            logger.info(f"[RESOLVER_CRAWL] [Depth {depth + 1}/{max_depth}] Fetching: {current_url}")
            async with httpx.AsyncClient(follow_redirects=True, timeout=12) as client:
                req_headers = {
                    "User-Agent": DEFAULT_USER_AGENT,
                    **current_headers,
                }
                r = await client.get(current_url, headers=req_headers)
                final_url = str(r.url)
                content_type = r.headers.get("content-type", "").lower()
                logger.info(f"[RESOLVER_CRAWL] [Depth {depth + 1}] Status: {r.status_code} | Content-Type: {content_type} | URL: {final_url}")

                # If the response is already direct binary media (not HTML), we found it!
                if "text/html" not in content_type and r.status_code in (200, 206):
                    logger.info(f"[RESOLVER_FOUND] Direct stream discovered at: {final_url}")
                    return final_url, current_headers

                # Scan HTML for direct media links
                media_links = extract_media_links_from_html(r.text, final_url)
                if media_links:
                    chosen = media_links[-1] if len(media_links) > 1 and "720p" in media_links[-1] else media_links[0]
                    logger.info(f"[RESOLVER_FOUND] Extracted media stream from HTML: {chosen}")
                    return chosen, {**current_headers, "Referer": final_url}

                # Otherwise, follow intermediate download/stream button
                next_links = extract_intermediate_links(r.text, final_url)
                if next_links:
                    logger.info(f"[RESOLVER_NEXT] Following intermediate download link: {next_links[0]}")
                    current_url = next_links[0]
                    current_headers["Referer"] = final_url
                    continue
                else:
                    logger.warning(f"[RESOLVER_CRAWL] No media or intermediate links found in HTML at: {final_url}")
                    break

        except Exception as e:
            logger.error(f"[RESOLVER_ERROR] Crawl error for {current_url}: {e}")
            break

    return None, current_headers


# ──────────────────────────────────────────────────────────────────────────────
# Provider-Specific Enhancements (Auto-Resolvers)
# ──────────────────────────────────────────────────────────────────────────────

async def resolve_moviezwap(url: str, headers: dict) -> tuple[str | None, dict]:
    """Specialized handler for MoviezWap sites to extract live stream tokens."""
    target_quality = "720p"
    if "1080p" in url.lower():
        target_quality = "1080p"
    elif "480p" in url.lower():
        target_quality = "480p"
    elif "320p" in url.lower():
        target_quality = "320p"

    logger.info(f"[RESOLVER_MOVIEZWAP] Initializing extractor (target_quality={target_quality}) for: {url}")

    async with httpx.AsyncClient(follow_redirects=True, timeout=12) as client:
        req_headers = {
            "User-Agent": DEFAULT_USER_AGENT,
            **headers,
        }

        # If dwload / download link directly
        if "file=" in url:
            file_id = parse_qs(urlparse(url).query).get("file", [""])[0]
            dl_url = f"https://www.moviezwap.taxi/download.php?file={file_id}"
            logger.info(f"[RESOLVER_MOVIEZWAP] Resolving file_id={file_id} via: {dl_url}")
            r = await client.get(dl_url, headers={**req_headers, "Referer": f"https://www.moviezwap.taxi/dwload.php?file={file_id}"})
            links = [l for l in re.findall(r'href=["\'](.*?)["\']', r.text) if ".mp4" in l]
            if links:
                logger.info(f"[RESOLVER_MOVIEZWAP] Direct stream extracted: {links[0]}")
                return links[0], req_headers

        # Extract movie slug from URL path
        movie_slug = None
        if "/movie/" in url:
            m = re.search(r'/movie/([^/]+?)\.html', url)
            if m:
                movie_slug = m.group(1)
        else:
            path = urlparse(url).path
            parts = [p for p in path.split("/") if p]
            if len(parts) >= 2:
                candidate = parts[-2]
                if "(" in candidate:
                    movie_slug = candidate
                else:
                    filename = parts[-1].replace(".mp4", "")
                    movie_slug = re.sub(r'-(?:DVDScr|HDRip|HD|HQ|720p|480p|320p|1080p).*', '', filename, flags=re.IGNORECASE)

        if movie_slug:
            movie_page = f"https://www.moviezwap.taxi/movie/{movie_slug}.html"
            logger.info(f"[RESOLVER_MOVIEZWAP] Scraping movie page: {movie_page}")
            r = await client.get(movie_page, headers=req_headers)
            dw_links = re.findall(r'href=["\'](/dwload\.php\?file=\d+|https?://[^"\']*/dwload\.php\?file=\d+)["\']', r.text)
            if dw_links:
                chosen = dw_links[-1] if target_quality in ("720p", "1080p") else dw_links[0]
                if not chosen.startswith("http"):
                    chosen = "https://www.moviezwap.taxi" + (chosen if chosen.startswith("/") else "/" + chosen)
                file_id = parse_qs(urlparse(chosen).query).get("file", [""])[0]
                dl_url = f"https://www.moviezwap.taxi/download.php?file={file_id}"
                logger.info(f"[RESOLVER_MOVIEZWAP] Fetching final download page: {dl_url}")
                r_dl = await client.get(dl_url, headers={**req_headers, "Referer": chosen})
                mp4_links = [l for l in re.findall(r'href=["\'](.*?)["\']', r_dl.text) if ".mp4" in l]
                if mp4_links:
                    logger.info(f"[RESOLVER_MOVIEZWAP] Stream token successfully generated: {mp4_links[0]}")
                    return mp4_links[0], req_headers
            else:
                logger.warning(f"[RESOLVER_MOVIEZWAP] No dwload links found on page: {movie_page}")

    return None, headers


# ──────────────────────────────────────────────────────────────────────────────
# Master Resolver
# ──────────────────────────────────────────────────────────────────────────────

async def resolve_stream(url: str, headers: dict) -> tuple[str, dict]:
    """
    Generalized entry point that handles ANY streaming provider:
    1. Provider-specific extractors if matched (e.g. MoviezWap).
    2. Universal recursive crawl extractor for any other streaming site or embed.
    3. Fallback to original URL and headers.
    """
    url_lower = url.lower()
    logger.info(f"[RESOLVER_START] Starting resolution for input URL: {url}")

    # Provider 1: MoviezWap
    if "moviezwap" in url_lower or "moviezzwaphd" in url_lower:
        try:
            res_url, res_headers = await resolve_moviezwap(url, headers)
            if res_url:
                logger.info(f"[RESOLVER_SUCCESS] Resolved via MoviezWap extractor -> {res_url}")
                return res_url, res_headers
        except Exception as e:
            logger.error(f"[RESOLVER_ERROR] MoviezWap resolver exception: {e}")

    # Universal / Generic Extractor for any other provider or HTML landing page
    try:
        res_url, res_headers = await generic_crawl_resolve(url, headers, max_depth=3)
        if res_url:
            logger.info(f"[RESOLVER_SUCCESS] Resolved via Universal Crawler -> {res_url}")
            return res_url, res_headers
    except Exception as e:
        logger.error(f"[RESOLVER_ERROR] Universal crawler exception: {e}")

    logger.warning(f"[RESOLVER_PASS] No alternative stream found, using original URL: {url}")
    return url, headers
