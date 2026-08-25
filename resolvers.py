import re
import urllib.parse
from urllib.parse import parse_qs, urlparse
import httpx

DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


async def resolve_moviezwap(url: str, existing_headers: dict | None = None) -> tuple[str | None, dict]:
    """
    Extracts a fresh direct MP4 video link from MoviezWap signed for the current server's IP.
    Supports movie detail pages, dwload pages, download pages, and direct/expired server links.
    """
    headers = dict(existing_headers or {})
    if not any(k.lower() == "user-agent" for k in headers):
        headers["User-Agent"] = DEFAULT_USER_AGENT

    async with httpx.AsyncClient(follow_redirects=True, timeout=12) as client:
        # Determine target quality from filename (e.g. 720p, 480p, 320p, 1080p)
        target_quality = "720p"
        if "1080p" in url.lower():
            target_quality = "1080p"
        elif "480p" in url.lower():
            target_quality = "480p"
        elif "320p" in url.lower():
            target_quality = "320p"

        # Case 1: dwload.php
        if "dwload.php" in url and "file=" in url:
            file_id = parse_qs(urlparse(url).query).get("file", [""])[0]
            dl_url = f"https://www.moviezwap.taxi/download.php?file={file_id}"
            r = await client.get(dl_url, headers={**headers, "Referer": url})
            links = [l for l in re.findall(r'href=["\'](.*?)["\']', r.text) if ".mp4" in l]
            if links:
                return links[0], headers

        # Case 2: download.php
        if "download.php" in url and "file=" in url:
            file_id = parse_qs(urlparse(url).query).get("file", [""])[0]
            r = await client.get(url, headers={**headers, "Referer": f"https://www.moviezwap.taxi/dwload.php?file={file_id}"})
            links = [l for l in re.findall(r'href=["\'](.*?)["\']', r.text) if ".mp4" in l]
            if links:
                return links[0], headers

        # Case 3: Extract movie page from direct URL or movie detail URL
        movie_slug = None
        if "/movie/" in url:
            match = re.search(r'/movie/([^/]+?)\.html', url)
            if match:
                movie_slug = match.group(1)
        else:
            # Extract from path: e.g. /Telugu (2026) Movies/Irumudi-(2026)-Telugu/Irumudi-(2026)-Telugu-DVDScr-720p-HQ.mp4
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
            r = await client.get(movie_page, headers=headers)
            dw_links = re.findall(r'href=["\'](/dwload\.php\?file=\d+|https?://[^"\']*/dwload\.php\?file=\d+)["\']', r.text)
            if dw_links:
                if target_quality in ("720p", "1080p") and len(dw_links) >= 1:
                    chosen = dw_links[-1]
                elif target_quality == "480p" and len(dw_links) >= 2:
                    chosen = dw_links[-2]
                else:
                    chosen = dw_links[0]

                if not chosen.startswith("http"):
                    chosen = "https://www.moviezwap.taxi" + (chosen if chosen.startswith("/") else "/" + chosen)

                file_id = parse_qs(urlparse(chosen).query).get("file", [""])[0]
                dl_url = f"https://www.moviezwap.taxi/download.php?file={file_id}"
                r_dl = await client.get(dl_url, headers={**headers, "Referer": chosen})
                mp4_links = [l for l in re.findall(r'href=["\'](.*?)["\']', r_dl.text) if ".mp4" in l]
                if mp4_links:
                    return mp4_links[0], headers

    return None, headers


async def resolve_stream(url: str, headers: dict) -> tuple[str, dict]:
    """
    Main entry point for resolving any stream provider dynamically.
    If the provider is recognized (e.g. MoviezWap), resolves a fresh stream URL from this server's IP.
    Otherwise, returns the original URL and headers.
    """
    url_lower = url.lower()

    if "moviezwap" in url_lower or "moviezzwaphd" in url_lower:
        try:
            resolved_url, resolved_headers = await resolve_moviezwap(url, headers)
            if resolved_url:
                return resolved_url, resolved_headers
        except Exception:
            pass

    return url, headers
