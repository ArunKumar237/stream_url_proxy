import asyncio
import logging
import re
from urllib.parse import quote_plus, urljoin, urlparse
import httpx

logger = logging.getLogger("stream_proxy.scrapers")

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.moviezwap.taxi/",
    "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
}


# ──────────────────────────────────────────────────────────────────────────────
# 1. MoviezWap Scraper
# ──────────────────────────────────────────────────────────────────────────────

MOVIEZWAP_DOMAINS = [
    "https://www.moviezwap.taxi",
    "https://moviezwap.land",
    "https://moviezwap.yachts",
]


async def search_moviezwap(query: str) -> list[dict]:
    results = []

    async with httpx.AsyncClient(follow_redirects=True, timeout=12) as client:
        for domain in MOVIEZWAP_DOMAINS:
            try:
                # Step 1: Visit homepage to resolve active domain redirect and establish session
                home_resp = await client.get(f"{domain}/", headers=DEFAULT_HEADERS)
                if home_resp.status_code != 200:
                    continue

                active_base = str(home_resp.url).rstrip("/")
                search_url = f"{active_base}/search.php?q={quote_plus(query)}"
                headers = {**DEFAULT_HEADERS, "Referer": f"{active_base}/"}

                # Step 2: Query search endpoint
                r = await client.get(search_url, headers=headers)
                logger.info(f"[SCRAPER_MOVIEZWAP] Search Status {r.status_code} from {active_base}")

                if r.status_code != 200:
                    continue

                # Match movie link items: <a href="/movie/Movie-Name.html">Title</a> or similar
                matches = re.finditer(
                    r'<a[^>]+href=["\'](/movie/[^"\']+\.html)["\'][^>]*>(.*?)</a>',
                    r.text,
                    re.IGNORECASE,
                )

                seen_urls = set()
                for m in matches:
                    href = m.group(1)
                    title = re.sub(r'<[^>]+>', '', m.group(2)).strip()
                    abs_url = urljoin(active_base, href)

                    if not title or abs_url in seen_urls:
                        continue
                    seen_urls.add(abs_url)

                    # Extract year if present
                    year_match = re.search(r'\((\d{4})\)', title)
                    year = year_match.group(1) if year_match else "Movie"

                    results.append({
                        "title": title,
                        "year": year,
                        "provider": "MoviezWap",
                        "url": abs_url,
                        "poster": "https://img.icons8.com/color/480/movie-projector.png",
                        "badge": "Telugu / Hindi / Tamil",
                    })

                if results:
                    break

            except Exception as e:
                logger.error(f"[SCRAPER_ERROR] MoviezWap search error for '{query}' on {domain}: {e}")

    return results


# ──────────────────────────────────────────────────────────────────────────────
# 2. HDHub4u Scraper
# ──────────────────────────────────────────────────────────────────────────────

async def search_hdhub4u(query: str) -> list[dict]:
    results = []
    # Primary mirror for HDHub4u
    search_url = f"https://hdhub4u.tv/?s={quote_plus(query)}"

    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=10) as client:
            r = await client.get(search_url, headers=DEFAULT_HEADERS)
            if r.status_code != 200:
                return []

            # Extract post cards: <article ...> <a href="..." title="..."> <img src="...">
            articles = re.findall(
                r'<article[^>]*>.*?<a[^>]+href=["\']([^"\']+)["\'][^>]+title=["\']([^"\']+)["\'].*?(?:<img[^>]+src=["\']([^"\']+)["\'])?.*?</article>',
                r.text,
                re.DOTALL | re.IGNORECASE,
            )

            for href, title, poster in articles:
                clean_title = title.replace("Download", "").strip()
                year_match = re.search(r'\b(19\d\d|20\d\d)\b', clean_title)
                year = year_match.group(1) if year_match else "HD"

                results.append({
                    "title": clean_title,
                    "year": year,
                    "provider": "HDHub4u",
                    "url": href,
                    "poster": poster or "https://img.icons8.com/color/480/clapperboard.png",
                    "badge": "1080p / 720p / Dual Audio",
                })

    except Exception as e:
        logger.error(f"[SCRAPER_ERROR] HDHub4u search error for '{query}': {e}")

    return results


# ──────────────────────────────────────────────────────────────────────────────
# Master Search Aggregator
# ──────────────────────────────────────────────────────────────────────────────

async def search_all_movies(query: str) -> list[dict]:
    """
    Runs searches across all enabled providers in parallel and returns aggregated results.
    """
    clean_query = query.strip()
    if not clean_query:
        return []

    logger.info(f"[SEARCH_START] Searching for '{clean_query}' across all providers...")

    # Run scrapers in parallel
    tasks = [
        search_moviezwap(clean_query),
        search_hdhub4u(clean_query),
    ]

    all_results = []
    completed = await asyncio.gather(*tasks, return_exceptions=True)

    for res in completed:
        if isinstance(res, list):
            all_results.extend(res)

    logger.info(f"[SEARCH_DONE] Found {len(all_results)} result(s) for '{clean_query}'")
    return all_results
