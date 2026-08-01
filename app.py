import httpx
from urllib.parse import urljoin
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response, RedirectResponse
import stream_loader
import re
from fastapi import Request
from fastapi import UploadFile, File
import json
from pathlib import Path

app = FastAPI()

sessions = stream_loader.load_streams()

client = httpx.AsyncClient(
    follow_redirects=True,
    timeout=30,
    http2=True
)

def get_session(stream_id: int):
    sessions = stream_loader.load_streams()
    return sessions.get(stream_id)

@app.get("/streams")
async def streams():
    sessions = stream_loader.load_streams()
    return {
        "count": len(sessions),
        "streams": [
            {
                "id": s["id"],
                "type": s["type"],
                "url": s["url"]
            }
            for s in sessions.values()
        ]
    }


@app.get("/play/{stream_id}")
async def play(stream_id: int, request: Request):

    session = get_session(stream_id)

    if session is None:
        raise HTTPException(404)

    if session["type"] == "DASH":
        return RedirectResponse(
            url=str(request.base_url) + f"manifest/{stream_id}.mpd"
        )

    elif session["type"] == "M3U8":
        return RedirectResponse(
            f"/playlist/{stream_id}.m3u8"
        )

    raise HTTPException(400)


@app.get("/playlist/{stream_id}.m3u8")
async def playlist(stream_id: int, request: Request):

    session = get_session(stream_id)

    if session is None:
        raise HTTPException(404)

    response = await client.get(
        session["url"],
        headers=session["headers"]
    )

    if response.status_code != 200:
        raise HTTPException(response.status_code)

    text = response.text

    proxy = f"{request.base_url}hls/{stream_id}/"

    text = text.replace(
        session["base_url"],
        proxy
    )

    return Response(
        text,
        media_type="application/vnd.apple.mpegurl"
    )


@app.get("/hls/{stream_id}/{path:path}")
async def hls(stream_id: int, path: str):

    session = get_session(stream_id)

    if session is None:
        raise HTTPException(404)

    url = urljoin(
        session["base_url"],
        path
    )

    r = await client.get(
        url,
        headers=session["headers"]
    )

    return Response(
        content=r.content,
        status_code=r.status_code,
        media_type=r.headers.get(
            "Content-Type",
            "application/octet-stream"
        )
    )


@app.get("/manifest/{stream_id}.mpd")
async def manifest(stream_id: int, request: Request):

    session = get_session(stream_id)

    if session is None:
        raise HTTPException(status_code=404, detail="Invalid stream id")

    response = await client.get(
        session["url"],
        headers=session["headers"]
    )

    if response.status_code != 200:
        raise HTTPException(
            status_code=response.status_code,
            detail="Unable to fetch MPD"
        )

    mpd = response.text

    proxy_base = str(request.base_url) + f"proxy/{stream_id}/"

    # Insert BaseURL inside the MPD element
    mpd = re.sub(
        r'(<MPD[^>]*>)',
        rf'\1\n<BaseURL>{proxy_base}</BaseURL>',
        mpd,
        count=1
    )

    return Response(
        content=mpd,
        media_type="application/dash+xml"
    )


@app.get("/proxy/{stream_id}/{path:path}")
async def proxy(stream_id: int, path: str, request: Request):

    session = get_session(stream_id)

    if session is None:
        raise HTTPException(404)

    url = urljoin(session["base_url"], path)

    headers = session["headers"].copy()

    if "range" in request.headers:
        headers["Range"] = request.headers["range"]

    response = await client.get(
        url,
        headers=headers
    )

    response_headers = {}

    for header in (
        "Content-Type",
        "Content-Length",
        "Content-Range",
        "Accept-Ranges",
        "ETag",
        "Last-Modified",
    ):
        if header in response.headers:
            response_headers[header] = response.headers[header]

    return Response(
        content=response.content,
        status_code=response.status_code,
        headers=response_headers,
    )

@app.on_event("shutdown")
async def shutdown():
    await client.aclose()


STREAM_FILE = Path("streams.json")


@app.post("/upload")
async def upload(
    request: Request,
    file: UploadFile | None = File(default=None),
):
    try:
        # -------- Case 1: File Upload --------
        if file is not None:
            content = await file.read()
            data = json.loads(content)

        # -------- Case 2: Raw JSON --------
        else:
            data = await request.json()
            content = json.dumps(data, indent=2).encode("utf-8")

        # -------- Validation --------
        if "streams" not in data:
            raise HTTPException(
                status_code=400,
                detail="Invalid CloudStream export. 'streams' field missing."
            )

        # Save JSON
        STREAM_FILE.write_bytes(content)

        # Reload to verify
        sessions = stream_loader.load_streams()

        return {
            "success": True,
            "message": "Upload successful",
            "stream_count": len(sessions),
            "types": sorted({s["type"] for s in sessions.values()})
        }

    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400,
            detail="Invalid JSON"
        )