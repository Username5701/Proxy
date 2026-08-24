from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="MovieBox Proxy")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create HTTP/2 client with proper TLS
client = httpx.AsyncClient(
    http2=True,
    timeout=httpx.Timeout(120.0, connect=30.0),
    follow_redirects=True,
    limits=httpx.Limits(max_keepalive_connections=10, max_connections=50)
)

@app.get("/")
async def root():
    return {
        "service": "MovieBox Proxy",
        "status": "running",
        "usage": "/stream?hash=...&sign=...&t=..."
    }

@app.get("/stream")
async def proxy_stream(request: Request, hash: str, sign: str, t: str):
    video_url = f"https://bcdnxw.hakunaymatata.com/bt/{hash}.mp4?sign={sign}&t={t}"
    logger.info(f"Fetching: {video_url}")

    # Critical: Order of headers matters
    headers = {
        "Accept": "video/mp4, video/webm, video/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://movieboxonline.net/",
        "Sec-Fetch-Dest": "video",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Upgrade-Insecure-Requests": "1",
    }

    range_header = request.headers.get("range")
    if range_header:
        headers["Range"] = range_header
        logger.info(f"Range request: {range_header}")

    try:
        # Use httpx with HTTP/2
        resp = await client.get(video_url, headers=headers)

        logger.info(f"Response status: {resp.status_code}")

        if resp.status_code == 426:
            # Try without HTTP/2, force HTTP/1.1
            logger.warning("426 Upgrade Required, trying HTTP/1.1 only...")
            client_http1 = httpx.AsyncClient(
                http2=False,
                timeout=120.0,
                follow_redirects=True
            )
            resp = await client_http1.get(video_url, headers=headers)
            logger.info(f"Retry response: {resp.status_code}")

        if resp.status_code != 200:
            logger.error(f"CDN error: {resp.status_code}")
            raise HTTPException(status_code=resp.status_code, detail=f"CDN error: {resp.status_code}")

        content_type = resp.headers.get("content-type", "video/mp4")
        content_length = resp.headers.get("content-length", "")

        response_headers = {
            "Content-Type": content_type,
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=3600",
        }

        if content_length:
            response_headers["Content-Length"] = content_length

        logger.info(f"Streaming: {content_length} bytes")

        # Streaming response
        async def stream_generator():
            async for chunk in resp.aiter_bytes(chunk_size=8192):
                yield chunk

        return StreamingResponse(
            stream_generator(),
            status_code=200,
            media_type=content_type,
            headers=response_headers,
        )

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.on_event("shutdown")
async def shutdown():
    await client.aclose()
