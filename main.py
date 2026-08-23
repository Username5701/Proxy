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

# Create a global HTTP/2 client
# This ensures HTTP/2 is always used
http_client = httpx.AsyncClient(
    http2=True,
    timeout=httpx.Timeout(120.0),
    follow_redirects=True,
    verify=False,  # Disable SSL verification if needed
)

@app.on_event("startup")
async def startup_event():
    logger.info("Starting HTTP/2 client...")

@app.on_event("shutdown")
async def shutdown_event():
    await http_client.aclose()

@app.get("/")
async def root():
    return {
        "service": "MovieBox Proxy",
        "status": "running",
        "usage": "/stream?hash=...&sign=...&t=..."
    }

@app.get("/stream")
async def proxy_stream(request: Request, hash: str, sign: str, t: str):
    """
    Proxy video stream from CDN with HTTP/2 support.
    """
    # Reconstruct the CDN URL
    video_url = f"https://bcdnxw.hakunaymatata.com/bt/{hash}.mp4?sign={sign}&t={t}"
    logger.info(f"Fetching: {video_url}")

    # Headers to mimic a real browser
    cdn_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://movieboxonline.net/",
        "Accept": "video/mp4, video/webm, video/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    # Handle range requests (for seeking)
    range_header = request.headers.get("range")
    if range_header:
        cdn_headers["Range"] = range_header
        logger.info(f"Range request: {range_header}")

    try:
        # Use the global HTTP/2 client
        resp = await http_client.get(video_url, headers=cdn_headers)

        # Handle errors
        if resp.status_code == 429:
            logger.warning("Rate limited by CDN")
            raise HTTPException(status_code=429, detail="CDN rate limit exceeded")

        if resp.status_code == 426:
            logger.error("HTTP/2 upgrade required — check httpx installation")
            # Try one more time with explicit HTTP/2
            async with httpx.AsyncClient(http2=True, timeout=120.0) as client:
                resp = await client.get(video_url, headers=cdn_headers)
                if resp.status_code == 426:
                    raise HTTPException(status_code=426, detail="HTTP/2 upgrade required")

        if resp.status_code not in [200, 206]:
            logger.error(f"CDN error: {resp.status_code}")
            raise HTTPException(status_code=resp.status_code, detail=f"CDN error: {resp.status_code}")

        # Get response headers
        content_type = resp.headers.get("content-type", "video/mp4")
        content_length = resp.headers.get("content-length", "")
        content_range = resp.headers.get("content-range", "")

        # Build response headers
        response_headers = {
            "Content-Type": content_type,
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Range, Content-Type",
            "Cache-Control": "public, max-age=3600",
        }

        # Set status code and headers based on response
        if resp.status_code == 206:
            response_headers["Content-Range"] = content_range
            status_code = 206
        else:
            response_headers["Content-Disposition"] = f"inline; filename=video_{hash}.mp4"
            status_code = 200

        if content_length:
            response_headers["Content-Length"] = content_length

        logger.info(f"Streaming: {content_length} bytes, status: {status_code}")

        # Stream directly without buffering
        return StreamingResponse(
            resp.aiter_bytes(chunk_size=8192),
            status_code=status_code,
            media_type=content_type,
            headers=response_headers,
        )

    except httpx.TimeoutException:
        logger.error("CDN timeout")
        raise HTTPException(status_code=504, detail="CDN timeout")

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
