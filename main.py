from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, Response
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="MovieBox Proxy")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"service": "MovieBox Proxy", "status": "running"}

@app.get("/stream")
async def proxy_stream(request: Request, hash: str = None, sign: str = None, t: str = None, url: str = None):
    """
    Proxy video stream from CDN.
    Supports both MP4 and M3U8 (HLS) streams.
    """
    # If URL is provided directly, use it
    if url:
        video_url = url
        logger.info(f"Fetching from URL: {video_url}")
    else:
        # Fallback to hash, sign, t format
        video_url = f"https://bcdnxw.hakunaymatata.com/bt/{hash}.mp4?sign={sign}&t={t}"
        logger.info(f"Fetching: {video_url}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://sportsnow.top/",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    range_header = request.headers.get("range")
    if range_header:
        headers["Range"] = range_header
        logger.info(f"Range request: {range_header}")

    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True, verify=False) as client:
        try:
            resp = await client.get(video_url, headers=headers)
            logger.info(f"CDN response status: {resp.status_code}")

            if resp.status_code == 403:
                logger.warning("403 Forbidden - trying with different headers...")
                headers["User-Agent"] = "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36"
                headers["Origin"] = "https://sportsnow.top"
                resp = await client.get(video_url, headers=headers)
                logger.info(f"Retry response: {resp.status_code}")

            if resp.status_code == 429:
                logger.warning("Rate limited by CDN")
                raise HTTPException(status_code=429, detail="CDN rate limit exceeded")

            if resp.status_code != 200:
                logger.error(f"CDN error: {resp.status_code}")
                raise HTTPException(status_code=resp.status_code, detail=f"CDN error: {resp.status_code}")

            content_type = resp.headers.get("content-type", "")
            content_length = resp.headers.get("content-length", "")

            # Determine if it's an M3U8 playlist
            is_m3u8 = 'm3u8' in content_type or video_url.endswith('.m3u8')
            is_mp4 = 'mp4' in content_type or video_url.endswith('.mp4')

            response_headers = {
                "Accept-Ranges": "bytes",
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=3600",
            }

            if is_m3u8:
                # For M3U8, we need to proxy the playlist and rewrite the URLs
                content = resp.text
                # Rewrite relative URLs to absolute URLs
                base_url = video_url.rsplit('/', 1)[0] + '/'
                rewritten_content = re.sub(
                    r'(https?://[^\s]+\.ts)',
                    lambda m: f"{base_url}{m.group(1)}",
                    content
                )
                response_headers["Content-Type"] = "application/vnd.apple.mpegurl"
                return Response(
                    content=rewritten_content,
                    status_code=200,
                    headers=response_headers
                )

            if content_length:
                response_headers["Content-Length"] = content_length

            if content_type:
                response_headers["Content-Type"] = content_type

            logger.info(f"Streaming: {content_length} bytes, type: {content_type}")

            return StreamingResponse(
                resp.aiter_bytes(chunk_size=8192),
                status_code=200,
                media_type=content_type or "video/mp4",
                headers=response_headers,
            )

        except httpx.TimeoutException:
            logger.error("CDN timeout")
            raise HTTPException(status_code=504, detail="CDN timeout")
        except Exception as e:
            logger.error(f"Error: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

# ============================================
# PROXY FOR M3U8 SEGMENTS (TS files)
# ============================================
@app.get("/proxy/ts")
async def proxy_ts(url: str):
    """
    Proxy individual TS segments from the M3U8 playlist.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36",
        "Referer": "https://sportsnow.top/",
        "Origin": "https://sportsnow.top",
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }

    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True, verify=False) as client:
        try:
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                logger.error(f"TS segment error: {resp.status_code}")
                raise HTTPException(status_code=resp.status_code, detail=f"TS error: {resp.status_code}")

            response_headers = {
                "Content-Type": "video/mp2t",
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "public, max-age=3600",
            }

            return StreamingResponse(
                resp.aiter_bytes(chunk_size=8192),
                status_code=200,
                media_type="video/mp2t",
                headers=response_headers,
            )
        except Exception as e:
            logger.error(f"TS error: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)