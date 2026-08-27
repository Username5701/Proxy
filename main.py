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

@app.get("/")
async def root():
    return {"service": "MovieBox Proxy", "status": "running"}

@app.get("/stream")
async def proxy_stream(request: Request, hash: str, sign: str, t: str):
    """
    Proxy video stream from CDN.
    """
    video_url = f"https://bcdnxw.hakunaymatata.com/bt/{hash}.mp4?sign={sign}&t={t}"
    logger.info(f"Fetching: {video_url}")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://movieboxonline.net/",
        "Accept": "video/mp4, video/webm, video/*",
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
                resp = await client.get(video_url, headers=headers)
                logger.info(f"Retry response: {resp.status_code}")

            if resp.status_code == 429:
                logger.warning("Rate limited by CDN")
                raise HTTPException(status_code=429, detail="CDN rate limit exceeded")

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

            return StreamingResponse(
                resp.aiter_bytes(chunk_size=8192),
                status_code=200,
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