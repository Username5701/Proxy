from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="MovieBox Proxy", description="Stream video from CDN via IPv6")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create HTTP/2 client
# httpx supports HTTP/2, but we need to enable it
# We'll create a client with HTTP/2 support
async def get_http2_client():
    # Use a custom transport with HTTP/2
    # httpx's default client supports HTTP/2 if the server supports it
    return httpx.AsyncClient(
        http2=True,
        timeout=httpx.Timeout(120.0),
        follow_redirects=True,
        verify=True,  # Use SSL verification
    )

@app.get("/")
async def root():
    return {
        "service": "MovieBox Proxy v2",
        "status": "running",
        "usage": "/stream?hash=...&sign=...&t=..."
    }

@app.get("/stream")
async def proxy_stream(hash: str, sign: str, t: str):
    """
    Proxy the video stream from the CDN using HTTP/2.
    """
    # Reconstruct the CDN URL
    video_url = f"https://bcdnxw.hakunaymatata.com/bt/{hash}.mp4?sign={sign}&t={t}"
    logger.info(f"Fetching: {video_url}")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://movieboxonline.net/",
        "Accept": "video/mp4, video/webm, video/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    
    try:
        # Use HTTP/2 client
        async with httpx.AsyncClient(http2=True, timeout=120.0, follow_redirects=True) as client:
            logger.info("Making request to CDN with HTTP/2...")
            resp = await client.get(video_url, headers=headers)
            
            logger.info(f"CDN response status: {resp.status_code}")
            logger.info(f"CDN response HTTP version: {resp.http_version}")
            
            if resp.status_code == 426:
                logger.error("CDN requires HTTP/2 or TLS 1.3")
                raise HTTPException(status_code=426, detail="CDN requires HTTP/2 upgrade")
            
            if resp.status_code == 429:
                logger.warning("Rate limited by CDN")
                raise HTTPException(status_code=429, detail="CDN rate limit exceeded")
            
            if resp.status_code != 200:
                logger.error(f"CDN error: {resp.status_code}")
                raise HTTPException(status_code=resp.status_code, detail=f"CDN error: {resp.status_code}")
            
            content_length = resp.headers.get("content-length", "")
            content_type = resp.headers.get("content-type", "video/mp4")
            
            logger.info(f"Streaming video: {content_length} bytes")
            
            return StreamingResponse(
                resp.aiter_bytes(chunk_size=8192),
                media_type=content_type,
                headers={
                    "Content-Disposition": f"inline; filename=video_{hash}.mp4",
                    "Content-Length": content_length,
                    "Cache-Control": "public, max-age=3600",
                    "Accept-Ranges": "bytes",
                    "Access-Control-Allow-Origin": "*",
                }
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
