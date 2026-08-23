from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx
import os
from urllib.parse import urlparse, parse_qs

app = FastAPI(title="MovieBox Proxy", description="Stream video from CDN via IPv6")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {
        "service": "MovieBox Proxy",
        "status": "running",
        "ipv6": "enabled",
        "usage": "/stream?hash=...&sign=...&t=..."
    }

@app.get("/stream")
async def proxy_stream(hash: str, sign: str, t: str):
    """
    Proxy the video stream from the CDN.
    Supports range requests for seeking.
    """
    # Reconstruct the CDN URL
    video_url = f"https://bcdnxw.hakunaymatata.com/bt/{hash}.mp4?sign={sign}&t={t}"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://movieboxonline.net/",
        "Accept": "video/mp4, video/webm, video/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        try:
            # Make the request
            resp = await client.get(video_url, headers=headers)
            
            if resp.status_code == 429:
                raise HTTPException(status_code=429, detail="CDN rate limit exceeded")
            
            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=f"CDN error: {resp.status_code}")
            
            content_length = resp.headers.get("content-length", "")
            content_type = resp.headers.get("content-type", "video/mp4")
            
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
            raise HTTPException(status_code=504, detail="CDN timeout")
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
