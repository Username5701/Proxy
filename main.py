# main.py - Simple Railway proxy, direct connection with chunked streaming
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx
import re
import json
import logging
import os
from urllib.parse import unquote
import base64
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="MovieBox Proxy - Railway")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== TOKEN CACHE ====================
cached_token = None
token_expiry = 0

async def get_fresh_token():
    """Get fresh token from the API"""
    global cached_token, token_expiry
    now = time.time()
    
    if cached_token and token_expiry > now:
        return cached_token
    
    url = "https://h5-api.aoneroom.com/wefeed-h5api-bff/country-code"
    headers = {
        "User-Agent": "Mozilla/5.0 (Android 15; Mobile; rv:153.0) Gecko/153.0 Firefox/153.0",
        "Accept": "application/json",
        "X-Client-Info": '{"timezone":"Africa/Lagos"}',
        "Origin": "https://movieboxonline.net",
        "Referer": "https://movieboxonline.net/",
    }
    
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
            set_cookie = resp.headers.get("set-cookie", "")
            match = re.search(r'token=([^;]+)', set_cookie)
            
            if match:
                token = match.group(1)
                cached_token = token
                token_expiry = now + 50 * 60
                logger.info(f"✅ Token obtained: {token[:30]}...")
                return token
            else:
                logger.error("❌ No token found in response")
                return None
    except Exception as e:
        logger.error(f"❌ Token error: {str(e)}")
        return None

def build_headers(token, range_header=None):
    """Build headers with the given token"""
    try:
        payload = json.loads(base64.b64decode(token.split('.')[1] + '==').decode('utf-8'))
        userId = str(payload.get('uid', '5449878944034578072'))
    except:
        userId = "5449878944034578072"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Android 15; Mobile; rv:154.0) Gecko/154.0 Firefox/154.0",
        "Accept": "video/mp4, video/webm, video/*, */*",
        "Accept-Language": "en-US",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Referer": "https://movieboxonline.net/",
        "Origin": "https://movieboxonline.net",
        "Connection": "keep-alive",
        "Authorization": f"Bearer {token}",
        "Cookie": f"token={token}",
        "X-Client-Info": '{"timezone":"Africa/Lagos"}',
        "X-User": json.dumps({
            "token": token,
            "userId": userId,
            "userType": 0,
            "appType": 3
        }),
        "X-Source": "",
        "Sec-Fetch-Dest": "video",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
        "Upgrade-Insecure-Requests": "1",
    }
    
    if range_header:
        headers["Range"] = range_header
    
    return headers

# ==================== ENDPOINTS ====================
@app.get("/")
async def root():
    return {
        "service": "MovieBox Proxy (Railway)",
        "status": "running",
        "endpoints": {
            "/proxy": "Proxy video stream from CDN (direct, chunked)"
        },
        "usage": "/proxy?url=ENCODED_CDN_URL"
    }

@app.get("/proxy")
async def proxy(request: Request, url: str):
    """Proxy video stream from CDN - simple, direct, chunked"""
    if not url:
        raise HTTPException(400, "Missing url parameter")
    
    decoded_url = unquote(url)
    logger.info(f"📥 Fetching: {decoded_url}")

    # Get token
    token = await get_fresh_token()
    if not token:
        raise HTTPException(401, "Failed to get authorization token")

    # Build headers
    range_header = request.headers.get("range")
    headers = build_headers(token, range_header)
    if range_header:
        logger.info(f"📊 Range request: {range_header}")

    try:
        # Simple direct request with streaming
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=30.0),
            follow_redirects=True,
            http2=True,
            limits=httpx.Limits(max_keepalive_connections=1, max_connections=1)
        ) as client:
            async with client.stream("GET", decoded_url, headers=headers) as resp:
                logger.info(f"📊 CDN response: {resp.status_code}")
                
                if resp.status_code == 426:
                    # Try HTTP/1.1 if HTTP/2 fails
                    logger.warning("⚠️ 426 Upgrade Required, trying HTTP/1.1...")
                    async with httpx.AsyncClient(
                        timeout=httpx.Timeout(120.0, connect=30.0),
                        follow_redirects=True,
                        http2=False,
                    ) as client_http1:
                        async with client_http1.stream("GET", decoded_url, headers=headers) as resp2:
                            if resp2.status_code != 200 and resp2.status_code != 206:
                                raise HTTPException(resp2.status_code, f"CDN error: {resp2.status_code}")
                            
                            content_type = resp2.headers.get("content-type", "video/mp4")
                            content_length = resp2.headers.get("content-length", "")
                            content_range = resp2.headers.get("content-range", "")
                            
                            response_headers = {
                                "Content-Type": content_type,
                                "Access-Control-Allow-Origin": "*",
                                "Cache-Control": "public, max-age=3600",
                                "Accept-Ranges": "bytes",
                            }
                            if content_length:
                                response_headers["Content-Length"] = content_length
                            if content_range:
                                response_headers["Content-Range"] = content_range
                            
                            async def stream_generator():
                                async for chunk in resp2.aiter_bytes(chunk_size=65536):
                                    if chunk:
                                        yield chunk
                            
                            logger.info(f"✅ Streaming started (HTTP/1.1)")
                            return StreamingResponse(
                                stream_generator(),
                                status_code=200,
                                media_type=content_type,
                                headers=response_headers,
                            )
                
                if resp.status_code != 200 and resp.status_code != 206:
                    error_body = await resp.aread()
                    logger.error(f"❌ CDN error: {resp.status_code} - {error_body[:200]}")
                    raise HTTPException(resp.status_code, f"CDN error: {resp.status_code}")
                
                content_type = resp.headers.get("content-type", "video/mp4")
                content_length = resp.headers.get("content-length", "")
                content_range = resp.headers.get("content-range", "")
                
                response_headers = {
                    "Content-Type": content_type,
                    "Access-Control-Allow-Origin": "*",
                    "Cache-Control": "public, max-age=3600",
                    "Accept-Ranges": "bytes",
                }
                if content_length:
                    response_headers["Content-Length"] = content_length
                if content_range:
                    response_headers["Content-Range"] = content_range
                
                async def stream_generator():
                    try:
                        async for chunk in resp.aiter_bytes(chunk_size=65536):
                            if chunk:
                                yield chunk
                    except Exception as e:
                        logger.error(f"Stream error: {str(e)}")
                
                logger.info(f"✅ Streaming started (HTTP/2)")
                return StreamingResponse(
                    stream_generator(),
                    status_code=200,
                    media_type=content_type,
                    headers=response_headers,
                )
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Proxy error: {str(e)}")
        raise HTTPException(500, f"Proxy error: {str(e)}")

@app.on_event("shutdown")
async def shutdown():
    pass

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
