# main.py - Fixed token generation with proxy support
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
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="MovieBox Proxy - Railway")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Proxy list - from your working test
PROXY_LIST = [
    "http://cfkenotp:n90v2vkzp69u@31.59.20.176:6754",
    "http://cfkenotp:n90v2vkzp69u@45.38.107.97:6014",
    "http://cfkenotp:n90v2vkzp69u@198.105.121.200:6462",
    "http://cfkenotp:n90v2vkzp69u@64.137.96.74:6641",
    "http://cfkenotp:n90v2vkzp69u@84.247.60.125:6095",
    "http://cfkenotp:n90v2vkzp69u@142.111.67.146:5611",
    "http://cfkenotp:n90v2vkzp69u@31.58.9.4:6077",
]

# Token cache
cached_token = None
token_expiry = 0

async def get_fresh_token():
    """Get fresh token from the API with caching"""
    global cached_token, token_expiry
    import time
    
    now = time.time()
    
    # Return cached token if still valid (50 minutes)
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
        # Use a direct client without proxy for token fetching
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as temp_client:
            resp = await temp_client.get(url, headers=headers)
            logger.info(f"Token response status: {resp.status_code}")
            
            set_cookie = resp.headers.get("set-cookie", "")
            match = re.search(r'token=([^;]+)', set_cookie)
            
            if match:
                token = match.group(1)
                cached_token = token
                token_expiry = now + 50 * 60  # 50 minutes
                logger.info(f"Token obtained: {token[:30]}...")
                return token
            else:
                logger.error("No token found in Set-Cookie header")
                return None
                
    except Exception as e:
        logger.error(f"Error getting token: {str(e)}")
        return None

def extract_user_id(token):
    """Extract userId from token"""
    try:
        payload = json.loads(base64.b64decode(token.split('.')[1] + '==').decode('utf-8'))
        return str(payload.get('uid', '5449878944034578072'))
    except Exception as e:
        logger.warning(f"Could not decode token: {e}")
        return "5449878944034578072"

def build_headers(token):
    """Build headers with the given token"""
    userId = extract_user_id(token)
    
    return {
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

@app.get("/")
async def root():
    return {
        "service": "MovieBox Proxy (Railway)",
        "status": "running",
        "proxies_available": len(PROXY_LIST),
        "token_cached": cached_token is not None,
        "endpoints": {
            "/proxy": "Proxy video stream from CDN",
            "/proxies": "List available proxies",
            "/refresh_token": "Force refresh token"
        }
    }

@app.get("/proxies")
async def list_proxies():
    return {
        "total": len(PROXY_LIST),
        "proxies": [p.split('@')[-1] if '@' in p else p for p in PROXY_LIST]
    }

@app.get("/refresh_token")
async def refresh_token():
    """Force refresh the token"""
    global cached_token, token_expiry
    cached_token = None
    token_expiry = 0
    token = await get_fresh_token()
    if token:
        return {"status": "success", "token": token[:30] + "..."}
    return {"status": "failed", "message": "Could not get token"}

@app.get("/proxy")
async def proxy(request: Request, url: str):
    if not url:
        raise HTTPException(400, "Missing url parameter")
    
    if not PROXY_LIST:
        raise HTTPException(503, "No proxies available")
    
    decoded_url = unquote(url)
    logger.info(f"Fetching: {decoded_url}")

    # Get fresh token
    token = await get_fresh_token()
    if not token:
        raise HTTPException(401, "Failed to get authorization token")

    # Build headers with the token
    headers = build_headers(token)

    # Pass through Range header
    range_header = request.headers.get("range")
    if range_header:
        headers["Range"] = range_header
        logger.info(f"Range request: {range_header}")

    # Shuffle proxies for load balancing
    random.shuffle(PROXY_LIST)
    
    # Try each proxy
    for i, proxy_url in enumerate(PROXY_LIST):
        proxy_ip = proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url
        logger.info(f"Attempt {i+1}/{len(PROXY_LIST)} using proxy: {proxy_ip}")
        
        try:
            async with httpx.AsyncClient(
                proxies=proxy_url,
                timeout=httpx.Timeout(60.0, connect=15.0),
                follow_redirects=True,
                http2=False,  # Disable HTTP/2 for proxies
            ) as client:
                resp = await client.get(decoded_url, headers=headers)
                logger.info(f"Response: {resp.status_code} from proxy {proxy_ip}")
                
                if resp.status_code == 200 or resp.status_code == 206:
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
                    
                    logger.info(f"✅ Success via proxy {proxy_ip}")
                    
                    async def stream_generator():
                        async for chunk in resp.aiter_bytes(chunk_size=65536):
                            yield chunk
                    
                    return StreamingResponse(
                        stream_generator(),
                        status_code=200,
                        media_type=content_type,
                        headers=response_headers,
                    )
                else:
                    logger.warning(f"Proxy {proxy_ip} returned {resp.status_code}, trying next...")
                    
        except Exception as e:
            logger.error(f"Proxy {proxy_ip} error: {str(e)}")
            continue

    # If all proxies fail, try direct connection (no proxy)
    logger.info("All proxies failed, trying direct connection...")
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=15.0),
            follow_redirects=True,
            http2=True,
        ) as client:
            resp = await client.get(decoded_url, headers=headers)
            logger.info(f"Direct response: {resp.status_code}")
            
            if resp.status_code == 200 or resp.status_code == 206:
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
                
                logger.info("✅ Success via direct connection")
                
                async def stream_generator():
                    async for chunk in resp.aiter_bytes(chunk_size=65536):
                        yield chunk
                
                return StreamingResponse(
                    stream_generator(),
                    status_code=200,
                    media_type=content_type,
                    headers=response_headers,
                )
            else:
                raise HTTPException(resp.status_code, f"CDN error: {resp.status_code}")
                
    except Exception as e:
        logger.error(f"Direct connection error: {str(e)}")
        raise HTTPException(503, f"All proxies and direct connection failed: {str(e)}")

@app.on_event("shutdown")
async def shutdown():
    pass

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
