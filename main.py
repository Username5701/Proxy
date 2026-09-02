# main.py - Railway proxy with automatic proxy rotation from public lists
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
import asyncio
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

# ==================== TOKEN CACHE ====================
cached_token = None
token_expiry = 0

# ==================== PROXY POOL ====================
proxy_pool = []
last_refresh = 0

# Public proxy sources
PROXY_SOURCES = [
    "https://raw.githubusercontent.com/proxmint/free-proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
]

# ==================== PROXY ROTATION ====================
async def refresh_proxy_pool():
    """Fetch fresh proxies from public sources"""
    global proxy_pool, last_refresh
    
    # Don't refresh more than once every 5 minutes
    if time.time() - last_refresh < 300:
        logger.info("Using cached proxy pool (refreshed within 5 minutes)")
        return
    
    logger.info("🔄 Fetching fresh proxies...")
    new_proxies = []
    
    for source in PROXY_SOURCES:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(source)
                if resp.status_code == 200:
                    lines = resp.text.strip().split('\n')
                    for line in lines:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            parts = line.split(':')
                            if len(parts) == 2:
                                proxy = f"http://{parts[0]}:{parts[1]}"
                                if proxy not in new_proxies:
                                    new_proxies.append(proxy)
            logger.info(f"✅ Fetched {len(new_proxies)} proxies from {source}")
        except Exception as e:
            logger.error(f"Failed to fetch from {source}: {str(e)}")
    
    if new_proxies:
        random.shuffle(new_proxies)
        proxy_pool = new_proxies[:50]
        last_refresh = time.time()
        logger.info(f"✅ Proxy pool updated: {len(proxy_pool)} proxies")
    else:
        logger.warning("⚠️ No proxies fetched, keeping existing pool")

async def get_proxy():
    """Get a random proxy from the pool"""
    global proxy_pool
    
    if not proxy_pool:
        await refresh_proxy_pool()
    
    if not proxy_pool:
        return None
    
    return random.choice(proxy_pool)

# ==================== TOKEN MANAGEMENT ====================
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
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as temp_client:
            resp = await temp_client.get(url, headers=headers)
            set_cookie = resp.headers.get("set-cookie", "")
            match = re.search(r'token=([^;]+)', set_cookie)
            
            if match:
                token = match.group(1)
                cached_token = token
                token_expiry = now + 50 * 60
                logger.info(f"Token obtained: {token[:30]}...")
                return token
            return None
    except Exception as e:
        logger.error(f"Token error: {str(e)}")
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
@app.on_event("startup")
async def startup_event():
    """Pre-fill proxy pool on startup"""
    logger.info("🔄 Pre-filling proxy pool...")
    await refresh_proxy_pool()
    logger.info(f"✅ Proxy pool ready: {len(proxy_pool)} proxies")

@app.get("/")
async def root():
    return {
        "service": "MovieBox Proxy (Railway)",
        "status": "running",
        "proxy_pool_size": len(proxy_pool),
        "endpoints": {
            "/proxy": "Proxy video stream from CDN",
            "/refresh_proxies": "Force refresh proxy pool"
        }
    }

@app.get("/refresh_proxies")
async def refresh_proxies():
    """Force refresh the proxy pool"""
    global proxy_pool, last_refresh
    proxy_pool = []
    last_refresh = 0
    await refresh_proxy_pool()
    return {
        "status": "refreshed",
        "proxy_pool_size": len(proxy_pool)
    }

@app.get("/proxy")
async def proxy(request: Request, url: str):
    if not url:
        raise HTTPException(400, "Missing url parameter")
    
    decoded_url = unquote(url)
    logger.info(f"Fetching: {decoded_url}")

    token = await get_fresh_token()
    if not token:
        raise HTTPException(401, "Failed to get authorization token")

    range_header = request.headers.get("range")
    headers = build_headers(token, range_header)
    if range_header:
        logger.info(f"Range request: {range_header}")

    # Try proxies, then fallback to direct
    max_attempts = 6  # 5 proxies + direct
    
    for attempt in range(max_attempts):
        proxy_url = None
        attempt_type = "proxy"
        
        # Use proxy for first 5 attempts
        if attempt < 5:
            proxy_url = await get_proxy()
            if proxy_url:
                logger.info(f"Attempt {attempt+1}/{max_attempts} using proxy: {proxy_url[:40]}...")
            else:
                attempt_type = "direct"
                logger.info(f"Attempt {attempt+1}/{max_attempts} - no proxy available, trying direct")
        else:
            attempt_type = "direct"
            logger.info(f"Attempt {attempt+1}/{max_attempts} - trying direct connection")
        
        try:
            # Build client with or without proxy
            client_kwargs = {
                "timeout": httpx.Timeout(120.0, connect=15.0),
                "follow_redirects": True,
                "limits": httpx.Limits(max_keepalive_connections=1, max_connections=1)
            }
            
            if proxy_url:
                client_kwargs["proxies"] = proxy_url
                client_kwargs["http2"] = False
            else:
                client_kwargs["http2"] = True
            
            async with httpx.AsyncClient(**client_kwargs) as client:
                async with client.stream("GET", decoded_url, headers=headers) as resp:
                    logger.info(f"Response: {resp.status_code}")
                    
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
                        
                        logger.info(f"✅ Success via {attempt_type}")
                        
                        async def stream_generator():
                            async for chunk in resp.aiter_bytes(chunk_size=65536):
                                yield chunk
                        
                        return StreamingResponse(
                            stream_generator(),
                            status_code=200,
                            media_type=content_type,
                            headers=response_headers,
                        )
                    elif resp.status_code in [403, 429, 426]:
                        logger.warning(f"CDN returned {resp.status_code}, trying next...")
                        continue
                    else:
                        logger.warning(f"CDN returned {resp.status_code}, trying next...")
                        continue
                        
        except Exception as e:
            logger.error(f"Attempt {attempt+1} error: {str(e)}")
            continue
    
    # All attempts failed
    raise HTTPException(503, "All proxy attempts failed. Please try again later.")

@app.on_event("shutdown")
async def shutdown():
    pass

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
