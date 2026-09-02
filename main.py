# main.py - Fixed proxy implementation
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

# Proxy list - hardcoded from your working test
PROXY_LIST = [
    "http://cfkenotp:n90v2vkzp69u@31.59.20.176:6754",
    "http://cfkenotp:n90v2vkzp69u@45.38.107.97:6014",
    "http://cfkenotp:n90v2vkzp69u@198.105.121.200:6462",
    "http://cfkenotp:n90v2vkzp69u@64.137.96.74:6641",
    "http://cfkenotp:n90v2vkzp69u@84.247.60.125:6095",
    "http://cfkenotp:n90v2vkzp69u@142.111.67.146:5611",
    "http://cfkenotp:n90v2vkzp69u@31.58.9.4:6077",
]

# Also load from file if exists
def load_proxies_from_file():
    proxies = []
    try:
        with open('proxies.txt', 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split(':')
                    if len(parts) == 4:
                        proxies.append(f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}")
    except:
        pass
    return proxies

file_proxies = load_proxies_from_file()
PROXIES = file_proxies if file_proxies else PROXY_LIST

async def get_fresh_token():
    url = "https://h5-api.aoneroom.com/wefeed-h5api-bff/country-code"
    headers = {
        "User-Agent": "Mozilla/5.0 (Android 15; Mobile; rv:153.0) Gecko/153.0 Firefox/153.0",
        "Accept": "application/json",
        "X-Client-Info": '{"timezone":"Africa/Lagos"}',
        "Origin": "https://movieboxonline.net",
        "Referer": "https://movieboxonline.net/",
    }
    try:
        async with httpx.AsyncClient() as temp_client:
            resp = await temp_client.get(url, headers=headers)
            set_cookie = resp.headers.get("set-cookie", "")
            match = re.search(r'token=([^;]+)', set_cookie)
            if match:
                return match.group(1)
            return None
    except Exception as e:
        logger.error(f"Token error: {e}")
        return None

@app.get("/")
async def root():
    return {
        "service": "MovieBox Proxy (Railway)",
        "status": "running",
        "proxies_available": len(PROXIES),
        "endpoints": {
            "/proxy": "Proxy video stream from CDN",
            "/proxies": "List available proxies"
        }
    }

@app.get("/proxies")
async def list_proxies():
    return {
        "total": len(PROXIES),
        "proxies": [p.split('@')[-1] if '@' in p else p for p in PROXIES]
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

    userId = "5449878944034578072"
    try:
        payload = json.loads(base64.b64decode(token.split('.')[1] + '==').decode('utf-8'))
        userId = str(payload.get('uid', userId))
    except Exception as e:
        logger.warning(f"Could not decode token: {e}")

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

    range_header = request.headers.get("range")
    if range_header:
        headers["Range"] = range_header

    # Shuffle proxies for load balancing
    random.shuffle(PROXIES)
    
    for i, proxy_url in enumerate(PROXIES):
        logger.info(f"Attempt {i+1}/{len(PROXIES)} using proxy: {proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url}")
        
        try:
            # Use httpx with proxy - correctly configured
            async with httpx.AsyncClient(
                proxies=proxy_url,  # This is the correct parameter
                timeout=httpx.Timeout(60.0, connect=15.0),
                follow_redirects=True,
                http2=False,  # Disable HTTP/2 for proxies
            ) as client:
                resp = await client.get(decoded_url, headers=headers)
                logger.info(f"Response: {resp.status_code} from proxy")
                
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
                    
                    logger.info(f"✅ Success via proxy")
                    
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
                    logger.warning(f"Proxy returned {resp.status_code}, trying next...")
                    
        except Exception as e:
            logger.error(f"Proxy error: {str(e)}")
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
