# main.py
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

# Load proxies from file ONLY
def load_proxies():
    proxies = []
    try:
        with open('proxies.txt', 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    parts = line.split(':')
                    if len(parts) == 4:
                        proxies.append({
                            'url': f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}",
                            'ip': parts[0],
                            'port': parts[1],
                            'working': True
                        })
    except FileNotFoundError:
        logger.error("proxies.txt not found! Please upload the file.")
    except Exception as e:
        logger.error(f"Failed to load proxies: {e}")
    return proxies

PROXIES = load_proxies()
if not PROXIES:
    logger.warning("No proxies loaded from proxies.txt")

# Token cache
cached_token = None
token_expiry = 0

async def get_fresh_token():
    """Get fresh token from the API with caching"""
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

@app.get("/")
async def root():
    return {
        "service": "MovieBox Proxy (Railway)",
        "status": "running",
        "proxies_available": len(PROXIES),
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
        "total": len(PROXIES),
        "proxies": [f"{p['ip']}:{p['port']} (working: {p['working']})" for p in PROXIES]
    }

@app.get("/refresh_token")
async def refresh_token():
    global cached_token, token_expiry
    cached_token = None
    token_expiry = 0
    token = await get_fresh_token()
    if token:
        return {"status": "success", "token": token[:30] + "..."}
    return {"status": "failed"}

@app.get("/proxy")
async def proxy(request: Request, url: str):
    if not url:
        raise HTTPException(400, "Missing url parameter")
    
    if not PROXIES:
        raise HTTPException(503, "No proxies available. Please upload proxies.txt")
    
    decoded_url = unquote(url)
    logger.info(f"Fetching: {decoded_url}")

    token = await get_fresh_token()
    if not token:
        raise HTTPException(401, "Failed to get authorization token")

    range_header = request.headers.get("range")
    headers = build_headers(token, range_header)
    if range_header:
        logger.info(f"Range request: {range_header}")

    # Get working proxies
    working_proxies = [p for p in PROXIES if p['working']]
    if not working_proxies:
        logger.warning("No working proxies, resetting all...")
        for p in PROXIES:
            p['working'] = True
        working_proxies = PROXIES
    
    random.shuffle(working_proxies)
    
    for proxy in working_proxies:
        proxy_url = proxy['url']
        proxy_ip = f"{proxy['ip']}:{proxy['port']}"
        logger.info(f"Attempt using proxy: {proxy_ip}")
        
        try:
            async with httpx.AsyncClient(
                proxies=proxy_url,
                timeout=httpx.Timeout(60.0, connect=15.0),
                follow_redirects=True,
                http2=False,
                limits=httpx.Limits(max_keepalive_connections=1, max_connections=1)
            ) as client:
                resp = await client.get(decoded_url, headers=headers)
                logger.info(f"Response: {resp.status_code} from {proxy_ip}")
                
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
                    
                    logger.info(f"✅ Success via {proxy_ip}")
                    
                    async def stream_generator():
                        async for chunk in resp.aiter_bytes(chunk_size=16384):
                            yield chunk
                    
                    return StreamingResponse(
                        stream_generator(),
                        status_code=200,
                        media_type=content_type,
                        headers=response_headers,
                    )
                elif resp.status_code == 403 or resp.status_code == 426:
                    logger.warning(f"Proxy {proxy_ip} returned {resp.status_code}, marking as dead")
                    proxy['working'] = False
                else:
                    logger.warning(f"Proxy {proxy_ip} returned {resp.status_code}, trying next...")
                    
        except Exception as e:
            logger.error(f"Proxy {proxy_ip} error: {str(e)}")
            proxy['working'] = False
            continue

    # Direct connection fallback
    logger.info("All proxies failed, trying direct connection...")
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=15.0),
            follow_redirects=True,
            http2=True,
            limits=httpx.Limits(max_keepalive_connections=1, max_connections=1)
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
                    async for chunk in resp.aiter_bytes(chunk_size=16384):
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
