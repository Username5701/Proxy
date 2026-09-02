# main.py - with proxy rotation support
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx
import re
import json
import logging
import os
from urllib.parse import unquote
import random
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="MovieBox Proxy - Railway")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load proxies from file
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
                            'ip': parts[0],
                            'port': parts[1],
                            'username': parts[2],
                            'password': parts[3],
                            'url': f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
                        })
    except Exception as e:
        logger.error(f"Failed to load proxies: {e}")
    return proxies

# Global proxy list
PROXIES = load_proxies()
current_proxy_index = 0

def get_next_proxy():
    """Rotate through proxies"""
    global current_proxy_index
    if not PROXIES:
        return None
    proxy = PROXIES[current_proxy_index % len(PROXIES)]
    current_proxy_index += 1
    return proxy

def mark_proxy_dead(proxy_ip):
    """Remove dead proxy from rotation"""
    global PROXIES
    PROXIES = [p for p in PROXIES if p['ip'] != proxy_ip]
    logger.warning(f"Proxy {proxy_ip} removed. {len(PROXIES)} remaining.")

# HTTP/2 client with proxy support
async def get_proxy_client(proxy_url=None):
    if proxy_url:
        return httpx.AsyncClient(
            http2=True,
            timeout=httpx.Timeout(120.0, connect=30.0),
            follow_redirects=True,
            proxy=proxy_url,
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=50)
        )
    return httpx.AsyncClient(
        http2=True,
        timeout=httpx.Timeout(120.0, connect=30.0),
        follow_redirects=True,
        limits=httpx.Limits(max_keepalive_connections=10, max_connections=50)
    )

async def get_fresh_token():
    """Get fresh token from the API"""
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
        },
        "usage": "/proxy?url=ENCODED_CDN_URL"
    }

@app.get("/proxies")
async def list_proxies():
    return {
        "total": len(PROXIES),
        "proxies": [f"{p['ip']}:{p['port']}" for p in PROXIES]
    }

@app.get("/proxy")
async def proxy(request: Request, url: str):
    """Proxy video stream from CDN with proxy rotation"""
    if not url:
        raise HTTPException(400, "Missing url parameter")
    
    if not PROXIES:
        raise HTTPException(503, "No proxies available")
    
    decoded_url = unquote(url)
    logger.info(f"Fetching: {decoded_url}")

    # Get token first (without proxy)
    token = await get_fresh_token()
    if not token:
        raise HTTPException(401, "Failed to get authorization token")

    # Extract userId from token
    userId = "5449878944034578072"
    try:
        import base64
        payload = json.loads(base64.b64decode(token.split('.')[1] + '==').decode('utf-8'))
        userId = str(payload.get('uid', userId))
    except Exception as e:
        logger.warning(f"Could not decode token: {e}")

    # Build headers
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
        logger.info(f"Range request: {range_header}")

    # Try proxies with retry
    max_retries = len(PROXIES)
    for attempt in range(max_retries):
        proxy = get_next_proxy()
        if not proxy:
            break
            
        proxy_url = proxy['url']
        logger.info(f"Attempt {attempt+1}/{max_retries} using proxy: {proxy['ip']}:{proxy['port']}")
        
        try:
            async with await get_proxy_client(proxy_url) as client:
                resp = await client.get(decoded_url, headers=headers)
                logger.info(f"Response status: {resp.status_code} from proxy {proxy['ip']}")
                
                if resp.status_code == 426:
                    # Try HTTP/1.1
                    async with httpx.AsyncClient(http2=False, timeout=30.0, proxy=proxy_url) as http1_client:
                        resp = await http1_client.get(decoded_url, headers=headers)
                        logger.info(f"HTTP/1.1 response: {resp.status_code}")
                
                if resp.status_code == 200 or resp.status_code == 206:
                    # Success - stream response
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
                    
                    logger.info(f"Streaming: {content_length} bytes via proxy {proxy['ip']}")
                    
                    async def stream_generator():
                        async for chunk in resp.aiter_bytes(chunk_size=8192):
                            yield chunk
                    
                    return StreamingResponse(
                        stream_generator(),
                        status_code=200,
                        media_type=content_type,
                        headers=response_headers,
                    )
                else:
                    # Proxy failed, mark and try next
                    logger.warning(f"Proxy {proxy['ip']} returned {resp.status_code}, trying next...")
                    mark_proxy_dead(proxy['ip'])
                    
        except Exception as e:
            logger.error(f"Proxy {proxy['ip']} error: {str(e)}")
            mark_proxy_dead(proxy['ip'])
            continue

    # All proxies failed
    raise HTTPException(503, "All proxies failed. Please upload new proxies.")

@app.on_event("shutdown")
async def shutdown():
    pass

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
