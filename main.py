# main.py - Complete Railway proxy with MP4 and HLS support
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx
import re
import json
import logging
import os
from urllib.parse import unquote, urlparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="MovieBox Proxy - Railway")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# HTTP/2 client for CDN requests
client = httpx.AsyncClient(
    http2=True,
    timeout=httpx.Timeout(120.0, connect=30.0),
    follow_redirects=True,
    limits=httpx.Limits(max_keepalive_connections=10, max_connections=50)
)

# Token cache
cached_token = None
token_expiry = 0

async def get_fresh_token():
    """Get fresh token from the API with caching"""
    global cached_token, token_expiry
    
    import time
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
        async with httpx.AsyncClient() as temp_client:
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
                logger.info(f"Response headers: {dict(resp.headers)}")
                return None
                
    except Exception as e:
        logger.error(f"Error getting token: {str(e)}")
        return None

async def build_headers(range_header=None):
    """Build headers with fresh token"""
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
        "endpoints": {
            "/proxy": "Proxy MP4 or HLS video stream",
            "/hls": "Proxy HLS playlist with rewritten segments",
            "/segment": "Proxy HLS segment"
        },
        "usage": "/proxy?url=ENCODED_CDN_URL  or  /hls?hash=...&sign=...&t=..."
    }

@app.get("/proxy")
async def proxy(request: Request, url: str):
    """Proxy any CDN URL (MP4, HLS playlist, or segment)"""
    if not url:
        raise HTTPException(400, "Missing url parameter")
    
    try:
        decoded_url = unquote(url)
        logger.info(f"Fetching: {decoded_url}")
        
        # Check if it's an HLS playlist
        if '.m3u8' in decoded_url:
            return await handle_hls_playlist(decoded_url, request)
        
        # Check if it's an HLS segment
        if '.ts' in decoded_url:
            return await handle_segment(decoded_url, request)
        
        # Default: MP4 video
        return await handle_mp4(decoded_url, request)
        
    except Exception as e:
        logger.error(f"Proxy error: {str(e)}")
        raise HTTPException(500, f"Proxy error: {str(e)}")

async def handle_mp4(video_url: str, request: Request):
    """Handle MP4 video streaming"""
    try:
        headers = await build_headers(request.headers.get("range"))
        
        logger.info(f"Sending MP4 request with token: {headers['Authorization'][:20]}...")
        
        resp = await client.get(video_url, headers=headers)
        logger.info(f"MP4 response status: {resp.status_code}")
        
        if resp.status_code == 426:
            logger.warning("426 Upgrade Required, trying HTTP/1.1...")
            async with httpx.AsyncClient(http2=False, timeout=30.0) as http1_client:
                resp = await http1_client.get(video_url, headers=headers)
                logger.info(f"HTTP/1.1 response: {resp.status_code}")
        
        if resp.status_code != 200:
            error_body = resp.text if hasattr(resp, 'text') else str(resp.content)
            logger.error(f"CDN error: {resp.status_code} - {error_body[:200]}")
            raise HTTPException(resp.status_code, detail=f"CDN error: {resp.status_code}")
        
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
        
        logger.info(f"Streaming MP4: {content_length} bytes")
        
        async def stream_generator():
            async for chunk in resp.aiter_bytes(chunk_size=65536):
                yield chunk
        
        return StreamingResponse(
            stream_generator(),
            status_code=200,
            media_type=content_type,
            headers=response_headers,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"MP4 error: {str(e)}")
        raise HTTPException(500, f"MP4 error: {str(e)}")

async def handle_hls_playlist(playlist_url: str, request: Request):
    """Handle HLS playlist with segment rewriting"""
    try:
        headers = await build_headers()
        
        logger.info(f"Fetching HLS playlist: {playlist_url}")
        
        resp = await client.get(playlist_url, headers=headers)
        logger.info(f"HLS playlist response: {resp.status_code}")
        
        if resp.status_code == 426:
            logger.warning("426 Upgrade Required, trying HTTP/1.1...")
            async with httpx.AsyncClient(http2=False, timeout=30.0) as http1_client:
                resp = await http1_client.get(playlist_url, headers=headers)
                logger.info(f"HTTP/1.1 response: {resp.status_code}")
        
        if resp.status_code != 200:
            error_body = resp.text if hasattr(resp, 'text') else str(resp.content)
            logger.error(f"HLS error: {resp.status_code} - {error_body[:200]}")
            raise HTTPException(resp.status_code, detail=f"HLS error: {resp.status_code}")
        
        playlist = resp.text
        
        # Extract base URL for segments
        parsed = urlparse(playlist_url)
        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path[:parsed.path.rfind('/') + 1]}"
        
        # Get the hash from the URL path
        hash_match = re.search(r'/moviebox/([^/]+)/', playlist_url)
        hash_value = hash_match.group(1) if hash_match else "unknown"
        
        # Extract sign and t from query string
        sign_match = re.search(r'sign=([^&]+)', playlist_url)
        t_match = re.search(r't=([^&]+)', playlist_url)
        sign = sign_match.group(1) if sign_match else ""
        t = t_match.group(1) if t_match else ""
        
        # Build proxy base URL
        proxy_base = f"{request.base_url}proxy?url="
        
        # Rewrite segment URLs
        # Full URLs (https://...)
        playlist = re.sub(
            r'(https?://[^\s]+\.ts)',
            lambda m: f"{proxy_base}{m.group(1)}",
            playlist
        )
        
        # Relative URLs (segment_001.ts)
        playlist = re.sub(
            r'^([a-zA-Z0-9_\-]+\.ts)$',
            lambda m: f"{proxy_base}{base_url}{m.group(1)}",
            playlist,
            flags=re.MULTILINE
        )
        
        response_headers = {
            "Content-Type": "application/vnd.apple.mpegurl",
            "Cache-Control": "public, max-age=300",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Range, Content-Type",
        }
        
        return StreamingResponse(
            iter([playlist]),
            status_code=200,
            media_type="application/vnd.apple.mpegurl",
            headers=response_headers,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"HLS playlist error: {str(e)}")
        raise HTTPException(500, f"HLS playlist error: {str(e)}")

async def handle_segment(segment_url: str, request: Request):
    """Handle HLS segment (.ts) streaming"""
    try:
        headers = await build_headers(request.headers.get("range"))
        
        logger.info(f"Fetching segment: {segment_url}")
        
        resp = await client.get(segment_url, headers=headers)
        logger.info(f"Segment response: {resp.status_code}")
        
        if resp.status_code == 426:
            logger.warning("426 Upgrade Required, trying HTTP/1.1...")
            async with httpx.AsyncClient(http2=False, timeout=30.0) as http1_client:
                resp = await http1_client.get(segment_url, headers=headers)
                logger.info(f"HTTP/1.1 response: {resp.status_code}")
        
        if resp.status_code != 200:
            error_body = resp.text if hasattr(resp, 'text') else str(resp.content)
            logger.error(f"Segment error: {resp.status_code} - {error_body[:200]}")
            raise HTTPException(resp.status_code, detail=f"Segment error: {resp.status_code}")
        
        content_type = resp.headers.get("content-type", "video/mp4")
        content_length = resp.headers.get("content-length", "")
        content_range = resp.headers.get("content-range", "")
        
        response_headers = {
            "Content-Type": content_type,
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=86400",
            "Accept-Ranges": "bytes",
        }
        
        if content_length:
            response_headers["Content-Length"] = content_length
        if content_range:
            response_headers["Content-Range"] = content_range
        
        logger.info(f"Streaming segment: {content_length} bytes")
        
        async def stream_generator():
            async for chunk in resp.aiter_bytes(chunk_size=65536):
                yield chunk
        
        return StreamingResponse(
            stream_generator(),
            status_code=200,
            media_type=content_type,
            headers=response_headers,
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Segment error: {str(e)}")
        raise HTTPException(500, f"Segment error: {str(e)}")

@app.get("/hls")
async def hls_direct(request: Request, hash: str, sign: str, t: str):
    """Direct HLS endpoint - builds URL and proxies it"""
    if not hash or not sign or not t:
        raise HTTPException(400, "Missing parameters: hash, sign, t")
    
    playlist_url = f"https://live-pull.aisports.mobi/moviebox/{hash}/playlist.m3u8?sign={sign}&t={t}"
    encoded_url = __import__('urllib.parse').quote(playlist_url)
    
    return await proxy(request, url=encoded_url)

@app.on_event("shutdown")
async def shutdown():
    await client.aclose()

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
