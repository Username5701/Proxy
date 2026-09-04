# main.py - Pure direct fetch with chunked streaming (FULLY FIXED)
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
                logger.info(f"✅ Token obtained")
                return token
            else:
                logger.error("❌ No token found")
                return None
    except Exception as e:
        logger.error(f"❌ Token error: {str(e)}")
        return None

def build_headers(token, range_header=None):
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
        logger.info(f"📊 Range header: {range_header}")
    
    return headers

# ==================== STREAMING ====================
async def stream_generator(client, resp, content_length=None):
    """
    Stream chunks without buffering while keeping the HTTP client alive.
    """
    chunk_size = 65536  # 64KB chunks
    bytes_streamed = 0
    
    try:
        async for chunk in resp.aiter_bytes(chunk_size=chunk_size):
            if chunk:
                bytes_streamed += len(chunk)
                yield chunk
                
                # Log progress for large files
                if content_length and bytes_streamed % (chunk_size * 100) == 0:
                    progress = (bytes_streamed / int(content_length)) * 100
                    logger.debug(f"📊 Progress: {progress:.1f}%")
                    
    except httpx.StreamClosed as e:
        logger.warning(f"Stream closed by remote server: {str(e)}")
        return
    except httpx.ReadTimeout as e:
        logger.warning(f"Read timeout during streaming: {str(e)}")
        return
    except Exception as e:
        logger.error(f"Stream error: {str(e)}")
        raise
    finally:
        # Clean up resources
        try:
            await resp.aclose()
        except:
            pass
        try:
            await client.aclose()
        except:
            pass

# ==================== ENDPOINTS ====================
@app.get("/")
async def root():
    return {
        "service": "MovieBox Proxy (Railway)",
        "status": "running",
        "endpoints": {
            "/proxy": "Proxy video stream from CDN"
        },
        "usage": "/proxy?url=ENCODED_CDN_URL"
    }

@app.get("/proxy")
async def proxy(request: Request, url: str):
    if not url:
        raise HTTPException(400, "Missing url parameter")
    
    decoded_url = unquote(url)
    logger.info(f"📥 Fetching: {decoded_url}")

    token = await get_fresh_token()
    if not token:
        raise HTTPException(401, "Failed to get authorization token")

    range_header = request.headers.get("range")
    headers = build_headers(token, range_header)

    client = None
    resp = None
    
    try:
        # Create the HTTP client
        client_kwargs = {
            "timeout": httpx.Timeout(300.0, connect=30.0),
            "follow_redirects": True,
            "http2": True,
            "limits": httpx.Limits(max_keepalive_connections=1, max_connections=1)
        }
        client = httpx.AsyncClient(**client_kwargs)
        
        # Start the streaming request
        resp_context = client.stream("GET", decoded_url, headers=headers)
        resp = await resp_context.__aenter__()
        
        logger.info(f"📊 CDN response: {resp.status_code}")
        
        # Handle 426 Upgrade Required
        if resp.status_code == 426:
            logger.warning("⚠️ 426 Upgrade Required, trying HTTP/1.1...")
            await resp_context.__aexit__(None, None, None)
            await client.aclose()
            
            client = httpx.AsyncClient(
                timeout=httpx.Timeout(300.0, connect=30.0),
                follow_redirects=True,
                http2=False,
            )
            resp_context = client.stream("GET", decoded_url, headers=headers)
            resp = await resp_context.__aenter__()
        
        # Check for error status codes
        if resp.status_code not in (200, 206):
            error_body = await resp.aread()
            error_text = error_body.decode('utf-8', errors='ignore')[:200]
            logger.error(f"❌ CDN error {resp.status_code}: {error_text}")
            await resp_context.__aexit__(None, None, None)
            await client.aclose()
            raise HTTPException(resp.status_code, f"CDN error: {resp.status_code}")
        
        # Get headers from CDN response
        content_type = resp.headers.get("content-type", "video/mp4")
        content_length = resp.headers.get("content-length")
        content_range = resp.headers.get("content-range")
        
        # CRITICAL: Build proper response headers for video playback
        response_headers = {
            "Content-Type": content_type,
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Range, Content-Range, Content-Type",
            "Accept-Ranges": "bytes",
            "Cache-Control": "public, max-age=3600",
        }
        
        # Determine status code
        if range_header:
            status_code = 206
            # For partial content, we MUST forward Content-Length and Content-Range
            if content_length:
                response_headers["Content-Length"] = content_length
            if content_range:
                response_headers["Content-Range"] = content_range
            else:
                # If CDN didn't return Content-Range, we need to calculate it
                # This is a fallback - might not work perfectly
                if content_length:
                    response_headers["Content-Range"] = f"bytes 0-{int(content_length)-1}/{content_length}"
        else:
            status_code = 200
            # For full content, browsers need Content-Length
            if content_length:
                response_headers["Content-Length"] = content_length
            else:
                # If no content-length, we need to send it chunked
                # FastAPI/Starlette will handle this with Transfer-Encoding: chunked
                pass
        
        logger.info(f"✅ Streaming started (status: {status_code})")
        logger.info(f"📋 Headers: {response_headers}")
        
        # Return StreamingResponse
        return StreamingResponse(
            stream_generator(client, resp, content_length),
            status_code=status_code,
            media_type=content_type,
            headers=response_headers,
        )
                
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Proxy error: {str(e)}")
        if resp:
            try:
                await resp.aclose()
            except:
                pass
        if client:
            try:
                await client.aclose()
            except:
                pass
        raise HTTPException(500, f"Proxy error: {str(e)}")

@app.on_event("shutdown")
async def shutdown():
    pass

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
