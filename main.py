# main.py
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx
import re
import json
import logging
from urllib.parse import unquote

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="MovieBox Proxy - Railway")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# HTTP/2 client
client = httpx.AsyncClient(
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
        async with httpx.AsyncClient() as temp_client:
            resp = await temp_client.get(url, headers=headers)
            logger.info(f"Token response status: {resp.status_code}")
            
            # Extract token from Set-Cookie header
            set_cookie = resp.headers.get("set-cookie", "")
            match = re.search(r'token=([^;]+)', set_cookie)
            
            if match:
                token = match.group(1)
                logger.info(f"Token obtained: {token[:30]}...")
                return token
            else:
                logger.error("No token found in Set-Cookie header")
                logger.info(f"Response headers: {dict(resp.headers)}")
                return None
                
    except Exception as e:
        logger.error(f"Error getting token: {str(e)}")
        return None

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
    """Proxy video stream from CDN"""
    if not url:
        raise HTTPException(400, "Missing url parameter")
    
    try:
        decoded_url = unquote(url)
        logger.info(f"Fetching: {decoded_url}")

        # Get fresh token
        token = await get_fresh_token()
        if not token:
            raise HTTPException(401, "Failed to get authorization token")

        # Extract userId from token or use default
        userId = "5449878944034578072"
        try:
            import base64
            payload = json.loads(base64.b64decode(token.split('.')[1] + '==').decode('utf-8'))
            userId = str(payload.get('uid', userId))
        except Exception as e:
            logger.warning(f"Could not decode token: {e}")

        # Build headers exactly like working browser
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

        # Pass through Range header
        range_header = request.headers.get("range")
        if range_header:
            headers["Range"] = range_header
            logger.info(f"Range request: {range_header}")

        logger.info(f"Sending request with token: {token[:20]}...")

        # Make the request
        resp = await client.get(decoded_url, headers=headers)
        logger.info(f"Response status: {resp.status_code}")

        if resp.status_code == 426:
            # Try with HTTP/1.1
            logger.warning("426 Upgrade Required, trying HTTP/1.1...")
            async with httpx.AsyncClient(http2=False, timeout=30.0) as http1_client:
                resp = await http1_client.get(decoded_url, headers=headers)
                logger.info(f"HTTP/1.1 response: {resp.status_code}")

        if resp.status_code != 200:
            error_body = resp.text if hasattr(resp, 'text') else str(resp.content)
            logger.error(f"CDN error: {resp.status_code} - {error_body[:200]}")
            raise HTTPException(
                resp.status_code,
                detail=f"CDN error: {resp.status_code} - {error_body[:100]}"
            )

        # Stream response
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

        logger.info(f"Streaming: {content_length} bytes")

        async def stream_generator():
            async for chunk in resp.aiter_bytes(chunk_size=8192):
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
        logger.error(f"Proxy error: {str(e)}")
        raise HTTPException(500, f"Proxy error: {str(e)}")

@app.on_event("shutdown")
async def shutdown():
    await client.aclose()

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
