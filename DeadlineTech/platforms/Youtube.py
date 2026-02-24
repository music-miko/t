import asyncio
import os
import re
import time
import uuid
import contextlib
import logging
import aiohttp
import aiofiles

from pathlib import Path
from typing import Union, Optional, Dict, Any, List
from urllib.parse import urlparse

from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import TCPConnector

from pyrogram.types import Message
from pyrogram.enums import MessageEntityType
from pyrogram.errors import FloodWait

from youtubesearchpython.__future__ import VideosSearch

from DeadlineTech import app as TG_APP
from DeadlineTech.utils.database import is_on_off
from DeadlineTech.utils.formatters import time_to_seconds
from DeadlineTech.core.dir import DOWNLOAD_DIR
import config

# === Configuration & Constants ===
API_KEY = config.API_KEY
API_URL = config.API_URL
MEDIA_CHANNEL_ID = config.MEDIA_CHANNEL_ID
DB_URI = config.DB_URI
MEDIA_DB_NAME = "arcapi"
MEDIA_COLLECTION_NAME = "medias"

# Settings from downloader.py
CHUNK_SIZE = 1024 * 1024
V2_HTTP_RETRIES = 5
V2_DOWNLOAD_CYCLES = 5
HARD_RETRY_WAIT = 3
JOB_POLL_ATTEMPTS = 10
JOB_POLL_INTERVAL = 2.0
JOB_POLL_BACKOFF = 1.2
NO_CANDIDATE_WAIT = 4
CDN_RETRIES = 5
CDN_RETRY_DELAY = 2
HARD_TIMEOUT = 80
TG_FLOOD_COOLDOWN = 0.0

# Regex
YOUTUBE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{11}$")
YOUTUBE_ID_IN_URL_RE = re.compile(r"""(?x)(?:v=|\/)([A-Za-z0-9_-]{11})|youtu\.be\/([A-Za-z0-9_-]{11})""")

# Globals
_inflight: Dict[str, asyncio.Future] = {}
_inflight_lock = asyncio.Lock()
_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()
_MONGO_CLIENT: Optional[AsyncIOMotorClient] = None

LOGGER = logging.getLogger(__name__)

# === Statistics System ===
DOWNLOAD_STATS: Dict[str, int] = {
    "total": 0, "success": 0, "failed": 0,
    "media_db_hit": 0, "media_db_miss": 0, "media_db_fail": 0,
    "api_fail_5xx": 0, "network_fail": 0, "timeout_fail": 0
}

def _inc(key: str):
    DOWNLOAD_STATS[key] = DOWNLOAD_STATS.get(key, 0) + 1

class V2HardAPIError(Exception):
    def __init__(self, status: int, body_preview: str = ""):
        super().__init__(f"Hard API error status={status}")
        self.status = status
        self.body_preview = body_preview[:200]

# === Helpers ===

def extract_video_id(link: str) -> str:
    """Fast local string parsing."""
    if not link: return ""
    s = link.strip()
    if YOUTUBE_ID_RE.match(s): return s
    m = YOUTUBE_ID_IN_URL_RE.search(s)
    if m: return m.group(1) or m.group(2) or ""
    if "v=" in s: return s.split("v=")[-1].split("&")[0]
    try:
        last = s.split("/")[-1].split("?")[0]
        if YOUTUBE_ID_RE.match(last): return last
    except: pass
    return ""

def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def _resolve_if_dir(download_result: str) -> Optional[str]:
    if not download_result: return None
    p = Path(download_result)
    if p.exists() and p.is_file(): return str(p)
    if p.exists() and p.is_dir():
        files = [x for x in p.iterdir() if x.is_file()]
        if not files: return None
        newest = max(files, key=lambda x: x.stat().st_mtime)
        return str(newest)
    return download_result

async def get_http_session() -> aiohttp.ClientSession:
    global _session
    if _session and not _session.closed: return _session
    async with _session_lock:
        if _session and not _session.closed: return _session
        timeout = aiohttp.ClientTimeout(total=HARD_TIMEOUT, sock_connect=10, sock_read=30)
        connector = TCPConnector(limit=100, ttl_dns_cache=300, enable_cleanup_closed=True)
        _session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return _session

# === Database Helpers ===

def _get_media_collection():
    global _MONGO_CLIENT
    if not DB_URI: return None
    if _MONGO_CLIENT is None:
        _MONGO_CLIENT = AsyncIOMotorClient(DB_URI)
    return _MONGO_CLIENT[MEDIA_DB_NAME][MEDIA_COLLECTION_NAME]

async def is_media(track_id: str, isVideo: bool = False) -> bool:
    col = _get_media_collection()
    if col is None: return False
    doc = await col.find_one({
        "$or": [
            {"track_id": track_id, "isVideo": isVideo},
            {"track_id": f"{track_id}.{'mp4' if isVideo else 'mp3'}"}
        ]
    }, {"_id": 1})
    return bool(doc)

async def get_media_id(track_id: str, isVideo: bool = False) -> Optional[int]:
    col = _get_media_collection()
    if col is None: return None
    doc = await col.find_one({
        "$or": [
            {"track_id": track_id, "isVideo": isVideo},
            {"track_id": f"{track_id}.{'mp4' if isVideo else 'mp3'}"}
        ]
    }, {"message_id": 1})
    return int(doc.get("message_id")) if doc and doc.get("message_id") else None

async def _download_from_media_db(track_id: str, is_video: bool) -> Optional[str]:
    global TG_FLOOD_COOLDOWN
    if not track_id or not TG_APP or not MEDIA_CHANNEL_ID: return None

    if time.time() < TG_FLOOD_COOLDOWN: 
        return None 

    ext = "mp4" if is_video else "mp3"
    
    # Try multiple key formats
    msg_id = None
    keys = [f"{track_id}.{ext}", track_id, f"{track_id}_{'v' if is_video else 'a'}"]
    
    for k in keys:
        if await is_media(k, isVideo=is_video):
            msg_id = await get_media_id(k, isVideo=is_video)
            break
            
    if not msg_id:
        _inc("media_db_miss")
        return None

    _inc("media_db_hit")
    out_dir = str(Path(DOWNLOAD_DIR))
    _ensure_dir(out_dir)
    
    final_path = os.path.join(out_dir, f"{track_id}.{ext}")
    tmp_path = final_path + ".temp"

    if os.path.exists(final_path) and os.path.getsize(final_path) > 0:
        return final_path

    try:
        msg = await TG_APP.get_messages(int(MEDIA_CHANNEL_ID), msg_id)
        if not msg or not msg.media: return None

        dl_res = await asyncio.wait_for(
            TG_APP.download_media(msg, file_name=tmp_path),
            timeout=HARD_TIMEOUT
        )
        
        fixed = _resolve_if_dir(dl_res)
        if fixed and os.path.exists(fixed) and os.path.getsize(fixed) > 0:
            if fixed != final_path:
                try: os.replace(fixed, final_path)
                except: final_path = fixed
            return final_path
            
    except FloodWait as e:
        TG_FLOOD_COOLDOWN = time.time() + e.value + 5
        print(f"⚠️ FloodWait: {e.value}s")
    except Exception:
        _inc("media_db_fail")
    
    return None

# === V2 API Helpers ===

def _extract_candidate(obj: Any) -> Optional[str]:
    if not obj: return None
    if isinstance(obj, str) and obj.strip(): return obj.strip()
    if isinstance(obj, list) and obj: return _extract_candidate(obj[0])
    if isinstance(obj, dict):
        job = obj.get("job")
        if isinstance(job, dict):
            res = job.get("result")
            if isinstance(res, dict):
                 for k in ("public_url", "cdnurl", "download_url", "url"):
                    if res.get(k): return res.get(k)
        for k in ("public_url", "cdnurl", "download_url", "url"):
            if obj.get(k): return obj.get(k)
    return None

def _looks_like_status_text(s: Optional[str]) -> bool:
    if not s: return False
    return any(x in s.lower() for x in ("processing", "queued", "job_id", "background"))

def _normalize_candidate_to_url(candidate: str) -> Optional[str]:
    if not candidate: return None
    c = candidate.strip()
    if c.startswith("http"): return c
    if c.startswith("/root/") or c.startswith("/home/"): return None
    return f"{API_URL.rstrip('/')}/{c.lstrip('/')}"

async def _download_from_cdn(cdn_url: str, out_path: str) -> Optional[str]:
    if not cdn_url: return None
    
    for attempt in range(1, CDN_RETRIES + 1):
        try:
            session = await get_http_session()
            async with session.get(cdn_url, timeout=HARD_TIMEOUT) as resp:
                if resp.status != 200:
                    if attempt < CDN_RETRIES:
                        await asyncio.sleep(CDN_RETRY_DELAY)
                        continue
                    return None
                
                _ensure_dir(str(Path(out_path).parent))
                async with aiofiles.open(out_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                        if not chunk: break
                        await f.write(chunk)
            
            if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                return out_path
        except Exception:
            _inc("network_fail")
            if attempt < CDN_RETRIES: await asyncio.sleep(CDN_RETRY_DELAY)
    return None

async def _v2_request_json(endpoint: str, params: Dict[str, Any]) -> Optional[Any]:
    if not API_URL or not API_KEY: return None
    
    base = API_URL.rstrip("/")
    url = f"{base}/{endpoint.lstrip('/')}"
    params["api_key"] = API_KEY

    for attempt in range(1, V2_HTTP_RETRIES + 1):
        try:
            session = await get_http_session()
            async with session.get(url, params=params, headers={"X-API-Key": API_KEY}) as resp:
                if 200 <= resp.status < 300:
                    try: return await resp.json()
                    except: return None
                if resp.status in (401, 403): raise V2HardAPIError(resp.status)
                if resp.status >= 500: _inc("api_fail_5xx")
                else: _inc("network_fail")
        except V2HardAPIError: raise
        except Exception: _inc("network_fail")
        
        if attempt < V2_HTTP_RETRIES: await asyncio.sleep(1)
    return None

async def v2_download_process(link: str, video: bool) -> Optional[str]:
    vid = extract_video_id(link)
    query = vid or link
    ext = "mp4" if video else "m4a"
    base_name = vid if vid else uuid.uuid4().hex[:10]
    out_path = os.path.join(str(Path(DOWNLOAD_DIR)), f"{base_name}.{ext}")
    
    if os.path.exists(out_path): return out_path

    for cycle in range(1, V2_DOWNLOAD_CYCLES + 1):
        try:
            resp = await _v2_request_json("youtube/v2/download", {"query": query, "isVideo": str(video).lower()})
        except V2HardAPIError: return None

        if not resp:
            if cycle < V2_DOWNLOAD_CYCLES: await asyncio.sleep(1); continue
            return None
            
        candidate = _extract_candidate(resp)
        if candidate and _looks_like_status_text(candidate): candidate = None
        
        job_id = resp.get("job_id") if isinstance(resp, dict) else None
        
        if job_id and not candidate:
            interval = JOB_POLL_INTERVAL
            for _ in range(JOB_POLL_ATTEMPTS):
                await asyncio.sleep(interval)
                status = await _v2_request_json("youtube/jobStatus", {"job_id": job_id})
                candidate = _extract_candidate(status)
                if candidate and not _looks_like_status_text(candidate): break
                interval *= JOB_POLL_BACKOFF

        if not candidate:
            if cycle < V2_DOWNLOAD_CYCLES: await asyncio.sleep(NO_CANDIDATE_WAIT); continue
            return None
            
        final_url = _normalize_candidate_to_url(candidate)
        if final_url:
            path = await _download_from_cdn(final_url, out_path)
            if path: return path

    return None

# === De-duplication ===

async def deduplicate_download(key: str, runner):
    async with _inflight_lock:
        if fut := _inflight.get(key):
            return await fut
        fut = asyncio.get_running_loop().create_future()
        _inflight[key] = fut
    try:
        result = await runner()
        if not fut.done(): fut.set_result(result)
        return result
    except Exception as e:
        if not fut.done(): fut.set_exception(e)
        return None
    finally:
        async with _inflight_lock:
            if _inflight.get(key) == fut:
                _inflight.pop(key, None)

# === MAIN CLASS ===

class YouTubeAPI:
    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="
        self.regex = r"(?:youtube\.com|youtu\.be)"
        self.listbase = "https://youtube.com/playlist?list="

    # === SECURITY PATCH ===
    def is_safe_youtube_url(self, url: str) -> bool:
        """Validates that a URL is a legitimate YouTube link and contains no shell injection characters."""
        if not url: return False
        try:
            p = urlparse(url)
            if p.scheme not in ("http", "https"):
                return False
            allowed = ("youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be")
            if not any(domain in p.netloc for domain in allowed):
                return False
            # Block shell metacharacters and newlines
            if any(x in url for x in [";", "|", "$", "`", "\n", "\r"]):
                LOGGER.warning(f"⚠️ Blocked potentially malicious URL: {url}")
                return False
            return True
        except Exception:
            return False
    # =======================

    async def exists(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        # Basic regex check is usually safe, but adding validation doesn't hurt
        if not self.is_safe_youtube_url(link): return False
        return bool(re.search(self.regex, link))

    async def url(self, message: Message) -> Union[str, None]:
        msgs = [message]
        if message.reply_to_message: msgs.append(message.reply_to_message)
        for msg in msgs:
            text = msg.text or msg.caption or ""
            if not text: continue
            if msg.entities:
                for entity in msg.entities:
                    if entity.type == MessageEntityType.URL:
                        u = text[entity.offset:entity.offset+entity.length]
                        if self.is_safe_youtube_url(u): return u
            if msg.caption_entities:
                for entity in msg.caption_entities:
                    if entity.type == MessageEntityType.TEXT_LINK:
                        if self.is_safe_youtube_url(entity.url): return entity.url
        return None

    # Metadata methods use VideosSearch (kept as requested)
    async def details(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        if not self.is_safe_youtube_url(link): return None # Validation Check
        
        if "&" in link: link = link.split("&")[0]
        results = VideosSearch(link, limit=1)
        for r in (await results.next())["result"]:
            sec = int(time_to_seconds(r["duration"])) if r["duration"] else 0
            return r["title"], r["duration"], sec, r["thumbnails"][0]["url"].split("?")[0], r["id"]
        return None

    async def title(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        if not self.is_safe_youtube_url(link): return "" # Validation Check
        
        results = VideosSearch(link, limit=1)
        for r in (await results.next())["result"]: return r["title"]
        return ""

    async def duration(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        if not self.is_safe_youtube_url(link): return "00:00" # Validation Check
        
        results = VideosSearch(link, limit=1)
        for r in (await results.next())["result"]: return r["duration"]
        return "00:00"

    async def thumbnail(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        if not self.is_safe_youtube_url(link): return "" # Validation Check
        
        results = VideosSearch(link, limit=1)
        for r in (await results.next())["result"]: return r["thumbnails"][0]["url"].split("?")[0]
        return ""

    async def track(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        if not self.is_safe_youtube_url(link): return None, None # Validation Check
        
        results = VideosSearch(link, limit=1)
        for r in (await results.next())["result"]:
            return {
                "title": r["title"], "link": r["link"], "vidid": r["id"],
                "duration_min": r["duration"], "thumb": r["thumbnails"][0]["url"].split("?")[0],
            }, r["id"]
        return None, None

    async def slider(self, link: str, query_type: int, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        if not self.is_safe_youtube_url(link): return None # Validation Check
        
        a = VideosSearch(link, limit=10)
        result = (await a.next()).get("result")
        if not result or query_type >= len(result): return None
        r = result[query_type]
        return r["title"], r["duration"], r["thumbnails"][0]["url"].split("?")[0], r["id"]

    # === NEW: Replaced Video Method ===
    async def video(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        if not self.is_safe_youtube_url(link): return 0, "Invalid or Unsafe URL" # Validation Check

        # Internal runner to fit download signature
        async def _run():
            vid = extract_video_id(link)
            if vid:
                db_path = await _download_from_media_db(vid, is_video=True)
                if db_path: return db_path
            return await v2_download_process(link, video=True)

        key = f"video:{link}"
        path = await deduplicate_download(key, _run)
        
        if path: return 1, path
        return 0, "Failed"

    # === NEW: Replaced Download Method (The Core) ===
    async def download(
        self,
        link: str,
        mystic,
        video: Union[bool, str] = None,
        videoid: Union[bool, str] = None,
        songaudio: Union[bool, str] = None,
        songvideo: Union[bool, str] = None,
        format_id: Union[bool, str] = None,
        title: Union[bool, str] = None,
    ) -> str:
        _inc("total")
        if videoid: link = self.base + link
        if not self.is_safe_youtube_url(link): return None, None # Validation Check
        
        is_vid = True if (video or songvideo) else False
        vid = extract_video_id(link)
        dedup_id = vid or link
        key = f"{'video' if is_vid else 'audio'}:{dedup_id}"
        
        async def _download_logic():
            # 1. DB Check
            if vid:
                db_path = await _download_from_media_db(vid, is_video=is_vid)
                if db_path: 
                    _inc("success")
                    return db_path

            # 2. V2 Process
            path = await v2_download_process(link, video=is_vid)
            if path:
                _inc("success")
                return path
            
            _inc("failed")
            return None

        # Execute with deduplication and Hard Timeout
        try:
            path = await asyncio.wait_for(
                deduplicate_download(key, _download_logic), 
                timeout=HARD_TIMEOUT
            )
            if path: return path, True
        except Exception as e:
            _inc("timeout_fail")
            print(f"Download Timeout/Error: {e}")

        return None, None
