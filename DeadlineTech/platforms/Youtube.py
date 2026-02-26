import asyncio
import os
import re
import time
import uuid
import random
import logging
import aiohttp
import aiofiles
import yt_dlp

from pathlib import Path
from typing import Union, Optional, Dict, Any, List
from urllib.parse import urlparse

from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import TCPConnector

from pyrogram.types import Message
from pyrogram.enums import MessageEntityType
from pyrogram.errors import FloodWait

# --- CHANGED: Use py_yt to fix httpx issues ---
try:
    from py_yt import VideosSearch, Playlist
except ImportError:
    VideosSearch = None
    Playlist = None

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

# Settings
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
HARD_TIMEOUT = 300  
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

# Configure Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
LOGGER = logging.getLogger("YouTubeAPI")

# === Statistics System ===
DOWNLOAD_STATS: Dict[str, int] = {
    "total": 0, "success": 0, "failed": 0,
    "db_hit": 0, "v2_success": 0, "cookie_success": 0,
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

def cookie_txt_file():
    cookie_dir = f"{os.getcwd()}/cookies"
    if not os.path.exists(cookie_dir): return None
    files = [f for f in os.listdir(cookie_dir) if f.endswith(".txt")]
    return os.path.join(cookie_dir, random.choice(files)) if files else None

def sec_to_min(sec):
    """Converts seconds (int) to MM:SS string."""
    try:
        sec = int(sec)
        m, s = divmod(sec, 60)
        return f"{m:02d}:{s:02d}"
    except:
        return "00:00"

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

    if time.time() < TG_FLOOD_COOLDOWN: return None 

    ext = "mp4" if is_video else "mp3"
    msg_id = None
    keys = [f"{track_id}.{ext}", track_id, f"{track_id}_{'v' if is_video else 'a'}"]
    
    for k in keys:
        if await is_media(k, isVideo=is_video):
            msg_id = await get_media_id(k, isVideo=is_video)
            break
            
    if not msg_id: return None

    LOGGER.info(f"📂 Database HIT: {track_id}")
    _inc("db_hit")
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
        LOGGER.warning(f"⚠️ FloodWait: {e.value}s")
    except Exception:
        pass
    
    return None

# === V2 API Helpers (Downloads) ===

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
    
    LOGGER.info(f"⬇️ CDN Download: {out_path}")
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

    LOGGER.info(f"🔄 V2 API Process: {query}")

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
            LOGGER.info(f"🔗 Joining download: {key}")
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

    # === FALLBACK API HELPER ===
    async def _search_api(self, query: str):
        """Fallback: Fetches video metadata from DeadlineTech API."""
        try:
            session = await get_http_session()
            base = API_URL.rstrip('/')
            url = f"{base}/youtube/search"
            params = {"query": query, "api_key": API_KEY}
            
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("status") == "success" and data.get("result"):
                        return data["result"]
        except Exception as e:
            LOGGER.error(f"⚠️ Metadata API Error: {e}")
        return None

    async def exists(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
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
                        return text[entity.offset:entity.offset+entity.length]
            if msg.caption_entities:
                for entity in msg.caption_entities:
                    if entity.type == MessageEntityType.TEXT_LINK:
                        return entity.url
        return None

    # --- METADATA (Includes API Fallback) ---
    async def details(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        if "&" in link: link = link.split("&")[0]
        
        # 1. Try Local py_yt
        if VideosSearch:
            try:
                results = VideosSearch(link, limit=1)
                res = await results.next()
                if res and "result" in res:
                    for r in res["result"]:
                        sec = int(time_to_seconds(r["duration"])) if r.get("duration") else 0
                        thumb = r.get("thumbnails", [{}])[0].get("url", "").split("?")[0]
                        return r["title"], r["duration"], sec, thumb, r["id"]
            except Exception:
                pass # Fallback
            
        # 2. Try Fallback API
        res = await self._search_api(link)
        if res:
            title = res.get("title", "Unknown")
            duration_sec = int(res.get("duration", 0))
            duration_str = sec_to_min(duration_sec)
            thumb = res.get("thumbnail", "")
            vid_url = res.get("url", "")
            vidid = extract_video_id(vid_url)
            return title, duration_str, duration_sec, thumb, vidid
            
        return None

    async def title(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        
        if VideosSearch:
            try:
                results = VideosSearch(link, limit=1)
                res = await results.next()
                for r in res.get("result", []): return r["title"]
            except: pass
        
        # Fallback
        res = await self._search_api(link)
        if res: return res.get("title", "")
        return ""

    async def duration(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        
        if VideosSearch:
            try:
                results = VideosSearch(link, limit=1)
                res = await results.next()
                for r in res.get("result", []): return r["duration"]
            except: pass
        
        # Fallback
        res = await self._search_api(link)
        if res: return sec_to_min(res.get("duration", 0))
        return "00:00"

    async def thumbnail(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        
        if VideosSearch:
            try:
                results = VideosSearch(link, limit=1)
                res = await results.next()
                for r in res.get("result", []): 
                     return r.get("thumbnails", [{}])[0].get("url", "").split("?")[0]
            except: pass
        
        # Fallback
        res = await self._search_api(link)
        if res: return res.get("thumbnail", "")
        return ""

    async def track(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        if "&" in link: link = link.split("&")[0]
        
        # 1. Try Local
        if VideosSearch:
            try:
                results = VideosSearch(link, limit=1)
                res = await results.next()
                if res and "result" in res:
                    for r in res["result"]:
                        thumb = r.get("thumbnails", [{}])[0].get("url", "").split("?")[0]
                        return {
                            "title": r.get("title"), "link": r.get("link"), "vidid": r.get("id"),
                            "duration_min": r.get("duration"), "thumb": thumb,
                        }, r.get("id")
            except: pass
        
        # 2. Fallback
        res = await self._search_api(link)
        if res:
            title = res.get("title", "Unknown")
            vid_url = res.get("url", "")
            vidid = extract_video_id(vid_url)
            duration_sec = int(res.get("duration", 0))
            duration_str = sec_to_min(duration_sec)
            thumb = res.get("thumbnail", "")
            
            return {
                "title": title, "link": vid_url, "vidid": vidid,
                "duration_min": duration_str, "thumb": thumb,
            }, vidid
            
        return None, None

    async def slider(self, link: str, query_type: int, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        
        # 1. Try Local
        if VideosSearch:
            try:
                a = VideosSearch(link, limit=10)
                res = await a.next()
                result = res.get("result")
                if not result or query_type >= len(result): 
                    if query_type == 0 and not result: raise Exception("Empty")
                    return None
                
                r = result[query_type]
                thumb = r.get("thumbnails", [{}])[0].get("url", "").split("?")[0]
                return r["title"], r["duration"], thumb, r["id"]
            except: pass
        
        # 2. Fallback (Only works for first result / index 0)
        if query_type == 0:
            res = await self._search_api(link)
            if res:
                title = res.get("title", "Unknown")
                duration_sec = int(res.get("duration", 0))
                duration_str = sec_to_min(duration_sec)
                thumb = res.get("thumbnail", "")
                vid_url = res.get("url", "")
                vidid = extract_video_id(vid_url)
                return title, duration_str, thumb, vidid
        
        return None

    async def playlist(self, link, limit, user_id, videoid: Union[bool, str] = None):
        if videoid: link = self.listbase + link
        
        # Use py_yt Playlist
        if Playlist:
            try:
                plist = await Playlist.get(link)
                if not plist or "videos" not in plist: return []
                ids = []
                for data in plist["videos"][:limit]:
                    if data.get("id"): ids.append(data["id"])
                return ids
            except: pass
        return []

    async def formats(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        cookie_file = cookie_txt_file()
        if not cookie_file: return [], link
        
        ytdl_opts = {"quiet": True, "cookiefile": cookie_file}
        out = []
        try:
            with yt_dlp.YoutubeDL(ytdl_opts) as ydl:
                r = ydl.extract_info(link, download=False)
                for f in r.get("formats", []):
                    if "dash" in str(f.get("format")).lower(): continue
                    out.append({
                        "format": f.get("format"), "filesize": f.get("filesize"),
                        "format_id": f.get("format_id"), "ext": f.get("ext"),
                        "format_note": f.get("format_note"), "yturl": link
                    })
        except: pass
        return out, link

    # === VIDEO METHOD (With 3-Layer Fallback) ===
    async def video(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link

        LOGGER.info(f"📹 Video Req: {link}")

        async def _run():
            vid = extract_video_id(link)
            
            # 1. DB
            if vid:
                db_path = await _download_from_media_db(vid, is_video=True)
                if db_path: return 1, db_path
            
            # 2. V2 API
            path = await v2_download_process(link, video=True)
            if path: 
                _inc("v2_success")
                return 1, path
            
            # 3. Cookie Fallback
            LOGGER.info("🍪 Fallback: Using yt-dlp cookies")
            cookie_file = cookie_txt_file()
            if not cookie_file: return 0, "No cookies/API failed"
            
            cmd = ["yt-dlp", "--cookies", cookie_file, "-g", "-f", "best[height<=?720]", "--", link]
            try:
                proc = await asyncio.create_subprocess_exec(
                    *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                if stdout: 
                    _inc("cookie_success")
                    return 1, stdout.decode().split("\n")[0]
            except Exception as e:
                LOGGER.error(f"Fallback error: {e}")

            return 0, "Failed"

        key = f"video:{link}"
        return await deduplicate_download(key, _run)

    # === DOWNLOAD METHOD (With 3-Layer Fallback) ===
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
        
        is_vid = True if (video or songvideo) else False
        vid = extract_video_id(link)
        dedup_id = vid or link
        key = f"{'video' if is_vid else 'audio'}:{dedup_id}"
        
        LOGGER.info(f"📥 Download Req: {dedup_id} (Video={is_vid})")

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
                _inc("v2_success")
                return path
            
            # 3. Cookie Fallback
            LOGGER.info("🍪 Fallback: Downloading via yt-dlp cookies")
            cookie_file = cookie_txt_file()
            if not cookie_file:
                _inc("failed")
                return None
            
            loop = asyncio.get_running_loop()
            def _legacy_dl():
                opts = {
                    "format": "bestaudio/best" if not is_vid else "(bestvideo+bestaudio)",
                    "outtmpl": "downloads/%(id)s.%(ext)s", "quiet": True, 
                    "cookiefile": cookie_file, "no_warnings": True
                }
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(link, download=True)
                    return os.path.join("downloads", f"{info['id']}.{info['ext']}")

            try:
                path = await loop.run_in_executor(None, _legacy_dl)
                if path and os.path.exists(path):
                    _inc("success")
                    _inc("cookie_success")
                    return path
            except Exception as e:
                LOGGER.error(f"Fallback failed: {e}")

            _inc("failed")
            return None

        # Execute
        try:
            path = await asyncio.wait_for(
                deduplicate_download(key, _download_logic), 
                timeout=HARD_TIMEOUT
            )
            if path: return path, True
        except Exception as e:
            _inc("timeout_fail")
            LOGGER.error(f"❌ Timed Out: {e}")

        return None, None
