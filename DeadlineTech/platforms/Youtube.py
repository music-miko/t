import asyncio
import os
import re
import json
import uuid
import random
import time
import aiohttp
import aiofiles
import yt_dlp
from pathlib import Path
from urllib.parse import urlparse, unquote
from typing import Union, Optional, Dict, Any, List
from motor.motor_asyncio import AsyncIOMotorClient

# Pyrogram
from pyrogram.types import Message
from pyrogram.enums import MessageEntityType
from pyrogram.errors import FloodWait

# Search & Utils
from youtubesearchpython.__future__ import VideosSearch
from DeadlineTech import app as TG_APP
from DeadlineTech.utils.database import is_on_off
from DeadlineTech.utils.formatters import time_to_seconds
from DeadlineTech.logging import LOGGER
import config

# === Configuration & Constants ===
YOUTUBE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{11}$")
CHUNK_SIZE = 1024 * 1024

# Security Constants
DANGEROUS_CHARS = [
    ";", "|", "$", "`", "\n", "\r", 
    "&", "(", ")", "<", ">", "{", "}", 
    "\\", "'", '"'
]
ALLOWED_DOMAINS = {
    "youtube.com", "www.youtube.com", "m.youtube.com", 
    "youtu.be", "music.youtube.com"
}

# Polling & Request Settings (Synced with downloader.py)
JOB_POLL_ATTEMPTS = 10     # How many times to check status
JOB_POLL_INTERVAL = 2.0    # Seconds between checks
JOB_POLL_BACKOFF = 1.2     # Increase interval slightly each time (1.2 from downloader.py)
HARD_TIMEOUT = 80          # 80 Seconds Max Total (from downloader.py)
TG_FLOOD_COOLDOWN = 0.0

V2_HTTP_RETRIES = 5
V2_DOWNLOAD_CYCLES = 5
NO_CANDIDATE_WAIT = 4
CDN_RETRIES = 5
CDN_RETRY_DELAY = 2

# === Database Config ===
MEDIA_CHANNEL_ID = config.MEDIA_CHANNEL_ID
DB_URI = config.DB_URI
MEDIA_DB_NAME = getattr(config, "MEDIA_DB_NAME", "arcapi")
MEDIA_COLLECTION_NAME = getattr(config, "MEDIA_COLLECTION_NAME", "medias")

# === Statistics System ===
DOWNLOAD_STATS: Dict[str, int] = {
    "total": 0, "success": 0, "failed": 0,
    "db_hit": 0, "v2_success": 0, "cookie_success": 0,
    "db_miss": 0, "db_fail": 0, "timeout_fail": 0,
    "no_candidate": 0, "cdn_fail": 0
}

def _inc(key: str):
    DOWNLOAD_STATS[key] = DOWNLOAD_STATS.get(key, 0) + 1

def get_stats() -> Dict[str, Any]:
    s = DOWNLOAD_STATS.copy()
    total = s["total"]
    s["success_rate"] = f"{(s['success'] / total) * 100:.2f}%" if total > 0 else "0.00%"
    return s

# === Database & Media Channel Helpers ===

_MONGO_CLIENT = None

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

def _resolve_if_dir(path_str: str) -> Optional[str]:
    if not path_str: return None
    p = Path(path_str)
    if p.exists() and p.is_file(): return str(p)
    if p.exists() and p.is_dir():
        files = [x for x in p.iterdir() if x.is_file()]
        return str(max(files, key=lambda x: x.stat().st_mtime)) if files else None
    return path_str

async def _download_from_media_db(track_id: str, is_video: bool) -> Optional[str]:
    global TG_FLOOD_COOLDOWN
    if not track_id or not TG_APP or not MEDIA_CHANNEL_ID: return None

    if time.time() < TG_FLOOD_COOLDOWN: 
        _inc("db_fail")
        return None

    msg_id = await get_media_id(track_id, isVideo=is_video)
    
    if not msg_id:
        _inc("db_miss")
        return None

    _inc("db_hit")
    LOGGER(__name__).info(f"⚡ Media DB Hit: {track_id} (MsgID: {msg_id})")
    
    ext = "mp4" if is_video else "mp3"
    out_dir = Path("downloads/video" if is_video else "downloads/audio")
    out_dir.mkdir(parents=True, exist_ok=True)
    
    final_path = out_dir / f"{track_id}.{ext}"
    tmp_path = str(final_path) + ".temp"

    if final_path.exists() and final_path.stat().st_size > 0:
        return str(final_path)

    try:
        msg = await TG_APP.get_messages(MEDIA_CHANNEL_ID, msg_id)
        if not msg or not msg.media: return None

        dl_res = await asyncio.wait_for(
            TG_APP.download_media(msg, file_name=tmp_path),
            timeout=HARD_TIMEOUT
        )
        
        fixed = _resolve_if_dir(dl_res)
        if fixed and os.path.exists(fixed):
            if fixed != str(final_path):
                os.replace(fixed, final_path)
            return str(final_path)

    except FloodWait as e:
        TG_FLOOD_COOLDOWN = time.time() + e.value + 10
        LOGGER(__name__).warning(f"⚠️ FloodWait: {e.value}s")
    except Exception as e:
        LOGGER(__name__).error(f"❌ DB Download Fail: {e}")
        _inc("db_fail")
    
    return None

# === Existing Helpers (UPDATED SECURITY) ===

def is_safe_url(text: str) -> bool:
    """
    Validates the input.
    - If it's a URL: Enforces strict domain and character checks.
    - If it's Text (Search Query): Returns True (Safe).
    """
    if not text: return False
    
    # 1. Check if it looks like a URL
    is_url = text.strip().lower().startswith(("http:", "https:", "www."))
    
    # 2. If NOT a URL, it's a search query -> Safe
    if not is_url:
        return True

    # 3. If it IS a URL -> Strict Validation
    try:
        # Normalize
        target_url = text.strip()
        if target_url.lower().startswith("www."):
            target_url = "https://" + target_url

        # Decode to find hidden malicious chars (e.g., %3B)
        decoded_url = unquote(target_url)

        # Check for Dangerous Characters
        if any(char in decoded_url for char in DANGEROUS_CHARS):
            LOGGER(__name__).warning(f"🚫 Blocked URL (Dangerous Chars): {text}")
            return False

        # Check Domain
        p = urlparse(target_url)
        if p.netloc not in ALLOWED_DOMAINS:
            LOGGER(__name__).warning(f"🚫 Blocked URL (Invalid Domain): {p.netloc}")
            return False
            
        return True
    except Exception as e:
        LOGGER(__name__).error(f"URL Parsing Error: {e}")
        return False

def extract_safe_id(link: str) -> Optional[str]:
    try:
        if "v=" in link: vid = link.split("v=")[-1].split("&")[0]
        elif "youtu.be" in link: vid = link.split("/")[-1].split("?")[0]
        else: return None
        if YOUTUBE_ID_RE.match(vid): return vid
    except: pass
    return None

def cookie_txt_file():
    cookie_dir = f"{os.getcwd()}/cookies"
    if not os.path.exists(cookie_dir): return None
    files = [f for f in os.listdir(cookie_dir) if f.endswith(".txt")]
    return os.path.join(cookie_dir, random.choice(files)) if files else None

# === V2 API Logic (Migrated from downloader.py) ===

def _looks_like_status_text(s: Optional[str]) -> bool:
    if not s: return False
    low = s.lower()
    return any(x in low for x in ("download started", "background", "jobstatus", "job_id", "processing", "queued"))

def _extract_candidate(obj: Any) -> Optional[str]:
    if obj is None: return None
    if isinstance(obj, str):
        s = obj.strip()
        return s if s else None
    if isinstance(obj, list) and obj:
        return _extract_candidate(obj[0])
    if isinstance(obj, dict):
        job = obj.get("job")
        if isinstance(job, dict):
            res = job.get("result")
            if isinstance(res, dict):
                for k in ("public_url", "cdnurl", "download_url", "url", "tg_link", "telegram_link", "message_link"):
                    v = res.get(k)
                    if isinstance(v, str) and v.strip(): return v.strip()
        for k in ("public_url", "cdnurl", "download_url", "url", "tg_link", "telegram_link", "message_link"):
            v = obj.get(k)
            if isinstance(v, str) and v.strip(): return v.strip()
        for wrap in ("result", "results", "data", "items", "payload", "message"):
            v = obj.get(wrap)
            if v:
                c = _extract_candidate(v)
                if c: return c
    return None

def _normalize_url(candidate: str) -> Optional[str]:
    api_url = getattr(config, "API_URL", None)
    if not api_url or not candidate: return None
    c = candidate.strip()
    if c.startswith(("http://", "https://")): return c
    if c.startswith("/"):
        if c.startswith("/root") or c.startswith("/home"): return None
        return f"{api_url.rstrip('/')}{c}"
    return f"{api_url.rstrip('/')}/{c.lstrip('/')}"

async def _download_cdn(url: str, out_path: str) -> bool:
    LOGGER(__name__).info(f"🔗 Downloading from CDN")
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    
    for attempt in range(1, CDN_RETRIES + 1):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=HARD_TIMEOUT)) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        if attempt < CDN_RETRIES:
                            await asyncio.sleep(CDN_RETRY_DELAY)
                            continue
                        return False

                    async with aiofiles.open(out_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                            if not chunk: break
                            await f.write(chunk)
            
            if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                return True

        except asyncio.TimeoutError:
            _inc("timeout_fail")
            if attempt < CDN_RETRIES: await asyncio.sleep(CDN_RETRY_DELAY)
        except Exception as e:
            LOGGER(__name__).error(f"CDN Fail: {e}")
            if attempt < CDN_RETRIES: await asyncio.sleep(CDN_RETRY_DELAY)
    
    return False

async def v2_download_process(link: str, video: bool) -> Optional[str]:
    vid = extract_safe_id(link) or uuid.uuid4().hex[:10]
    ext = "mp4" if video else "m4a"
    out_path = Path("downloads/video" if video else "downloads/audio") / f"{vid}.{ext}"

    if out_path.exists() and out_path.stat().st_size > 0:
        return str(out_path)

    api_key = getattr(config, "API_KEY", None)
    api_url = getattr(config, "API_URL", None)
    if not api_url or not api_key:
        LOGGER(__name__).error("API Creds Missing")
        return None

    # V2 Retry Cycles (from downloader.py)
    for cycle in range(1, V2_DOWNLOAD_CYCLES + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # 1. Start Job
                url = f"{api_url.rstrip('/')}/youtube/v2/download"
                params = {"query": vid, "isVideo": str(video).lower(), "api_key": api_key}
                
                LOGGER(__name__).info(f"📡 V2 Job Start (Cycle {cycle}): {vid}...")
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        LOGGER(__name__).error(f"V2 Request Failed")
                        if cycle < V2_DOWNLOAD_CYCLES:
                            await asyncio.sleep(1)
                            continue
                        return None
                    data = await resp.json()

                # 2. Extract Job ID or Immediate Candidate
                candidate = _extract_candidate(data)
                if candidate and _looks_like_status_text(candidate):
                    candidate = None # It's just a status message, not a URL

                job_id = data.get("job_id")
                if isinstance(data.get("job"), dict):
                     job_id = data.get("job").get("id")

                # 3. Poll if we have a Job ID but no Candidate yet
                if job_id and not candidate:
                    LOGGER(__name__).info(f"⏳ Polling Job: {job_id}")
                    interval = JOB_POLL_INTERVAL
                    
                    for _ in range(JOB_POLL_ATTEMPTS):
                        await asyncio.sleep(interval)
                        status_url = f"{api_url.rstrip('/')}/youtube/jobStatus"
                        try:
                            async with session.get(status_url, params={"job_id": job_id}) as s_resp:
                                if s_resp.status == 200:
                                    s_data = await s_resp.json()
                                    candidate = _extract_candidate(s_data)
                                    if candidate and not _looks_like_status_text(candidate):
                                        break
                                    
                                    # Check explicit status
                                    job_data = s_data.get("job", {}) if isinstance(s_data, dict) else {}
                                    if job_data.get("status") == "error":
                                        LOGGER(__name__).error(f"failed Api Download")
                                        break
                        except Exception as e:
                            LOGGER(__name__).warning(f"Polling warning: {e}")
                        
                        interval *= JOB_POLL_BACKOFF
                
                # 4. Process Candidate
                if not candidate:
                    _inc("no_candidate")
                    if cycle < V2_DOWNLOAD_CYCLES:
                        await asyncio.sleep(NO_CANDIDATE_WAIT)
                        continue
                    return None

                final_url = _normalize_url(candidate)
                if not final_url:
                     if cycle < V2_DOWNLOAD_CYCLES:
                        await asyncio.sleep(NO_CANDIDATE_WAIT)
                        continue
                     return None

                # 5. Download
                if await _download_cdn(final_url, str(out_path)):
                    return str(out_path)
                else:
                    _inc("cdn_fail")
        
        except Exception as e:
            LOGGER(__name__).error(f"V2 Cycle Error: {e}")
            if cycle < V2_DOWNLOAD_CYCLES:
                await asyncio.sleep(1)
    
    return None

# === MAIN CLASS ===

class YouTubeAPI:
    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="
        self.regex = r"(?:youtube\.com|youtu\.be)"
        self.listbase = "https://youtube.com/playlist?list="

    async def exists(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        # 'is_safe_url' now allows text but enforces strict URL checks.
        return bool(re.search(self.regex, link) and is_safe_url(link))

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

    async def details(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        if "&" in link: link = link.split("&")[0]
        
        # Security Check
        if not is_safe_url(link): return "Unsafe URL", "0", 0, "", ""
        
        results = VideosSearch(link, limit=1)
        for r in (await results.next())["result"]:
            sec = int(time_to_seconds(r["duration"])) if r["duration"] else 0
            return r["title"], r["duration"], sec, r["thumbnails"][0]["url"].split("?")[0], r["id"]
        return None

    async def title(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        results = VideosSearch(link, limit=1)
        for r in (await results.next())["result"]: return r["title"]
        return ""

    async def duration(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        results = VideosSearch(link, limit=1)
        for r in (await results.next())["result"]: return r["duration"]
        return "00:00"

    async def thumbnail(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        results = VideosSearch(link, limit=1)
        for r in (await results.next())["result"]: return r["thumbnails"][0]["url"].split("?")[0]
        return ""

    async def video(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        
        # Security Check
        if not is_safe_url(link): return 0, "Unsafe URL"
        
        # 1. DB CHECK
        vid = extract_safe_id(link)
        if vid:
            db_path = await _download_from_media_db(vid, is_video=True)
            if db_path: return 1, db_path

        # 2. V2 API (Polling)
        path = await v2_download_process(link, video=True)
        if path: return 1, path
        
        # 3. Cookies
        cookie_file = cookie_txt_file()
        if not cookie_file: return 0, "No cookies"
        cmd = ["yt-dlp", "--cookies", cookie_file, "-g", "-f", "best[height<=?720]", "--", link]
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if stdout: return 1, stdout.decode().split("\n")[0]
        return 0, stderr.decode()

    async def playlist(self, link, limit, user_id, videoid: Union[bool, str] = None):
        if videoid: link = self.listbase + link
        if not is_safe_url(link): return []
        cookie_file = cookie_txt_file()
        if not cookie_file: return []
        cmd = [
            "yt-dlp", "-i", "--get-id", "--flat-playlist", "--cookies", cookie_file,
            "--playlist-end", str(limit), "--skip-download", "--", link
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await proc.communicate()
            if stdout: return [x for x in stdout.decode().split("\n") if x]
        except: pass
        return []

    async def track(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        results = VideosSearch(link, limit=1)
        for r in (await results.next())["result"]:
            return {
                "title": r["title"], "link": r["link"], "vidid": r["id"],
                "duration_min": r["duration"], "thumb": r["thumbnails"][0]["url"].split("?")[0],
            }, r["id"]
        return None, None

    async def formats(self, link: str, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        if not is_safe_url(link): return [], link
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

    async def slider(self, link: str, query_type: int, videoid: Union[bool, str] = None):
        if videoid: link = self.base + link
        a = VideosSearch(link, limit=10)
        result = (await a.next()).get("result")
        if not result or query_type >= len(result): return None
        r = result[query_type]
        return r["title"], r["duration"], r["thumbnails"][0]["url"].split("?")[0], r["id"]

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
        
        # Security: Allow Text Queries, Block Bad URLs
        if not is_safe_url(link):
            _inc("failed")
            return None, None

        is_vid = True if (video or songvideo) else False
        vid = extract_safe_id(link) or videoid

        # 1. PRIORITY: MEDIA DB
        if vid:
            db_path = await _download_from_media_db(vid, is_video=is_vid)
            if db_path:
                _inc("success")
                return db_path, True
        
        # 2. V2 API (Polling)
        path = await v2_download_process(link, video=is_vid)
        if path:
            _inc("success")
            _inc("v2_success")
            return path, True
            
        # 3. COOKIE FALLBACK
        _inc("v2_error")
        cookie_file = cookie_txt_file()
        if not cookie_file:
            _inc("failed")
            return None, None
        
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
                return path, True
        except Exception:
            _inc("cookie_error")

        _inc("failed")
        return None, None
