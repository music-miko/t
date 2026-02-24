import asyncio
import os
import re
import json
import uuid
import random
import logging
import aiohttp
import aiofiles
import config
import time
import requests
import yt_dlp
from pathlib import Path
from urllib.parse import urlparse
from typing import Union, Optional, Dict, Any, List
from pyrogram.types import Message
from pyrogram.enums import MessageEntityType
from youtubesearchpython.__future__ import VideosSearch

from DeadlineTech.utils.database import is_on_off
from DeadlineTech.utils.formatters import time_to_seconds

# === Constants & Configuration ===
YOUTUBE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{11}$")
V2_HTTP_RETRIES = 3
V2_DOWNLOAD_CYCLES = 5
HARD_RETRY_WAIT = 3
JOB_POLL_ATTEMPTS = 15
JOB_POLL_INTERVAL = 2.0
JOB_POLL_BACKOFF = 1.1
NO_CANDIDATE_WAIT = 4
CDN_RETRIES = 3
CDN_RETRY_DELAY = 2
CHUNK_SIZE = 1024 * 1024  # 1MB

# === Security Helpers ===

def is_safe_url(url: str) -> bool:
    """Strictly validates that the URL is a safe YouTube link."""
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return False
        
        allowed_domains = (
            "youtube.com", "www.youtube.com", "m.youtube.com", 
            "youtu.be", "music.youtube.com"
        )
        if not any(domain in p.netloc for domain in allowed_domains):
            return False

        # Block common shell injection characters
        if any(char in url for char in [";", "|", "$", "`", "\n", "\r", "<", ">"]):
            return False

        return True
    except Exception:
        return False

def extract_safe_id(link: str) -> Optional[str]:
    """Extracts and validates ID to prevent Path Traversal."""
    try:
        if "v=" in link:
            vid = link.split("v=")[-1].split("&")[0]
        elif "youtu.be" in link:
            vid = link.split("/")[-1].split("?")[0]
        else:
            return None
            
        if YOUTUBE_ID_RE.match(vid):
            return vid
    except Exception:
        pass
    return None

def cookie_txt_file():
    cookie_dir = f"{os.getcwd()}/cookies"
    if not os.path.exists(cookie_dir):
        return None
    cookies_files = [f for f in os.listdir(cookie_dir) if f.endswith(".txt")]
    if not cookies_files:
        return None
    cookie_file = os.path.join(cookie_dir, random.choice(cookies_files))
    return cookie_file

# === V2 API Implementation ===

class V2HardAPIError(Exception):
    def __init__(self, status: int, body_preview: str = ""):
        super().__init__(f"Hard API error status={status}")
        self.status = status
        self.body_preview = body_preview[:200]

def _extract_candidate(obj: Any) -> Optional[str]:
    """Extracts the download URL from various JSON structures."""
    if obj is None:
        return None
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
                    if isinstance(v, str) and v.strip():
                        return v.strip()
        for k in ("public_url", "cdnurl", "download_url", "url", "tg_link", "telegram_link", "message_link"):
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        for wrap in ("result", "results", "data", "items", "payload", "message"):
            v = obj.get(wrap)
            if v:
                c = _extract_candidate(v)
                if c:
                    return c
    return None

def _looks_like_status_text(s: Optional[str]) -> bool:
    if not s:
        return False
    low = s.lower()
    return any(x in low for x in ("download started", "background", "jobstatus", "job_id", "processing", "queued"))

def _normalize_candidate_to_url(candidate: str) -> Optional[str]:
    api_url = getattr(config, "API_URL", None)
    if not api_url or not candidate:
        return None
    c = candidate.strip()
    if c.startswith(("http://", "https://")):
        return c
    if c.startswith("/"):
        if c.startswith("/root") or c.startswith("/home"):
            return None
        return f"{api_url.rstrip('/')}{c}"
    return f"{api_url.rstrip('/')}/{c.lstrip('/')}"

async def _v2_request_json(endpoint: str, params: Dict[str, Any]) -> Optional[Any]:
    api_key = getattr(config, "API_KEY", None)
    api_url = getattr(config, "API_URL", None)
    
    if not api_url or not api_key:
        print("❌ API configuration missing.")
        return None

    base = api_url.rstrip("/")
    url = f"{base}/{endpoint.lstrip('/')}"
    if "api_key" not in params:
        params["api_key"] = api_key

    timeout = aiohttp.ClientTimeout(total=20)
    
    for attempt in range(1, V2_HTTP_RETRIES + 1):
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, params=params, headers={"X-API-Key": api_key, "Accept": "application/json"}) as resp:
                    text = await resp.text()
                    try:
                        data = await resp.json(content_type=None)
                    except Exception:
                        data = None

                    if 200 <= resp.status < 300:
                        return data

                    if resp.status in (401, 403):
                        raise V2HardAPIError(resp.status, text)

                    if attempt < V2_HTTP_RETRIES:
                        await asyncio.sleep(1)
                        continue
                        
        except V2HardAPIError:
            raise
        except Exception as e:
            print(f"⚠️ V2 Request Error: {e}")
            if attempt < V2_HTTP_RETRIES:
                await asyncio.sleep(1)

    return None

async def _download_from_cdn(cdn_url: str, out_path: str) -> Optional[str]:
    if not cdn_url:
        return None
    
    print(f"🔗 Downloading from CDN: {cdn_url}")
    
    folder = Path(out_path).parent
    folder.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, CDN_RETRIES + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=300) # 5 min limit for large files
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(cdn_url) as resp:
                    if resp.status != 200:
                        if attempt < CDN_RETRIES:
                            await asyncio.sleep(CDN_RETRY_DELAY)
                            continue
                        return None

                    async with aiofiles.open(out_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                            if not chunk:
                                break
                            await f.write(chunk)

            if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                print(f"✅ Download completed: {out_path}")
                return out_path
            
        except Exception as e:
            print(f"⚠️ CDN Download Error (Attempt {attempt}): {e}")
            if attempt < CDN_RETRIES:
                await asyncio.sleep(CDN_RETRY_DELAY)
    
    return None

async def v2_download_logic(link: str, video: bool) -> Optional[str]:
    """
    Orchestrates the V2 download process:
    1. Request download (gets job_id)
    2. Poll job status
    3. Get CDN URL
    4. Download file
    """
    # Security: Validate ID again before sending to API
    vid = extract_safe_id(link)
    query = vid or link # Fallback to link if extraction fails (API handles search)

    print(f"🔄 Starting V2 Download for: {query} (Video={video})")

    for cycle in range(1, V2_DOWNLOAD_CYCLES + 1):
        try:
            # 1. Initiate Download Job
            resp = await _v2_request_json(
                "youtube/v2/download",
                {"query": query, "isVideo": str(video).lower()},
            )
        except V2HardAPIError:
            print("❌ Hard API Error (401/403)")
            return None

        if not resp:
            if cycle < V2_DOWNLOAD_CYCLES:
                await asyncio.sleep(1)
                continue
            return None

        # 2. Check for Immediate Result or Job ID
        candidate = _extract_candidate(resp)
        if candidate and _looks_like_status_text(candidate):
            candidate = None

        job_id = None
        if isinstance(resp, dict):
            job_id = resp.get("job_id") or resp.get("job")
            if isinstance(job_id, dict) and "id" in job_id:
                job_id = job_id.get("id")

        # 3. Poll for Job Completion
        if job_id and not candidate:
            print(f"⏳ Job Queued (ID: {job_id}). Polling...")
            interval = JOB_POLL_INTERVAL
            for _ in range(1, JOB_POLL_ATTEMPTS + 1):
                await asyncio.sleep(interval)
                try:
                    status = await _v2_request_json("youtube/jobStatus", {"job_id": str(job_id)})
                except V2HardAPIError:
                    candidate = None
                    break

                candidate = _extract_candidate(status) if status else None
                if candidate and _looks_like_status_text(candidate):
                    candidate = None
                if candidate:
                    print("✅ Job Finished.")
                    break
                interval *= JOB_POLL_BACKOFF

        if not candidate:
            print("❌ No candidate URL found after polling.")
            if cycle < V2_DOWNLOAD_CYCLES:
                await asyncio.sleep(NO_CANDIDATE_WAIT)
                continue
            return None

        # 4. Normalize URL
        normalized = _normalize_candidate_to_url(candidate)
        if not normalized:
            if cycle < V2_DOWNLOAD_CYCLES:
                await asyncio.sleep(NO_CANDIDATE_WAIT)
                continue
            return None

        # 5. Download File
        ext = "mp4" if video else "m4a"
        folder = Path("downloads/video" if video else "downloads/audio")
        
        # Use safe ID for filename if available, else UUID
        safe_name = vid if vid else uuid.uuid4().hex[:10]
        out_path = folder / f"{safe_name}.{ext}"

        if out_path.exists() and out_path.stat().st_size > 0:
            return str(out_path)

        path = await _download_from_cdn(normalized, str(out_path))
        if not path:
            if cycle < V2_DOWNLOAD_CYCLES:
                await asyncio.sleep(2)
                continue
        return path

    return None

async def check_file_size(link):
    if not is_safe_url(link):
        print("❌ Unsafe URL rejected in check_file_size")
        return None

    async def get_format_info(link):
        cookie_file = cookie_txt_file()
        if not cookie_file:
            print("No cookies found. Cannot check file size.")
            return None
            
        # SECURE: Using create_subprocess_exec with "--" separator
        cmd = [
            "yt-dlp",
            "--cookies", cookie_file,
            "-J",
            "--", # Prevents argument injection
            link
        ]
        
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            print(f'Error:\n{stderr.decode()}')
            return None
        return json.loads(stdout.decode())

    def parse_size(formats):
        total_size = 0
        for format in formats:
            if 'filesize' in format:
                total_size += format['filesize']
        return total_size

    info = await get_format_info(link)
    if info is None:
        return None
    
    formats = info.get('formats', [])
    if not formats:
        print("No formats found.")
        return None
    
    total_size = parse_size(formats)
    return total_size


class YouTubeAPI:
    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="
        self.regex = r"(?:youtube\.com|youtu\.be)"
        self.status = "https://www.youtube.com/oembed?url="
        self.listbase = "https://youtube.com/playlist?list="
        self.reg = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

    async def exists(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if re.search(self.regex, link) and is_safe_url(link):
            return True
        else:
            return False

    async def url(self, message_1: Message) -> Union[str, None]:
        messages = [message_1]
        if message_1.reply_to_message:
            messages.append(message_1.reply_to_message)
        text = ""
        offset = None
        length = None
        for message in messages:
            if offset:
                break
            if message.entities:
                for entity in message.entities:
                    if entity.type == MessageEntityType.URL:
                        text = message.text or message.caption
                        offset, length = entity.offset, entity.length
                        break
            elif message.caption_entities:
                for entity in message.caption_entities:
                    if entity.type == MessageEntityType.TEXT_LINK:
                        return entity.url
        if offset in (None,):
            return None
        return text[offset : offset + length]

    async def details(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        
        if not is_safe_url(link):
            return "Unsafe URL", "0", 0, "", ""

        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            title = result["title"]
            duration_min = result["duration"]
            thumbnail = result["thumbnails"][0]["url"].split("?")[0]
            vidid = result["id"]
            if str(duration_min) == "None":
                duration_sec = 0
            else:
                duration_sec = int(time_to_seconds(duration_min))
        return title, duration_min, duration_sec, thumbnail, vidid

    async def title(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            title = result["title"]
        return title

    async def duration(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            duration = result["duration"]
        return duration

    async def thumbnail(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            thumbnail = result["thumbnails"][0]["url"].split("?")[0]
        return thumbnail

    async def video(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        
        if not is_safe_url(link):
             return 0, "Security Check Failed: Unsafe URL"

        # Try V2 API First
        downloaded_file = await v2_download_logic(link, video=True)
        if downloaded_file:
            return 1, downloaded_file
        
        print("⚠️ V2 API failed, falling back to local cookies...")
        
        # Fallback to cookies
        cookie_file = cookie_txt_file()
        if not cookie_file:
            return 0, "No cookies found. Cannot download video."
            
        # SECURE: Using create_subprocess_exec with "--"
        cmd = [
            "yt-dlp",
            "--cookies", cookie_file,
            "-g",
            "-f", "best[height<=?720][width<=?1280]",
            "--", # Anti-injection
            f"{link}"
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if stdout:
            return 1, stdout.decode().split("\n")[0]
        else:
            return 0, stderr.decode()

    async def playlist(self, link, limit, user_id, videoid: Union[bool, str] = None):
        if videoid:
            link = self.listbase + link
        if "&" in link:
            link = link.split("&")[0]
        
        # Security validation
        if not is_safe_url(link):
            return []

        cookie_file = cookie_txt_file()
        if not cookie_file:
            return []
            
        # SECURE REPLACEMENT FOR shell_cmd
        # We use create_subprocess_exec to prevent shell injection
        cmd = [
            "yt-dlp",
            "-i",
            "--get-id",
            "--flat-playlist",
            "--cookies", cookie_file,
            "--playlist-end", str(limit),
            "--skip-download",
            "--", # Anti-injection
            link
        ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            
            if stdout:
                result = stdout.decode("utf-8").split("\n")
                result = [key for key in result if key != ""]
                return result
            else:
                return []
        except Exception as e:
            print(f"Playlist fetch error: {e}")
            return []

    async def track(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            title = result["title"]
            duration_min = result["duration"]
            vidid = result["id"]
            yturl = result["link"]
            thumbnail = result["thumbnails"][0]["url"].split("?")[0]
        track_details = {
            "title": title,
            "link": yturl,
            "vidid": vidid,
            "duration_min": duration_min,
            "thumb": thumbnail,
        }
        return track_details, vidid

    async def formats(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        
        if not is_safe_url(link):
            return [], link

        cookie_file = cookie_txt_file()
        if not cookie_file:
            return [], link
            
        ytdl_opts = {"quiet": True, "cookiefile" : cookie_file}
        ydl = yt_dlp.YoutubeDL(ytdl_opts)
        with ydl:
            formats_available = []
            try:
                r = ydl.extract_info(link, download=False)
                for format in r["formats"]:
                    try:
                        str(format["format"])
                    except:
                        continue
                    if not "dash" in str(format["format"]).lower():
                        try:
                            format["format"]
                            format["filesize"]
                            format["format_id"]
                            format["ext"]
                            format["format_note"]
                        except:
                            continue
                        formats_available.append(
                            {
                                "format": format["format"],
                                "filesize": format["filesize"],
                                "format_id": format["format_id"],
                                "ext": format["ext"],
                                "format_note": format["format_note"],
                                "yturl": link,
                            }
                        )
            except Exception as e:
                print(f"Formats error: {e}")
                
        return formats_available, link

    async def slider(
        self,
        link: str,
        query_type: int,
        videoid: Union[bool, str] = None,
    ):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        a = VideosSearch(link, limit=10)
        result = (await a.next()).get("result")
        title = result[query_type]["title"]
        duration_min = result[query_type]["duration"]
        vidid = result[query_type]["id"]
        thumbnail = result[query_type]["thumbnails"][0]["url"].split("?")[0]
        return title, duration_min, thumbnail, vidid

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
        if videoid:
            link = self.base + link
        
        # Global Security Check
        if not is_safe_url(link):
            print("❌ Security Block: Unsafe URL in download()")
            return None, None

        loop = asyncio.get_running_loop()

        # --- Legacy Fallback Functions (Using Cookies) ---
        def audio_dl():
            cookie_file = cookie_txt_file()
            if not cookie_file:
                raise Exception("No cookies found. Cannot download audio.")
            ydl_optssx = {
                "format": "bestaudio/best",
                "outtmpl": "downloads/%(id)s.%(ext)s",
                "geo_bypass": True, "nocheckcertificate": True, "quiet": True,
                "cookiefile" : cookie_file, "no_warnings": True,
            }
            x = yt_dlp.YoutubeDL(ydl_optssx)
            info = x.extract_info(link, False)
            xyz = os.path.join("downloads", f"{info['id']}.{info['ext']}")
            if os.path.exists(xyz): return xyz
            x.download([link])
            return xyz

        def video_dl():
            cookie_file = cookie_txt_file()
            if not cookie_file:
                raise Exception("No cookies found. Cannot download video.")
            ydl_optssx = {
                "format": "(bestvideo[height<=?720][width<=?1280][ext=mp4])+(bestaudio[ext=m4a])",
                "outtmpl": "downloads/%(id)s.%(ext)s",
                "geo_bypass": True, "nocheckcertificate": True, "quiet": True,
                "cookiefile" : cookie_file, "no_warnings": True,
            }
            x = yt_dlp.YoutubeDL(ydl_optssx)
            info = x.extract_info(link, False)
            xyz = os.path.join("downloads", f"{info['id']}.{info['ext']}")
            if os.path.exists(xyz): return xyz
            x.download([link])
            return xyz

        # --- Main Logic ---

        if songvideo:
            # V2 API Attempt
            path = await v2_download_logic(link, video=True)
            if path: return path, True
            return None, None

        elif songaudio:
            # V2 API Attempt
            path = await v2_download_logic(link, video=False)
            if path: return path, True
            return None, None
            
        elif video:
            # V2 API Attempt
            path = await v2_download_logic(link, video=True)
            if path: return path, True
            
            print("⚠️ V2 failed for video, falling back to local cookies...")
            
            # Fallback
            cookie_file = cookie_txt_file()
            if not cookie_file:
                print("No cookies found.")
                return None, None
                
            if await is_on_off(1):
                path = await v2_download_logic(link, video=True)
                return path, True
            else:
                # Direct Stream URL extraction via yt-dlp subprocess
                cmd = [
                    "yt-dlp", "--cookies", cookie_file, "-g",
                    "-f", "best[height<=?720][width<=?1280]",
                    "--", link
                ]
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await proc.communicate()
                if stdout:
                    return stdout.decode().split("\n")[0], False
                else:
                   file_size = await check_file_size(link)
                   if not file_size or (file_size / (1024*1024) > 250):
                     return None, None
                   path = await loop.run_in_executor(None, video_dl)
                   return path, True

        else: # Default Audio
            # V2 API Attempt
            path = await v2_download_logic(link, video=False)
            if path: return path, True
            
            print("⚠️ V2 failed for audio, falling back to local cookies...")
            
            # Fallback
            cookie_file = cookie_txt_file()
            if not cookie_file: return None, None
            path = await loop.run_in_executor(None, audio_dl)
            return path, True
            
        return None, None
