import os
import re
import aiohttp
import aiofiles
import textwrap
import traceback
import asyncio
from PIL import (
    Image,
    ImageDraw,
    ImageEnhance,
    ImageFilter,
    ImageFont,
    ImageOps,
    ImageChops
)
from youtubesearchpython.__future__ import VideosSearch
import config  # Ensure this imports your config file where YOUTUBE_IMG_URL is defined

# --- Constants & Helpers ---

# Handle Pillow version differences for Resampling
RESAMPLE_FILTER = getattr(Image, "Resampling", Image).LANCZOS

def make_col():
    """Generate a consistent pastel color for accents."""
    return (255, 255, 255)  # Clean White for modern look, or use random if preferred

def truncate(text, limit):
    """Smartly truncate text to a limit."""
    if len(text) > limit:
        return text[:limit] + "..."
    return text

def add_corners(im, rad):
    """Adds rounded corners to an image."""
    circle = Image.new('L', (rad * 2, rad * 2), 0)
    draw = ImageDraw.Draw(circle)
    draw.ellipse((0, 0, rad * 2, rad * 2), fill=255)
    alpha = Image.new('L', im.size, 255)
    w, h = im.size
    alpha.paste(circle.crop((0, 0, rad, rad)), (0, 0))
    alpha.paste(circle.crop((0, rad, rad, rad * 2)), (0, h - rad))
    alpha.paste(circle.crop((rad, 0, rad * 2, rad)), (w - rad, 0))
    alpha.paste(circle.crop((rad, rad, rad * 2, rad * 2)), (w - rad, h - rad))
    im.putalpha(alpha)
    return im

async def download_image(url, filename):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                f = await aiofiles.open(filename, mode="wb")
                await f.write(await resp.read())
                await f.close()
                return True
    return False

# --- Main Function ---

async def get_thumb(videoid: str):
    """
    Generates a custom thumbnail.
    Returns local path on success, or config.YOUTUBE_IMG_URL on failure.
    """
    if os.path.isfile(f"cache/{videoid}.png"):
        return f"cache/{videoid}.png"

    url = f"https://www.youtube.com/watch?v={videoid}"
    temp_thumb_path = f"cache/temp{videoid}.png"
    final_thumb_path = f"cache/{videoid}.png"

    try:
        # 1. Fetch Details
        results = VideosSearch(url, limit=1)
        res_json = await results.next()
        if not res_json["result"]:
            raise Exception("No video results found")
            
        result = res_json["result"][0]
        
        title = result.get("title", "Unknown Title")
        title = re.sub(r"\W+", " ", title).title()
        duration = result.get("duration", "Live")
        thumbnail_url = result["thumbnails"][0]["url"].split("?")[0]
        views = result.get("viewCount", {}).get("short", "Unknown Views")
        channel = result.get("channel", {}).get("name", "Unknown Channel")

        # 2. Download Thumbnail
        success = await download_image(thumbnail_url, temp_thumb_path)
        if not success:
            raise Exception("Failed to download thumbnail")

        # 3. Process Image
        # Load Assets (Wrap in try block to fallback if missing)
        youtube = Image.open(temp_thumb_path)
        
        # Create Canvas (1280x720)
        image1 = ImageOps.fit(youtube, (1280, 720), centering=(0.5, 0.5))
        image2 = image1.convert("RGBA")

        # -- Background Layer --
        # Blur and Darken
        background = image2.filter(ImageFilter.GaussianBlur(20))
        enhancer = ImageEnhance.Brightness(background)
        background = enhancer.enhance(0.6) # Darken background by 40%

        # Add a dark overlay for better text contrast
        overlay = Image.new("RGBA", background.size, (0, 0, 0, 120))
        background = Image.alpha_composite(background, overlay)

        # -- Main Art Layer (Left Side) --
        # Resize original thumb to a nice square
        art_size = (450, 450)
        art = ImageOps.fit(youtube, art_size, centering=(0.5, 0.5)).convert("RGBA")
        art = add_corners(art, 40) # Rounded corners

        # Add Drop Shadow to Art
        shadow_canvas = Image.new("RGBA", background.size, (0,0,0,0))
        shadow_draw = ImageDraw.Draw(shadow_canvas)
        shadow_rect = (140, 130, 140 + 450, 130 + 450) # Position slightly offset
        shadow_draw.rounded_rectangle(shadow_rect, radius=40, fill=(0,0,0,150))
        shadow_canvas = shadow_canvas.filter(ImageFilter.GaussianBlur(15))
        
        background = Image.alpha_composite(background, shadow_canvas)
        background.paste(art, (140, 130), art)

        # -- Text & Graphics Layer --
        draw = ImageDraw.Draw(background)

        # Load Fonts
        try:
            # Main Title Font
            font_title = ImageFont.truetype("DeadlineTech/assets/font3.ttf", 55)
            # Secondary Font
            font_regular = ImageFont.truetype("DeadlineTech/assets/font2.ttf", 35)
            # Small Font
            font_small = ImageFont.truetype("DeadlineTech/assets/font2.ttf", 28)
        except OSError:
            # Fallback to default if assets missing
            font_title = ImageFont.load_default()
            font_regular = ImageFont.load_default()
            font_small = ImageFont.load_default()

        # Draw Title (Wrapped)
        # Position: Right side of the art
        text_x = 640
        text_y = 160
        para = textwrap.wrap(title, width=20) # Wrap text at 20 chars
        
        current_y = text_y
        for line in para[:2]: # Limit to 2 lines max
            draw.text((text_x, current_y), line, fill=(255, 255, 255), font=font_title, stroke_width=1, stroke_fill="black")
            current_y += 70 # Line height

        # Draw Channel & Views
        draw.text((text_x, current_y + 10), f"By: {channel}", fill=(220, 220, 220), font=font_regular)
        draw.text((text_x, current_y + 55), f"Views: {views}", fill=(200, 200, 200), font=font_small)

        # -- Progress Bar --
        # Draw a clean playback line
        bar_start = (640, 480)
        bar_end = (1150, 480)
        
        # Background line
        draw.line([bar_start, bar_end], fill=(150, 150, 150, 100), width=6)
        
        # Active line (Random progress for visual effect, or fixed)
        import random
        progress = random.randint(30, 90) / 100
        active_end_x = bar_start[0] + (bar_end[0] - bar_start[0]) * progress
        draw.line([bar_start, (active_end_x, 480)], fill=(255, 255, 255), width=6)
        
        # Dot at end of active line
        draw.ellipse((active_end_x - 10, 480 - 10, active_end_x + 10, 480 + 10), fill=(255, 255, 255))

        # Time Stamps
        draw.text((640, 500), "00:00", fill="white", font=font_small)
        draw.text((1080, 500), duration, fill="white", font=font_small)

        # -- Icons (Optional) --
        try:
            icons = Image.open("DeadlineTech/assets/icons.png").convert("RGBA")
            icons = icons.resize((500, 50), RESAMPLE_FILTER) # Resize to fit
            background.paste(icons, (640, 550), icons)
        except Exception:
            pass # Skip icons if file missing

        # Save Result
        background.save(final_thumb_path)
        
        # Clean up temp file
        if os.path.exists(temp_thumb_path):
            os.remove(temp_thumb_path)

        return final_thumb_path

    except Exception as e:
        # Log error for debugging (optional)
        print(f"Thumbnail Generation Failed for {videoid}: {e}")
        traceback.print_exc()
        
        # Cleanup
        if os.path.exists(temp_thumb_path):
            try: os.remove(temp_thumb_path)
            except: pass
            
        # RETURN FALLBACK URL
        return config.YOUTUBE_IMG_URL
