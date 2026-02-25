# Authored By Certified Coders © 2025
import asyncio
from typing import Optional
from datetime import datetime

from pyrogram import filters, errors, types
from pyrogram.types import Message

from config import LOGGER_ID
from DeadlineTech import app
from DeadlineTech.misc import SUDOERS
# Import the new DB functions
from DeadlineTech.utils.database import update_bot_stats, get_bot_stats, get_served_chats

# --- CONFIGURATION ---
BOT_INFO: Optional[types.User] = None
BOT_ID: Optional[int] = None
LEFT_ID = -1003499984720

async def _ensure_bot_info() -> None:
    global BOT_INFO, BOT_ID
    if BOT_INFO is None:
        try:
            BOT_INFO = await app.get_me()
            BOT_ID = BOT_INFO.id
        except Exception as e:
            print(f"Failed to get bot info: {e}")

async def safe_send_message(chat_id, text, reply_markup=None, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            return await app.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_markup
            )
        except errors.FloodWait as e:
            await asyncio.sleep(e.value + 1)
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"Failed to send message after {max_retries} attempts: {e}")
                raise
            await asyncio.sleep(1)

# --- STATISTICS COMMAND ---
@app.on_message(filters.command("data") & SUDOERS)
async def chat_stats_command(_, message: Message):
    """
    Displays the chat growth statistics (Daily, Weekly, Monthly, Yearly).
    """
    msg = await message.reply_text("<emoji id='5456327427795982532'>🪶</emoji> **Fetching Database Statistics...**")
    
    # Fetch Data
    stats = await get_bot_stats()
    total_chats = len(await get_served_chats())
    
    # Helper to format sections safely
    def fmt(data):
        j = data.get("joined", 0)
        l = data.get("left", 0)
        net = j - l
        sign = "+" if net >= 0 else ""
        return j, l, f"{sign}{net}"

    d_j, d_l, d_n = fmt(stats["daily"])
    w_j, w_l, w_n = fmt(stats["weekly"])
    m_j, m_l, m_n = fmt(stats["monthly"])
    y_j, y_l, y_n = fmt(stats["yearly"])
    
    # Check if last year data exists
    last_year_txt = ""
    if stats["last_yearly"].get("joined", 0) > 0 or stats["last_yearly"].get("left", 0) > 0:
        ly_j, ly_l, ly_n = fmt(stats["last_yearly"])
        last_year_txt = (
            f"\n**<emoji id='5364233403300330811'>📅</emoji> Previous Year ({stats['last_year']})**\n"
            f"┣ <emoji id='5208880351690112495'>✅</emoji> Joined: `{ly_j}`\n"
            f"┣ <emoji id='5210952531676504517'>❌</emoji> Left: `{ly_l}`\n"
            f"┗ 📈 Net Growth: `{ly_n}`\n"
        )

    text = (
        f"<emoji id='5231200819986047254'>📊</emoji> **Bot Growth Statistics**\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"**<emoji id='5463172695132745432'>📦</emoji> Total Active Chats:** `{total_chats}`\n\n"

        f"**<emoji id='5364233403300330811'>📅</emoji> Today's Activity**\n"
        f"┣ <emoji id='5208880351690112495'>✅</emoji> Joined: `{d_j}`\n"
        f"┣ <emoji id='5210952531676504517'>❌</emoji> Left: `{d_l}`\n"
        f"┗ 📈 Net Growth: `{d_n}`\n\n"
        
        f"**<emoji id='5364233403300330811'>📅</emoji> Weekly Insights**\n"
        f"┣ <emoji id='5208880351690112495'>✅</emoji> Joined: `{w_j}`\n"
        f"┣ <emoji id='5210952531676504517'>❌</emoji> Left: `{w_l}`\n"
        f"┗ 📈 Net Growth: `{w_n}`\n\n"
        
        f"**<emoji id='5364233403300330811'>📅</emoji> Monthly Overview**\n"
        f"┣ <emoji id='5208880351690112495'>✅</emoji> Joined: `{m_j}`\n"
        f"┣ <emoji id='5210952531676504517'>❌</emoji> Left: `{m_l}`\n"
        f"┗ 📈 Net Growth: `{m_n}`\n\n"

        f"**<emoji id='5364233403300330811'>📅</emoji> Yearly Summary ({stats['current_year']})**\n"
        f"┣ <emoji id='5208880351690112495'>✅</emoji> Joined: `{y_j}`\n"
        f"┣ <emoji id='5210952531676504517'>❌</emoji> Left: `{y_l}`\n"
        f"┗ 📈 Net Growth: `{y_n}`"
        f"{last_year_txt}"
    )

    await msg.edit_text(text)

# --- JOIN HANDLER ---
@app.on_message(filters.new_chat_members)
async def join_watcher(_, message: Message):
    try:
        await _ensure_bot_info()
        if BOT_INFO is None or BOT_ID is None:
            return

        chat = message.chat
        
        for member in message.new_chat_members:
            if member.id == BOT_ID:
                # 1. Update Database Stats
                await update_bot_stats("joined")

                # 2. Get Member Count safely
                count_str = "?"
                try:
                    count_str = await app.get_chat_members_count(chat.id)
                except Exception:
                    pass

                adder = message.from_user.mention if message.from_user else "Unknown"
                
                # 3. Formatted Date
                date_str = datetime.now().strftime("%d %b %Y | %I:%M %p")

                # 4. Log Message
                text = (
                    "<emoji id='5456327427795982532'>🪶</emoji> **New Group Entry**\n\n"
                    f"<emoji id='5208880351690112495'>✅</emoji> **Bot Added Successfully**\n\n"
                    f"📌 **Chat:** `{chat.title}`\n"
                    f"🆔 **ID:** `{chat.id}`\n"
                    f"🔐 **Username:** @{chat.username if chat.username else 'Private'}\n"
                    f"📈 **Members:** `{count_str}`\n"
                    f"👤 **Added By:** {adder}\n"
                    f"🕰 **Date:** `{date_str}`"
                )
                await safe_send_message(LOGGER_ID, text)

    except Exception as e:
        print(f"Error in join_watcher: {e}")

# --- LEAVE HANDLER ---
@app.on_message(filters.left_chat_member)
async def on_left_chat_member(_, message: Message):
    try:
        await _ensure_bot_info()
        if BOT_INFO is None or BOT_ID is None:
            return

        if message.left_chat_member and message.left_chat_member.id == BOT_ID:
            # 1. Update Database Stats
            await update_bot_stats("left")

            chat = message.chat
            actor = message.from_user
            
            # Determine reason
            if actor and actor.id == BOT_ID:
                reason = "🤖 **Reason:** Auto-left or self-invoked."
                remover_line = f"👤 **Actor:** @{BOT_INFO.username}"
            else:
                reason = "🚫 **Reason:** Kicked/Removed manually."
                remover = actor.mention if actor else "**Unknown User**"
                remover_line = f"👤 **Removed By:** {remover}"

            # 2. Formatted Date
            date_str = datetime.now().strftime("%d %b %Y | %I:%M %p")

            # 3. Log Message
            text = (
                "<emoji id='5210952531676504517'>❌</emoji> **Bot Left Group**\n\n"
                f"📌 **Chat:** `{chat.title}`\n"
                f"🆔 **ID:** `{chat.id}`\n"
                f"{remover_line}\n"
                f"{reason}\n"
                f"🕰 **Date:** `{date_str}`"
            )
            await safe_send_message(LEFT_ID, text)

    except Exception as e:
        print(f"Error in on_left_chat_member: {e}")
