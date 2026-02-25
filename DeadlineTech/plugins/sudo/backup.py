# Authored By Certified Coders © 2025
import os
import json
import shutil
import zipfile
import asyncio
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from pyrogram import Client, filters
from pyrogram.types import Message
from DeadlineTech import app
from config import MONGO_DB_URI, OWNER_ID, DB_NAME
from DeadlineTech.logging import LOGGER
from DeadlineTech.core.dir import CACHE_DIR

TEMP_DIR = os.path.join(BACKUP_DIR, "tmp")
LOGGER_ID = -1003302898507

# --- Helper to convert strings back to datetime during restore ---
def _json_decoder_hook(dct):
    for key, value in dct.items():
        if isinstance(value, str):
            # Attempt to parse ISO format datetimes
            try:
                # Simple check for ISO format (YYYY-MM-DD...)
                if len(value) >= 19 and value[4] == "-" and value[10] == "T":
                    dct[key] = datetime.fromisoformat(value)
            except ValueError:
                pass
    return dct

async def _dump_collection(collection, path: str):
    data = []
    async for doc in collection.find({}):
        doc.pop("_id", None)
        # Serialize datetime objects to string
        for key, value in doc.items():
            if isinstance(value, datetime):
                doc[key] = value.isoformat()
        data.append(doc)
    
    if data:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

async def _create_backup_zip() -> str:
    LOGGER(__name__).info("🗂️ Starting backup process for all collections…")

    client = AsyncIOMotorClient(MONGO_DB_URI)
    db = client[DB_NAME]
    collections = await db.list_collection_names()

    # Cleanup old backup files
    for fname in os.listdir(BACKUP_DIR):
        fpath = os.path.join(BACKUP_DIR, fname)
        if os.path.isfile(fpath) and fname.endswith(".zip"):
            try:
                os.remove(fpath)
            except Exception:
                pass

    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
    os.makedirs(TEMP_DIR, exist_ok=True)

    # Dump all collections
    tasks = [
        _dump_collection(db[coll], os.path.join(TEMP_DIR, f"{coll}.json"))
        for coll in collections
    ]
    await asyncio.gather(*tasks)

    # Nice File Name: TeamArc_Data_2025-05-20_14-30.zip
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    zip_name = f"TeamArc_Data_{timestamp}.zip"
    zip_path = os.path.join(BACKUP_DIR, zip_name)

    LOGGER(__name__).info(f"📦 Creating backup archive: {zip_name}")

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(TEMP_DIR):
            for file in files:
                fp = os.path.join(root, file)
                arc = os.path.relpath(fp, TEMP_DIR)
                zf.write(fp, arc)

    shutil.rmtree(TEMP_DIR)
    return zip_path

async def _send_backup(zip_path: str, chat_id: int, title: str):
    # Dynamic Date/Time for Caption
    now = datetime.now()
    date_str = now.strftime("%d %B %Y")
    time_str = now.strftime("%I:%M %p")
    
    # Calculate file size
    try:
        file_size = os.path.getsize(zip_path)
        size_str = f"{file_size / 1024 / 1024:.2f} MB"
    except Exception:
        size_str = "Unknown"

    caption = (
        f"{title}\n\n"
        f"__Authentication successful. Data export complete.__\n\n"
        f"<emoji id='5341492148468465410'>📂</emoji> **File:** `{os.path.basename(zip_path)}`\n"
        f"<emoji id='5231200819986047254'>📊</emoji> **Size:** `{size_str}`\n"
        f"<emoji id='5364233403300330811'>📅</emoji> **Date:** `{date_str}`\n"
        f"<emoji id='5382194935057372936'>⏱</emoji> **Time:** `{time_str}`"
    )
    
    await app.send_document(chat_id=chat_id, document=zip_path, caption=caption)
    
    # Cleanup after sending
    if os.path.exists(zip_path):
        try:
            os.remove(zip_path)
        except Exception:
            pass

# --- Manual Backup Command ---
@app.on_message(filters.command("backup") & filters.user(OWNER_ID))
async def manual_backup(_: Client, message: Message):
    processing = await message.reply_text(
        "<emoji id='5456327427795982532'>🪶</emoji> **Starting Backup…**\n"
        "__Please wait while we securely export your database.__"
    )
    try:
        zip_path = await _create_backup_zip()
        await _send_backup(
            zip_path, 
            message.chat.id, 
            "<emoji id='5208880351690112495'>✅</emoji> **Manual Backup — Completed**"
        )
        await processing.delete()
    except Exception as e:
        await processing.edit_text(
            f"<emoji id='5210952531676504517'>❌</emoji> **Backup Failed!**\n**Error:** `{e}`"
        )
        LOGGER(__name__).error(f"Manual backup failed: {e}")

# --- Restore Command ---
@app.on_message(filters.command("restore") & filters.user(OWNER_ID))
async def restore_backup(_: Client, message: Message):
    if not message.reply_to_message or not message.reply_to_message.document:
        return await message.reply_text(
            "<emoji id='5334544901428229844'>ℹ️</emoji> **Usage:** Reply to a backup ZIP file with `/restore`."
        )
    
    doc = message.reply_to_message.document
    if not doc.file_name.endswith(".zip"):
        return await message.reply_text("<emoji id='5210952531676504517'>❌</emoji> Please reply to a valid `.zip` backup file.")

    processing = await message.reply_text(
        "<emoji id='5017470156276761427'>🔄</emoji> **Restoring Database...**\n"
        "__Downloading and unpacking data.__"
    )

    try:
        # Download
        file_path = await message.reply_to_message.download(file_name=os.path.join(BACKUP_DIR, "restore.zip"))
        
        # Unzip
        extract_path = os.path.join(BACKUP_DIR, "restore_tmp")
        if os.path.exists(extract_path):
            shutil.rmtree(extract_path)
        os.makedirs(extract_path, exist_ok=True)
        
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)

        # Connect DB
        client = AsyncIOMotorClient(MONGO_DB_URI)
        db = client[DB_NAME]
        
        restored_colls = []

        # Restore collections
        for root, _, files in os.walk(extract_path):
            for file in files:
                if file.endswith(".json"):
                    coll_name = file.replace(".json", "")
                    json_path = os.path.join(root, file)
                    
                    with open(json_path, "r", encoding="utf-8") as f:
                        # Use custom hook to restore ISO strings to datetime objects
                        data = json.load(f, object_hook=_json_decoder_hook)
                    
                    if data:
                        collection = db[coll_name]
                        # Wipe existing data to ensure clean state
                        await collection.delete_many({}) 
                        await collection.insert_many(data)
                        restored_colls.append(coll_name)

        # Cleanup
        shutil.rmtree(extract_path)
        os.remove(file_path)

        colls_str = ", ".join(f"`{c}`" for c in restored_colls)
        await processing.edit_text(
            f"<emoji id='5208880351690112495'>✅</emoji> **System Restored Successfully!**\n\n"
            f"**Collections Imported:** {len(restored_colls)}\n"
            f"**List:** {colls_str}"
        )

    except Exception as e:
        await processing.edit_text(
            f"<emoji id='5210952531676504517'>❌</emoji> **Restore Failed!**\n**Error:** `{e}`"
        )
        LOGGER(__name__).error(f"Restore failed: {e}")

# --- Auto Backup Scheduler ---
async def daily_backup_task():
    while True:
        now = datetime.now()
        target = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        
        wait_seconds = (target - now).total_seconds()
        await asyncio.sleep(wait_seconds)
        
        try:
            zip_path = await _create_backup_zip()
            
            # Formatted string for the title: "17 Feb 2026 | 05:30 AM"
            date_time_str = datetime.now().strftime("%d %b %Y | %I:%M %p")
            
            await _send_backup(
                zip_path, 
                LOGGER_ID, 
                f"<emoji id='5463172695132745432'>📦</emoji> **Daily Backup —**"
            )
            LOGGER(__name__).info("Daily backup sent to LOGGER_ID.")
        except Exception as e:
            LOGGER(__name__).error(f"Daily backup failed: {e}")

asyncio.create_task(daily_backup_task())
