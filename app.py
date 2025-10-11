import os
import asyncio
import json
from pyrogram import Client, filters
from pymongo import MongoClient

# ---------------- CONFIG ----------------
API_ID = int(os.getenv("API_ID", 16457832))
API_HASH = os.getenv("API_HASH", "3030874d0befdb5d05597deacc3e83ab")
STRING_SESSION = os.getenv("STRING_SESSION", "BQD7IGgAMBfbXie5kc-L_74G59Ixh8ZMoAg8rXxRQwX_1Vd5P5ELAigTUfjkbKv7dBWQ7SQxv_LWOFmnjlzhVJGH7-nqpZ104usYkG0YTf9eCXLcu7QYcJPDOQ-WwXYtGhn2l-KFJq1DrrSCdNsXIC6pSy8UjJIlfRjjsQPIBNIaOJ3262EUX_mdppodDkOZEATogiqoydVQqOhNWOhQUJ2gloTi_xUFNkzj0q7RqU9u2NsAy4uz8inyhKqZ-U6O-9Ok19kG995iLAUGViD9ue8aSzRij_r5EqEz_KTYqA5FsTr1ZQBouwcJTXr-4h6oFDJqJdqT55KvZmEq8aNdrq-3v1XboQAAAAH14mY2AA")
TARGET_CHANNEL = "@babyytapi"
MONGO_URL = os.getenv(
    "MONGO_URL",
    "mongodb+srv://vivek:1234567890@cluster0.c48d8ih.mongodb.net/?retryWrites=true&w=majority"
)
OWNER_ID = int(os.getenv("OWNER_ID", 6355097973))

BATCH_LIMIT = 100
BATCH_SLEEP = 1  # pause 1s between 100 msgs
CHECK_INTERVAL = 10
BACKUP_FILE = "audio_backup.txt"
# ----------------------------------------

# ✅ MongoDB setup
mongo = MongoClient(MONGO_URL)
db = mongo["telegram_audio_db"]
collection = db["audio_files"]

# ✅ Pyrogram client
app = Client(
    name="monitor",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=STRING_SESSION
)

# ---------------- UTILITIES ----------------
def save_to_txt(file_name, msg_id):
    """Append data to text backup"""
    try:
        with open(BACKUP_FILE, "a", encoding="utf-8") as f:
            f.write(f"{file_name} - {msg_id}\n")
    except Exception as e:
        print(f"⚠️ Could not save to backup file: {e}")

async def handle_duplicate(file_name, msg_id):
    """Notify owner if duplicate detected"""
    link = f"https://t.me/{TARGET_CHANNEL.replace('@', '')}/{msg_id}"
    text = (
        f"⚠️ **Duplicate Detected!**\n\n"
        f"🎵 File name: `{file_name}`\n"
        f"🗑 Please delete this duplicate: [Open Message]({link})"
    )
    try:
        await app.send_message(OWNER_ID, text, disable_web_page_preview=True)
        print(f"📩 Duplicate alert sent → {file_name}")
    except Exception as e:
        print(f"❌ Could not notify owner: {e}")

async def save_to_mongo(file_name, msg_id):
    """Insert new record in MongoDB + backup"""
    if not file_name:
        return
    existing = collection.find_one({"file_name": file_name})
    if existing:
        await handle_duplicate(file_name, msg_id)
        return

    collection.insert_one({"file_name": file_name, "msg_id": msg_id})
    save_to_txt(file_name, msg_id)
    print(f"💾 Saved → {file_name} : {msg_id}")

# ---------------- BACKUP COMMAND ----------------
@app.on_message(filters.user(OWNER_ID) & filters.command("backup"))
async def backup_command(client, message):
    """When owner sends /backup, export MongoDB to file"""
    try:
        data = list(collection.find({}, {"_id": 0}))
        if not data:
            await message.reply("⚠️ No data found in MongoDB.")
            return

        file_name = "mongo_backup.json"
        with open(file_name, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        file_size = os.path.getsize(file_name)
        if file_size > 49 * 1024 * 1024:  # >49MB fallback to txt
            txt_file = "mongo_backup.txt"
            with open(txt_file, "w", encoding="utf-8") as f:
                for item in data:
                    f.write(f"{item['file_name']} - {item['msg_id']}\n")
            await message.reply_document(txt_file, caption="📦 Backup (.txt)")
            os.remove(txt_file)
        else:
            await message.reply_document(file_name, caption="📦 MongoDB Backup (.json)")
            os.remove(file_name)

        print("✅ Backup file sent successfully")

    except Exception as e:
        await message.reply(f"❌ Backup failed: {e}")
        print(f"❌ Backup error: {e}")

# ---------------- MAIN TASK ----------------
async def scan_and_save():
    """Scan channel messages and save audio metadata"""
    print("🤖 Scanning for audio files...")

    last_id = 0
    while True:
        try:
            batch = []
            async for msg in app.get_chat_history(TARGET_CHANNEL, offset_id=last_id, limit=BATCH_LIMIT):
                media = msg.audio or msg.document
                if not media:
                    continue
                file_name = getattr(media, "file_name", None)
                if not file_name:
                    continue

                await save_to_mongo(file_name, msg.id)
                batch.append(msg)
                last_id = msg.id

            if not batch:
                print("✅ Scan complete! Waiting for new files...")
                break

            print(f"🌀 Processed {len(batch)} files. Sleeping {BATCH_SLEEP}s...")
            await asyncio.sleep(BATCH_SLEEP)

        except Exception as e:
            print(f"❌ Error while scanning: {e}")
            await asyncio.sleep(3)
            continue

async def main():
    async with app:
        try:
            chat = await app.get_chat(TARGET_CHANNEL)
            print(f"🎯 Monitoring channel: {chat.title}")
        except Exception as e:
            print(f"❌ Channel access error: {e}")
            return

        while True:
            await scan_and_save()
            print("⏳ Waiting before next scan...")
            await asyncio.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
