import os, re, io, time, json, asyncio, random, aiohttp
from urllib.parse import quote_plus
from pymongo import MongoClient
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import RPCError
from youtubesearchpython.__future__ import VideosSearch
from pytgcalls import PyTgCalls              # provided by py-tgcalls 2.2.6
from pytgcalls.types import MediaStream      # correct for this build
from PIL import Image, ImageDraw

# ---------------- CONFIG ----------------
API_ID = int(os.getenv("API_ID", "16457832"))
API_HASH = os.getenv("API_HASH", "3030874d0befdb5d05597deacc3e83ab")
STRING_SESSION = os.getenv("STRING_SESSION", "BQD7IGgAUHl8J-qZ9YLsK-ue7kMhsJGDIUU8FBvVcuWQYTIIvEkxH5a1pRRpXB4gACbbkiHvsn-SsrbgSKTpk7muOJ3kWdLEwWZMNT_z7Kt-PMfZ23UJjSZhU7vBdLRKwmEsYaRgUfu-VsIXrZmBZviHwbEN3J4DgdFZzEZbKTbJyMQhCG_eJRk5pMwecSHkKaloQkXwbZKi6tJUEun4n6J-5x6sPl87FMNLIC0_hC9xQn8zTMBeacCYl6vo1n6TJpLmXtY38M5cOr65aKr14yQwadA2aZif82eL7Bd9m8lwvP4rppjgqhT-dns_ME_wWCSCITIuvFjmkzlOHAog1fuiHsTHfQAAAAGF4OEZAA")
BOT_TOKEN = os.getenv("BOT_TOKEN", "7062994457:AAHL5LHDdY1MedP2-mtYMT1UEYeDk94Kafg")
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://vivek:1234567890@cluster0.c48d8ih.mongodb.net/?retryWrites=true&w=majority")
API_BASE = os.getenv("API_BASE", "http://46.250.243.52:1470").rstrip("/") if os.getenv("API_BASE") else ""
OWNER_ID = int(os.getenv("OWNER_ID", "6657539971"))
DURATION_LIMIT = 7200  # 2 hours

os.makedirs("downloads", exist_ok=True)

# ---------------- CLIENTS ----------------
bot = Client("music-bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user = Client("music-user", api_id=API_ID, api_hash=API_HASH, session_string=STRING_SESSION)
vc = PyTgCalls(user)

mongo = MongoClient(MONGO_URI)
db = mongo["music_db"]
col = db["queues"]

# ---------------- HELPERS ----------------
def sec_to_time(sec): m, s = divmod(int(sec), 60); return f"{m:02d}:{s:02d}"

def sec(s):
    try:
        p = s.split(":")
        return int(p[0])*60+int(p[1]) if len(p)==2 else int(p[0])
    except: return 0

async def youtube_search(q):
    s = VideosSearch(q, limit=1)
    r = (await s.next())["result"][0]
    return r["id"], r["title"], r.get("duration","0:00"), r["thumbnails"][0]["url"]

async def get_api_link(vidid):
    if not API_BASE: return None
    url=f"{API_BASE}/song?query={quote_plus(vidid)}"
    async with aiohttp.ClientSession() as s:
        async with s.get(url) as r:
            if r.status==200:
                j=await r.json(); return j.get("link")

def get_queue(cid): return col.find_one({"chat_id":cid}) or {"chat_id":cid,"queue":[],"now":None}
def save_queue(q): col.update_one({"chat_id":q["chat_id"]},{"$set":q},upsert=True)

async def background_download_tme(link,path):
    try:
        m=re.search(r"t\.me\/(.*?)\/(\d+)",link)
        if not m:return
        cname,msgid=m.group(1),int(m.group(2))
        msg=await user.get_messages(cname,msgid)
        await msg.download(file_name=path)
    except Exception as e: print("BG download:",e)

# ---- image helpers ----
async def get_user_dp_bytes(uid):
    try:
        p=await bot.get_profile_photos(uid,limit=1)
        if not p:return None
        f=await bot.download_media(p[0].file_id,in_memory=True)
        return f.getbuffer().tobytes()
    except: return None

async def fetch_img(url):
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url,timeout=20) as r:
                if r.status==200:return await r.read()
    except: return None

def circ_mask(sz):
    m=Image.new("L",(sz,sz),0); d=ImageDraw.Draw(m); d.ellipse((0,0,sz,sz),fill=255); return m

async def make_thumb(thumb, uid):
    W,H=640,360
    base=Image.new("RGB",(W,H),(24,24,24))
    if thumb:
        raw=await fetch_img(thumb)
        if raw:
            try:
                img=Image.open(io.BytesIO(raw)).convert("RGB")
                base.paste(img.resize((W,H)),(0,0))
            except: pass
    if uid:
        dpb=await get_user_dp_bytes(uid)
        if dpb:
            dp=Image.open(io.BytesIO(dpb)).convert("RGB").resize((108,108))
            m=circ_mask(108)
            border=Image.new("RGB",(116,116),(255,255,255))
            bmask=circ_mask(116)
            base.paste(border,(W-132,H-132),bmask)
            base.paste(dp,(W-128,H-128),m)
    out=io.BytesIO(); base.save(out,"JPEG",quality=85); out.seek(0); return out.getvalue()

# ---------------- PLAYER ----------------
async def join_and_stream(cid,link):
    await vc.join_group_call(cid,MediaStream(link))
    print(f"🎧 Streaming: {link}")

async def play_next(cid):
    data=get_queue(cid)
    if not data["queue"]:
        try: await vc.leave_group_call(cid)
        except: pass
        data["now"]=None; save_queue(data); return
    song=data["queue"].pop(0); save_queue(data)
    title,link,thumb,dur=req=None,None,None,None
    title,link,thumb,dur=song["title"],song["link"],song["thumb"],song["dur"]
    req,req_id=song["req"],song.get("req_id")
    await join_and_stream(cid,link)
    asyncio.create_task(background_download_tme(link,f"downloads/{int(time.time())}_{random.randint(100,999)}.mp3"))
    comp=await make_thumb(thumb,req_id)
    photo=comp or thumb or "https://i.ibb.co/4pDNDk1/music.jpg"
    msg=await bot.send_photo(cid,photo=photo,
        caption=f"🎵 **{title}**\n⏱ `{dur}`\n👤 Requested by: {req}",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⏭ Skip",callback_data=f"skip_{cid}")],
            [InlineKeyboardButton("📜 Queue",callback_data=f"queue_{cid}")]
        ]))
    data["now"]={"msg":msg.id,"start":time.time(),"dur":sec(dur)}; save_queue(data)
    asyncio.create_task(update_progress(cid))

async def update_progress(cid):
    while True:
        await asyncio.sleep(5)
        d=get_queue(cid); n=d.get("now")
        if not n:return
        el=int(time.time()-n["start"]); dur=n.get("dur",0)
        if dur and el>=dur:
            try: await vc.leave_group_call(cid)
            except: pass
            await play_next(cid); return
        try:
            await bot.edit_message_caption(cid,n["msg"],
                caption=f"🎶 Playing… {sec_to_time(el)} / {sec_to_time(dur)}")
        except RPCError: pass

# ---------------- COMMANDS ----------------
@bot.on_message(filters.command("start"))
async def start_cmd(_,m:Message):
    await m.reply_text("👋 Hey! Use /play <song name> to stream instantly 🎧")

@bot.on_message(filters.command("play") & filters.group)
async def play_cmd(_,m:Message):
    if len(m.command)<2:return await m.reply_text("Usage: /play <song name>")
    query=m.text.split(None,1)[1].strip()
    await m.reply_chat_action("typing")
    try: vidid,title,duration,thumb=await youtube_search(query)
    except: return await m.reply_text("❌ YouTube search failed.")
    link=await get_api_link(vidid)
    if not link:return await m.reply_text("❌ API didn’t return a link.")
    data=get_queue(m.chat.id)
    data["queue"].append({"title":title,"link":link,"thumb":thumb,"dur":duration,
        "req":m.from_user.first_name,"req_id":m.from_user.id}); save_queue(data)
    if not data.get("now"): await play_next(m.chat.id); await m.reply_text(f"▶️ Now playing: **{title}**")
    else: await m.reply_text(f"➕ Added to queue: **{title}**")

@bot.on_callback_query(filters.regex(r"^skip_(\-\d+|\d+)$"))
async def skip_cb(_,q):
    cid=int(q.data.split("_")[1])
    try: await vc.leave_group_call(cid)
    except: pass
    await play_next(cid); await q.answer("⏭ Skipped!")

@bot.on_callback_query(filters.regex(r"^queue_(\-\d+|\d+)$"))
async def cb_queue(_,q):
    cid=int(q.data.split("_")[1])
    d=get_queue(cid); ql=d.get("queue",[])
    if not ql:return await q.message.edit_text("📭 Queue empty.")
    t="\n".join([f"{i+1}. {x['title']} ({x['dur']})" for i,x in enumerate(ql)])
    await q.message.edit_text("📜 **Queue:**\n"+t)

@bot.on_message(filters.command("stop") & filters.group)
async def stop_cmd(_,m:Message):
    try: await vc.leave_group_call(m.chat.id)
    except: pass
    d=get_queue(m.chat.id); d["queue"]=[]; d["now"]=None; save_queue(d)
    await m.reply_text("⏹ Stopped and cleared queue.")

# ---------------- STARTUP ----------------
async def main():
    await bot.start(); await user.start(); await vc.start()
    print("✅ Bot started with String Session userbot.")
    if OWNER_ID:
        try: await bot.send_message(OWNER_ID,"🚀 Music Bot started successfully!")
        except: pass
    await asyncio.get_event_loop().create_future()

if __name__=="__main__":
    try: import uvloop; uvloop.install()
    except: pass
    asyncio.run(main())
