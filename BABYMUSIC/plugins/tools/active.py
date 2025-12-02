from pyrogram import filters, Client
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from unidecode import unidecode

from BABYMUSIC import app
from BABYMUSIC.misc import SUDOERS
from BABYMUSIC.utils.database import (
    get_active_chats,
    get_active_video_chats,
    remove_active_chat,
    remove_active_video_chat,
)


@app.on_message(filters.command(["activevc", "activevoice","vc"]) & SUDOERS)
async def activevc(_, message: Message):
    mystic = await message.reply_text("Getting active voice chats list...")
    served_chats = await get_active_chats()
    text = ""
    j = 0
    for x in served_chats:
        try:
            title = (await app.get_chat(x)).title
        except:
            await remove_active_chat(x)
            continue
        try:
            if (await app.get_chat(x)).username:
                user = (await app.get_chat(x)).username
                text += f"<b>{j + 1}.</b> <a href=https://t.me/{user}>{unidecode(title).upper()}</a>\n"
            else:
                text += (
                    f"<b>{j + 1}.</b> {unidecode(title).upper()}\n"
                )
            j += 1
        except:
            continue
    if not text:
        await mystic.edit_text(f"No active voice chats on {app.mention}.")
    else:
        await mystic.edit_text(
            f"<b>» ʟɪsᴛ ᴏғ ᴄᴜʀʀᴇɴᴛʟʏ ᴀᴄᴛɪᴠᴇ ᴠᴏɪᴄᴇ ᴄʜᴀᴛs :</b>\n\n{text}",
            disable_web_page_preview=True,
        )


@app.on_message(filters.command(["activev", "activevideo","vvc"]) & SUDOERS)
async def activevi_(_, message: Message):
    mystic = await message.reply_text("Getting active video chats list...")
    served_chats = await get_active_video_chats()
    text = ""
    j = 0
    for x in served_chats:
        try:
            title = (await app.get_chat(x)).title
        except:
            await remove_active_video_chat(x)
            continue
        try:
            if (await app.get_chat(x)).username:
                user = (await app.get_chat(x)).username
                text += f"<b>{j + 1}.</b> <a href=https://t.me/{user}>{unidecode(title).upper()}</a> [<code>{x}</code>]\n"
            else:
                text += (
                    f"<b>{j + 1}.</b> {unidecode(title).upper()} [<code>{x}</code>]\n"
                )
            j += 1
        except:
            continue
    if not text:
        await mystic.edit_text(f"No active video chats on {app.mention}.")
    else:
        await mystic.edit_text(
            f"<b>List of currently active video chats :</b>\n\n{text}",
            disable_web_page_preview=True,
        )

@app.on_message(filters.command(["ac","av"]) & SUDOERS)
async def start(client: Client, message: Message):
    ac_audio = str(len(await get_active_chats()))
    ac_video = str(len(await get_active_video_chats()))
    await message.reply_text(f"✫ <b><u>Active chats info</u></b> :\n\nVoice : {ac_audio}\nVideo  : {ac_video}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('Close', callback_data=f"close")]]))
