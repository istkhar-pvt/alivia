# Optimized fast version of your code
# (callbacks, regex, mappings, zero slow if/elif chains)

from typing import Union
from pyrogram import filters, types
from pyrogram.types import InlineKeyboardMarkup, Message, InlineKeyboardButton
from BABYMUSIC import app
from BABYMUSIC.utils import help_pannel
from BABYMUSIC.utils.database import get_lang
from BABYMUSIC.utils.decorators.language import LanguageStart, languageCB
from BABYMUSIC.utils.inline.help import help_back_markup, private_help_panel
from config import BANNED_USERS, START_IMG_URL, SUPPORT_CHAT
from strings import get_string, helpers
from BABYMUSIC.utils.stuffs.helper import Helper
from pyrogram import enums

###############################
# FAST HELP CALLBACK MAPPING
###############################
HELP_MAP = {
    "hb1": helpers.HELP_1,
    "hb3": helpers.HELP_3,
    "hb6": helpers.HELP_6,
    "hb7": helpers.HELP_7,
    "hb10": helpers.HELP_10,
    "hb11": helpers.HELP_11,
    "hb12": helpers.HELP_12,
    "hb13": helpers.HELP_13,
    "hb15": helpers.HELP_15,
}

#########################################
# PRIVATE HELP HANDLER
#########################################
@app.on_message(filters.command(["help"]) & filters.private & ~BANNED_USERS)
@app.on_callback_query(filters.regex("settings_back_helper") & ~BANNED_USERS)
async def helper_private(client: app, update: Union[types.Message, types.CallbackQuery]):
    is_callback = isinstance(update, types.CallbackQuery)

    if is_callback:
        try:
            await update.answer()
        except:
            pass

        chat_id = update.message.chat.id
        language = await get_lang(chat_id)
        _ = get_string(language)

        keyboard = help_pannel(_, True)
        await update.edit_message_text(
            _["help_1"].format(SUPPORT_CHAT), reply_markup=keyboard
        )

    else:
        try:
            await update.delete()
        except:
            pass

        language = await get_lang(update.chat.id)
        _ = get_string(language)
        keyboard = help_pannel(_)

        await update.reply_photo(
            photo=START_IMG_URL,
            caption=_["help_1"].format(SUPPORT_CHAT),
            reply_markup=keyboard,
        )

#########################################
# GROUP HELP PANEL
#########################################
@app.on_message(filters.command(["help"]) & filters.group & ~BANNED_USERS)
@LanguageStart
async def help_com_group(client, message: Message, _):
    keyboard = private_help_panel(_)
    await message.reply_text(_["help_2"], reply_markup=InlineKeyboardMarkup(keyboard))

#########################################
# SUPER FAST HELP CALLBACK (NO IF/ELIF)
#########################################
@app.on_callback_query(filters.regex("^h:") & ~BANNED_USERS)
@languageCB
async def helper_cb(client, query, _):
try:
_, key = query.data.split(":")
except:
return await query.answer("Invalid!", show_alert=True)


text = HELP_MAP.get(key)
if not text:
return await query.answer("Invalid!", show_alert=True)


keyboard = help_back_markup(_)
await query.edit_message_text(text, reply_markup=keyboard, disable_web_page_preview=True)

#########################################
# MANAGE BOT CALLBACK (kept same, optimized extract)
#########################################
@app.on_callback_query(filters.regex('managebot123'))
async def on_back_button(client, query):
    callback_data = query.data.split(None, 1)
    if len(callback_data) < 2:
        return
    cb = callback_data[1]

    chat_id = query.message.chat.id
    language = await get_lang(chat_id)
    _ = get_string(language)

    keyboard = help_pannel(_, True)

    if cb == "settings_back_helper":
        await query.edit_message_text(
            _["help_1"].format(SUPPORT_CHAT), reply_markup=keyboard
        )

#########################################
# MPLUS CALLBACK BUTTON (optimized parsing)
#########################################
@app.on_callback_query(filters.regex('mplus'))
async def mb_plugin_button(client, query):
    callback_data = query.data.split(None, 1)
    if len(callback_data) < 2:
        return

    cb = callback_data[1]

    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("ʙᴀᴄᴋ", callback_data="mbot_cb")]]
    )

    if cb == "Okieeeeee":
        await query.edit_message_text(
            "`something errors`",
            reply_markup=keyboard,
            parse_mode=enums.ParseMode.MARKDOWN,
        )
    else:
        await query.edit_message_text(getattr(Helper, cb), reply_markup=keyboard)
