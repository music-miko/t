from pyrogram.types import InlineKeyboardButton
from pyrogram import enums
import config
from DeadlineTech import app


def start_panel(_):
    buttons = [
        [
            InlineKeyboardButton(
                text=_["S_B_1"],
                url=f"https://t.me/{app.username}?startgroup=true",
                style=enums.ButtonStyle.DEFAULT,
                icon_custom_emoji_id=5440425405871847617
            ), 
            InlineKeyboardButton(text=_["S_B_2"], url=config.SUPPORT_CHAT, style=enums.ButtonStyle.DEFAULT, icon_custom_emoji_id=5443038326535759644)
        ],
        [
            InlineKeyboardButton(
                text=_["S_B_5"],
                url="https://github.com/DeadlineTech/music",
                style=enums.ButtonStyle.PRIMARY,
                icon_custom_emoji_id=5346181118884331907
            )
        ]
    ]
    return buttons


def private_panel(_):
    buttons = [
        [
            InlineKeyboardButton(
                text=_["S_B_3"],
                url=f"https://t.me/{app.username}?startgroup=true",
                style=enums.ButtonStyle.PRIMARY,
                icon_custom_emoji_id=5440425405871847617 
            )
        ],
        [
             
            InlineKeyboardButton(text=_["S_B_4"], callback_data="settings_back_helper", style=enums.ButtonStyle.DEFAULT, icon_custom_emoji_id=5238025132177369293)
        ],

        [
            InlineKeyboardButton(text=_["S_B_10"], user_id=config.OWNER_ID, style=enums.ButtonStyle.DEFAULT, icon_custom_emoji_id=5929345782560853059),
            InlineKeyboardButton(text=_["S_B_9"], url=config.SUPPORT_CHAT, style=enums.ButtonStyle.DEFAULT, icon_custom_emoji_id=5443038326535759644)
            
        ],
        [
            InlineKeyboardButton(text=_["S_B_7"], url=config.SUPPORT_CHANNEL, style=enums.ButtonStyle.DEFAULT, icon_custom_emoji_id=5931641120458018914), 
            InlineKeyboardButton(text=_["S_B_5"], url="https://github.com/DeadlineTech/music", style=enums.ButtonStyle.DEFAULT, icon_custom_emoji_id=5346181118884331907)
        ]
    ]
    return buttons
