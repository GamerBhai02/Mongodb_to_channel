from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pymongo import MongoClient
from pyrogram.errors import FloodWait
import asyncio
import logging
import base64
from pyrogram.file_id import FileId
from struct import pack
from info import *
# Global variables
cancel_process = False
skip_count = 0  # Default skip count
failed = 0
total = 0

def get_status_message(index, skip_count, failed, e_value=None):
    """Formats and returns the current status message."""
    global total
    total += 1
    status = f"""
╔════❰ ꜰꜱʙᴏᴛᴢ - {total} ❱═❍⊱❁۪۪
║╭━━━━━━━━━━━━━━━➣
║┣⪼<b>🕵 ғᴇᴄʜᴇᴅ Msɢ :</b> <code>{index}</code>
║┣⪼<b>✅ Cᴏᴍᴩʟᴇᴛᴇᴅ:</b> <code>{(index-failed)-skip_count}</code>
║┣⪼<b>🪆 Sᴋɪᴩᴩᴇᴅ Msɢ :</b> <code>{skip_count}</code>
║┣⪼<b>⚠️ Fᴀɪʟᴇᴅ:</b> <code>{failed}</code>
║┣⪼<b>📊 Cᴜʀʀᴇɴᴛ Sᴛᴀᴛᴜs:</b> <code>{'Sleeping ' + str(e_value) if e_value else 'Sending Files'}</code>
║╰━━━━━━━━━━━━━━━➣ 
╚════❰ ꜰꜱʙᴏᴛᴢ ❱══❍⊱❁۪۪
"""
    return status

def unpack_new_file_id(new_file_id):
    """Decodes `_id` from MongoDB into a usable `file_id` and `file_ref`."""
    decoded = FileId.decode(new_file_id)
    file_id = base64.urlsafe_b64encode(
        pack(
            "<iiqq",
            int(decoded.file_type),
            decoded.dc_id,
            decoded.media_id,
            decoded.access_hash
        )
    ).decode().rstrip("=")

    file_ref = base64.urlsafe_b64encode(decoded.file_reference).decode().rstrip("=")
    return file_id, file_ref

@Client.on_message(filters.command("setskip"))
async def set_skip(client, message):
    """Sets the number of files to skip before sending."""
    global skip_count
    try:
        skip_count = int(message.text.split(" ")[1])
        await message.reply_text(f"✅ Skip count set to {skip_count} files.")
    except (IndexError, ValueError):
        await message.reply_text("❌ Invalid format! Use `/setskip <number>` (e.g., `/setskip 5`).")

@Client.on_message(filters.command("send"))
async def send_files(client, message):
    """Fetches files from MongoDB and sends them to a Telegram channel."""
    global cancel_process, skip_count, failed, total
    cancel_process = False  # Reset cancel flag
    failed = 0  # Reset failed count
    total = 0   # Reset total count

    # MongoDB Setup
    fs = await client.ask(chat_id=message.from_user.id, text="Now Send Me The MongoDB URL")
    MONGO_URI = fs.text.strip()
    fs2 = await client.ask(chat_id=message.from_user.id, text="Now Send Me The DB Name")
    DB_NAME = fs2.text.strip()
    fs3 = await client.ask(chat_id=message.from_user.id, text="Now Send Me The Collection Name")
    COLLECTION_NAME = fs3.text.strip()
    
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    movies_collection = db[COLLECTION_NAME]

    fsd = await client.ask(chat_id=message.from_user.id, text="Now Send Me The Destination Channel ID Or Username\nMake Sure That Bot Is Admin In The Destination Channel")
    CHANNEL_ID = fsd.text.strip()

    # Check if skip count is valid
    file_count = movies_collection.count_documents({})
    if skip_count >= file_count:
        await message.reply_text("❌ Skip count is greater than available files.")
        return

    # Notify user about process start
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_process")]])
    status_message = await client.send_message(
        message.chat.id,
        "Starting to send files to the channel...",
        reply_markup=keyboard
    )

    # Apply the skip count and stream files from MongoDB
    cursor = movies_collection.find().skip(skip_count)
    index = skip_count

    for file in cursor:
        if cancel_process:
            await status_message.edit_text("❌ Process canceled by the user.")
            return

        index += 1
        try:
            # Decode `_id` to get `file_id`
            file_id, file_ref = unpack_new_file_id(file["_id"])
            file_name = file.get("file_name", "Unknown File Name")
            file_size = file.get("file_size", 0)
            file_type = file.get("file_type", "document")  # Default to document
            caption = file.get("caption", "")

            # Format File Caption
            size_str = f"{round(file_size / (1024 * 1024), 2)} MB"
            file_caption = f"📌 <b>{file_name}</b>\n📦 Size: <code>{size_str}</code>\n\n{caption}"

            # Send Cached File Based on Type (No need for stream link)
            if file_type == "photo":
                await client.send_cached_media(chat_id=CHANNEL_ID, file_id=file_id, caption=file_caption)
            elif file_type == "video":
                await client.send_cached_media(chat_id=CHANNEL_ID, file_id=file_id, caption=file_caption)
            elif file_type == "audio":
                await client.send_cached_media(chat_id=CHANNEL_ID, file_id=file_id, caption=file_caption)
            else:
                await client.send_cached_media(chat_id=CHANNEL_ID, file_id=file_id, caption=file_caption)

        except FloodWait as e:
            logging.warning(f'Flood wait of {e.value} seconds detected')
            await asyncio.sleep(e.value)  # Wait before retrying
            index -= 1  # Decrement index to retry the same file
            continue

        except Exception as e:
            logging.error(f'Failed to send file: {e}')
            failed += 1

        # Update status message
        new_status_message = get_status_message(index, skip_count, failed)
        if status_message.text != new_status_message:
            await status_message.edit_text(new_status_message, reply_markup=keyboard)

    await status_message.edit_text("✅ All files have been sent successfully!")

@Client.on_callback_query()
async def handle_callbacks(client, callback_query):
    """Handles cancel process callback button."""
    global cancel_process

    if callback_query.data == "cancel_process":
        cancel_process = True
        await callback_query.message.edit_text("❌ Process canceled by the user.")
        await callback_query.answer("Process canceled!")
