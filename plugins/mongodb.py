from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pymongo import MongoClient
from pyrogram.errors import FloodWait
import asyncio
import logging
import gc
from info import *

# Global control variables
cancel_process = False
skip_count = 0  # Default skip count
batch_size = 10  # Reduce batch size to lower memory consumption
failed = 0
total = 0

def get_status_message(index, skip_count, failed, e_value=None):
    global total
    total += 1
    if e_value:
        return f"""
╔════❰ ꜰꜱʙᴏᴛᴢ - {total} ❱═❍⊱❁۪۪
║╭━━━━━━━━━━━━━━━➣
║┣⪼<b>🕵 ғᴇᴄʜᴇᴅ Msɢ :</b> <code>{index}</code>
║┃
║┣⪼<b>✅ Cᴏᴍᴩʟᴇᴛᴇᴅ:</b> <code>{(index-failed)-skip_count}</code>
║┃
║┣⪼<b>🪆 Sᴋɪᴩᴩᴇᴅ Msɢ :</b> <code>{skip_count}</code>
║┃
║┣⪼<b>⚠️ Fᴀɪʟᴇᴅ:</b> <code>{failed}</code>
║┃
║┣⪼<b>📊 Cᴜʀʀᴇɴᴛ Sᴛᴀᴛᴜs:</b> <code>Sleeping {e_value}</code>
║┃
║╰━━━━━━━━━━━━━━━➣ 
╚════❰ ꜰꜱʙᴏᴛᴢ ❱══❍⊱❁۪۪
"""
    else:
        return f"""
╔════❰ ꜰꜱʙᴏᴛᴢ - {total} ❱═❍⊱❁۪۪
║╭━━━━━━━━━━━━━━━➣
║┣⪼<b>🕵 ғᴇᴄʜᴇᴅ Msɢ :</b> <code>{index}</code>
║┃
║┣⪼<b>✅ Cᴏᴍᴩʟᴇᴛᴇᴅ:</b> <code>{(index-failed)-skip_count}</code>
║┃
║┣⪼<b>🪆 Sᴋɪᴩᴩᴇᴅ Msɢ :</b> <code>{skip_count}</code>
║┃
║┣⪼<b>⚠️ Fᴀɪʟᴇᴅ:</b> <code>{failed}</code>
║┃
║┣⪼<b>📊 Cᴜʀʀᴇɴᴛ Sᴛᴀᴛᴜs:</b> <code>Sending Files</code>
║┃
║╰━━━━━━━━━━━━━━━➣ 
╚════❰ ꜰꜱʙᴏᴛᴢ ❱══❍⊱❁۪۪
"""

@Client.on_message(filters.command("setskip"))
async def set_skip(client, message):
    global skip_count
    try:
        skip_count = int(message.text.split(" ")[1])
        await message.reply_text(f"✅ Skip count set to {skip_count} files.")
    except (IndexError, ValueError):
        await message.reply_text("❌ Invalid format! Use `/setskip <number>` (e.g., `/setskip 5`).")

@Client.on_message(filters.command("send"))
async def send_files(client, message):
    global cancel_process, skip_count, failed, total
    cancel_process = False  # Reset cancel flag
    failed = 0  # Reset failed count
    total = 0   # Reset total count

    # MongoDB Setup Start
    DBUSER = message.from_user.id
    fs = await client.ask(chat_id=message.from_user.id, text="Now Send Me The MongoDB URL")
    MONGO_URI = fs.text
    fs2 = await client.ask(chat_id=message.from_user.id, text="Now Send Me The DB Name")
    DB_NAME = fs2.text
    fs3 = await client.ask(chat_id=message.from_user.id, text="Now Send Me The Collection Name")
    COLLECTION_NAME = fs3.text
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    movies_collection = db[COLLECTION_NAME]
    # MongoDB Setup End

    fsd = await client.ask(chat_id=message.from_user.id, text="Now Send Me The Destination Channel ID Or Username\nMake Sure That Bot Is Admin In The Destination Channel")
    CHANNEL_ID = fsd.text

    # Notify user about the process start with cancel button
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
            file_id = file.get("file_ref")
            if not file_id:
                raise ValueError("Invalid file ID")

            file_name = file.get("file_name", "Unknown File Name")
            file_size = file.get("file_size", "Unknown Size")
            caption = file.get("caption", "No caption provided.")

            # Format file size for readability
            file_size_mb = round(file_size / (1024 * 1024), 2) if isinstance(file_size, int) else file_size

            # Create the message caption
            file_message = f"**{file_name}**\n📦 Size: {file_size_mb} MB\n\n{caption}"

            # Detect file type based on file extension or metadata
            if file_name.endswith(('.jpg', '.jpeg', '.png', '.bmp', '.gif')):
                await client.send_photo(chat_id=CHANNEL_ID, photo=file_id, caption=file_message)
            elif file_name.endswith(('.mp4', '.mkv', '.avi', '.mov')):
                await client.send_video(chat_id=CHANNEL_ID, video=file_id, caption=file_message)
            elif file_name.endswith(('.mp3', '.wav', '.aac')):
                await client.send_audio(chat_id=CHANNEL_ID, audio=file_id, caption=file_message)
            else:
                await client.send_document(chat_id=CHANNEL_ID, document=file_id, caption=file_message)

        except FloodWait as e:
            logging.warning(f'Flood wait of {e.value} seconds detected')
            new_status_message = get_status_message(index, skip_count, failed, e.value)
            if new_status_message != status_message.text:
                await status_message.edit_text(new_status_message, reply_markup=keyboard)
            await asyncio.sleep(e.value)
            continue  # Skip the current file and continue with the next one
        except Exception as e:
            logging.error(f'Failed to send file: {e}')
            failed += 1
            new_status_message = get_status_message(index, skip_count, failed)
            if new_status_message != status_message.text:
                await status_message.edit_text(new_status_message, reply_markup=keyboard)

        # Trigger garbage collection to free memory
        gc.collect()

        # Update status in the user chat
        new_status_message = get_status_message(index, skip_count, failed)
        if new_status_message != status_message.text:
            await status_message.edit_text(new_status_message, reply_markup=keyboard)

    await status_message.edit_text("✅ All files have been sent successfully!")

@Client.on_callback_query()
async def handle_callbacks(client, callback_query):
    global cancel_process

    if callback_query.data == "cancel_process":
        cancel_process = True
        await callback_query.message.edit_text("❌ Process canceled by the user.")
        await callback_query.answer("Process canceled!")
