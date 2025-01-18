from pyrogram import Client, filters
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from pymongo import MongoClient
from pyrogram.errors import FloodWait
import asyncio
import logging
from info import *


# Global control variables
cancel_process = False
skip_count = 0  # Default skip count

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
    global cancel_process, skip_count
    cancel_process = False  # Reset cancel flag
    #MongoDB Setup Start
    DBUSER=message.from_user.id
    fs = await client.ask(chat_id = message.from_user.id, text = "Now Send Me The MongoDB URL")
    MONGO_URI=fs.text
    fs2= await client.ask(chat_id = message.from_user.id, text = "Now Send Me The DB Name")
    DB_NAME=fs2.text
    fs3= await client.ask(chat_id = message.from_user.id, text = "Now Send Me The Collection Name")
    COLLECTION_NAME=fs3.text
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]
    movies_collection = db[COLLECTION_NAME]
    #MongoDB Setup End
    fsd= await client.ask(chat_id = message.from_user.id, text= "Now Send Me The Destination Channel ID Or Username\n Make Sure That Bot Is Admin In The Destination Challe")
    CHANNEL_ID=fsd.text
    
    files = list(movies_collection.find())
    if not files:
        await client.send_message(message.chat.id, "No files found in the database.")
        return

    # Notify user about the process start with cancel button
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="cancel_process")]])
    status_message = await client.send_message(
        message.chat.id,
        f"Starting to send {len(files)} files to the channel after skipping {skip_count} files...",
        reply_markup=keyboard
    )

    # Apply the skip count
    files_to_send = files[skip_count:] if skip_count < len(files) else []

    for index, file in enumerate(files_to_send, start=1):
        if cancel_process:
            await status_message.edit_text("❌ Process canceled by the user.")
            return

        try:
            file_id = file.get("file_id")
            file_name = file.get("file_name", "Unknown File Name")
            file_size = file.get("file_size", "Unknown Size")
            caption = file.get("caption", "No caption provided.")

            # Format file size for readability
            file_size_mb = round(file_size / (1024 * 1024), 2) if isinstance(file_size, int) else file_size

            # Create the message caption
            file_message = f"**{file_name}**\n📦 Size: {file_size_mb} MB\n\n{caption}"

            # Detect file type based on file extension or metadata
            if file_id:
                # Certain file types based on their extensions or other metadata
                if file_id.endswith(('.jpg', '.jpeg', '.png', '.bmp', '.gif')):
                    await client.send_message(
                        chat_id=DBUSER,text=f"Photo Skipped"
                    )
                elif file_id.endswith(('.mp4', '.mkv', '.avi', '.mov')):
                    await client.send_video(
                        chat_id=CHANNEL_ID,
                        video=file_id,
                        caption=file_message
                    )
                elif file_id.endswith(('.mp3', '.wav', '.aac')):
                    await client.send_message(
                        chat_id=DBUSER,text=f"Photo Skipped"
                    )
                else:
                    await client.send_document(
                        chat_id=CHANNEL_ID,
                        document=file_id,
                        caption=file_message
                    )
            else:
                await client.send_message(
                    chat_id=CHANNEL_ID,
                    text=f"Invalid file ID: {file_id}"
                )

        except FloodWait as e:
            logging.warning(f'Flood wait of {e.value} seconds detected')
            new_status_message = f"""
╔════❰ ꜰꜱʙᴏᴛᴢ ❱═❍⊱❁۪۪
║╭━━━━━━━━━━━━━━━➣
║┃
║┣⪼<b>ᴄᴏᴍᴩʟᴇᴛᴇᴅ:</b> <code>{index}</code>
║┃
║┣⪼<b>🪆 Sᴋɪᴘᴘᴇᴅ Msɢ :</b> <code>{skip_count}</code>
║┃
║┣⪼<b>ʀᴇᴍᴀɪɴɪɴɢ:</b> <code>{((len(files_to_send))-index)}</code>
║┃
║┣⪼<b>📊 Cᴜʀʀᴇɴᴛ Sᴛᴀᴛᴜs:</b> <code>Sleeping {e.value}</code>
║┃
║╰━━━━━━━━━━━━━━━➣ 
╚════❰ ꜰꜱʙᴏᴛᴢ ❱══❍⊱❁۪۪
"""
            await status_message.edit_text(new_status_message, reply_markup=keyboard)
            await asyncio.sleep(e.value)
        except Exception as e:
            logging.error(f'Failed to send file: {e}')

        # Update status in the user chat
        new_status_message = f"""
╔════❰ ꜰꜱʙᴏᴛᴢ ❱═❍⊱❁۪۪
║╭━━━━━━━━━━━━━━━➣
║┃
║┣⪼<b>ᴄᴏᴍᴩʟᴇᴛᴇᴅ:</b> <code>{index}</code>
║┃
║┣⪼<b>🪆 Sᴋɪᴘᴘᴇᴅ Msɢ :</b> <code>{skip_count}</code>
║┃
║┣⪼<b>ʀᴇᴍᴀɪɴɪɴɢ:</b> <code>{((len(files_to_send))-index)}</code>
║┃
║┣⪼<b>📊 Cᴜʀʀᴇɴᴛ Sᴛᴀᴛᴜs:</b> <code>Sending Files</code>
║┃
║╰━━━━━━━━━━━━━━━➣ 
╚════❰ ꜰꜱʙᴏᴛᴢ ❱══❍⊱❁۪۪
"""
        if status_message.text != new_status_message:
            await status_message.edit_text(new_status_message, reply_markup=keyboard)

    await status_message.edit_text("✅ All files have been sent successfully!")

@Client.on_callback_query()
async def handle_callbacks(client, callback_query):
    global cancel_process

    if callback_query.data == "cancel_process":
        cancel_process = True
        await callback_query.message.edit_text("❌ Process canceled by the user.")
        await callback_query.answer("Process canceled!")
