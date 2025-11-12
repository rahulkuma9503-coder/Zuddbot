import os
import logging
import threading
import time
import sys
import asyncio
from flask import Flask, Response
from pymongo import MongoClient
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder,
    ContextTypes,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters
)

# Create Flask app for health check
app = Flask(__name__)

@app.route('/')
def home():
    return "Bot is running", 200

@app.route('/health')
def health_check():
    return Response(status=200)

# Enhanced logging setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bot start time for uptime calculation
bot_start_time = time.time()

# Global variables for broadcast control
broadcast_active = False
broadcast_cancelled = False
broadcast_task = None

# Helper function to format uptime
def format_uptime(seconds):
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    return f"{int(days)}d {int(hours)}h {int(minutes)}m {int(seconds)}s"

# Load environment variables
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "").strip()
GROUP_ID = os.getenv("TELEGRAM_GROUP_ID", "").strip()
MONGODB_URI = os.getenv("MONGODB_URI")
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID")
TUTORIAL_VIDEO_LINK = os.getenv("TUTORIAL_VIDEO_LINK", "https://youtube.com/shorts/UhccqnGY3PY?si=1aswpXBhcFP8L8tM")

# Verify required environment variables
if not all([TOKEN, MONGODB_URI, ADMIN_USER_ID]):
    logger.error("Missing required environment variables!")
    missing = [var for var in ["TOKEN", "MONGODB_URI", "ADMIN_USER_ID"] 
               if not os.getenv(var)]
    logger.error(f"Missing variables: {', '.join(missing)}")
    exit(1)

# Check if any verification is required
REQUIRES_VERIFICATION = bool(CHANNEL_ID or GROUP_ID)

# MongoDB setup
try:
    client = MongoClient(MONGODB_URI)
    db = client.telegram_bot_db
    users_collection = db.users
    custom_commands_collection = db.custom_commands
    logger.info("Connected to MongoDB successfully")
    
    # Create index for command names
    custom_commands_collection.create_index("command", unique=True)
except Exception as e:
    logger.error(f"MongoDB connection failed: {e}")
    exit(1)

async def is_owner(user_id: int) -> bool:
    return str(user_id) == ADMIN_USER_ID

async def generate_invite_link(context: ContextTypes.DEFAULT_TYPE, chat_id: str) -> str:
    """Generate a temporary invite link that expires in 5 minutes"""
    try:
        # Create an invite link that expires in 5 minutes
        expire_date = int(time.time()) + 300  # 5 minutes from now
        invite_link = await context.bot.create_chat_invite_link(
            chat_id=chat_id,
            expire_date=expire_date,
            member_limit=1  # Single use link
        )
        return invite_link.invite_link
    except Exception as e:
        logger.error(f"Failed to generate invite link for {chat_id}: {e}")
        # Fallback to a basic link if generation fails
        if chat_id.startswith('@'):
            return f"https://t.me/{chat_id[1:]}"
        elif str(chat_id).startswith('-'):
            # For group IDs, we can't create a public link, so use the bot's invite
            return f"https://t.me/{context.bot.username}?startgroup=true"
        else:
            return f"https://t.me/{chat_id}"

async def check_membership(user_id: int, context: ContextTypes.DEFAULT_TYPE, chat_id: str) -> bool:
    """Check if user is a member of a specific chat with improved error handling"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Try different approaches to check membership
            try:
                # First try the standard method
                member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
                status = member.status
                logger.info(f"Membership check for user {user_id} in {chat_id}: {status} (attempt {attempt+1})")
                
                # Check all possible member statuses
                return status in ['member', 'administrator', 'creator', 'restricted']
            except Exception as e:
                logger.warning(f"Standard membership check failed for {chat_id}: {e}")
                
                # Try alternative method for groups
                try:
                    # Get chat information first
                    chat = await context.bot.get_chat(chat_id)
                    member = await context.bot.get_chat_member(chat_id=chat.id, user_id=user_id)
                    status = member.status
                    logger.info(f"Alternative membership check for user {user_id} in {chat_id}: {status} (attempt {attempt+1})")
                    return status in ['member', 'administrator', 'creator', 'restricted']
                except Exception as e2:
                    logger.error(f"Alternative membership check also failed for {chat_id}: {e2}")
                    
                    # If this is the last attempt, return False
                    if attempt == max_retries - 1:
                        return False
                    
                    # Wait before retrying
                    await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Membership check error for {chat_id}: {e}")
            if attempt == max_retries - 1:
                return False
            await asyncio.sleep(1)
    
    return False

async def check_all_memberships(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user is a member of all required chats"""
    if not REQUIRES_VERIFICATION:
        return True
        
    results = []
    
    if CHANNEL_ID:
        channel_member = await check_membership(user_id, context, CHANNEL_ID)
        results.append(channel_member)
        logger.info(f"User {user_id} channel membership: {channel_member}")
    
    if GROUP_ID:
        group_member = await check_membership(user_id, context, GROUP_ID)
        results.append(group_member)
        logger.info(f"User {user_id} group membership: {group_member}")
    
    return all(results)

# Add restricted decorator to limit bot access
def restricted(func):
    from functools import wraps
    
    @wraps(func)
    async def wrapped(update, context, *args, **kwargs):
        user_id = update.effective_user.id
        
        # Check if user is member of required groups/channels
        is_member = await check_all_memberships(user_id, context)
        if not is_member and REQUIRES_VERIFICATION:
            logger.warning(f"Unauthorized access attempt by user {user_id}")
            await send_verification_request(update, context)
            return
        
        return await func(update, context, *args, **kwargs)
    return wrapped

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        username = update.effective_user.username or "User"
        first_name = update.effective_user.first_name or "Member"
        
        logger.info(f"New user: {user_id} ({username})")
        
        # Check if user exists in DB
        user_data = users_collection.find_one({"user_id": user_id})
        if not user_data:
            users_collection.insert_one({
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "date_added": time.time()
            })
            logger.info(f"Added new user to DB: {user_id}")
        
        # Check if verification is required
        if not REQUIRES_VERIFICATION:
            welcome_message = (
                "╭───❖━❀🌟❀━❖───╮\n"
                f"  𝗪𝗲𝗹𝗰𝗼𝗺𝗲, {first_name}! 🎉\n"
                "╰───❖━❀🌟❀━❖───╯\n\n"
                "🎯 𝗪𝗲'𝗿𝗲 𝗴𝗹𝗮𝗱 𝘁𝗼 𝗵𝗮𝘃𝗲 𝘆𝗼𝘂 𝗵𝗲𝗿𝗲.\n\n"
                "➡️ 𝗨𝘀𝗲 𝘁𝗵𝗲𝘀𝗲 𝗰𝗼𝗺𝗺𝗮𝗻𝗱𝘀:\n\n"
                "📚 `/lecture` - Show all available lecture groups\n"
                "❓ `/help` - Get help with bot commands"
            )
            await update.message.reply_text(
                welcome_message,
                protect_content=True
            )
            logger.info(f"User {user_id} started bot (no verification required)")
            return
        
        # Check membership in all required chats
        is_member = await check_all_memberships(user_id, context)
        if is_member:
            welcome_message = (
                "╭───❖━❀🌟❀━❖───╮\n"
                f"  𝗪𝗲𝗹𝗰𝗼𝗺𝗲, {first_name}! 🎉\n"
                "╰───❖━❀🌟❀━❖───╯\n\n"
                "🙏 𝗧𝗵𝗮𝗻𝗸 𝘆𝗼𝘂 𝗳𝗼𝗿 𝘀𝘂𝗯𝘀𝗰𝗿𝗶𝗯𝗶𝗻𝗴 𝘁𝗼 𝗼𝘂𝗿 𝗰𝗼𝗺𝗺𝘂𝗻𝗶𝘁𝘆!\n"
                "🎯 𝗪𝗲'𝗿𝗲 𝗴𝗹𝗮𝗱 𝘁𝗼 𝗵𝗮𝘃𝗲 𝘆𝗼𝘂 𝗵𝗲𝗿𝗲.\n\n"
                "➡️ 𝗨𝘀𝗲 𝘁𝗵𝗲𝘀𝗲 𝗰𝗼𝗺𝗺𝗮𝗻𝗱𝘀:\n\n"
                "📚 `/lecture` - Show all available lecture groups\n"
                "❓ `/help` - Get help with bot commands"
            )
            await update.message.reply_text(
                welcome_message,
                protect_content=True
            )
            logger.info(f"User {user_id} is verified in all required chats")
        else:
            await send_verification_request(update, context)
            logger.info(f"User {user_id} needs verification")
    except Exception as e:
        logger.error(f"Start command error: {e}")

async def send_verification_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not REQUIRES_VERIFICATION:
        return
        
    keyboard = []
    chat_count = 0
    
    if CHANNEL_ID:
        channel_invite = await generate_invite_link(context, CHANNEL_ID)
        keyboard.append([InlineKeyboardButton("✅ Join Channel", url=channel_invite)])
        chat_count += 1
    
    if GROUP_ID:
        group_invite = await generate_invite_link(context, GROUP_ID)
        keyboard.append([InlineKeyboardButton("✅ Join Group", url=group_invite)])
        chat_count += 1
    
    # Add verification button
    keyboard.append([InlineKeyboardButton("🔄 I've Joined", callback_data="check_membership")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Create appropriate message based on what needs to be joined
    if CHANNEL_ID and GROUP_ID:
        join_message = (
            "⚠️ Please Join Our Channel and Group to Use This Bot!\n\n"
            "📢 Our community provides:\n"
            "— 📝 Important Updates\n"  
            "— 🎁 Free Resources\n"  
            "— 📚 Daily Quiz & Guidance\n"  
            "— ❗ Exclusive Content\n\n"
            "✅ After Joining, tap \"I've Joined\" below to continue!\n\n"
            "🔒 Invite links expire in 5 minutes\n\n"
            "ℹ️ If you've already joined, please wait a moment and try again. "
            "Sometimes it takes a few seconds for the system to update."
        )
    elif CHANNEL_ID:
        join_message = (
            "⚠️ Please Join Our Channel to Use This Bot!\n\n"
            "📢 Our channel provides:\n"
            "— 📝 Important Updates\n"  
            "— 🎁 Free Resources\n"  
            "— 📚 Daily Quiz & Guidance\n"  
            "— ❗ Exclusive Content\n\n"
            "✅ After Joining, tap \"I've Joined\" below to continue!\n\n"
            "🔒 Invite link expires in 5 minutes\n\n"
            "ℹ️ If you've already joined, please wait a moment and try again. "
            "Sometimes it takes a few seconds for the system to update."
        )
    else:  # Only group
        join_message = (
            "⚠️ Please Join Our Group to Use This Bot!\n\n"
            "📢 Our group provides:\n"
            "— 📝 Important Updates\n"  
            "— 🎁 Free Resources\n"  
            "— 📚 Daily Quiz & Guidance\n"  
            "— ❗ Exclusive Content\n\n"
            "✅ After Joining, tap \"I've Joined\" below to continue!\n\n"
            "🔒 Invite link expires in 5 minutes\n\n"
            "ℹ️ If you've already joined, please wait a moment and try again. "
            "Sometimes it takes a few seconds for the system to update."
        )
    
    await update.message.reply_text(
        join_message,
        reply_markup=reply_markup,
        protect_content=True
    )

async def check_membership_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        user_id = query.from_user.id
        
        logger.info(f"Membership check callback from user: {user_id}")
        
        # Check membership in all required chats
        is_member = await check_all_memberships(user_id, context)
        if is_member:
            await query.edit_message_text(
                "✅ Verification successful!\n"
                "Use /lecture to see all available groups or /help for assistance."
            )
            logger.info(f"User {user_id} verified successfully in all required chats")
        else:
            # Find out which chats the user is missing
            missing_chats = []
            
            if CHANNEL_ID:
                channel_member = await check_membership(user_id, context, CHANNEL_ID)
                if not channel_member:
                    missing_chats.append("channel")
            
            if GROUP_ID:
                group_member = await check_membership(user_id, context, GROUP_ID)
                if not group_member:
                    missing_chats.append("group")
            
            # Create a more helpful error message
            if missing_chats:
                error_message = (
                    f"❌ We couldn't verify your membership in the {', '.join(missing_chats)}!\n\n"
                    "This could be because:\n"
                    "1. You haven't joined yet\n"
                    "2. You just joined and the system needs time to update\n"
                    "3. There's a temporary issue with verification\n\n"
                    "Please make sure you've joined and wait a moment before trying again.\n\n"
                    "If the problem persists, please contact support."
                )
            else:
                error_message = (
                    "❌ We couldn't verify your membership!\n\n"
                    "Please make sure you've joined all required chats and wait a moment before trying again.\n\n"
                    "If the problem persists, please contact support."
                )
                
            await query.edit_message_text(error_message)
            logger.info(f"User {user_id} still not in: {', '.join(missing_chats) if missing_chats else 'unknown'}")
    except Exception as e:
        logger.error(f"Callback handler error: {e}")
        await query.edit_message_text("⚠️ Error verifying membership. Please try again.")

# Unified lecture command to list all custom commands with descriptions
@restricted
async def lecture(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        logger.info(f"Lecture command from user: {user_id}")
        
        # Get all custom commands
        commands = list(custom_commands_collection.find({}))
        
        if not commands:
            await update.message.reply_text(
                "📚 No lecture groups available yet. Check back later!",
                protect_content=True
            )
            return
            
        # Create response with all commands and descriptions
        response = "📚 Available Lecture Groups:\n\n"
        for cmd in commands:
            response += f"🔹 /{cmd['command']} - {cmd.get('description', 'No description')}\n\n"
        
        response += "\nUse any command above to join its group!"
        
        await update.message.reply_text(
            response,
            protect_content=True
        )
        logger.info(f"Sent lecture list to user {user_id}")
        
    except Exception as e:
        logger.error(f"Lecture command error: {e}")

# Admin command to add new lecture group command with description
@restricted
async def add_lecture(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        logger.info(f"Addlecture command from user: {user_id}")
        
        if not await is_owner(user_id):
            await update.message.reply_text("❌ This command is for bot owner only!")
            logger.warning(f"Unauthorized addlecture attempt by {user_id}")
            return
        
        if len(context.args) < 3:
            await update.message.reply_text(
                "⚠️ Please provide command name, link, and description.\n"
                "Usage: /addlecture <command_name> <link> <description>\n"
                "Example: /addlecture maths https://t.me/mathsgroup \"Mathematics study group\""
            )
            return
        
        command_name = context.args[0].lower().strip()
        group_link = context.args[1].strip()
        
        # Combine all remaining arguments as description
        description = ' '.join(context.args[2:])
        
        # Validate command name
        if command_name.startswith('/'):
            command_name = command_name[1:]
            
        if not command_name.isalpha():
            await update.message.reply_text("❌ Command name must contain only letters!")
            return
            
        # Save to database with description
        custom_commands_collection.update_one(
            {"command": command_name},
            {"$set": {
                "link": group_link,
                "description": description
            }},
            upsert=True
        )
        
        await update.message.reply_text(
            f"✅ Lecture group command added successfully!\n\n"
            f"🔹 Command: /{command_name}\n"
            f"🔗 Link: {group_link}\n"
            f"📝 Description: {description}\n\n"
            f"Users can now use /{command_name} to join this group."
        )
        logger.info(f"Added lecture command: /{command_name} -> {group_link} ({description})")
        
    except Exception as e:
        logger.error(f"Addlecture command error: {e}")
        await update.message.reply_text("⚠️ Failed to add lecture command. Please try again.")

# Admin command to remove lecture command
@restricted
async def remove_lecture(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        logger.info(f"Removelecture command from user: {user_id}")
        
        if not await is_owner(user_id):
            await update.message.reply_text("❌ This command is for bot owner only!")
            logger.warning(f"Unauthorized removelecture attempt by {user_id}")
            return
        
        if not context.args:
            await update.message.reply_text(
                "⚠️ Please provide a command to remove.\n"
                "Usage: /removelecture <command_name>\n"
                "Example: /removelecture maths"
            )
            return
        
        command_name = context.args[0].lower().strip()
        
        # Remove from database
        result = custom_commands_collection.delete_one({"command": command_name})
        
        if result.deleted_count > 0:
            await update.message.reply_text(f"✅ Command /{command_name} has been removed.")
            logger.info(f"Removed lecture command: /{command_name}")
        else:
            await update.message.reply_text(f"❌ Command /{command_name} not found.")
            logger.info(f"Attempted to remove non-existent command: /{command_name}")
        
    except Exception as e:
        logger.error(f"Removelecture command error: {e}")
        await update.message.reply_text("⚠️ Failed to remove lecture command. Please try again.")

# Handler for custom lecture commands - UPDATED WITH TUTORIAL VIDEO
@restricted
async def lecture_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        command = update.message.text.split()[0][1:].lower()  # Remove slash
        
        logger.info(f"Lecture command from user: {user_id} - /{command}")
        
        # Find command in database
        cmd_data = custom_commands_collection.find_one({"command": command})
        if not cmd_data:
            return  # Not a lecture command
        
        # Create inline buttons for group link and tutorial
        keyboard = [
            [InlineKeyboardButton(f"👉 Join {command.capitalize()} Group 👈", url=cmd_data["link"])],
            [InlineKeyboardButton("📺 Watch Tutorial Video", url=TUTORIAL_VIDEO_LINK)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Get description or use default
        description = cmd_data.get("description", f"Join the {command} group")
        
        await update.message.reply_text(
            f"📚 {description}\n\n"
            "Click the button below to join the group:\n"
            "Need help joining? Watch the tutorial video!",
            reply_markup=reply_markup,
            protect_content=True
        )
        logger.info(f"Sent lecture group link to user {user_id} for /{command}")
    except Exception as e:
        logger.error(f"Lecture command handler error: {e}")

@restricted
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        logger.info(f"Stats command from user: {user_id}")
        
        if not await is_owner(user_id):
            await update.message.reply_text("❌ This command is for bot owner only!")
            logger.warning(f"Unauthorized stats access attempt by {user_id}")
            return
        
        # Calculate ping
        start_time = time.time()
        test_message = await update.message.reply_text("🏓 Pinging...")
        ping_time = (time.time() - start_time) * 1000  # in milliseconds
        
        # Get user count
        user_count = users_collection.count_documents({})
        
        # Get lecture command count
        command_count = custom_commands_collection.count_documents({})
        
        # Get bot uptime
        uptime_seconds = time.time() - bot_start_time
        uptime_str = format_uptime(uptime_seconds)
        
        # Get versions
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        
        try:
            mongo_version = db.command("buildInfo")["version"]
        except Exception as e:
            logger.error(f"Failed to get MongoDB version: {e}")
            mongo_version = "Unknown"
        
        # Get verification requirements
        verification_status = "No verification required"
        if CHANNEL_ID and GROUP_ID:
            verification_status = f"Requires both channel ({CHANNEL_ID}) and group ({GROUP_ID})"
        elif CHANNEL_ID:
            verification_status = f"Requires channel only ({CHANNEL_ID})"
        elif GROUP_ID:
            verification_status = f"Requires group only ({GROUP_ID})"
        
        # Format stats message
        stats_message = (
            "📊 Bot Statistics:\n\n"
            f"🏓 Ping: {ping_time:.2f} ms\n"
            f"👥 Total Users: {user_count}\n"
            f"📚 Lecture Groups: {command_count}\n"
            f"⏱️ Uptime: {uptime_str}\n"
            f"🔐 Verification: {verification_status}\n\n"
            f"🐍 Python: {python_version}\n"
            f"🍃 MongoDB: {mongo_version}"
        )
        
        await test_message.edit_text(stats_message)
        logger.info(f"Admin stats request: {user_count} users, {command_count} commands")
        
    except Exception as e:
        logger.error(f"Stats command error: {e}")

async def run_broadcast(update, context, replied_message, is_forward=False):
    global broadcast_active, broadcast_cancelled
    
    try:
        user_id = update.effective_user.id
        total_users = users_collection.count_documents({})
        success_count = 0
        failed_count = 0
        
        progress_msg = await update.message.reply_text(
            f"📢 Starting {'forward' if is_forward else 'broadcast'} to {total_users} users...\n"
            f"✅ Success: {success_count}\n"
            f"❌ Failed: {failed_count}\n\n"
            f"⏸️ Use /cancel to stop the {'forward' if is_forward else 'broadcast'}"
        )
        
        # Set broadcast as active
        broadcast_active = True
        broadcast_cancelled = False
        
        # Function to send message to a user
        async def send_to_user(user_id, send_func, *args, **kwargs):
            try:
                await send_func(chat_id=user_id, *args, **kwargs)
                return True
            except Exception as e:
                logger.error(f"Failed to send to user {user_id}: {e}")
                return False
        
        for user in users_collection.find():
            # Check if broadcast was cancelled
            if broadcast_cancelled:
                await progress_msg.edit_text(
                    f"❌ {'Forward' if is_forward else 'Broadcast'} cancelled!\n"
                    f"📢 Sent to: {success_count + failed_count} users\n"
                    f"✅ Success: {success_count}\n"
                    f"❌ Failed: {failed_count}"
                )
                broadcast_active = False
                broadcast_cancelled = False
                return
            
            try:
                if is_forward:
                    # Forward the message
                    await context.bot.forward_message(
                        chat_id=user['user_id'],
                        from_chat_id=replied_message.chat_id,
                        message_id=replied_message.message_id,
                        protect_content=True
                    )
                    success_count += 1
                else:
                    if replied_message.text:
                        success = await send_to_user(
                            user['user_id'], 
                            context.bot.send_message,
                            text=replied_message.text,
                            entities=replied_message.entities,
                            parse_mode=None,
                            protect_content=True,
                            disable_web_page_preview=True
                        )
                    elif replied_message.photo:
                        success = await send_to_user(
                            user['user_id'],
                            context.bot.send_photo,
                            photo=replied_message.photo[-1].file_id,
                            caption=replied_message.caption,
                            caption_entities=replied_message.caption_entities,
                            parse_mode=None,
                            protect_content=True
                        )
                    elif replied_message.video:
                        success = await send_to_user(
                            user['user_id'],
                            context.bot.send_video,
                            video=replied_message.video.file_id,
                            caption=replied_message.caption,
                            caption_entities=replied_message.caption_entities,
                            parse_mode=None,
                            protect_content=True
                        )
                    elif replied_message.document:
                        success = await send_to_user(
                            user['user_id'],
                            context.bot.send_document,
                            document=replied_message.document.file_id,
                            caption=replied_message.caption,
                            caption_entities=replied_message.caption_entities,
                            parse_mode=None,
                            protect_content=True
                        )
                    elif replied_message.audio:
                        success = await send_to_user(
                            user['user_id'],
                            context.bot.send_audio,
                            audio=replied_message.audio.file_id,
                            caption=replied_message.caption,
                            caption_entities=replied_message.caption_entities,
                            parse_mode=None,
                            protect_content=True
                        )
                    elif replied_message.voice:
                        success = await send_to_user(
                            user['user_id'],
                            context.bot.send_voice,
                            voice=replied_message.voice.file_id,
                            caption=replied_message.caption,
                            caption_entities=replied_message.caption_entities,
                            parse_mode=None,
                            protect_content=True
                        )
                    elif replied_message.sticker:
                        success = await send_to_user(
                            user['user_id'],
                            context.bot.send_sticker,
                            sticker=replied_message.sticker.file_id,
                            protect_content=True
                        )
                    else:
                        # Fallback: forward the message
                        await context.bot.forward_message(
                            chat_id=user['user_id'],
                            from_chat_id=replied_message.chat_id,
                            message_id=replied_message.message_id,
                            protect_content=True
                        )
                        success = True
                    
                    if success:
                        success_count += 1
                    else:
                        failed_count += 1
                
                # Update progress every 10 sends
                if (success_count + failed_count) % 10 == 0:
                    await progress_msg.edit_text(
                        f"📢 {'Forwarding' if is_forward else 'Broadcasting'} to {total_users} users...\n"
                        f"✅ Success: {success_count}\n"
                        f"❌ Failed: {failed_count}\n\n"
                        f"⏸️ Use /cancel to stop the {'forward' if is_forward else 'broadcast'}"
                    )
                
                # Small delay to avoid rate limiting but allow other tasks to run
                await asyncio.sleep(0.1)
                    
            except Exception as e:
                failed_count += 1
                logger.error(f"Failed to send to user {user['user_id']}: {e}")
        
        await progress_msg.edit_text(
            f"🎉 {'Forward' if is_forward else 'Broadcast'} completed!\n"
            f"📢 Sent to: {total_users} users\n"
            f"✅ Success: {success_count}\n"
            f"❌ Failed: {failed_count}"
        )
        logger.info(f"{'Forward' if is_forward else 'Broadcast'} completed. Success: {success_count}, Failed: {failed_count}")
        
    except Exception as e:
        logger.error(f"{'Fcast' if is_forward else 'Broadcast'} error: {e}")
        await update.message.reply_text(f"⚠️ An error occurred during {'forward' if is_forward else 'broadcast'}.")
    finally:
        # Reset broadcast status
        broadcast_active = False
        broadcast_cancelled = False

@restricted
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global broadcast_task
    
    try:
        user_id = update.effective_user.id
        logger.info(f"Broadcast command from user: {user_id}")
        
        if not await is_owner(user_id):
            await update.message.reply_text("❌ This command is for bot owner only!")
            logger.warning(f"Unauthorized broadcast attempt by {user_id}")
            return
        
        # Check if broadcast is already active
        if broadcast_active:
            await update.message.reply_text("⚠️ A broadcast is already in progress. Please wait for it to finish or use /cancel to stop it.")
            return
        
        # Check if message is a reply
        replied_message = update.message.reply_to_message
        
        if not replied_message and not context.args:
            await update.message.reply_text(
                "⚠️ Please provide a message to broadcast or reply to a message.\n"
                "Usage: /broadcast <your message> OR reply to a message with /broadcast"
            )
            return
        
        if not replied_message:
            # Create a message from text arguments
            message_text = ' '.join(context.args)
            replied_message = type('MockMessage', (), {
                'text': message_text,
                'entities': None,
                'chat_id': update.message.chat_id,
                'message_id': update.message.message_id
            })()
        
        # Run broadcast in background task
        broadcast_task = asyncio.create_task(run_broadcast(update, context, replied_message, is_forward=False))
        
    except Exception as e:
        logger.error(f"Broadcast command error: {e}")
        await update.message.reply_text("⚠️ An error occurred while starting broadcast.")

# New command to forward messages to all users
@restricted
async def fcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global broadcast_task
    
    try:
        user_id = update.effective_user.id
        logger.info(f"Fcast command from user: {user_id}")
        
        if not await is_owner(user_id):
            await update.message.reply_text("❌ This command is for bot owner only!")
            logger.warning(f"Unauthorized fcast attempt by {user_id}")
            return
        
        # Check if broadcast is already active
        if broadcast_active:
            await update.message.reply_text("⚠️ A broadcast is already in progress. Please wait for it to finish or use /cancel to stop it.")
            return
        
        # Check if message is a reply
        replied_message = update.message.reply_to_message
        
        if not replied_message:
            await update.message.reply_text(
                "⚠️ Please reply to a message to forward it.\n"
                "Usage: Reply to a message with /fcast"
            )
            return
        
        # Run forward in background task
        broadcast_task = asyncio.create_task(run_broadcast(update, context, replied_message, is_forward=True))
        
    except Exception as e:
        logger.error(f"Fcast command error: {e}")
        await update.message.reply_text("⚠️ An error occurred while starting forward.")

# Command to cancel ongoing broadcast
@restricted
async def cancel_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global broadcast_active, broadcast_cancelled, broadcast_task
    
    try:
        user_id = update.effective_user.id
        logger.info(f"Cancel command from user: {user_id}")
        
        if not await is_owner(user_id):
            await update.message.reply_text("❌ This command is for bot owner only!")
            logger.warning(f"Unauthorized cancel attempt by {user_id}")
            return
        
        if not broadcast_active:
            await update.message.reply_text("❌ No active broadcast to cancel!")
            return
        
        # Set cancellation flag
        broadcast_cancelled = True
        
        # Wait for task to complete
        if broadcast_task:
            try:
                await asyncio.wait_for(broadcast_task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Broadcast task didn't cancel gracefully")
        
        await update.message.reply_text("⏹️ Broadcast cancelled successfully.")
        logger.info(f"Broadcast cancelled by {user_id}")
        
    except Exception as e:
        logger.error(f"Cancel command error: {e}")
        await update.message.reply_text("⚠️ An error occurred while trying to cancel.")

@restricted
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        is_admin = await is_owner(user_id)
        
        commands = [
            "/start - Begin using the bot",
            "/lecture - Show all lecture groups",
            "/help - Show this help message"
        ]
        
        # Create inline button for tutorial video
        tutorial_button = InlineKeyboardButton(
            "📺 Watch Tutorial Video", 
            url=TUTORIAL_VIDEO_LINK
        )
        reply_markup = InlineKeyboardMarkup([[tutorial_button]])
        
        if is_admin:
            admin_commands = [
                "\n\n👑 Admin Commands:",
                "/addlecture <name> <link> <description> - Add new lecture group",
                "/removelecture <name> - Remove a lecture group",
                "/stats - View bot statistics",
                "/broadcast <message> - Send message to all users (or reply to a message)",
                "/fcast - Forward a message to all users (reply to a message)",
                "/cancel - Cancel ongoing broadcast/forward"
            ]
            commands.extend(admin_commands)
        
        help_message = "\n".join(commands) + "\n\nNeed help using the bot? Watch our tutorial video!"
        
        await update.message.reply_text(
            help_message,
            reply_markup=reply_markup,
            protect_content=True
        )
        logger.info(f"Help command sent to {update.effective_user.id}")
    except Exception as e:
        logger.error(f"Help command error: {e}")

# Handler to ignore commands in groups
async def ignore_group_commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ignore all commands in groups and supergroups"""
    # Simply return without doing anything
    return

def main():
    try:
        # Start Flask health check in a separate thread
        flask_thread = threading.Thread(
            target=lambda: app.run(host='0.0.0.0', port=8080, debug=False, use_reloader=False)
        )
        flask_thread.daemon = True
        flask_thread.start()
        logger.info("Flask health check server started on port 8080")

        # Log verification requirements
        if not REQUIRES_VERIFICATION:
            logger.info("No verification required - bot will work without channel/group membership")
        else:
            if CHANNEL_ID and GROUP_ID:
                logger.info(f"Verification required for both channel {CHANNEL_ID} and group {GROUP_ID}")
            elif CHANNEL_ID:
                logger.info(f"Verification required for channel {CHANNEL_ID}")
            else:
                logger.info(f"Verification required for group {GROUP_ID}")

        # Start Telegram bot
        logger.info("Starting bot application...")
        application = ApplicationBuilder().token(TOKEN).build()
        
        # Add handlers with private chat filter - bot will only respond in private chats
        application.add_handler(CommandHandler("start", start, filters.ChatType.PRIVATE))
        application.add_handler(CommandHandler("lecture", lecture, filters.ChatType.PRIVATE))
        application.add_handler(CommandHandler("addlecture", add_lecture, filters.ChatType.PRIVATE))
        application.add_handler(CommandHandler("removelecture", remove_lecture, filters.ChatType.PRIVATE))
        application.add_handler(CommandHandler("stats", stats, filters.ChatType.PRIVATE))
        application.add_handler(CommandHandler("broadcast", broadcast, filters.ChatType.PRIVATE))
        application.add_handler(CommandHandler("fcast", fcast, filters.ChatType.PRIVATE))
        application.add_handler(CommandHandler("cancel", cancel_broadcast, filters.ChatType.PRIVATE))
        application.add_handler(CommandHandler("help", help_command, filters.ChatType.PRIVATE))
        application.add_handler(CallbackQueryHandler(check_membership_callback))
        
        # Add handler for custom lecture commands with private chat filter
        application.add_handler(MessageHandler(filters.COMMAND & filters.ChatType.PRIVATE, lecture_command_handler))
        
        # Add handler to silently ignore all commands in groups and supergroups
        application.add_handler(MessageHandler(
            filters.COMMAND & (filters.ChatType.GROUP | filters.ChatType.SUPERGROUP), 
            ignore_group_commands
        ))
        
        logger.info("Bot is now polling... (Will only respond in private chats)")
        application.run_polling()
    except Exception as e:
        logger.critical(f"Fatal error in main: {e}")
        exit(1)

if __name__ == '__main__':
    main()