#!/usr/bin/env python3
"""
Telegram converter/uploader bot (2025 Fixed Version)
- Shows titles in captions instead of URLs
- Includes resolution for videos
- Enhanced caption formatting
- Fixed: No 30-second clip, full lecture conversion regardless of duration
"""

import asyncio
import logging
import os
import re
import shutil
import tempfile
import json
import aiohttp
from pathlib import Path
from subprocess import PIPE
from urllib.parse import unquote
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.enums import ParseMode

# ----------------- CONFIG -----------------
API_ID = 29899250
API_HASH = "611d045796c79af3e5ddfa3d6fd536a7"
BOT_TOKEN = "8376613962:AAG6SwgLwQggSyfuGqN1rr6iL8Xaps1qQX8"

WORKDIR = os.environ.get("BOT_WORKDIR", "/tmp/tgbot_converter_2025")
os.makedirs(WORKDIR, exist_ok=True)

FFMPEG = shutil.which("ffmpeg") or "ffmpeg"
FFPROBE = shutil.which("ffprobe") or "ffprobe"

# Large timeout for complete downloads
LARGE_TIMEOUT = 943488

# REMOVED: CONVERSION_TIMEOUT - No time limit for conversions

# Enhanced URL regex patterns
URL_RE = re.compile(r'https?://[^\s<>"\']+|www\.[^\s<>"\']+', re.IGNORECASE)
MPD_RE = re.compile(r'\.mpd(\?[^\s]*)?\b', re.IGNORECASE)
PDF_RE = re.compile(r'\.pdf(\?[^\s]*)?\b', re.IGNORECASE)

# Quality options
QUALITY_OPTIONS = {
    "360p": "640x360",
    "480p": "854x480", 
    "720p": "1280x720",
    "1080p": "1920x1080",
    "Original": "original"
}

# User sessions to store quality preferences
user_sessions = {}

# Track active progress messages to avoid editing deleted messages
active_progress_messages = set()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("converter-bot-2025")

app = Client("converter-bot-2025", api_id=API_ID, api_hash=API_HASH, 
             bot_token=BOT_TOKEN, workdir=WORKDIR)

# ----------------- Utilities -----------------
def make_progress_bar(current: int, total: int, length: int = 10) -> str:
    """Create visual progress bar"""
    try:
        if total <= 0:
            filled = 0
            percent = 0
        else:
            filled = int(length * current / total)
            percent = min(100, int((current * 100) / total))
    except Exception:
        filled = 0
        percent = 0
    
    filled = max(0, min(length, filled))
    bar = "🟩" * filled + "⬜️" * (length - filled)
    return f"[{bar}] {percent}%"

def resolution_from_height(height: int) -> tuple[int, int]:
    """Get optimized resolution for thumbnails"""
    mapping = {
        144: (256, 144),
        240: (426, 240),
        360: (640, 360),
        480: (854, 480),
        720: (1280, 720),
        1080: (1920, 1080),
        1440: (2560, 1440),
        2160: (3840, 2160),
    }
    return mapping.get(height, (640, 360))

def extract_title_from_line(line: str) -> str:
    """Extract title from a line in the format 'Title — URL'"""
    try:
        # Split on the em dash or hyphen used as separator
        parts = re.split(r' [—–-] ', line.strip(), maxsplit=1)
        if len(parts) >= 1:
            title = parts[0].strip()
            # Clean up any extra spaces
            title = re.sub(r'\s+', ' ', title)
            if title:
                return title
        return "Downloaded Content"
    except Exception:
        return "Downloaded Content"

async def get_video_resolution(file_path: Path) -> str:
    """Get video resolution from file"""
    try:
        cmd = [
            FFPROBE, "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height",
            "-of", "csv=p=0",
            str(file_path)
        ]
        
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        out, err = await proc.communicate()
        
        if proc.returncode == 0:
            resolution = out.decode().strip()
            if ',' in resolution:
                width, height = resolution.split(',')
                return f"{width}x{height}"
    except Exception as e:
        logger.warning(f"Could not get video resolution: {e}")
    
    return "Unknown Resolution"

async def run_ffprobe_get_video_stream(file_path: str) -> dict | None:
    """Get video stream info safely"""
    if not FFPROBE:
        return None
    
    cmd = [
        FFPROBE, "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        "-select_streams", "v:0",
        file_path
    ]
    
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        out, err = await proc.communicate()
        
        if proc.returncode != 0:
            logger.warning(f"FFprobe failed: {err.decode()[:500]}")
            return None
            
        data = json.loads(out.decode())
        return data.get("streams", [{}])[0] if data.get("streams") else None
        
    except Exception as e:
        logger.error(f"FFprobe error: {e}")
        return None

async def safe_delete(file_path: Path):
    """Safely delete files"""
    try:
        if file_path.exists():
            file_path.unlink()
    except Exception as e:
        logger.warning(f"Could not delete {file_path}: {e}")

# ----------------- Auto-delete helper -----------------
async def _del_later(msg, delay: int = 3):
    await asyncio.sleep(delay)
    try:
        # Remove from active messages before deleting
        if msg.id in active_progress_messages:
            active_progress_messages.remove(msg.id)
        await msg.delete()
    except Exception:
        pass

def schedule_delete(msg, delay: int = 3, exempt_pdf: bool = False):
    """Schedule deletion for temporary messages except PDFs."""
    if exempt_pdf:
        return
    # Fire-and-forget
    try:
        asyncio.create_task(_del_later(msg, delay))
    except Exception:
        pass

# ----------------- Quality Selection -----------------
def get_quality_keyboard():
    """Create inline keyboard for quality selection"""
    buttons = []
    row = []
    
    for quality, resolution in QUALITY_OPTIONS.items():
        row.append(InlineKeyboardButton(quality, callback_data=f"quality_{quality}"))
        if len(row) == 2:  # 2 buttons per row
            buttons.append(row)
            row = []
    
    if row:  # Add remaining buttons if any
        buttons.append(row)
    
    return InlineKeyboardMarkup(buttons)

@app.on_callback_query(filters.regex(r"^quality_"))
async def handle_quality_selection(client, callback_query):
    """Handle quality selection from user"""
    user_id = callback_query.from_user.id
    quality = callback_query.data.replace("quality_", "")
    
    if quality in QUALITY_OPTIONS:
        user_sessions[user_id] = quality
        await callback_query.message.edit_text(
            f"✅ **Quality set to: {quality}**\n\n"
            f"Now processing your URLs with {quality} quality...",
            parse_mode=ParseMode.MARKDOWN
        )
        await callback_query.answer(f"Quality set to {quality}")
    else:
        await callback_query.answer("Invalid quality selection")

# ----------------- Download/Convert/Send Helpers -----------------
async def download_file_stream(session: aiohttp.ClientSession, url: str, 
                              dest_path: Path, message: Message):
    """Download file with progress tracking"""
    logger.info(f"Downloading: {url} -> {dest_path}")
    
    async with session.get(url) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        downloaded = 0
        
        progress_msg = await message.reply_text(
            f"**Downloading:** `{dest_path.name}`\n"
            f"{make_progress_bar(0, total)}",
            parse_mode=ParseMode.MARKDOWN
        )
        active_progress_messages.add(progress_msg.id)
        schedule_delete(progress_msg, delay=3, exempt_pdf=False)
        
        with open(dest_path, "wb") as f:
            async for chunk in resp.content.iter_chunked(64 * 1024):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Update progress every 1MB or 5%
                    if downloaded % (1024 * 1024) == 0 or (
                        total > 0 and (downloaded * 100 // total) % 5 == 0
                    ):
                        try:
                            if progress_msg.id in active_progress_messages:
                                await progress_msg.edit_text(
                                    f"**Downloading:** `{dest_path.name}`\n"
                                    f"{make_progress_bar(downloaded, total)}",
                                    parse_mode=ParseMode.MARKDOWN
                                )
                        except Exception:
                            pass
        
        try:
            if progress_msg.id in active_progress_messages:
                active_progress_messages.remove(progress_msg.id)
                await progress_msg.delete()
        except Exception:
            pass
        
    return dest_path

async def convert_mpd_to_mp4(mpd_url: str, out_path: Path, message: Message, quality: str = "Original"):
    """Convert MPD to MP4 with selected quality - NO TIME LIMIT"""
    temp_out = out_path.with_suffix(".temp.mp4")
    
    # Build FFmpeg command based on quality
    if quality == "Original":
        # Fast copy for original quality
        cmd = [
            FFMPEG, "-y",
            "-protocol_whitelist", "file,https,tcp,tls,crypto",
            "-i", mpd_url,
            "-c", "copy",
            "-movflags", "+faststart",
            str(temp_out)
        ]
        progress_text = "🔄 Converting with original quality..."
    else:
        # Re-encode for specific quality
        resolution = QUALITY_OPTIONS[quality]
        cmd = [
            FFMPEG, "-y",
            "-protocol_whitelist", "file,https,tcp,tls,crypto",
            "-i", mpd_url,
            "-vf", f"scale={resolution}",
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "23",
            "-c:a", "aac",
            "-b:a", "128k",
            "-movflags", "+faststart",
            str(temp_out)
        ]
        progress_text = f"🔄 Converting to {quality} ({resolution})..."
    
    progress_msg = await message.reply_text(progress_text)
    active_progress_messages.add(progress_msg.id)
    schedule_delete(progress_msg, delay=3, exempt_pdf=False)
    
    try:
        # Run conversion WITHOUT timeout - let it complete fully
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        out, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            error_msg = (stderr.decode()[:1000] if stderr else "Unknown error")
            raise RuntimeError(f"FFmpeg failed: {error_msg}")
        
        # If output file looks valid, move into place
        if temp_out.exists() and temp_out.stat().st_size > 0:
            temp_out.rename(out_path)
            try:
                if progress_msg.id in active_progress_messages:
                    await progress_msg.edit_text(f"✅ Conversion to {quality} successful!")
            except Exception:
                pass
            return out_path
        else:
            raise RuntimeError("Conversion produced empty file")
            
    except Exception as e:
        try:
            if progress_msg.id in active_progress_messages:
                await progress_msg.edit_text(f"❌ Conversion failed: {e}")
        except Exception:
            pass
        raise
    finally:
        # Clean up progress message tracking
        if progress_msg.id in active_progress_messages:
            active_progress_messages.remove(progress_msg.id)
        await safe_delete(temp_out)

async def create_thumbnail(video_path: Path, thumb_path: Path, at_seconds: int = 3):
    """Create optimized thumbnail"""
    stream_info = await run_ffprobe_get_video_stream(str(video_path))
    
    if stream_info:
        height = stream_info.get("height", 360)
        width, height = resolution_from_height(height)
    else:
        width, height = 640, 360
    
    # Simple thumbnail filter
    vf_filter = f"scale={width}:{height}:force_original_aspect_ratio=decrease"
    
    cmd = [
        FFMPEG, "-y",
        "-ss", str(at_seconds),
        "-i", str(video_path),
        "-frames:v", "1",
        "-vf", vf_filter,
        "-q:v", "2",
        str(thumb_path)
    ]
    
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        await proc.communicate()
        return thumb_path if thumb_path.exists() else None
    except Exception as e:
        logger.error(f"Thumbnail creation failed: {e}")
        return None

async def send_file_with_progress(message: Message, file_path: Path, 
                                 title: str = "", file_type: str = "", 
                                 resolution: str = "", thumb_path: Path = None):
    """Send file with progress tracking and ensure local deletion afterwards"""
    if not file_path.exists():
        m = await message.reply_text("❌ File not found for upload")
        schedule_delete(m, delay=3, exempt_pdf=False)
        return

    file_size = file_path.stat().st_size
    progress_msg = await message.reply_text(
        f"**Uploading:** `{file_path.name}`\n"
        f"{make_progress_bar(0, file_size)}",
        parse_mode=ParseMode.MARKDOWN
    )
    active_progress_messages.add(progress_msg.id)
    schedule_delete(progress_msg, delay=3, exempt_pdf=False)

    last_update = 0
    
    async def progress_callback(current, total):
        nonlocal last_update
        # Update every 5% or 10MB
        if (total > 0 and (current - last_update) > total * 0.05) or (current - last_update) > 10 * 1024 * 1024:
            last_update = current
            try:
                if progress_msg.id in active_progress_messages:
                    await progress_msg.edit_text(
                        f"**Uploading:** `{file_path.name}`\n"
                        f"{make_progress_bar(current, total)}",
                        parse_mode=ParseMode.MARKDOWN
                    )
            except Exception:
                pass

    # Create enhanced caption
    caption_parts = []
    if title:
        caption_parts.append(f"📁 **{title}**")
    
    if file_type:
        caption_parts.append(f"📄 **Type:** {file_type}")
    
    if resolution and resolution != "Unknown Resolution":
        caption_parts.append(f"🎬 **Resolution:** {resolution}")
    
    caption = "\n".join(caption_parts) if caption_parts else "📦 Downloaded File"

    try:
        if file_path.suffix.lower() in ['.mp4', '.avi', '.mkv', '.mov']:
            # Send as video
            await message.reply_video(
                video=str(file_path),
                thumb=str(thumb_path) if thumb_path and thumb_path.exists() else None,
                caption=caption[:1024],
                duration=0,  # Will be auto-detected
                supports_streaming=True,
                progress=progress_callback
            )
        else:
            # Send as document
            m = await message.reply_document(
                document=str(file_path),
                thumb=str(thumb_path) if thumb_path and thumb_path.exists() else None,
                caption=caption[:1024],
                progress=progress_callback
            )
            # If it's a PDF document, do NOT auto-delete the announcement/messages
            if file_path.suffix.lower() == '.pdf':
                schedule_delete(m, delay=0, exempt_pdf=True)
    except Exception as e:
        await message.reply_text(f"❌ Upload failed: {e}")
        logger.error(f"Upload error: {e}")
    finally:
        try:
            if progress_msg.id in active_progress_messages:
                active_progress_messages.remove(progress_msg.id)
                await progress_msg.delete()
        except Exception:
            pass
        # Ensure local file is removed after send (as requested)
        try:
            await safe_delete(file_path)
        except Exception:
            pass

# ----------------- Handlers -----------------
@app.on_message(filters.command("start") & filters.private)
async def start_command(_, message: Message):
    """Start command handler"""
    user_name = message.from_user.first_name or "User"
    
    welcome_text = f"""
🌟 **Welcome {user_name}!** 🌟

I can help you convert and upload files from text links:

✅ **MPD URLs** → MP4 videos (with quality selection)
✅ **PDF URLs** → Direct downloads  
✅ **Other files** → Direct uploads

**How to use:**
1. Send me a `.txt` file containing URLs
2. Select your preferred video quality
3. I'll process each link automatically
4. Receive converted/uploaded files

**Features:**
- Quality selection (360p, 480p, 720p, 1080p, Original)
- Progress tracking for all operations
- Automatic thumbnail generation
- Support for large files

Send me a .txt file to get started! 🚀
    """
    m = await message.reply_text(welcome_text, parse_mode=ParseMode.MARKDOWN)
    # welcome is informational; schedule deletion (user asked extra msgs to be removed) but keep welcome briefly
    schedule_delete(m, delay=3, exempt_pdf=False)

@app.on_message(filters.document & filters.private)
async def handle_text_file(client: Client, message: Message):
    """Main handler for text files with URLs"""
    if not message.document or not message.document.file_name.lower().endswith('.txt'):
        m = await message.reply_text("📄 Please send a .txt file containing URLs")
        schedule_delete(m, delay=3, exempt_pdf=False)
        return

    user_id = message.from_user.id
    
    # Ask for quality selection if not already set
    if user_id not in user_sessions:
        m = await message.reply_text(
            "🎬 **Please select video quality:**\n\n"
            "• **Original**: Keep original quality (fastest)\n"
            "• **1080p/720p**: High quality\n" 
            "• **480p/360p**: Lower size, faster processing",
            reply_markup=get_quality_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
        schedule_delete(m, delay=3, exempt_pdf=False)
        return

    # Create temporary directory
    temp_dir = Path(tempfile.mkdtemp(dir=WORKDIR))
    
    try:
        # Download the text file
        txt_file = temp_dir / "urls.txt"
        m = await message.reply_text("📥 Downloading your text file...")
        schedule_delete(m, delay=3, exempt_pdf=False)
        await client.download_media(message, file_name=str(txt_file))
        
        # Read and parse URLs
        content = txt_file.read_text(encoding='utf-8', errors='ignore')
        lines = content.strip().split('\n')
        
        # Extract titles and URLs from each line
        entries = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Extract title (part before — or -)
            title = extract_title_from_line(line)
            
            # Extract URL using regex
            url_match = URL_RE.search(line)
            if url_match:
                url = url_match.group(0)
                entries.append((title, url))
        
        if not entries:
            m = await message.reply_text("❌ No valid URLs found in the file")
            schedule_delete(m, delay=3, exempt_pdf=False)
            return
            
        selected_quality = user_sessions.get(user_id, "Original")
        m = await message.reply_text(
            f"🔍 Found {len(entries)} unique URLs.\n"
            f"🎯 **Quality:** {selected_quality}\n"
            f"🚀 Starting processing..."
        )
        schedule_delete(m, delay=3, exempt_pdf=False)
        
        # Use large timeout
        timeout = aiohttp.ClientTimeout(total=LARGE_TIMEOUT)
        connector = aiohttp.TCPConnector(limit=3)  # Reduced limit for stability
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            for idx, (title, url) in enumerate(entries, 1):
                try:
                    # PDF handling
                    if PDF_RE.search(url):
                        m = await message.reply_text(f"📚 [{idx}/{len(entries)}] Processing PDF: {title}")
                        # PDF messages are exempt from auto-delete
                        schedule_delete(m, delay=0, exempt_pdf=True)
                        pdf_file = temp_dir / f"document_{idx}.pdf"
                        
                        await download_file_stream(session, url, pdf_file, message)
                        await send_file_with_progress(
                            message, pdf_file, 
                            title=title,
                            file_type="PDF Document"
                        )
                        await safe_delete(pdf_file)
                    
                    # MPD video handling
                    elif MPD_RE.search(url):
                        m = await message.reply_text(
                            f"🎬 [{idx}/{len(entries)}] Converting video: {title} to {selected_quality}"
                        )
                        schedule_delete(m, delay=3, exempt_pdf=False)
                        video_file = temp_dir / f"video_{idx}.mp4"
                        thumb_file = temp_dir / f"thumb_{idx}.jpg"
                        
                        try:
                            # Convert WITHOUT timeout - full conversion
                            await convert_mpd_to_mp4(url, video_file, message, selected_quality)
                            
                            # Get actual resolution
                            actual_resolution = await get_video_resolution(video_file)
                            
                            # Create thumbnail
                            thumb_result = await create_thumbnail(video_file, thumb_file)
                            if not thumb_result:
                                logger.warning("Thumbnail creation failed")
                            
                            await send_file_with_progress(
                                message, video_file,
                                title=title,
                                file_type="Video",
                                resolution=actual_resolution,
                                thumb_path=thumb_file if thumb_result else None
                            )
                            
                        except Exception as e:
                            await message.reply_text(f"❌ Video conversion failed: {e}")
                            logger.error(f"Video conversion error: {e}")
                        finally:
                            await safe_delete(video_file)
                            await safe_delete(thumb_file)
                    
                    # Generic file handling
                    else:
                        m = await message.reply_text(f"📦 [{idx}/{len(entries)}] Downloading: {title}")
                        schedule_delete(m, delay=3, exempt_pdf=False)
                        file_ext = Path(url.split('?')[0]).suffix or '.bin'
                        generic_file = temp_dir / f"file_{idx}{file_ext}"
                        
                        await download_file_stream(session, url, generic_file, message)
                        
                        # Determine file type
                        file_type = "Document"
                        if file_ext.lower() in ['.jpg', '.jpeg', '.png', '.gif', '.bmp']:
                            file_type = "Image"
                        elif file_ext.lower() in ['.mp3', '.wav', '.flac', '.aac']:
                            file_type = "Audio"
                        
                        await send_file_with_progress(
                            message, generic_file,
                            title=title,
                            file_type=file_type
                        )
                        await safe_delete(generic_file)
                        
                except Exception as e:
                    error_msg = f"❌ Error processing URL {idx}: {str(e)[:200]}"
                    m_err = await message.reply_text(error_msg)
                    schedule_delete(m_err, delay=3, exempt_pdf=False)
                    logger.error(f"URL processing error: {e}")
                
                await asyncio.sleep(2)  # Increased pause for stability
        
        m_done = await message.reply_text("✅ All tasks completed! Ready for more files.")
        schedule_delete(m_done, delay=3, exempt_pdf=False)
        
    except Exception as e:
        m_err = await message.reply_text(f"❌ Critical error: {e}")
        schedule_delete(m_err, delay=3, exempt_pdf=False)
        logger.error(f"Main handler error: {e}")
        
    finally:
        # Cleanup
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as e:
            logger.warning(f"Cleanup error: {e}")

# ----------------- Main Execution -----------------
if __name__ == "__main__":
    print("🚀 Starting Fixed Converter Bot (2025 Version)...")
    print(f"📁 Work directory: {WORKDIR}")
    print(f"⏰ Timeout set to: {LARGE_TIMEOUT} seconds")
    print("✅ Bot is ready and running!")
    
    try:
        app.run()
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")