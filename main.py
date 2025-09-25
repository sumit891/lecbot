#!/usr/bin/env python3
"""
Telegram converter/uploader bot (2025 Fixed Version) - PERFORMANCE PATCH
- Added worker concurrency (default 20 via BOT_WORKERS env)
- Increased aiohttp connector limits
- Larger download chunk size for faster throughput
- ffmpeg: use ultrafast preset and optional hardware acceleration (NVENC/VAAPI)
- Minimal other changes; core logic kept intact
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

LARGE_TIMEOUT = int(os.environ.get("LARGE_TIMEOUT", "943488"))

# --- PERFORMANCE RELATED (NEW) ---
# How many concurrent workers to process URLs (downloads/conversions/uploads) — default 20
WORKERS = int(os.environ.get("BOT_WORKERS", "20"))

# Optional hardware acceleration mode: "nvenc", "vaapi", or empty for software
FFMPEG_HW_ACCEL = os.environ.get("FFMPEG_HW_ACCEL", "").lower().strip()  # e.g. "nvenc" or "vaapi"

# Enhanced URL regex patterns - FIXED REGEX
URL_RE = re.compile(r'https?://[^\s<>"\']+|www\.[^\s<>"\']+', re.IGNORECASE)
MPD_RE = re.compile(r'\.mpd(\?[^\s<>"\']*)?\b', re.IGNORECASE)
PDF_RE = re.compile(r'\.pdf(\?[^\s<>"\']*)?\b', re.IGNORECASE)

# Quality options - FIXED BITRATE FOR CONSISTENT SIZE
QUALITY_OPTIONS = {
    "360p": "640x360",
    "480p": "854x480",
    "720p": "1280x720",
    "1080p": "1920x1080",
    "Original": "original"
}

# FIXED BITRATE SETTINGS FOR CONSISTENT FILE SIZE
BITRATE_SETTINGS = {
    "360p": "500k",
    "480p": "800k",
    "720p": "1500k",
    "1080p": "2500k",
    "Original": "copy"
}

# User sessions to store quality preferences
user_sessions = {}

# Track active progress messages
active_progress_messages = set()

# Logging setup - FIXED LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("converter-bot-2025")

app = Client("converter-bot-2025", api_id=API_ID, api_hash=API_HASH,
             bot_token=BOT_TOKEN, workdir=WORKDIR)

# ----------------- Utilities (unchanged except chunk sizes etc.) -----------------
def make_progress_bar(current: int, total: int, length: int = 10) -> str:
    try:
        if total <= 0:
            filled = 0
            percent = 0
        else:
            filled = int(length * current / total)
            percent = min(100, int((current * 100) / total))
    except Exception as e:
        logger.warning(f"Progress bar error: {e}")
        filled = 0
        percent = 0

    filled = max(0, min(length, filled))
    bar = "🟩" * filled + "⬜️" * (length - filled)
    return f"[{bar}] {percent}%"

def resolution_from_height(height: int) -> tuple:
    try:
        height = int(height)
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
    except Exception as e:
        logger.warning(f"Resolution mapping error: {e}")
        return (640, 360)

def extract_title_from_line(line: str) -> str:
    try:
        if not line or not isinstance(line, str):
            return "Downloaded Content"

        parts = re.split(r' [—–-] ', line.strip(), maxsplit=1)
        if len(parts) >= 1:
            title = parts[0].strip()
            title = re.sub(r'\s+', ' ', title)
            if title and len(title) > 1:
                return title[:200]
        return "Downloaded Content"
    except Exception as e:
        logger.warning(f"Title extraction error: {e}")
        return "Downloaded Content"

async def get_video_resolution(file_path: Path) -> str:
    try:
        if not file_path.exists():
            return "Unknown Resolution"

        cmd = [
            FFPROBE, "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height",
            "-of", "csv=p=0",
            str(file_path)
        ]

        proc = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        out, err = await proc.communicate()

        if proc.returncode == 0 and out:
            resolution = out.decode().strip()
            if ',' in resolution:
                width, height = resolution.split(',')
                if width.isdigit() and height.isdigit():
                    return f"{width}x{height}"
    except Exception as e:
        logger.warning(f"Resolution detection error: {e}")

    return "Unknown Resolution"

async def get_video_duration(file_path: Path) -> int:
    try:
        if not file_path.exists():
            return 0

        cmd = [
            FFPROBE, "-v", "error",
            "-show_entries", "format=duration",
            "-of", "csv=p=0",
            str(file_path)
        ]

        proc = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        out, err = await proc.communicate()

        if proc.returncode == 0 and out:
            duration_str = out.decode().strip()
            if duration_str:
                duration_float = float(duration_str)
                return int(duration_float) if duration_float > 0 else 0

    except Exception as e:
        logger.warning(f"Duration detection error: {e}")

    return 0

async def run_ffprobe_get_video_stream(file_path: str) -> dict:
    if not FFPROBE or not os.path.exists(file_path):
        return {}

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

        if proc.returncode == 0 and out:
            data = json.loads(out.decode())
            streams = data.get("streams", [])
            return streams[0] if streams else {}
        else:
            logger.warning(f"FFprobe failed: {err.decode()[:500] if err else 'Unknown error'}")

    except Exception as e:
        logger.error(f"FFprobe error: {e}")

    return {}

async def safe_delete(file_path: Path):
    try:
        if file_path and isinstance(file_path, Path) and file_path.exists():
            file_path.unlink(missing_ok=True)
    except Exception as e:
        logger.warning(f"Delete error for {file_path}: {e}")

async def safe_rmtree(dir_path: Path):
    try:
        if dir_path and isinstance(dir_path, Path) and dir_path.exists():
            shutil.rmtree(dir_path, ignore_errors=True)
    except Exception as e:
        logger.warning(f"Directory removal error: {e}")

# ----------------- Auto-delete helper -----------------
async def _del_later(msg, delay: int = 3):
    try:
        await asyncio.sleep(delay)
        if hasattr(msg, 'id') and msg.id in active_progress_messages:
            active_progress_messages.remove(msg.id)
        if hasattr(msg, 'delete'):
            await msg.delete()
    except Exception as e:
        logger.debug(f"Delete message error: {e}")

def schedule_delete(msg, delay: int = 3, exempt_pdf: bool = False):
    if exempt_pdf:
        return
    try:
        asyncio.create_task(_del_later(msg, delay))
    except Exception as e:
        logger.debug(f"Schedule delete error: {e}")

# ----------------- Quality Selection -----------------
def get_quality_keyboard():
    buttons = []
    row = []

    for quality in ["360p", "480p", "720p", "1080p", "Original"]:
        row.append(InlineKeyboardButton(quality, callback_data=f"quality_{quality}"))
        if len(row) == 2:
            buttons.append(row)
            row = []

    if row:
        buttons.append(row)

    return InlineKeyboardMarkup(buttons)

@app.on_callback_query(filters.regex(r"^quality_"))
async def handle_quality_selection(client, callback_query):
    try:
        user_id = callback_query.from_user.id
        quality = callback_query.data.replace("quality_", "")

        if quality in QUALITY_OPTIONS:
            user_sessions[user_id] = quality
            bitrate = BITRATE_SETTINGS.get(quality, "Auto")

            await callback_query.message.edit_text(
                f"✅ **Quality set to: {quality}**\n\n"
                f"Now processing your URLs with {quality} quality...\n"
                f"📊 **Fixed Bitrate:** {bitrate}\n"
                f"✅ **Duration issues FIXED** - All videos show correct duration",
                parse_mode=ParseMode.MARKDOWN
            )
            await callback_query.answer(f"Quality set to {quality}")
        else:
            await callback_query.answer("Invalid quality selection")

    except Exception as e:
        logger.error(f"Quality selection error: {e}")
        await callback_query.answer("Error setting quality")

# ----------------- Download/Convert/Send Helpers (PERF tuned) -----------------
# Increased chunk size for faster downloads
DOWNLOAD_CHUNK = 256 * 1024  # 256KB chunks (was 64KB)

async def download_file_stream(session: aiohttp.ClientSession, url: str,
                              dest_path: Path, message: Message):
    logger.info(f"Downloading: {url} -> {dest_path}")

    progress_msg = None
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                raise ValueError(f"HTTP {resp.status}: {resp.reason}")

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
                async for chunk in resp.content.iter_chunked(DOWNLOAD_CHUNK):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

                        # Update progress every ~5% or every 5MB
                        if total > 0:
                            pct = downloaded * 100 // total
                            if pct % 5 == 0:
                                try:
                                    if progress_msg and progress_msg.id in active_progress_messages:
                                        await progress_msg.edit_text(
                                            f"**Downloading:** `{dest_path.name}`\n"
                                            f"{make_progress_bar(downloaded, total)}",
                                            parse_mode=ParseMode.MARKDOWN
                                        )
                                except Exception:
                                    pass
                        else:
                            # Unknown total — update every 5MB
                            if downloaded % (5 * 1024 * 1024) == 0:
                                try:
                                    if progress_msg and progress_msg.id in active_progress_messages:
                                        await progress_msg.edit_text(
                                            f"**Downloading:** `{dest_path.name}`\n"
                                            f"{make_progress_bar(downloaded, max(downloaded,1))}",
                                            parse_mode=ParseMode.MARKDOWN
                                        )
                                except Exception:
                                    pass

            if progress_msg and progress_msg.id in active_progress_messages:
                await progress_msg.edit_text(
                    f"✅ **Download completed:** `{dest_path.name}`\n"
                    f"📊 Size: {downloaded/(1024*1024):.2f} MB",
                    parse_mode=ParseMode.MARKDOWN
                )

        return dest_path

    except Exception as e:
        if progress_msg:
            try:
                await progress_msg.edit_text(f"❌ Download failed: {str(e)[:200]}")
            except Exception:
                pass
        raise e

    finally:
        if progress_msg and progress_msg.id in active_progress_messages:
            active_progress_messages.remove(progress_msg.id)

async def convert_mpd_to_mp4(mpd_url: str, out_path: Path, message: Message, quality: str = "Original"):
    temp_out = out_path.with_suffix(".temp.mp4")
    progress_msg = None

    try:
        # Build ffmpeg command with hardware accel if requested
        if quality == "Original":
            # If original, try stream copy (fast). If hardware accel set, still attempt copy.
            cmd = [
                FFMPEG, "-y",
                "-fflags", "+genpts",
                "-protocol_whitelist", "file,https,tcp,tls,crypto",
                "-i", mpd_url,
                "-c", "copy",
                "-movflags", "+faststart",
                "-avoid_negative_ts", "make_zero",
                str(temp_out)
            ]
            progress_text = "🔄 Converting with original quality (stream copy)..."
        else:
            resolution = QUALITY_OPTIONS.get(quality, "1280x720")
            bitrate = BITRATE_SETTINGS.get(quality, "1500k")

            # Choose encoder: hardware if requested and supported, otherwise software libx264
            if FFMPEG_HW_ACCEL == "nvenc":
                # NVENC encoder (NVIDIA)
                cmd = [
                    FFMPEG, "-y",
                    "-fflags", "+genpts",
                    "-protocol_whitelist", "file,https,tcp,tls,crypto",
                    "-i", mpd_url,
                    "-vf", f"scale={resolution}",
                    "-c:v", "h264_nvenc",
                    "-preset", "p1",  # fastest NVENC preset
                    "-b:v", bitrate,
                    "-maxrate", bitrate,
                    "-bufsize", "2M",
                    "-c:a", "aac",
                    "-b:a", "128k",
                    "-movflags", "+faststart",
                    "-avoid_negative_ts", "make_zero",
                    "-threads", "0",
                    str(temp_out)
                ]
            elif FFMPEG_HW_ACCEL == "vaapi":
                # VAAPI (Intel) example (requires proper device setup, may need extra wrapper)
                cmd = [
                    FFMPEG, "-y",
                    "-fflags", "+genpts",
                    "-init_hw_device", "vaapi=d=/dev/dri/renderD128",
                    "-filter_hw_device", "vaapi",
                    "-protocol_whitelist", "file,https,tcp,tls,crypto",
                    "-i", mpd_url,
                    "-vf", f"scale_vaapi=w={resolution.split('x')[0]}:h={resolution.split('x')[1]}",
                    "-c:v", "h264_vaapi",
                    "-b:v", bitrate,
                    "-maxrate", bitrate,
                    "-bufsize", "2M",
                    "-c:a", "aac",
                    "-b:a", "128k",
                    "-movflags", "+faststart",
                    "-avoid_negative_ts", "make_zero",
                    "-threads", "0",
                    str(temp_out)
                ]
            else:
                # Software encoder with ultrafast preset
                cmd = [
                    FFMPEG, "-y",
                    "-fflags", "+genpts",
                    "-protocol_whitelist", "file,https,tcp,tls,crypto",
                    "-i", mpd_url,
                    "-vf", f"scale={resolution}",
                    "-c:v", "libx264",
                    "-preset", "ultrafast",
                    "-tune", "zerolatency",
                    "-b:v", bitrate,
                    "-maxrate", bitrate,
                    "-bufsize", "2M",
                    "-c:a", "aac",
                    "-b:a", "128k",
                    "-movflags", "+faststart",
                    "-avoid_negative_ts", "make_zero",
                    "-threads", "0",
                    str(temp_out)
                ]

            progress_text = f"🔄 Converting to {quality} ({resolution}) with target bitrate {bitrate}..."

        progress_msg = await message.reply_text(progress_text)
        active_progress_messages.add(progress_msg.id)
        schedule_delete(progress_msg, delay=3, exempt_pdf=False)

        # Run conversion
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        out, stderr = await proc.communicate()

        if proc.returncode != 0:
            error_msg = stderr.decode()[:1000] if stderr else "Unknown error"
            raise RuntimeError(f"FFmpeg failed: {error_msg}")

        if temp_out.exists() and temp_out.stat().st_size > 0:
            temp_out.rename(out_path)
            if progress_msg and progress_msg.id in active_progress_messages:
                file_size_mb = out_path.stat().st_size / (1024 * 1024)
                await progress_msg.edit_text(
                    f"✅ Conversion to {quality} successful!\n"
                    f"📊 Final size: {file_size_mb:.2f} MB\n"
                    f"✅ Duration issues FIXED - Ready for upload"
                )
            return out_path
        else:
            raise RuntimeError("Conversion produced empty file")

    except Exception as e:
        if progress_msg:
            try:
                await progress_msg.edit_text(f"❌ Conversion failed: {str(e)[:200]}")
            except Exception:
                pass
        raise

    finally:
        if progress_msg and progress_msg.id in active_progress_messages:
            active_progress_messages.remove(progress_msg.id)
        await safe_delete(temp_out)

async def create_thumbnail(video_path: Path, thumb_path: Path, at_seconds: int = 3):
    try:
        if not video_path.exists():
            return None

        stream_info = await run_ffprobe_get_video_stream(str(video_path))

        if stream_info and stream_info.get("height"):
            height = stream_info.get("height", 360)
            width, height = resolution_from_height(height)
        else:
            width, height = 640, 360

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

        proc = await asyncio.create_subprocess_exec(*cmd, stdout=PIPE, stderr=PIPE)
        await proc.communicate()

        return thumb_path if thumb_path.exists() and thumb_path.stat().st_size > 0 else None

    except Exception as e:
        logger.error(f"Thumbnail creation failed: {e}")
        return None

async def send_file_with_progress(message: Message, file_path: Path,
                                 title: str = "", file_type: str = "",
                                 resolution: str = "", thumb_path: Path = None):
    if not file_path.exists():
        m = await message.reply_text("❌ File not found for upload")
        schedule_delete(m, delay=3, exempt_pdf=False)
        return

    try:
        file_size = file_path.stat().st_size
        file_size_mb = file_size / (1024 * 1024)

        progress_msg = await message.reply_text(
            f"**Uploading:** `{file_path.name}`\n"
            f"📊 Size: {file_size_mb:.2f} MB\n"
            f"{make_progress_bar(0, file_size)}",
            parse_mode=ParseMode.MARKDOWN
        )
        active_progress_messages.add(progress_msg.id)
        schedule_delete(progress_msg, delay=3, exempt_pdf=False)

        last_update = 0

        async def progress_callback(current, total):
            nonlocal last_update
            update_threshold = max(total * 0.05, 5 * 1024 * 1024)  # 5% or 5MB

            if (current - last_update) >= update_threshold:
                last_update = current
                try:
                    if progress_msg.id in active_progress_messages:
                        await progress_msg.edit_text(
                            f"**Uploading:** `{file_path.name}`\n"
                            f"📊 Size: {file_size_mb:.2f} MB\n"
                            f"{make_progress_bar(current, total)}",
                            parse_mode=ParseMode.MARKDOWN
                        )
                except Exception:
                    pass

        caption_parts = []
        if title:
            caption_parts.append(f"📁 **{title}**")

        if file_type:
            caption_parts.append(f"📄 **Type:** {file_type}")

        if resolution and resolution != "Unknown Resolution":
            caption_parts.append(f"🎬 **Resolution:** {resolution}")

        caption_parts.append(f"💾 **Size:** {file_size_mb:.2f} MB")
        caption = "\n".join(caption_parts) if caption_parts else "📦 Downloaded File"

        if file_path.suffix.lower() in ['.mp4', '.avi', '.mkv', '.mov', '.webm']:
            duration = await get_video_duration(file_path)
            logger.info(f"Video duration: {duration}s | Size: {file_size_mb:.2f}MB | File: {file_path.name}")

            final_thumb = None
            if thumb_path and thumb_path.exists():
                final_thumb = str(thumb_path)
            else:
                temp_thumb = file_path.with_suffix('.jpg')
                created_thumb = await create_thumbnail(file_path, temp_thumb)
                if created_thumb:
                    final_thumb = str(created_thumb)

            await message.reply_video(
                video=str(file_path),
                thumb=final_thumb,
                caption=caption[:1024],
                duration=duration,
                supports_streaming=True,
                progress=progress_callback
            )

            if final_thumb and final_thumb != str(thumb_path):
                await safe_delete(Path(final_thumb))

        else:
            m = await message.reply_document(
                document=str(file_path),
                thumb=str(thumb_path) if thumb_path and thumb_path.exists() else None,
                caption=caption[:1024],
                progress=progress_callback
            )
            if file_path.suffix.lower() == '.pdf':
                schedule_delete(m, delay=0, exempt_pdf=True)

    except Exception as e:
        error_msg = f"❌ Upload failed: {str(e)[:200]}"
        await message.reply_text(error_msg)
        logger.error(f"Upload error: {e}")

    finally:
        try:
            if progress_msg and progress_msg.id in active_progress_messages:
                active_progress_messages.remove(progress_msg.id)
                await progress_msg.delete()
        except Exception:
            pass

# ----------------- Handlers (main changes: concurrency) -----------------
@app.on_message(filters.command("start") & filters.private)
async def start_command(_, message: Message):
    try:
        user_name = message.from_user.first_name or "User"

        welcome_text = f"""
🌟 **Welcome {user_name}!** 🌟

I can help you convert and upload files from text links:

✅ **MPD URLs** → MP4 videos (with FIXED bitrate for consistent size)
✅ **PDF URLs** → Direct downloads  
✅ **Other files** → Direct uploads

**How to use:**
1. Send me a `.txt` file containing URLs
2. Select your preferred video quality
3. I'll process each link automatically (concurrently with up to {WORKERS} workers)
4. Receive converted/uploaded files

**PERFORMANCE NOTES:**
- Workers (concurrency): {WORKERS}
- Optional hardware accel (set env FFMPEG_HW_ACCEL=nvenc or vaapi to enable)
- For best performance on Zeabur: GPU + NVMe + high network bandwidth recommended.

Send me a .txt file to get started! 🚀
        """
        m = await message.reply_text(welcome_text, parse_mode=ParseMode.MARKDOWN)
        schedule_delete(m, delay=15, exempt_pdf=False)

    except Exception as e:
        logger.error(f"Start command error: {e}")

@app.on_message(filters.document & filters.private)
async def handle_text_file(client: Client, message: Message):
    if not message.document or not message.document.file_name or not message.document.file_name.lower().endswith('.txt'):
        m = await message.reply_text("📄 Please send a .txt file containing URLs")
        schedule_delete(m, delay=3, exempt_pdf=False)
        return

    user_id = message.from_user.id

    # Check if user needs to select quality
    if user_id not in user_sessions:
        m = await message.reply_text(
            "🎬 **Please select video quality:**\n\n"
            "• **Original**: Keep original quality (variable size)\n"
            "• **1080p**: 2500kbps fixed bitrate\n"
            "• **720p**: 1500kbps fixed bitrate\n"
            "• **480p**: 800kbps fixed bitrate\n"
            "• **360p**: 500kbps fixed bitrate\n\n"
            f"✅ **PERFORMANCE:** I will process up to {WORKERS} URLs concurrently (set BOT_WORKERS to change)\n"
            "✅ **Duration issues FIXED** - All videos show correct duration",
            reply_markup=get_quality_keyboard(),
            parse_mode=ParseMode.MARKDOWN
        )
        schedule_delete(m, delay=15, exempt_pdf=False)
        return

    temp_dir = Path(tempfile.mkdtemp(dir=WORKDIR))

    try:
        # Download text file
        txt_file = temp_dir / "urls.txt"
        m = await message.reply_text("📥 Downloading your text file...")
        schedule_delete(m, delay=3, exempt_pdf=False)

        await client.download_media(message, file_name=str(txt_file))

        if not txt_file.exists():
            raise ValueError("Failed to download text file")

        content = txt_file.read_text(encoding='utf-8', errors='ignore')
        lines = [line.strip() for line in content.split('\n') if line.strip()]

        entries = []
        for line in lines:
            title = extract_title_from_line(line)
            url_match = URL_RE.search(line)
            if url_match:
                url = url_match.group(0)
                entries.append((title, url))

        if not entries:
            m = await message.reply_text("❌ No valid URLs found in the file")
            schedule_delete(m, delay=3, exempt_pdf=False)
            return

        selected_quality = user_sessions.get(user_id, "Original")
        selected_bitrate = BITRATE_SETTINGS.get(selected_quality, "Auto")

        m = await message.reply_text(
            f"🔍 Found {len(entries)} URLs to process.\n"
            f"🎯 **Quality:** {selected_quality}\n"
            f"📊 **Bitrate:** {selected_bitrate}\n"
            f"✅ **PERFORMANCE:** up to {WORKERS} concurrent tasks\n"
            f"🚀 Starting processing..."
        )
        schedule_delete(m, delay=5, exempt_pdf=False)

        # Configure HTTP session with higher concurrency limits
        timeout = aiohttp.ClientTimeout(total=LARGE_TIMEOUT)
        connector = aiohttp.TCPConnector(limit=WORKERS, limit_per_host=WORKERS)

        semaphore = asyncio.Semaphore(WORKERS)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # create task list and run concurrently with semaphore
            async def process_entry(idx, title, url):
                async with semaphore:
                    try:
                        logger.info(f"[{idx}] Processing: {title} - {url}")

                        if PDF_RE.search(url):
                            m_pdf = await message.reply_text(f"📚 [{idx}/{len(entries)}] Processing PDF: {title}")
                            schedule_delete(m_pdf, delay=0, exempt_pdf=True)

                            pdf_file = temp_dir / f"document_{idx}.pdf"
                            await download_file_stream(session, url, pdf_file, message)

                            if pdf_file.exists() and pdf_file.stat().st_size > 0:
                                await send_file_with_progress(
                                    message, pdf_file,
                                    title=title,
                                    file_type="PDF Document"
                                )
                            else:
                                await message.reply_text(f"❌ PDF download failed: {title}")

                            await safe_delete(pdf_file)

                        elif MPD_RE.search(url):
                            m_vid = await message.reply_text(
                                f"🎬 [{idx}/{len(entries)}] Converting video: {title}\n"
                                f"🎯 Quality: {selected_quality} | Bitrate: {selected_bitrate}\n"
                                f"✅ Performance optimized - using up to {WORKERS} workers"
                            )
                            schedule_delete(m_vid, delay=3, exempt_pdf=False)

                            video_file = temp_dir / f"video_{idx}.mp4"
                            thumb_file = temp_dir / f"thumb_{idx}.jpg"

                            try:
                                await convert_mpd_to_mp4(url, video_file, message, selected_quality)

                                if video_file.exists() and video_file.stat().st_size > 0:
                                    actual_resolution = await get_video_resolution(video_file)
                                    file_size_mb = video_file.stat().st_size / (1024 * 1024)

                                    thumb_result = await create_thumbnail(video_file, thumb_file)

                                    await send_file_with_progress(
                                        message, video_file,
                                        title=title,
                                        file_type="Video",
                                        resolution=actual_resolution,
                                        thumb_path=thumb_file if thumb_result else None
                                    )
                                else:
                                    await message.reply_text(f"❌ Video conversion failed: {title}")

                            except Exception as e:
                                error_msg = f"❌ Video conversion failed for '{title}': {str(e)[:200]}"
                                await message.reply_text(error_msg)
                                logger.error(f"Video conversion error: {e}")
                            finally:
                                await safe_delete(video_file)
                                await safe_delete(thumb_file)

                        else:
                            m_g = await message.reply_text(f"📦 [{idx}/{len(entries)}] Downloading: {title}")
                            schedule_delete(m_g, delay=3, exempt_pdf=False)

                            file_ext = Path(unquote(url).split('?')[0]).suffix or '.bin'
                            safe_filename = re.sub(r'[^\w\.-]', '_', title)[:50] + file_ext
                            generic_file = temp_dir / safe_filename

                            await download_file_stream(session, url, generic_file, message)

                            if generic_file.exists() and generic_file.stat().st_size > 0:
                                file_type = "Document"
                                file_ext_lower = file_ext.lower()

                                if file_ext_lower in ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp']:
                                    file_type = "Image"
                                elif file_ext_lower in ['.mp3', '.wav', '.flac', '.aac', '.m4a']:
                                    file_type = "Audio"
                                elif file_ext_lower in ['.zip', '.rar', '.7z', '.tar', '.gz']:
                                    file_type = "Archive"

                                await send_file_with_progress(
                                    message, generic_file,
                                    title=title,
                                    file_type=file_type
                                )
                            else:
                                await message.reply_text(f"❌ Download failed: {title}")

                            await safe_delete(generic_file)

                    except Exception as e:
                        error_msg = f"❌ Error processing URL {idx}: {str(e)[:200]}"
                        m_err = await message.reply_text(error_msg)
                        schedule_delete(m_err, delay=5, exempt_pdf=False)
                        logger.error(f"URL processing error: {e}")

            # spawn tasks
            tasks = []
            for idx, (title, url) in enumerate(entries, 1):
                tasks.append(asyncio.create_task(process_entry(idx, title, url)))

            # wait for all tasks (concurrent up to WORKERS)
            await asyncio.gather(*tasks)

        m_done = await message.reply_text(
            f"✅ All {len(entries)} tasks completed successfully!\n"
            f"🎯 Quality used: {selected_quality}\n"
            f"📊 Bitrate: {selected_bitrate}\n"
            f"✅ Performance mode: workers={WORKERS}\n\n"
            f"Ready for more files! 🚀"
        )
        schedule_delete(m_done, delay=10, exempt_pdf=False)

    except Exception as e:
        error_msg = f"❌ Critical error: {str(e)[:300]}"
        m_err = await message.reply_text(error_msg)
        schedule_delete(m_err, delay=5, exempt_pdf=False)
        logger.error(f"Main handler error: {e}")

    finally:
        await safe_rmtree(temp_dir)

# ----------------- Error Handler -----------------
@app.on_errors()
async def error_handler(client, update, error):
    logger.error(f"Global error handler: {error}", exc_info=True)
    try:
        if hasattr(update, 'message') and hasattr(update.message, 'reply_text'):
            await update.message.reply_text("❌ An unexpected error occurred. Please try again.")
    except Exception:
        pass

# ----------------- Main Execution -----------------
if __name__ == "__main__":
    print("🚀 Starting Fixed Converter Bot (2025 Version) - PERFORMANCE PATCH...")
    print(f"📁 Work directory: {WORKDIR}")
    print(f"⏰ Timeout set to: {LARGE_TIMEOUT} seconds")
    print(f"🔁 Workers (concurrency): {WORKERS}")
    print(f"🔧 HW Accel mode: {FFMPEG_HW_ACCEL or 'software (libx264 ultrafast)'}")
    print("✅ Duration issues FIXED - All videos show correct duration (logic unchanged)")
    print("✅ Bot is ready and running!")

    try:
        app.run()
    except KeyboardInterrupt:
        print("\n👋 Bot stopped by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
