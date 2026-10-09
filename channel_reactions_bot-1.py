#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
channel_reactions_bot.py
بوت تليغرام لإدارة التفاعلات (Reactions) على منشورات القنوات مع لوحة تحكم إدارية.

التوكن والكود السري موضوعان كقيم افتراضية داخل الملف (ويمكن تجاوزهما بمتغيرات البيئة / ملف .env).
"""
from __future__ import annotations

import asyncio
import hmac
import html
import logging
import os
import re
import secrets
import sqlite3
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

try:  # اختياري: تحميل ملف .env
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:  # pragma: no cover
    pass

from telegram import (
    InlineKeyboardButton as IKB,
    InlineKeyboardMarkup as IKM,
    LinkPreviewOptions,
    ReactionTypeEmoji,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.constants import ChatType, ParseMode
from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TelegramError, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ============================== الإعدادات العامة ==============================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8911350186:AAH-fkkqCrolal4YvPwfHpdn3cuBSQuwCFY").strip()
ADMIN_SECRET = os.getenv("ADMIN_SECRET", "ABSIRQ17MAR").strip()
ADMIN_IDS = {int(x) for x in re.split(r"[,\s]+", os.getenv("ADMIN_IDS", "8983540044")) if x.strip().lstrip("-").isdigit()}
DB_PATH = os.getenv("DB_PATH", "channel_reactions.db")
BACKUP_DIR = os.getenv("BACKUP_DIR", "backups")

MAX_LOGIN_ATTEMPTS = 5
LOGIN_LOCK_SECONDS = 15 * 60
DUP_WINDOW_SECONDS = 20

# الإيموجيات المطلوبة (كلها ضمن قائمة ReactionTypeEmoji الرسمية في Bot API)
EMOJIS = ["👍", "❤️", "🔥", "😍", "👏", "😁", "🎉", "🤔", "😢", "🤯", "🙏", "💯"]

PERM_LABELS = {
    "react": "التفاعل على رابط",
    "channels": "إدارة القنوات",
    "auto": "التفاعل التلقائي",
    "logs": "عرض السجل",
    "stats": "عرض الإحصائيات",
    "settings": "إعدادات البوت",
    "backups": "النسخ الاحتياطية",
    "notifications": "الإشعارات",
}
OWNER_ONLY = {"users", "security"}
VIEWER_PERMS = {"view", "stats", "logs"}

DEFAULTS = {
    "auto_enabled": "1",
    "retry_delay": "2",
    "max_retries": "3",
    "notify_fail": "1",
    "notify_repeated": "1",
    "notify_restart": "1",
    "notify_access": "1",
    "notify_success": "1",
    "default_emojis": "👍,🔥,❤️",
    "log_page_size": "10",
    "maintenance": "0",
    "session_timeout": "30",
    "max_reactions": "3",
    "backup_interval_hours": "0",
    "backup_keep": "10",
    "last_auto_backup": "0",
}
REQUIRED_TABLES = {"settings", "channels", "operations", "users", "processed"}
OP_NAMES = {
    "reaction": "تفاعل", "add_channel": "إضافة قناة", "remove_channel": "حذف قناة",
    "toggle_auto": "تبديل تفاعل تلقائي", "setting": "تعديل إعداد", "backup": "نسخ احتياطي",
    "restore": "استعادة نسخة", "user_add": "إضافة مستخدم", "user_remove": "حذف مستخدم",
    "user_perms": "تعديل صلاحيات", "login": "دخول", "security": "إجراء أمني",
}

log = logging.getLogger("reactions_bot")


def redact(text: str) -> str:
    for s in (BOT_TOKEN, ADMIN_SECRET):
        if s and s in text:
            text = text.replace(s, "***")
    return text


class RedactFormatter(logging.Formatter):
    def format(self, record):  # noqa: A003
        return redact(super().format(record))


def setup_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(RedactFormatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)  # يطبع روابط تحتوي التوكن
    logging.getLogger("httpcore").setLevel(logging.WARNING)


# ================================ قاعدة البيانات ===============================
def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class DB:
    def __init__(self, path: str):
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.init()

    def init(self) -> None:
        with self.lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS channels(
                    chat_id INTEGER PRIMARY KEY, username TEXT, title TEXT,
                    auto_enabled INTEGER NOT NULL DEFAULT 0, emojis TEXT NOT NULL DEFAULT '',
                    added_at TEXT, last_post_id INTEGER, last_post_at TEXT,
                    last_error TEXT, last_error_at TEXT);
                CREATE TABLE IF NOT EXISTS operations(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, op_type TEXT NOT NULL, chat_id INTEGER,
                    channel_label TEXT, post_url TEXT, emojis TEXT, executed_at TEXT NOT NULL,
                    success INTEGER NOT NULL, error TEXT, admin_id INTEGER, mode TEXT, details TEXT);
                CREATE INDEX IF NOT EXISTS idx_ops_time ON operations(executed_at);
                CREATE TABLE IF NOT EXISTS users(
                    user_id INTEGER PRIMARY KEY, role TEXT NOT NULL, perms TEXT NOT NULL DEFAULT '',
                    added_by INTEGER, added_at TEXT);
                CREATE TABLE IF NOT EXISTS processed(
                    chat_id INTEGER NOT NULL, message_id INTEGER NOT NULL, created_at TEXT NOT NULL,
                    PRIMARY KEY(chat_id, message_id));
                """
            )
            self.conn.executemany("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", list(DEFAULTS.items()))
            self.conn.commit()

    def q(self, sql: str, params=()):
        with self.lock:
            cur = self.conn.execute(sql, params)
            rows = cur.fetchall()
            self.conn.commit()
            return rows

    def x(self, sql: str, params=()):
        with self.lock:
            cur = self.conn.execute(sql, params)
            self.conn.commit()
            return cur


db: DB | None = None


def init_db(path: str) -> DB:
    global db
    db = DB(path)
    return db


def S(key: str) -> str:
    r = db.q("SELECT value FROM settings WHERE key=?", (key,))
    return r[0]["value"] if r else DEFAULTS.get(key, "")


def S_int(key: str) -> int:
    try:
        return int(S(key))
    except ValueError:
        return int(DEFAULTS[key])


def set_S(key: str, value) -> None:
    db.x("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
         (key, str(value)))


def default_emojis() -> list[str]:
    return [e for e in S("default_emojis").split(",") if e] or ["👍"]


def get_channel(cid: int):
    r = db.q("SELECT * FROM channels WHERE chat_id=?", (cid,))
    return r[0] if r else None


def ch_emojis(row) -> list[str]:
    return [e for e in (row["emojis"] or "").split(",") if e] or default_emojis()


def ch_label(row) -> str:
    return ("@" + row["username"]) if row["username"] else (row["title"] or str(row["chat_id"]))


def log_op(op_type, chat_id=None, label="", url="", emojis="", success=True, error="",
           admin_id=None, mode="manual", details=""):
    db.x(
        "INSERT INTO operations(op_type,chat_id,channel_label,post_url,emojis,executed_at,success,error,admin_id,mode,details)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (op_type, chat_id, label, url, emojis, utcnow(), 1 if success else 0, redact(error or ""),
         admin_id, mode, redact(details or "")),
    )


# ================================== الروابط ====================================
LINK_RE = re.compile(
    r"^(?:https?://)?(?:www\.)?(?:t|telegram)\.me/"
    r"(?:c/(?P<cid>\d{4,15})|(?P<user>[A-Za-z][A-Za-z0-9_]{3,31}))"
    r"(?:/\d+)?/(?P<mid>\d+)/?(?:\?.*)?$",
    re.IGNORECASE,
)


def parse_post_link(text: str):
    m = LINK_RE.match((text or "").strip())
    if not m:
        return None
    mid = int(m.group("mid"))
    if mid <= 0 or mid >= 2 ** 31:
        return None
    if m.group("cid"):
        chat = int("-100" + m.group("cid"))
        return {"chat": chat, "message_id": mid, "private": True, "label": f"قناة خاصة {chat}"}
    return {"chat": "@" + m.group("user"), "message_id": mid, "private": False, "label": "@" + m.group("user")}


def norm_emoji(e: str) -> str:
    return e.replace("\ufe0f", "")


SUPPORTED = {norm_emoji(e) for e in EMOJIS}


def explain(e: Exception) -> str:
    m = str(e).lower().replace("_", " ")
    if "reaction invalid" in m:
        return "إيموجي غير مسموح به في هذه القناة أو غير مدعوم (REACTION_INVALID)"
    if "too many" in m:
        return "عدد التفاعلات يتجاوز الحد المسموح"
    if "chat not found" in m:
        return "القناة غير موجودة أو لا يمكن للبوت الوصول إليها"
    if "message to react not found" in m or "message not found" in m or "message id invalid" in m:
        return "المنشور غير موجود أو تعذر الوصول إليه"
    if "not enough rights" in m or "forbidden" in m or "rights" in m:
        return "صلاحيات غير كافية (يجب أن يكون البوت عضواً/مشرفاً في القناة)"
    if "reactions" in m and "disabled" in m:
        return "التفاعلات معطّلة في هذه القناة"
    return redact(str(e))[:200] or e.__class__.__name__


# ============================ الهوية والصلاحيات والجلسات ======================
SESSIONS: dict[int, float] = {}
EXPIRED: set[int] = set()
FAILS: dict[int, list[float]] = {}
LOCKED: dict[int, float] = {}
RECENT: dict = {}
FAIL_STREAK: dict = defaultdict(int)
LAST_ERR_NOTIFY = 0.0
START_TIME = datetime.now(timezone.utc)
BOT_ID = 0


def role_of(uid: int):
    if uid in ADMIN_IDS:
        return "owner"
    r = db.q("SELECT role FROM users WHERE user_id=?", (uid,))
    return r[0]["role"] if r else None


def user_perms(uid: int) -> set[str]:
    r = db.q("SELECT perms FROM users WHERE user_id=?", (uid,))
    return {p for p in (r[0]["perms"] if r else "").split(",") if p}


def can(uid: int, perm: str) -> bool:
    role = role_of(uid)
    if role == "owner":
        return True
    if role is None:
        return False
    if perm == "view":
        return True
    if perm in OWNER_ONLY:
        return False
    if role == "viewer":
        return perm in VIEWER_PERMS
    return perm in user_perms(uid)


def session_ok(uid: int) -> bool:
    t = SESSIONS.get(uid)
    if t is None:
        return False
    if time.time() - t > S_int("session_timeout") * 60:
        SESSIONS.pop(uid, None)
        EXPIRED.add(uid)
        return False
    SESSIONS[uid] = time.time()
    return True


def role_name(role: str) -> str:
    return {"owner": "المدير العام", "assistant": "مدير مساعد", "viewer": "مشاهد"}.get(role, "-")


# ================================ أدوات الواجهة ================================
h = html.escape
NOPREVIEW = LinkPreviewOptions(is_disabled=True)


def rk(*rows) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([list(r) for r in rows], resize_keyboard=True, is_persistent=True)


async def say(update: Update, text: str, markup=None):
    if len(text) > 4000:
        text = text[:3990] + "…"
    return await update.effective_message.reply_text(
        text, parse_mode=ParseMode.HTML, reply_markup=markup, link_preview_options=NOPREVIEW)


async def safe_edit(q, text: str, markup=None):
    if len(text) > 4000:
        text = text[:3990] + "…"
    try:
        await q.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=markup,
                                  link_preview_options=NOPREVIEW)
    except BadRequest as e:
        if "not modified" not in str(e).lower():
            raise


# أزرار القوائم
B_STATS, B_LINK, B_EMO, B_CH = "📊 الإحصائيات", "🔗 التفاعل على رابط", "🎭 إعدادات التفاعلات", "📢 إدارة القنوات"
B_AUTO, B_LOGS, B_USERS, B_SEC = "🤖 التفاعل التلقائي", "📋 سجل العمليات", "👥 إدارة المستخدمين", "🛡️ الأمان والصلاحيات"
B_SET, B_BAK, B_NOT, B_HELP = "⚙️ إعدادات البوت", "💾 النسخ الاحتياطية", "🔔 الإشعارات", "ℹ️ المساعدة"
B_LOCK, B_HOME = "🔒 قفل لوحة التحكم", "🔙 القائمة الرئيسية"
MAIN_ITEMS = [(B_STATS, "stats"), (B_LINK, "react"), (B_EMO, "view"), (B_CH, "view"), (B_AUTO, "view"),
              (B_LOGS, "logs"), (B_USERS, "users"), (B_SEC, "security"), (B_SET, "view"), (B_BAK, "backups"),
              (B_NOT, "view"), (B_HELP, None), (B_LOCK, None)]

# إحصائيات
B_ST_SUM, B_ST_TOP, B_ST_LAST = "📊 ملخص الإحصائيات", "🏆 أكثر القنوات استخداماً", "🕒 آخر عملية وآخر خطأ"
# رابط
# تفاعلات
B_EM_PICK, B_EM_SHOW, B_EM_RESET = "🎭 اختيار التفاعلات الافتراضية", "👁️ عرض التفاعلات الحالية", "♻️ استعادة الافتراضي"
B_EM_MAX, B_EM_SUP = "🔢 الحد الأقصى للتفاعلات", "✅ الإيموجيات المدعومة"
# قنوات
B_CH_ADD, B_CH_DEL, B_CH_LIST = "➕ إضافة قناة", "🗑️ حذف قناة", "📋 عرض القنوات"
B_CH_ACT, B_CH_EMO, B_CH_LAST = "✅ القنوات المفعّلة", "🎭 تعديل تفاعلات قناة", "📌 آخر منشور معالج"
B_CH_ERR, B_CH_STAT = "⚠️ آخر خطأ لقناة", "🩺 حالة قناة"
# تلقائي
B_AU_ON, B_AU_OFF, B_AU_STATUS = "🟢 تفعيل التلقائي العام", "🔴 إيقاف التلقائي العام", "📋 حالة القنوات"
B_AU_ADD, B_AU_RM = "➕ إضافة قناة للمراقبة", "➖ إزالة قناة"
B_AU_ENCH, B_AU_DISCH, B_AU_EMO = "✅ تفعيل على قناة", "⛔ إيقاف على قناة", "🎭 تفاعلات قناة"
B_AU_RECENT = "📈 آخر العمليات التلقائية"
# سجل
B_LG_ALL, B_LG_OK, B_LG_FAIL = "📜 آخر العمليات", "✅ الناجحة فقط", "❌ الفاشلة فقط"
B_LG_SEARCH, B_LG_DETAIL = "🔍 بحث في السجل", "🔎 تفاصيل عملية"
# مستخدمون
B_US_ADDA, B_US_ADDV, B_US_LIST = "➕ إضافة مدير مساعد", "➕ إضافة مشاهد", "📋 عرض المستخدمين"
B_US_EDIT, B_US_DEL = "✏️ تعديل صلاحيات مساعد", "🗑️ إزالة مستخدم"
# أمان
B_SC_STATUS, B_SC_SESS, B_SC_KILL = "🛡️ حالة الأمان", "🟢 الجلسات النشطة", "🚪 إنهاء جلسات الآخرين"
B_SC_LOCKED, B_SC_UNLOCK, B_SC_PERMS = "🚫 المحظورون مؤقتاً", "🔓 رفع الحظر عن الجميع", "📜 جدول الصلاحيات"
# إعدادات
B_S_AUTO, B_S_DELAY, B_S_RETRY = "🔁 تبديل التفاعل التلقائي", "⏱️ فترة الانتظار", "🔂 عدد المحاولات"
B_S_NOTIFY, B_S_EMO, B_S_LOGN = "🔔 إشعارات الأخطاء", "🎭 الإيموجيات الافتراضية", "📄 عدد السجلات المعروضة"
B_S_MAINT, B_S_SESS = "🛠️ وضع الصيانة", "⌛ مهلة الجلسة"
# نسخ احتياطية
B_BK_NOW, B_BK_LIST, B_BK_VER = "💾 إنشاء نسخة الآن", "📂 عرض النسخ", "✅ التحقق من نسخة"
B_BK_RES, B_BK_AUTO = "♻️ استعادة نسخة", "⏰ النسخ الدوري"
# إشعارات
B_N_ALL_ON, B_N_ALL_OFF = "🔔 تشغيل الكل", "🔕 إيقاف الكل"
B_N_FAIL, B_N_REP, B_N_RST = "❌ فشل التفاعل", "🔁 الأخطاء المتكررة", "🔄 تشغيل/إعادة تشغيل البوت"
B_N_ACC, B_N_OK, B_N_TEST = "🚫 مشاكل الوصول", "✅ العمليات الإدارية الناجحة", "🧪 إشعار تجريبي"

ROUTES: dict = {}


def route(btn: str, perm: str | None = None):
    def deco(f):
        ROUTES[btn] = (f, perm)
        return f
    return deco


def main_kb(uid: int) -> ReplyKeyboardMarkup:
    btns = [b for b, p in MAIN_ITEMS if p is None or can(uid, p)]
    rows = [btns[i:i + 2] for i in range(0, len(btns), 2)]
    return rk(*rows)


async def show_main(update: Update, uid: int, note: str = ""):
    role = role_name(role_of(uid))
    await say(update, f"{note}👑 <b>لوحة التحكم الرئيسية</b>\nصلاحيتك: <b>{role}</b>", main_kb(uid))


# ================================ الإشعارات ====================================
def recipients() -> set[int]:
    ids = set(ADMIN_IDS)
    for r in db.q("SELECT user_id, perms FROM users WHERE role='assistant'"):
        if "notifications" in (r["perms"] or "").split(","):
            ids.add(r["user_id"])
    return ids


async def notify(bot, kind: str, text: str, exclude: int | None = None, force: bool = False):
    if not force and S(f"notify_{kind}") != "1":
        return
    for uid in recipients() - ({exclude} if exclude else set()):
        try:
            await bot.send_message(uid, redact(text), parse_mode=ParseMode.HTML)
        except TelegramError as e:
            log.warning("تعذر إرسال إشعار إلى %s: %s", uid, e.__class__.__name__)


# ================================ تنفيذ التفاعل ================================
async def apply_reaction(bot, chat, message_id: int, emojis: list[str]):
    """يعيد (نجاح، التفاعلات المطبقة فعلاً، سبب الفشل)."""
    retries, delay, mx = max(1, S_int("max_retries")), S_int("retry_delay"), max(1, S_int("max_reactions"))
    use = [e for e in dict.fromkeys(emojis) if norm_emoji(e) in SUPPORTED][:mx]
    if not use:
        return False, [], "لا توجد إيموجيات مدعومة للإرسال"
    attempt, last_err = 0, ""
    while attempt < retries:
        attempt += 1
        try:
            await bot.set_message_reaction(chat_id=chat, message_id=message_id,
                                           reaction=[ReactionTypeEmoji(norm_emoji(e)) for e in use])
            return True, use, ""
        except RetryAfter as e:
            ra = e.retry_after
            ra = ra.total_seconds() if hasattr(ra, "total_seconds") else float(ra)
            last_err = f"تجاوز حدود Telegram، مطلوب الانتظار {int(ra)} ثانية"
            await asyncio.sleep(min(ra, 60) + 1)
        except Forbidden as e:
            return False, [], explain(e)
        except BadRequest as e:
            if "too many" in str(e).lower().replace("_", " ") and len(use) > 1:
                use = use[:-1]
                attempt -= 1
                continue
            return False, [], explain(e)
        except (TimedOut, NetworkError):
            last_err = "انقطاع الاتصال أو انتهاء المهلة"
            await asyncio.sleep(delay)
        except TelegramError as e:
            last_err = explain(e)
            await asyncio.sleep(delay)
    return False, [], f"{last_err or 'فشل'} (بعد {retries} محاولات)"


# ================================ الدخول (الكود السري) =========================
async def login(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    uid, msg, now = update.effective_user.id, update.effective_message, time.time()
    if LOCKED.get(uid, 0) > now:
        rem = int((LOCKED[uid] - now) // 60) + 1
        await say(update, f"🚫 تم حظر المحاولات مؤقتاً. حاول بعد <b>{rem}</b> دقيقة.")
        return
    if ADMIN_SECRET and hmac.compare_digest(text.encode(), ADMIN_SECRET.encode()):
        FAILS.pop(uid, None)
        LOCKED.pop(uid, None)
        SESSIONS[uid] = now
        EXPIRED.discard(uid)
        try:
            await msg.delete()
        except TelegramError:
            pass
        log_op("login", admin_id=uid, mode="system", success=True)
        ctx.user_data.clear()
        await show_main(update, uid, "✅ تم التحقق.\n\n")
        return
    lst = [t for t in FAILS.get(uid, []) if now - t < LOGIN_LOCK_SECONDS] + [now]
    FAILS[uid] = lst
    log_op("login", admin_id=uid, mode="system", success=False, error="كود سري غير صحيح")
    try:
        await msg.delete()
    except TelegramError:
        pass
    if len(lst) >= MAX_LOGIN_ATTEMPTS:
        LOCKED[uid] = now + LOGIN_LOCK_SECONDS
        FAILS.pop(uid, None)
        await say(update, "🚫 تجاوزت عدد المحاولات. تم الحظر مؤقتاً لمدة 15 دقيقة.")
        await notify(ctx.bot, "fail", f"🚨 <b>تنبيه أمني</b>: محاولات دخول فاشلة متكررة من المستخدم <code>{uid}</code>.",
                     exclude=uid)
    else:
        await say(update, f"❌ الكود غير صحيح. المحاولات المتبقية: <b>{MAX_LOGIN_ATTEMPTS - len(lst)}</b>")


# ================================ الإحصائيات ===================================
def stats_text() -> str:
    def c(sql: str) -> int:
        return db.q(sql)[0][0]

    rx = "op_type='reaction'"
    n_ch = c("SELECT COUNT(*) FROM channels")
    n_act = c("SELECT COUNT(*) FROM channels WHERE auto_enabled=1")
    n_ok = c("SELECT COUNT(*) FROM operations WHERE " + rx + " AND success=1")
    n_bad = c("SELECT COUNT(*) FROM operations WHERE " + rx + " AND success=0")
    n_today = c("SELECT COUNT(*) FROM operations WHERE " + rx + " AND date(executed_at)=date('now')")
    n_week = c("SELECT COUNT(*) FROM operations WHERE " + rx + " AND executed_at>=datetime('now','-7 days')")
    last_ok = db.q("SELECT * FROM operations WHERE " + rx + " AND success=1 ORDER BY id DESC LIMIT 1")
    last_er = db.q("SELECT * FROM operations WHERE success=0 AND op_type!='login' ORDER BY id DESC LIMIT 1")
    top = db.q("SELECT channel_label, COUNT(*) n FROM operations WHERE " + rx +
               " GROUP BY channel_label ORDER BY n DESC LIMIT 3")
    ok_txt = h(last_ok[0]["executed_at"]) if last_ok else "—"
    er_txt = h(last_er[0]["executed_at"] + " — " + (last_er[0]["error"] or "")[:80]) if last_er else "—"
    top_txt = "، ".join(f"{h(r['channel_label'] or '-')} ({r['n']})" for r in top) if top else "—"
    return "\n".join([
        "<b>📊 الإحصائيات (التوقيت UTC)</b>\n",
        f"📢 القنوات المسجلة: <b>{n_ch}</b>",
        f"🟢 القنوات النشطة (تلقائي): <b>{n_act}</b>",
        f"✅ تفاعلات ناجحة: <b>{n_ok}</b>",
        f"❌ تفاعلات فاشلة: <b>{n_bad}</b>",
        f"📅 عمليات اليوم: <b>{n_today}</b>",
        f"🗓️ آخر 7 أيام: <b>{n_week}</b>",
        f"🕒 آخر عملية ناجحة: {ok_txt}",
        f"⚠️ آخر خطأ: {er_txt}",
        f"🏆 الأكثر استخداماً: {top_txt}",
        f"🚀 وقت بدء التشغيل: {START_TIME.strftime('%Y-%m-%d %H:%M:%S')}",
    ])


def stats_kb():
    return rk((B_ST_SUM, B_ST_TOP), (B_ST_LAST,), (B_HOME,))


@route(B_STATS, "stats")
async def m_stats(update, ctx):
    await say(update, stats_text(), stats_kb())


@route(B_ST_SUM, "stats")
async def st_sum(update, ctx):
    await say(update, stats_text(), stats_kb())


@route(B_ST_TOP, "stats")
async def st_top(update, ctx):
    rows = db.q("SELECT channel_label, COUNT(*) n, SUM(success) ok FROM operations WHERE op_type='reaction' "
                "GROUP BY channel_label ORDER BY n DESC LIMIT 10")
    if not rows:
        await say(update, "لا توجد عمليات بعد.", stats_kb())
        return
    body = "\n".join(f"{i}. {h(r['channel_label'] or '-')} — <b>{r['n']}</b> (نجاح {r['ok']})" for i, r in enumerate(rows, 1))
    await say(update, "<b>🏆 أكثر القنوات استخداماً</b>\n\n" + body, stats_kb())


@route(B_ST_LAST, "stats")
async def st_last(update, ctx):
    ok = db.q("SELECT * FROM operations WHERE op_type='reaction' AND success=1 ORDER BY id DESC LIMIT 1")
    er = db.q("SELECT * FROM operations WHERE success=0 AND op_type!='login' ORDER BY id DESC LIMIT 1")
    t = "<b>آخر عملية ناجحة:</b>\n" + (fmt_op(ok[0], True) if ok else "—")
    t += "\n\n<b>آخر خطأ:</b>\n" + (fmt_op(er[0], True) if er else "—")
    await say(update, t, stats_kb())


# ================================= سجل العمليات ================================
def fmt_op(r, detail: bool = False) -> str:
    icon = "✅" if r["success"] else "❌"
    name = OP_NAMES.get(r["op_type"], r["op_type"])
    if not detail:
        return f"{icon} <b>#{r['id']}</b> {h(name)} • {h(r['channel_label'] or '-')} • {r['executed_at'][5:16]}"
    parts = [f"{icon} <b>العملية #{r['id']}</b>", f"النوع: {h(name)}", f"القناة: {h(r['channel_label'] or '-')}"]
    if r["post_url"]:
        parts.append(f"الرابط: {h(r['post_url'])}")
    if r["emojis"]:
        parts.append(f"الإيموجيات: {h(r['emojis'])}")
    parts += [f"الوقت (UTC): {r['executed_at']}", f"النتيجة: {'نجاح' if r['success'] else 'فشل'}"]
    if r["error"]:
        parts.append(f"سبب الفشل: {h(r['error'])}")
    parts += [f"المدير: <code>{r['admin_id'] or '-'}</code>",
              f"النمط: {'تلقائي' if r['mode'] == 'auto' else ('يدوي' if r['mode'] == 'manual' else 'نظام')}"]
    if r["details"]:
        parts.append(f"تفاصيل: {h(r['details'])}")
    return "\n".join(parts)


def logs_page(flt: str, page: int):
    size = S_int("log_page_size")
    where = {"all": "1=1", "ok": "success=1", "fail": "success=0"}.get(flt, "1=1")
    total = db.q(f"SELECT COUNT(*) FROM operations WHERE {where}")[0][0]
    rows = db.q(f"SELECT * FROM operations WHERE {where} ORDER BY id DESC LIMIT ? OFFSET ?", (size, page * size))
    pages = max(1, -(-total // size))
    text = f"<b>📋 السجل</b> (صفحة {page + 1}/{pages} — الإجمالي {total})\n\n" + (
        "\n".join(fmt_op(r) for r in rows) if rows else "لا توجد عمليات.")
    text += "\n\nلعرض تفاصيل عملية استخدم زر «🔎 تفاصيل عملية»."
    nav = []
    if page > 0:
        nav.append(IKB("◀️ السابق", callback_data=f"lg|{flt}|{page - 1}"))
    if page + 1 < pages:
        nav.append(IKB("التالي ▶️", callback_data=f"lg|{flt}|{page + 1}"))
    return text, IKM([nav]) if nav else None


def logs_kb():
    return rk((B_LG_ALL, B_LG_OK, B_LG_FAIL), (B_LG_SEARCH, B_LG_DETAIL), (B_HOME,))


@route(B_LOGS, "logs")
async def m_logs(update, ctx):
    t, m = logs_page("all", 0)
    await say(update, "📋 <b>سجل العمليات</b>", logs_kb())
    await say(update, t, m)


def _mk_log(btn, flt):
    @route(btn, "logs")
    async def _h(update, ctx):
        t, m = logs_page(flt, 0)
        await say(update, t, m)
    return _h


_mk_log(B_LG_ALL, "all")
_mk_log(B_LG_OK, "ok")
_mk_log(B_LG_FAIL, "fail")


@route(B_LG_SEARCH, "logs")
async def lg_search(update, ctx):
    ctx.user_data["state"] = "log_search"
    await say(update, "🔍 أرسل كلمة البحث (اسم قناة، جزء من رابط، نوع عملية، أو نص الخطأ):", rk((B_HOME,)))


@route(B_LG_DETAIL, "logs")
async def lg_detail(update, ctx):
    ctx.user_data["state"] = "log_detail"
    await say(update, "🔎 أرسل رقم العملية (مثال: 15):", rk((B_HOME,)))


# ================================= التفاعل على رابط ============================
def pending_text(p) -> str:
    mx = S_int("max_reactions")
    note = f"\n⚠️ سيُرسل أول {mx} فقط حسب الحد المضبوط." if len(p["emojis"]) > mx else ""
    return (
        "🔗 <b>عملية تفاعل على منشور</b>\n\n"
        f"📢 القناة: <b>{h(p['label'])}</b>\n🆔 رقم المنشور: <code>{p['message_id']}</code>\n"
        f"🤖 حالة البوت في القناة: {h(p['bot_status'])}\n"
        f"🎭 التفاعلات المحددة: {' '.join(p['emojis']) or '—'}{note}\n\n"
        "ℹ️ لا يوفر Bot API طريقة للتحقق من وجود المنشور مسبقاً؛ ستظهر النتيجة الفعلية بعد التنفيذ.\n"
        "راجع التفاعلات ثم اضغط «✅ تنفيذ»."
    )


def pending_markup(p):
    t = p["token"]
    return IKM([[IKB("✏️ تغيير التفاعلات", callback_data=f"ln|ed|{t}")],
                [IKB("✅ تنفيذ", callback_data=f"ln|go|{t}"), IKB("❌ إلغاء", callback_data=f"ln|no|{t}")]])


async def bot_status_in(bot, chat_id) -> str:
    try:
        m = await bot.get_chat_member(chat_id, BOT_ID)
        return {"administrator": "مشرف", "creator": "مالك", "member": "عضو", "left": "غير عضو",
                "kicked": "محظور", "restricted": "مقيّد"}.get(m.status, m.status)
    except TelegramError:
        return "غير معروف / غير عضو"


async def start_link_flow(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    uid = update.effective_user.id
    if not can(uid, "react"):
        await say(update, "⛔ لا تملك صلاحية التفاعل على الروابط.")
        return
    p = parse_post_link(text)
    if not p:
        await say(update, "❌ رابط غير صالح.\nالصيغة: <code>https://t.me/channelname/123</code> أو "
                          "<code>https://t.me/c/1234567890/123</code>")
        return
    try:
        chat = await ctx.bot.get_chat(p["chat"])
    except (BadRequest, Forbidden) as e:
        extra = "\nالقنوات الخاصة تتطلب أن يكون البوت عضواً فيها." if p["private"] else ""
        await say(update, f"❌ لا يمكن الوصول إلى القناة: {h(explain(e))}{extra}")
        return
    except TelegramError as e:
        await say(update, f"❌ خطأ في الاتصال بـ Telegram: {h(explain(e))}")
        return
    if chat.type not in (ChatType.CHANNEL, ChatType.SUPERGROUP, ChatType.GROUP):
        await say(update, "❌ الرابط لا يشير إلى قناة أو مجموعة.")
        return
    status = await bot_status_in(ctx.bot, chat.id)
    label = ("@" + chat.username) if chat.username else (chat.title or str(chat.id))
    pend = {"token": secrets.token_hex(3), "chat": chat.id, "message_id": p["message_id"], "label": label,
            "url": text.strip(), "bot_status": status, "emojis": default_emojis()[:S_int("max_reactions")]}
    ctx.user_data["pending"] = pend
    await say(update, pending_text(pend), pending_markup(pend))


@route(B_LINK, "react")
async def m_link(update, ctx):
    ctx.user_data["state"] = "await_link"
    await say(update, "🔗 أرسل رابط منشور القناة الآن، مثال:\n<code>https://t.me/channelname/123</code>",
              rk((B_HOME,)))


# ================================= المنتقي (الإيموجي) ===========================
def get_sel(ctx, scope: str):
    if scope == "p":
        p = ctx.user_data.get("pending")
        return list(p["emojis"]) if p else None
    if scope == "d":
        return default_emojis()
    row = get_channel(int(scope[1:]))
    return ch_emojis(row) if row else None


def set_sel(ctx, scope: str, sel: list[str]):
    if scope == "p":
        ctx.user_data["pending"]["emojis"] = sel
    elif scope == "d":
        if sel:
            set_S("default_emojis", ",".join(sel))
    else:
        db.x("UPDATE channels SET emojis=? WHERE chat_id=?", (",".join(sel), int(scope[1:])))


def picker_markup(scope: str, sel: list[str]):
    rows, row = [], []
    for i, e in enumerate(EMOJIS):
        row.append(IKB(("✅" if e in sel else "▫️") + e, callback_data=f"pk|{scope}|{i}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([IKB("💾 تم", callback_data=f"pk|{scope}|ok"), IKB("🧹 مسح", callback_data=f"pk|{scope}|clr")])
    return IKM(rows)


def picker_text(scope: str, sel: list[str]) -> str:
    return (f"🎭 <b>اختر التفاعلات</b> (الحد الأقصى {S_int('max_reactions')})\n"
            f"المختارة: {' '.join(sel) or '—'}")


async def show_picker(update: Update, scope: str, ctx):
    sel = get_sel(ctx, scope)
    await say(update, picker_text(scope, sel), picker_markup(scope, sel))


# ================================= إدارة القنوات ===============================
def ch_kb():
    return rk((B_CH_ADD, B_CH_DEL), (B_CH_LIST, B_CH_ACT), (B_CH_EMO, B_CH_LAST), (B_CH_ERR, B_CH_STAT), (B_HOME,))


def channel_line(r) -> str:
    return (f"{'🟢' if r['auto_enabled'] else '⚪'} <b>{h(ch_label(r))}</b> — <code>{r['chat_id']}</code>\n"
            f"   🎭 {' '.join(ch_emojis(r))}")


async def send_chooser(update: Update, action: str, prompt: str):
    rows = db.q("SELECT * FROM channels ORDER BY added_at DESC LIMIT 40")
    if not rows:
        await say(update, "لا توجد قنوات مسجلة. أضف قناة أولاً.")
        return
    kb = IKM([[IKB(f"{'🟢' if r['auto_enabled'] else '⚪'} {ch_label(r)}"[:60],
                   callback_data=f"cs|{action}|{r['chat_id']}")] for r in rows])
    await say(update, prompt, kb)


@route(B_CH, "view")
async def m_ch(update, ctx):
    n = db.q("SELECT COUNT(*) FROM channels")[0][0]
    await say(update, f"📢 <b>إدارة القنوات</b>\nالقنوات المسجلة: <b>{n}</b>", ch_kb())


@route(B_CH_ADD, "channels")
async def ch_add(update, ctx):
    ctx.user_data["state"] = "add_channel"
    await say(update, "➕ أرسل معرّف القناة (<code>@channelname</code>) أو رقمها (<code>-100…</code>) أو رابط منشور منها.\n"
                      "⚠️ يجب أن يكون البوت <b>مشرفاً</b> في القناة.", rk((B_HOME,)))


@route(B_CH_DEL, "channels")
async def ch_del(update, ctx):
    await send_chooser(update, "del", "🗑️ اختر القناة المراد حذفها:")


@route(B_CH_LIST, "view")
async def ch_list(update, ctx):
    rows = db.q("SELECT * FROM channels ORDER BY added_at DESC LIMIT 30")
    await say(update, "<b>📋 القنوات المسجلة</b>\n\n" + ("\n".join(channel_line(r) for r in rows) if rows else "لا توجد قنوات."))


@route(B_CH_ACT, "view")
async def ch_act(update, ctx):
    rows = db.q("SELECT * FROM channels WHERE auto_enabled=1")
    glob = "🟢 مفعّل" if S("auto_enabled") == "1" else "🔴 متوقف"
    await say(update, f"<b>✅ القنوات المفعّل عليها التفاعل التلقائي</b>\nالتفاعل التلقائي العام: {glob}\n\n" +
              ("\n".join(channel_line(r) for r in rows) if rows else "لا توجد قنوات مفعّلة."))


@route(B_CH_EMO, "channels")
async def ch_emo(update, ctx):
    await send_chooser(update, "emo", "🎭 اختر القناة لتعديل تفاعلاتها:")


@route(B_CH_LAST, "view")
async def ch_last(update, ctx):
    await send_chooser(update, "last", "📌 اختر القناة:")


@route(B_CH_ERR, "view")
async def ch_err(update, ctx):
    await send_chooser(update, "err", "⚠️ اختر القناة:")


@route(B_CH_STAT, "view")
async def ch_stat(update, ctx):
    await send_chooser(update, "status", "🩺 اختر القناة:")


async def channel_status(bot, row) -> str:
    cid = row["chat_id"]
    ok = db.q("SELECT COUNT(*) FROM operations WHERE chat_id=? AND op_type='reaction' AND success=1", (cid,))[0][0]
    bad = db.q("SELECT COUNT(*) FROM operations WHERE chat_id=? AND op_type='reaction' AND success=0", (cid,))[0][0]
    live = ""
    try:
        chat = await bot.get_chat(cid)
        live = f"🌐 النوع: {chat.type} • العنوان: {h(chat.title or '-')}\n🤖 حالة البوت: {h(await bot_status_in(bot, cid))}"
    except TelegramError as e:
        live = f"🔴 تعذر الوصول للقناة حالياً: {h(explain(e))}"
    return (f"🩺 <b>{h(ch_label(row))}</b>\n{live}\n"
            f"🔁 التلقائي: {'🟢 مفعّل' if row['auto_enabled'] else '⚪ متوقف'}\n"
            f"🎭 التفاعلات: {' '.join(ch_emojis(row))}\n"
            f"✅ ناجحة: {ok} • ❌ فاشلة: {bad}\n"
            f"📌 آخر منشور: {row['last_post_id'] or '—'} ({row['last_post_at'] or '—'})\n"
            f"⚠️ آخر خطأ: {h(row['last_error'] or '—')} ({row['last_error_at'] or '—'})")


# ================================= التفاعل التلقائي ============================
def auto_kb():
    return rk((B_AU_ON, B_AU_OFF), (B_AU_STATUS, B_AU_RECENT), (B_AU_ADD, B_AU_RM), (B_AU_ENCH, B_AU_DISCH),
              (B_AU_EMO,), (B_HOME,))


@route(B_AUTO, "view")
async def m_auto(update, ctx):
    n = db.q("SELECT COUNT(*) FROM channels WHERE auto_enabled=1")[0][0]
    glob = "🟢 مفعّل" if S("auto_enabled") == "1" else "🔴 متوقف"
    await say(update, f"🤖 <b>التفاعل التلقائي</b>\nالحالة العامة: {glob}\nالقنوات المفعّلة: <b>{n}</b>", auto_kb())


def _mk_global(btn, val):
    @route(btn, "auto")
    async def _h(update, ctx):
        uid = update.effective_user.id
        set_S("auto_enabled", val)
        log_op("toggle_auto", admin_id=uid, mode="manual", details=f"auto_enabled={val}")
        await say(update, "🟢 تم تفعيل التفاعل التلقائي العام." if val == "1" else "🔴 تم إيقاف التفاعل التلقائي العام.", auto_kb())
        await notify(ctx.bot, "success", f"ℹ️ المدير <code>{uid}</code> غيّر التفاعل التلقائي العام إلى {'تفعيل' if val == '1' else 'إيقاف'}.", exclude=uid)
    return _h


_mk_global(B_AU_ON, "1")
_mk_global(B_AU_OFF, "0")


@route(B_AU_STATUS, "view")
async def au_status(update, ctx):
    rows = db.q("SELECT * FROM channels ORDER BY added_at DESC LIMIT 30")
    out = []
    for r in rows:
        ok = db.q("SELECT COUNT(*) FROM operations WHERE chat_id=? AND mode='auto' AND success=1", (r["chat_id"],))[0][0]
        bad = db.q("SELECT COUNT(*) FROM operations WHERE chat_id=? AND mode='auto' AND success=0", (r["chat_id"],))[0][0]
        out.append(f"{channel_line(r)}\n   ✅ {ok} • ❌ {bad} • ⚠️ {h((r['last_error'] or '—')[:60])}")
    await say(update, "<b>📋 حالة القنوات</b>\n\n" + ("\n".join(out) if out else "لا توجد قنوات."))


@route(B_AU_RECENT, "view")
async def au_recent(update, ctx):
    ok = db.q("SELECT * FROM operations WHERE mode='auto' AND success=1 ORDER BY id DESC LIMIT 5")
    bad = db.q("SELECT * FROM operations WHERE mode='auto' AND success=0 ORDER BY id DESC LIMIT 5")
    await say(update, "<b>✅ آخر الناجحة:</b>\n" + ("\n".join(fmt_op(r) for r in ok) or "—") +
              "\n\n<b>❌ آخر الفاشلة:</b>\n" + ("\n".join(fmt_op(r) for r in bad) or "—"))


ROUTES[B_AU_ADD] = (ch_add, "channels")
ROUTES[B_AU_RM] = (ch_del, "channels")
ROUTES[B_AU_EMO] = (ch_emo, "channels")


@route(B_AU_ENCH, "auto")
async def au_ench(update, ctx):
    await send_chooser(update, "on", "✅ اختر القناة لتفعيل التفاعل التلقائي عليها:")


@route(B_AU_DISCH, "auto")
async def au_disch(update, ctx):
    await send_chooser(update, "off", "⛔ اختر القناة لإيقاف التفاعل التلقائي عليها:")


async def do_add_channel(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str):
    uid, t, ref = update.effective_user.id, text.strip(), None
    p = parse_post_link(t)
    if p:
        ref = p["chat"]
    elif re.fullmatch(r"@?[A-Za-z][A-Za-z0-9_]{3,31}", t):
        ref = "@" + t.lstrip("@")
    elif re.fullmatch(r"-100\d{5,15}", t):
        ref = int(t)
    else:
        m = re.fullmatch(r"(?:https?://)?t\.me/([A-Za-z][A-Za-z0-9_]{3,31})/?", t, re.IGNORECASE)
        ref = "@" + m.group(1) if m else None
    if ref is None:
        await say(update, "❌ مدخل غير صالح. أرسل <code>@channelname</code> أو رقماً يبدأ بـ <code>-100</code>.")
        return
    try:
        chat = await ctx.bot.get_chat(ref)
    except (BadRequest, Forbidden) as e:
        await say(update, f"❌ لا يمكن الوصول إلى القناة: {h(explain(e))}")
        return
    except TelegramError as e:
        await say(update, f"❌ خطأ اتصال: {h(explain(e))}")
        return
    if chat.type != ChatType.CHANNEL:
        await say(update, "❌ هذا ليس قناة. التفاعل التلقائي يعمل على القنوات فقط.")
        return
    status = await bot_status_in(ctx.bot, chat.id)
    if status not in ("مشرف", "مالك"):
        await say(update, f"❌ البوت ليس مشرفاً في القناة (حالته: {h(status)}). أضفه كمشرف ثم أعد المحاولة.")
        return
    if get_channel(chat.id):
        await say(update, "ℹ️ القناة مسجلة مسبقاً.")
        ctx.user_data.pop("state", None)
        return
    db.x("INSERT INTO channels(chat_id,username,title,auto_enabled,emojis,added_at) VALUES(?,?,?,?,?,?)",
         (chat.id, chat.username, chat.title, 0, "", utcnow()))
    log_op("add_channel", chat.id, ("@" + chat.username) if chat.username else chat.title, admin_id=uid)
    ctx.user_data.pop("state", None)
    await say(update, f"✅ تمت إضافة القناة <b>{h(chat.title or '')}</b>. التفاعل التلقائي عليها متوقف؛ فعّله من قسم 🤖.", ch_kb())
    await notify(ctx.bot, "success", f"✅ المدير <code>{uid}</code> أضاف القناة {h(chat.title or '')}.", exclude=uid)


# ================================= التفاعلات (الإعدادات) =======================
def emo_kb():
    return rk((B_EM_PICK, B_EM_SHOW), (B_EM_RESET, B_EM_MAX), (B_EM_SUP,), (B_HOME,))


@route(B_EMO, "view")
async def m_emo(update, ctx):
    await say(update, f"🎭 <b>إعدادات التفاعلات</b>\nالافتراضية: {' '.join(default_emojis())}\n"
                      f"الحد الأقصى للتفاعلات في المنشور: <b>{S_int('max_reactions')}</b>", emo_kb())


@route(B_EM_PICK, "settings")
async def em_pick(update, ctx):
    await show_picker(update, "d", ctx)


ROUTES[B_S_EMO] = (em_pick, "settings")


@route(B_EM_SHOW, "view")
async def em_show(update, ctx):
    await say(update, f"🎭 التفاعلات الافتراضية الحالية: {' '.join(default_emojis())}")


@route(B_EM_RESET, "settings")
async def em_reset(update, ctx):
    set_S("default_emojis", DEFAULTS["default_emojis"])
    log_op("setting", admin_id=update.effective_user.id, details="default_emojis reset")
    await say(update, f"♻️ أُعيدت الافتراضية: {' '.join(default_emojis())}")


@route(B_EM_SUP, "view")
async def em_sup(update, ctx):
    await say(update, "✅ <b>الإيموجيات المدعومة في البوت:</b>\n" + " ".join(EMOJIS) +
              "\n\nجميعها ضمن القائمة الرسمية لـ Bot API. قد تقيّد القناة نفسها التفاعلات المسموحة؛ "
              "وسيظهر ذلك كخطأ صريح عند التنفيذ.")


# ================================= إدارة المستخدمين ============================
def users_kb():
    return rk((B_US_ADDA, B_US_ADDV), (B_US_LIST,), (B_US_EDIT, B_US_DEL), (B_HOME,))


@route(B_USERS, "users")
async def m_users(update, ctx):
    n = db.q("SELECT COUNT(*) FROM users")[0][0]
    await say(update, f"👥 <b>إدارة المستخدمين</b>\nالمدراء العامون (ADMIN_IDS): <b>{len(ADMIN_IDS)}</b> • مستخدمون مضافون: <b>{n}</b>",
              users_kb())


def _mk_adduser(btn, role):
    @route(btn, "users")
    async def _h(update, ctx):
        ctx.user_data["state"] = f"add_user:{role}"
        await say(update, "أرسل المعرّف الرقمي (User ID) للمستخدم:", rk((B_HOME,)))
    return _h


_mk_adduser(B_US_ADDA, "assistant")
_mk_adduser(B_US_ADDV, "viewer")


@route(B_US_LIST, "users")
async def us_list(update, ctx):
    rows = db.q("SELECT * FROM users ORDER BY added_at DESC LIMIT 40")
    t = "<b>👑 المدراء العامون:</b> " + ", ".join(f"<code>{i}</code>" for i in sorted(ADMIN_IDS)) + "\n\n"
    for r in rows:
        ps = "، ".join(PERM_LABELS[p] for p in (r["perms"] or "").split(",") if p in PERM_LABELS)
        t += f"• <code>{r['user_id']}</code> — {role_name(r['role'])}" + (f"\n   الصلاحيات: {h(ps) or '—'}" if r["role"] == "assistant" else "") + "\n"
    await say(update, t)


async def _user_chooser(update, action, prompt, only_assistant=False):
    rows = db.q("SELECT * FROM users" + (" WHERE role='assistant'" if only_assistant else "") + " LIMIT 40")
    if not rows:
        await say(update, "لا يوجد مستخدمون مضافون.")
        return
    kb = IKM([[IKB(f"{role_name(r['role'])} • {r['user_id']}", callback_data=f"um|{action}|{r['user_id']}")] for r in rows])
    await say(update, prompt, kb)


@route(B_US_EDIT, "users")
async def us_edit(update, ctx):
    await _user_chooser(update, "ed", "✏️ اختر المدير المساعد:", True)


@route(B_US_DEL, "users")
async def us_del(update, ctx):
    await _user_chooser(update, "rm", "🗑️ اختر المستخدم المراد إزالته:")


def perm_markup(uid: int):
    cur = user_perms(uid)
    rows = [[IKB(("✅ " if k in cur else "⬜ ") + v, callback_data=f"up|{uid}|{k}")] for k, v in PERM_LABELS.items()]
    rows.append([IKB("💾 تم", callback_data=f"up|{uid}|x")])
    return IKM(rows)


async def do_add_user(update, ctx, text, role):
    uid = update.effective_user.id
    if not re.fullmatch(r"\d{5,15}", text.strip()):
        await say(update, "❌ معرّف غير صالح (أرقام فقط).")
        return
    target = int(text.strip())
    if target in ADMIN_IDS:
        await say(update, "ℹ️ هذا المستخدم مدير عام بالفعل.")
        return
    db.x("INSERT INTO users(user_id,role,perms,added_by,added_at) VALUES(?,?,?,?,?) "
         "ON CONFLICT(user_id) DO UPDATE SET role=excluded.role, perms=CASE WHEN excluded.role='viewer' THEN '' ELSE users.perms END",
         (target, role, "", uid, utcnow()))
    log_op("user_add", admin_id=uid, details=f"{target} as {role}")
    ctx.user_data.pop("state", None)
    await say(update, f"✅ أُضيف <code>{target}</code> بصفة <b>{role_name(role)}</b>. "
                      "يجب أن يفتح البوت ويُدخل الكود السري.", users_kb())
    if role == "assistant":
        await say(update, "حدّد صلاحياته (لا يملك أي صلاحية افتراضياً):", perm_markup(target))
    await notify(ctx.bot, "success", f"✅ أُضيف مستخدم <code>{target}</code> ({role_name(role)}).", exclude=uid)


# ================================= الأمان ======================================
def sec_kb():
    return rk((B_SC_STATUS, B_SC_PERMS), (B_SC_SESS, B_SC_KILL), (B_SC_LOCKED, B_SC_UNLOCK), (B_HOME,))


@route(B_SEC, "security")
async def m_sec(update, ctx):
    await say(update, "🛡️ <b>الأمان والصلاحيات</b>", sec_kb())


@route(B_SC_STATUS, "security")
async def sc_status(update, ctx):
    await say(update, "<b>🛡️ حالة الأمان</b>\n"
                      f"• المدراء العامون: {len(ADMIN_IDS)}\n• جلسات نشطة: {sum(1 for u in list(SESSIONS) if session_ok_peek(u))}\n"
                      f"• محظورون مؤقتاً: {sum(1 for t in LOCKED.values() if t > time.time())}\n"
                      f"• مهلة الجلسة: {S_int('session_timeout')} دقيقة\n• أقصى محاولات دخول: {MAX_LOGIN_ATTEMPTS}\n"
                      f"• وضع الصيانة: {'مفعّل' if S('maintenance') == '1' else 'متوقف'}\n"
                      "• الأسرار محفوظة في متغيرات البيئة ولا تُسجَّل.", sec_kb())


def session_ok_peek(uid: int) -> bool:
    t = SESSIONS.get(uid)
    return t is not None and time.time() - t <= S_int("session_timeout") * 60


@route(B_SC_SESS, "security")
async def sc_sess(update, ctx):
    act = [u for u in SESSIONS if session_ok_peek(u)]
    await say(update, "<b>🟢 الجلسات النشطة</b>\n" + ("\n".join(f"• <code>{u}</code> — {role_name(role_of(u))}" for u in act) or "—"), sec_kb())


@route(B_SC_KILL, "security")
async def sc_kill(update, ctx):
    me = update.effective_user.id
    n = 0
    for u in list(SESSIONS):
        if u != me:
            SESSIONS.pop(u, None)
            n += 1
    log_op("security", admin_id=me, details=f"terminated {n} sessions")
    await say(update, f"🚪 أُنهيت <b>{n}</b> جلسة.", sec_kb())


@route(B_SC_LOCKED, "security")
async def sc_locked(update, ctx):
    now = time.time()
    rows = [f"• <code>{u}</code> — {int((t - now) // 60) + 1} دقيقة" for u, t in LOCKED.items() if t > now]
    await say(update, "<b>🚫 المحظورون مؤقتاً</b>\n" + ("\n".join(rows) or "—"), sec_kb())


@route(B_SC_UNLOCK, "security")
async def sc_unlock(update, ctx):
    LOCKED.clear()
    FAILS.clear()
    log_op("security", admin_id=update.effective_user.id, details="cleared lockouts")
    await say(update, "🔓 رُفع الحظر عن الجميع.", sec_kb())


@route(B_SC_PERMS, "security")
async def sc_perms(update, ctx):
    await say(update, "<b>📜 الأدوار</b>\n👑 <b>المدير العام</b>: كل شيء (يُحدَّد بـ ADMIN_IDS).\n"
                      "🧑‍💼 <b>المدير المساعد</b>: فقط ما يمنحه له المدير العام:\n" +
              "\n".join(f"   • {v}" for v in PERM_LABELS.values()) +
              "\n👁️ <b>المشاهد</b>: عرض القنوات والإحصائيات والسجل فقط دون أي تغيير.", sec_kb())


# ================================= إعدادات البوت ===============================
NUM_SETTINGS = {
    B_S_DELAY: ("retry_delay", 0, 60, "ثانية", "settings"),
    B_S_RETRY: ("max_retries", 1, 10, "محاولة", "settings"),
    B_S_LOGN: ("log_page_size", 3, 30, "سجل", "settings"),
    B_S_SESS: ("session_timeout", 1, 1440, "دقيقة", "settings"),
    B_EM_MAX: ("max_reactions", 1, 7, "تفاعل", "settings"),
    B_BK_AUTO: ("backup_interval_hours", 0, 168, "ساعة (0 = إيقاف)", "backups"),
}
NUM_BY_KEY = {v[0]: v for v in NUM_SETTINGS.values()}


def _mk_num(btn, key, lo, hi, unit, perm):
    @route(btn, perm)
    async def _h(update, ctx):
        ctx.user_data["state"] = f"num:{key}"
        await say(update, f"أرسل القيمة الجديدة ({lo} – {hi} {unit}).\nالحالية: <b>{S(key)}</b>", rk((B_HOME,)))
    return _h


for _b, (_k, _lo, _hi, _u, _p) in NUM_SETTINGS.items():
    _mk_num(_b, _k, _lo, _hi, _u, _p)


def settings_kb():
    return rk((B_S_AUTO, B_S_NOTIFY), (B_S_DELAY, B_S_RETRY), (B_S_EMO, B_S_LOGN), (B_S_MAINT, B_S_SESS), (B_HOME,))


def settings_text() -> str:
    oo = lambda k: "🟢 مفعّل" if S(k) == "1" else "🔴 متوقف"  # noqa: E731
    return ("<b>⚙️ إعدادات البوت</b>\n\n"
            f"🔁 التفاعل التلقائي: {oo('auto_enabled')}\n"
            f"⏱️ فترة الانتظار بين المحاولات: <b>{S_int('retry_delay')}</b> ثانية\n"
            f"🔂 عدد المحاولات: <b>{S_int('max_retries')}</b>\n"
            f"🔔 إشعارات الأخطاء: {oo('notify_fail')}\n"
            f"🎭 الافتراضية: {' '.join(default_emojis())} (الحد الأقصى {S_int('max_reactions')})\n"
            f"📄 عدد السجلات المعروضة: <b>{S_int('log_page_size')}</b>\n"
            f"🛠️ وضع الصيانة: {oo('maintenance')}\n"
            f"⌛ مهلة الجلسة: <b>{S_int('session_timeout')}</b> دقيقة")


@route(B_SET, "view")
async def m_set(update, ctx):
    await say(update, settings_text(), settings_kb())


def _mk_toggle(btn, key, label, perm="settings", kb=settings_kb, show=settings_text):
    @route(btn, perm)
    async def _h(update, ctx):
        uid = update.effective_user.id
        new = "0" if S(key) == "1" else "1"
        set_S(key, new)
        log_op("setting", admin_id=uid, details=f"{key}={new}")
        await say(update, f"{'🟢' if new == '1' else '🔴'} {label}: {'مفعّل' if new == '1' else 'متوقف'}\n\n" + show(), kb())
        if key == "maintenance":
            await notify(ctx.bot, "success", f"🛠️ وضع الصيانة {'فُعّل' if new == '1' else 'أُوقف'} بواسطة <code>{uid}</code>.", exclude=uid)
    return _h


_mk_toggle(B_S_AUTO, "auto_enabled", "التفاعل التلقائي")
_mk_toggle(B_S_NOTIFY, "notify_fail", "إشعارات الأخطاء")
_mk_toggle(B_S_MAINT, "maintenance", "وضع الصيانة")


# ================================= النسخ الاحتياطية ============================
BK_RE = re.compile(r"backup_[a-z_]+_\d{8}_\d{6}\.db")


def make_backup(tag: str = "manual") -> Path:
    d = Path(BACKUP_DIR)
    d.mkdir(parents=True, exist_ok=True)
    dest = d / f"backup_{tag}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.db"
    try:
        dst = sqlite3.connect(str(dest))
        try:
            with db.lock:
                db.conn.backup(dst)
        finally:
            dst.close()
        ok, msg = verify_backup(dest)
        if not ok:
            raise RuntimeError(f"النسخة غير سليمة: {msg}")
    except Exception:
        dest.unlink(missing_ok=True)
        raise
    if tag == "auto":
        keep = max(1, S_int("backup_keep"))
        autos = sorted(p for p in d.glob("backup_auto_*.db") if BK_RE.fullmatch(p.name))
        for old in autos[:-keep]:
            old.unlink(missing_ok=True)
    return dest


def verify_backup(path) -> tuple[bool, str]:
    try:
        con = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
        try:
            if con.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                return False, "فشل فحص سلامة SQLite"
            names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            miss = REQUIRED_TABLES - names
            if miss:
                return False, "جداول مفقودة: " + ", ".join(sorted(miss))
            return True, "سليمة"
        finally:
            con.close()
    except sqlite3.Error as e:
        return False, f"ملف غير صالح: {e}"


def list_backups() -> list[Path]:
    d = Path(BACKUP_DIR)
    if not d.exists():
        return []
    return sorted((p for p in d.glob("backup_*.db") if BK_RE.fullmatch(p.name)), reverse=True)


def resolve_backup(name: str) -> Path | None:
    return next((p for p in list_backups() if p.name == name), None)


def restore_backup(path: Path) -> str:
    """يتحقق من النسخة أولاً، ثم يأخذ نسخة أمان من الحالية، ثم يستعيد."""
    ok, msg = verify_backup(path)
    if not ok:
        raise ValueError(f"النسخة غير صالحة: {msg}")
    safety = make_backup("pre_restore")
    src = sqlite3.connect(str(path))
    try:
        with db.lock:
            src.backup(db.conn)
            db.conn.commit()
    finally:
        src.close()
    db.init()
    return safety.name


def bk_kb():
    return rk((B_BK_NOW, B_BK_LIST), (B_BK_VER, B_BK_RES), (B_BK_AUTO,), (B_HOME,))


def fmt_size(p: Path) -> str:
    return f"{p.stat().st_size / 1024:.1f}KB"


@route(B_BAK, "backups")
async def m_bak(update, ctx):
    n = len(list_backups())
    h_ = S_int("backup_interval_hours")
    await say(update, f"💾 <b>النسخ الاحتياطية</b>\nعدد النسخ: <b>{n}</b>\nالنسخ الدوري: "
                      f"{'كل ' + str(h_) + ' ساعة' if h_ else 'متوقف'}", bk_kb())


@route(B_BK_NOW, "backups")
async def bk_now(update, ctx):
    uid = update.effective_user.id
    try:
        p = await asyncio.to_thread(make_backup, "manual")
    except Exception as e:  # noqa: BLE001
        log_op("backup", admin_id=uid, success=False, error=str(e))
        await say(update, f"❌ فشل إنشاء النسخة: {h(redact(str(e)))}", bk_kb())
        await notify(ctx.bot, "fail", f"❌ فشل نسخ احتياطي يدوي: {h(redact(str(e)))}", exclude=uid)
        return
    log_op("backup", admin_id=uid, details=p.name)
    await say(update, f"✅ أُنشئت النسخة وتم التحقق منها:\n<code>{p.name}</code> ({fmt_size(p)})", bk_kb())
    await notify(ctx.bot, "success", f"💾 نسخة احتياطية جديدة بواسطة <code>{uid}</code>.", exclude=uid)


@route(B_BK_LIST, "backups")
async def bk_list(update, ctx):
    bs = list_backups()[:20]
    await say(update, "<b>📂 النسخ الموجودة</b>\n" + ("\n".join(f"• <code>{p.name}</code> ({fmt_size(p)})" for p in bs) or "لا توجد نسخ."), bk_kb())


async def _bk_chooser(update, action, prompt):
    bs = list_backups()[:15]
    if not bs:
        await say(update, "لا توجد نسخ.")
        return
    await say(update, prompt, IKM([[IKB(p.name[7:-3], callback_data=f"bk|{action}|{p.name}")] for p in bs]))


@route(B_BK_VER, "backups")
async def bk_ver(update, ctx):
    await _bk_chooser(update, "v", "✅ اختر نسخة للتحقق من سلامتها:")


@route(B_BK_RES, "backups")
async def bk_res(update, ctx):
    await _bk_chooser(update, "r", "♻️ اختر النسخة المراد استعادتها:")


async def periodic_backup(ctx: ContextTypes.DEFAULT_TYPE):
    try:
        hrs = S_int("backup_interval_hours")
        db.x("DELETE FROM processed WHERE created_at < datetime('now','-30 days')")
        if hrs <= 0:
            return
        if time.time() - float(S("last_auto_backup") or 0) < hrs * 3600:
            return
        p = await asyncio.to_thread(make_backup, "auto")
        set_S("last_auto_backup", int(time.time()))
        log_op("backup", mode="auto", details=p.name)
    except Exception as e:  # noqa: BLE001
        log.error("فشل النسخ الدوري: %s", redact(str(e)))
        log_op("backup", mode="auto", success=False, error=str(e))
        await notify(ctx.bot, "fail", f"❌ فشل النسخ الاحتياطي الدوري: {h(redact(str(e)))}")


# ================================= الإشعارات (قسم) =============================
NOTIF_KEYS = {B_N_FAIL: ("notify_fail", "فشل التفاعل"), B_N_REP: ("notify_repeated", "الأخطاء المتكررة"),
              B_N_RST: ("notify_restart", "تشغيل/إعادة تشغيل البوت"), B_N_ACC: ("notify_access", "مشاكل الوصول للقنوات"),
              B_N_OK: ("notify_success", "العمليات الإدارية الناجحة")}


def notif_kb():
    return rk((B_N_FAIL, B_N_REP), (B_N_RST, B_N_ACC), (B_N_OK,), (B_N_ALL_ON, B_N_ALL_OFF), (B_N_TEST,), (B_HOME,))


def notif_text() -> str:
    return "<b>🔔 الإشعارات</b>\n\n" + "\n".join(
        f"{'🟢' if S(k) == '1' else '🔴'} {label}" for k, label in NOTIF_KEYS.values()) + \
        "\n\nتصل الإشعارات إلى المدراء العامين والمساعدين الذين يملكون صلاحية الإشعارات."


@route(B_NOT, "view")
async def m_not(update, ctx):
    await say(update, notif_text(), notif_kb())


def _mk_notif(btn, key, label):
    _mk_toggle(btn, key, label, "notifications", notif_kb, notif_text)


for _b, (_k, _l) in NOTIF_KEYS.items():
    _mk_notif(_b, _k, _l)


def _mk_all(btn, val):
    @route(btn, "notifications")
    async def _h(update, ctx):
        for k, _ in NOTIF_KEYS.values():
            set_S(k, val)
        log_op("setting", admin_id=update.effective_user.id, details=f"all notifications={val}")
        await say(update, notif_text(), notif_kb())
    return _h


_mk_all(B_N_ALL_ON, "1")
_mk_all(B_N_ALL_OFF, "0")


@route(B_N_TEST, "notifications")
async def n_test(update, ctx):
    await notify(ctx.bot, "fail", f"🧪 إشعار تجريبي من <code>{update.effective_user.id}</code>.", force=True)
    await say(update, "🧪 أُرسل الإشعار التجريبي إلى المستلمين (قد يفشل الإرسال لمن لم يبدأ محادثة مع البوت).", notif_kb())


# ================================= مساعدة / قفل ================================
@route(B_HELP)
async def m_help(update, ctx):
    await say(update,
              "<b>ℹ️ المساعدة</b>\n"
              "• 🔗 أرسل رابط منشور <code>https://t.me/name/123</code> لإضافة تفاعلات (مراجعة ← تأكيد ← تنفيذ).\n"
              "• 🤖 التفاعل التلقائي يتطلب أن يكون البوت <b>مشرفاً</b> في القناة، وتفعيل القناة والخيار العام.\n"
              "• 🎭 يسمح Telegram للبوتات عادةً بتفاعل واحد فقط في المنشور؛ إن رفض الأكثر يخفّض البوت العدد تلقائياً "
              "ويعرض ما طُبّق فعلاً.\n"
              "• 📋 كل عملية تُسجَّل مع نتيجتها الفعلية.\n• /cancel لإلغاء إدخال جارٍ.\n"
              "• تنتهي الجلسة بعد فترة خمول ويلزم إعادة إدخال الكود السري.")


@route(B_LOCK)
async def m_lock(update, ctx):
    SESSIONS.pop(update.effective_user.id, None)
    ctx.user_data.clear()
    await say(update, "🔒 قُفلت لوحة التحكم. أرسل الكود السري لفتحها من جديد.", ReplyKeyboardRemove())


# ================================= معالجة النصوص ===============================
async def handle_state(update: Update, ctx: ContextTypes.DEFAULT_TYPE, text: str, state: str):
    uid = update.effective_user.id
    if state == "await_link":
        if not can(uid, "react"):
            return
        ctx.user_data.pop("state", None)
        await start_link_flow(update, ctx, text)
    elif state == "add_channel":
        if can(uid, "channels"):
            await do_add_channel(update, ctx, text)
    elif state.startswith("add_user:"):
        if can(uid, "users"):
            await do_add_user(update, ctx, text, state.split(":", 1)[1])
    elif state.startswith("num:"):
        key = state[4:]
        _, lo, hi, unit, perm = NUM_BY_KEY[key]
        if not can(uid, perm):
            return
        if not re.fullmatch(r"\d{1,6}", text.strip()) or not lo <= int(text) <= hi:
            await say(update, f"❌ أدخل رقماً صحيحاً بين {lo} و {hi}.")
            return
        set_S(key, int(text))
        log_op("setting", admin_id=uid, details=f"{key}={int(text)}")
        ctx.user_data.pop("state", None)
        await say(update, f"✅ حُفظ: {key} = <b>{int(text)}</b>", main_kb(uid))
    elif state == "log_search":
        if not can(uid, "logs"):
            return
        ctx.user_data.pop("state", None)
        like = "%" + text.strip().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
        rows = db.q("SELECT * FROM operations WHERE channel_label LIKE ? ESCAPE '\\' OR post_url LIKE ? ESCAPE '\\' "
                    "OR op_type LIKE ? ESCAPE '\\' OR error LIKE ? ESCAPE '\\' ORDER BY id DESC LIMIT ?",
                    (like, like, like, like, S_int("log_page_size")))
        await say(update, f"🔍 نتائج «{h(text.strip()[:40])}»:\n\n" + ("\n".join(fmt_op(r) for r in rows) or "لا نتائج."), logs_kb())
    elif state == "log_detail":
        if not can(uid, "logs"):
            return
        if not text.strip().isdigit():
            await say(update, "❌ أرسل رقماً.")
            return
        ctx.user_data.pop("state", None)
        r = db.q("SELECT * FROM operations WHERE id=?", (int(text),))
        await say(update, fmt_op(r[0], True) if r else "لا توجد عملية بهذا الرقم.", logs_kb())
    else:
        ctx.user_data.pop("state", None)


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg, uid = update.effective_message, update.effective_user.id
    text = (msg.text or "").strip()
    role = role_of(uid)
    if role is None:
        await say(update, "⛔ هذا البوت خاص ولا يمكنك استخدامه.")
        return
    if S("maintenance") == "1" and role != "owner":
        await say(update, "🛠️ البوت في وضع الصيانة حالياً.")
        return
    if not session_ok(uid):
        if uid in EXPIRED:
            EXPIRED.discard(uid)
            await say(update, "⌛ انتهت الجلسة الإدارية بسبب عدم النشاط. أرسل الكود السري من جديد.", ReplyKeyboardRemove())
            return
        await login(update, ctx, text)
        return
    if text == B_HOME:
        ctx.user_data.pop("state", None)
        ctx.user_data.pop("pending", None)
        await show_main(update, uid)
        return
    if text in ROUTES:
        func, perm = ROUTES[text]
        ctx.user_data.pop("state", None)
        if perm and not can(uid, perm):
            await say(update, "⛔ لا تملك صلاحية لهذا الإجراء.")
            return
        await func(update, ctx)
        return
    state = ctx.user_data.get("state")
    if state:
        await handle_state(update, ctx, text, state)
        return
    if parse_post_link(text):
        await start_link_flow(update, ctx, text)
        return
    await show_main(update, uid)


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if role_of(uid) is None:
        await say(update, "⛔ هذا البوت خاص ولا يمكنك استخدامه.")
    elif session_ok(uid):
        await show_main(update, uid)
    else:
        await say(update, "🔐 أرسل الكود السري لفتح لوحة التحكم.", ReplyKeyboardRemove())


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if role_of(uid) is None or not session_ok(uid):
        return
    ctx.user_data.pop("state", None)
    ctx.user_data.pop("pending", None)
    await show_main(update, uid, "تم الإلغاء.\n\n")


# ================================= معالجة الأزرار المضمّنة =====================
CB: dict = {}


def cb(kind: str):
    def deco(f):
        CB[kind] = f
        return f
    return deco


async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    uid, data = q.from_user.id, q.data or ""
    role = role_of(uid)
    if role is None:
        await q.answer("⛔ غير مصرح.", show_alert=True)
        return
    if S("maintenance") == "1" and role != "owner":
        await q.answer("🛠️ البوت في وضع الصيانة.", show_alert=True)
        return
    if not session_ok(uid):
        await q.answer("⌛ انتهت الجلسة الإدارية. أرسل الكود السري من جديد.", show_alert=True)
        return
    parts = data.split("|")
    fn = CB.get(parts[0])
    if not fn:
        await q.answer()
        return
    await fn(update, ctx, q, uid, parts[1:])


async def deny(q):
    await q.answer("⛔ لا تملك صلاحية لهذا الإجراء.", show_alert=True)


@cb("lg")
async def cb_logs(update, ctx, q, uid, a):
    if not can(uid, "logs"):
        return await deny(q)
    await q.answer()
    t, m = logs_page(a[0], max(0, int(a[1])))
    await safe_edit(q, t, m)


@cb("pk")
async def cb_pick(update, ctx, q, uid, a):
    scope, act = a[0], a[1]
    need = "react" if scope == "p" else ("settings" if scope == "d" else "channels")
    if not can(uid, need):
        return await deny(q)
    sel = get_sel(ctx, scope)
    if sel is None:
        await q.answer("⚠️ الطلب منتهٍ.", show_alert=True)
        return
    mx = S_int("max_reactions")
    if act == "ok":
        if scope == "d" and not sel:
            await q.answer("اختر إيموجي واحداً على الأقل.", show_alert=True)
            return
        await q.answer("تم")
        if scope == "p":
            p = ctx.user_data["pending"]
            await safe_edit(q, pending_text(p), pending_markup(p))
        else:
            if scope == "d":
                log_op("setting", admin_id=uid, emojis=",".join(sel), details="default_emojis")
            await safe_edit(q, f"💾 حُفظت التفاعلات: {' '.join(sel) or '(الافتراضية)'}")
        return
    if act == "clr":
        sel = []
    else:
        e = EMOJIS[int(act)]
        if e in sel:
            sel.remove(e)
        elif len(sel) >= mx:
            await q.answer(f"الحد الأقصى {mx} تفاعلات (يمكن تغييره من الإعدادات).", show_alert=True)
            return
        else:
            sel.append(e)
    set_sel(ctx, scope, sel)
    await q.answer()
    await safe_edit(q, picker_text(scope, sel), picker_markup(scope, sel))


@cb("ln")
async def cb_link(update, ctx, q, uid, a):
    act, token = a[0], a[1]
    if not can(uid, "react"):
        return await deny(q)
    p = ctx.user_data.get("pending")
    if not p or p["token"] != token:
        await q.answer("⚠️ هذا الطلب منتهٍ أو نُفّذ مسبقاً.", show_alert=True)
        return
    if act == "no":
        ctx.user_data.pop("pending", None)
        await q.answer()
        await safe_edit(q, "❌ أُلغيت العملية.")
    elif act == "ed":
        await q.answer()
        await safe_edit(q, picker_text("p", p["emojis"]), picker_markup("p", p["emojis"]))
    elif act == "go":
        if not p["emojis"]:
            await q.answer("اختر تفاعلاً واحداً على الأقل.", show_alert=True)
            return
        ctx.user_data.pop("pending", None)  # منع التنفيذ المزدوج
        key = (p["chat"], p["message_id"])
        now = time.time()
        for k in [k for k, t in RECENT.items() if now - t > 300]:
            RECENT.pop(k, None)
        if now - RECENT.get(key, 0) < DUP_WINDOW_SECONDS:
            await q.answer("⚠️ نُفّذ طلب مماثل للتو.", show_alert=True)
            return
        RECENT[key] = now
        await q.answer("⏳ جارٍ التنفيذ…")
        await safe_edit(q, "⏳ جارٍ تنفيذ العملية…")
        ok, applied, err = await apply_reaction(ctx.bot, p["chat"], p["message_id"], p["emojis"])
        log_op("reaction", p["chat"] if isinstance(p["chat"], int) else None, p["label"], p["url"],
               " ".join(applied if ok else p["emojis"]), ok, err, uid, "manual")
        if ok:
            note = "" if len(applied) == len(p["emojis"]) else f"\n⚠️ طُبّق {len(applied)} من {len(p['emojis'])} فقط (حد Telegram/الإعداد)."
            await safe_edit(q, f"✅ <b>نجحت العملية</b>\n📢 {h(p['label'])} • المنشور <code>{p['message_id']}</code>\n"
                               f"🎭 المطبّق فعلاً: {' '.join(applied)}{note}")
        else:
            await safe_edit(q, f"❌ <b>فشلت العملية</b>\n📢 {h(p['label'])} • المنشور <code>{p['message_id']}</code>\nالسبب: {h(err)}")
            await notify(ctx.bot, "fail", f"❌ فشل تفاعل يدوي على {h(p['label'])}/{p['message_id']}: {h(err)}", exclude=uid)


@cb("cs")
async def cb_chan(update, ctx, q, uid, a):
    act, cid = a[0], int(a[1])
    need = {"del": "channels", "emo": "channels", "on": "auto", "off": "auto"}.get(act, "view")
    if not can(uid, need):
        return await deny(q)
    row = get_channel(cid)
    if not row:
        await q.answer("القناة غير موجودة.", show_alert=True)
        return
    await q.answer()
    lbl = ch_label(row)
    if act == "del":
        await safe_edit(q, f"⚠️ حذف القناة <b>{h(lbl)}</b> من القائمة؟ (لن يُحذف سجلها)",
                        IKM([[IKB("🗑️ نعم، احذف", callback_data=f"cd|{cid}|y"), IKB("إلغاء", callback_data=f"cd|{cid}|n")]]))
    elif act == "emo":
        scope = f"c{cid}"
        sel = get_sel(ctx, scope)
        await safe_edit(q, f"{h(lbl)}\n" + picker_text(scope, sel), picker_markup(scope, sel))
    elif act == "last":
        await safe_edit(q, f"📌 <b>{h(lbl)}</b>\nآخر منشور معالج: <code>{row['last_post_id'] or '—'}</code>\nالوقت: {row['last_post_at'] or '—'}")
    elif act == "err":
        await safe_edit(q, f"⚠️ <b>{h(lbl)}</b>\nآخر خطأ: {h(row['last_error'] or 'لا يوجد')}\nالوقت: {row['last_error_at'] or '—'}")
    elif act == "status":
        await safe_edit(q, await channel_status(ctx.bot, row))
    elif act in ("on", "off"):
        val = 1 if act == "on" else 0
        db.x("UPDATE channels SET auto_enabled=? WHERE chat_id=?", (val, cid))
        log_op("toggle_auto", cid, lbl, admin_id=uid, details=f"auto_enabled={val}")
        extra = "\n⚠️ التفاعل التلقائي العام متوقف حالياً." if val and S("auto_enabled") != "1" else ""
        await safe_edit(q, f"{'🟢 فُعّل' if val else '⛔ أُوقف'} التفاعل التلقائي على <b>{h(lbl)}</b>.{extra}")


@cb("cd")
async def cb_chan_del(update, ctx, q, uid, a):
    if not can(uid, "channels"):
        return await deny(q)
    cid, ans = int(a[0]), a[1]
    row = get_channel(cid)
    await q.answer()
    if ans != "y" or not row:
        await safe_edit(q, "تم الإلغاء." if ans != "y" else "القناة غير موجودة.")
        return
    db.x("DELETE FROM channels WHERE chat_id=?", (cid,))
    log_op("remove_channel", cid, ch_label(row), admin_id=uid)
    await safe_edit(q, f"🗑️ حُذفت القناة <b>{h(ch_label(row))}</b>.")
    await notify(ctx.bot, "success", f"🗑️ المدير <code>{uid}</code> حذف القناة {h(ch_label(row))}.", exclude=uid)


@cb("um")
async def cb_user(update, ctx, q, uid, a):
    if not can(uid, "users"):
        return await deny(q)
    act, target = a[0], int(a[1])
    r = db.q("SELECT * FROM users WHERE user_id=?", (target,))
    if not r:
        await q.answer("المستخدم غير موجود.", show_alert=True)
        return
    await q.answer()
    if act == "ed":
        if r[0]["role"] != "assistant":
            await safe_edit(q, "الصلاحيات تُعدَّل للمدير المساعد فقط.")
            return
        await safe_edit(q, f"✏️ صلاحيات <code>{target}</code>:", perm_markup(target))
    elif act == "rm":
        await safe_edit(q, f"⚠️ إزالة <code>{target}</code> ({role_name(r[0]['role'])})؟",
                        IKM([[IKB("🗑️ نعم", callback_data=f"um|rmy|{target}"), IKB("إلغاء", callback_data="um|no|0")]]))
    elif act == "rmy":
        db.x("DELETE FROM users WHERE user_id=?", (target,))
        SESSIONS.pop(target, None)
        log_op("user_remove", admin_id=uid, details=str(target))
        await safe_edit(q, f"🗑️ أُزيل <code>{target}</code> وأُنهيت جلسته.")
        await notify(ctx.bot, "success", f"🗑️ أُزيل المستخدم <code>{target}</code> بواسطة <code>{uid}</code>.", exclude=uid)


@cb("up")
async def cb_perm(update, ctx, q, uid, a):
    if not can(uid, "users"):
        return await deny(q)
    target, perm = int(a[0]), a[1]
    r = db.q("SELECT * FROM users WHERE user_id=? AND role='assistant'", (target,))
    if not r:
        await q.answer("المستخدم غير موجود أو ليس مساعداً.", show_alert=True)
        return
    await q.answer()
    if perm == "x":
        await safe_edit(q, f"💾 حُفظت صلاحيات <code>{target}</code>.")
        return
    if perm not in PERM_LABELS:
        return
    cur = user_perms(target)
    cur.symmetric_difference_update({perm})
    db.x("UPDATE users SET perms=? WHERE user_id=?", (",".join(sorted(cur)), target))
    log_op("user_perms", admin_id=uid, details=f"{target}: {','.join(sorted(cur))}")
    await safe_edit(q, f"✏️ صلاحيات <code>{target}</code>:", perm_markup(target))


@cb("bk")
async def cb_backup(update, ctx, q, uid, a):
    if not can(uid, "backups"):
        return await deny(q)
    act = a[0]
    if act == "x":
        await q.answer()
        await safe_edit(q, "تم الإلغاء.")
        return
    path = resolve_backup(a[1]) if len(a) > 1 else None
    if not path:
        await q.answer("النسخة غير موجودة.", show_alert=True)
        return
    await q.answer()
    if act == "v":
        ok, msg = await asyncio.to_thread(verify_backup, path)
        await safe_edit(q, f"{'✅' if ok else '❌'} <code>{path.name}</code>\nالنتيجة: {h(msg)}")
    elif act == "r":
        ok, msg = await asyncio.to_thread(verify_backup, path)
        if not ok:
            await safe_edit(q, f"❌ لا يمكن استعادة هذه النسخة: {h(msg)}\nلم تُمَس البيانات الحالية.")
            return
        await safe_edit(q, f"⚠️ <b>تأكيد الاستعادة</b>\n<code>{path.name}</code>\nستُستبدل البيانات الحالية بمحتوى النسخة "
                           "(تؤخذ نسخة أمان من الحالية أولاً).",
                        IKM([[IKB("♻️ نعم، استعد", callback_data=f"bk|rc|{path.name}"), IKB("إلغاء", callback_data="bk|x")]]))
    elif act == "rc":
        try:
            safety = await asyncio.to_thread(restore_backup, path)
        except Exception as e:  # noqa: BLE001
            log_op("restore", admin_id=uid, success=False, error=str(e), details=path.name)
            await safe_edit(q, f"❌ فشلت الاستعادة: {h(redact(str(e)))}\nلم تُحذف البيانات الحالية قبل التحقق.")
            await notify(ctx.bot, "fail", f"❌ فشلت استعادة نسخة: {h(redact(str(e)))}", exclude=uid)
            return
        log_op("restore", admin_id=uid, details=f"{path.name} (safety: {safety})")
        await safe_edit(q, f"✅ تمت الاستعادة من <code>{path.name}</code>.\nنسخة الأمان: <code>{safety}</code>")
        await notify(ctx.bot, "success", f"♻️ استُعيدت نسخة احتياطية بواسطة <code>{uid}</code>.", exclude=uid)


# ================================= التفاعل التلقائي (تحديثات القنوات) ==========
async def on_channel_post(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    post = update.channel_post
    if not post:
        return
    row = get_channel(post.chat_id)
    if not row or not row["auto_enabled"] or S("auto_enabled") != "1" or S("maintenance") == "1":
        return
    if db.x("INSERT OR IGNORE INTO processed(chat_id,message_id,created_at) VALUES(?,?,?)",
            (post.chat_id, post.message_id, utcnow())).rowcount == 0:
        return  # تمت معالجته سابقاً
    label = ch_label(row)
    url = f"https://t.me/{row['username']}/{post.message_id}" if row["username"] else f"t.me/c/{str(post.chat_id)[4:]}/{post.message_id}"
    ok, applied, err = await apply_reaction(ctx.bot, post.chat_id, post.message_id, ch_emojis(row))
    now = utcnow()
    if ok:
        FAIL_STREAK[post.chat_id] = 0
        db.x("UPDATE channels SET last_post_id=?, last_post_at=? WHERE chat_id=?", (post.message_id, now, post.chat_id))
    else:
        FAIL_STREAK[post.chat_id] += 1
        db.x("UPDATE channels SET last_post_id=?, last_post_at=?, last_error=?, last_error_at=? WHERE chat_id=?",
             (post.message_id, now, err, now, post.chat_id))
    log_op("reaction", post.chat_id, label, url, " ".join(applied if ok else ch_emojis(row)), ok, err, None, "auto")
    if not ok:
        await notify(ctx.bot, "fail", f"❌ فشل تفاعل تلقائي في {h(label)} (منشور {post.message_id}): {h(err)}")
        if FAIL_STREAK[post.chat_id] == 3:
            await notify(ctx.bot, "repeated", f"🔁 تكرر الفشل 3 مرات متتالية في {h(label)}. آخر سبب: {h(err)}")
        if any(k in err for k in ("صلاحيات", "الوصول", "غير موجودة")):
            await notify(ctx.bot, "access", f"🚫 مشكلة وصول في القناة {h(label)}: {h(err)}")


async def on_my_chat_member(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cm = update.my_chat_member
    if not cm or cm.chat.type != ChatType.CHANNEL:
        return
    row = get_channel(cm.chat.id)
    if not row:
        return
    st = cm.new_chat_member.status
    if st in ("administrator", "creator"):
        return
    msg = f"البوت لم يعد مشرفاً (الحالة: {st})"
    db.x("UPDATE channels SET last_error=?, last_error_at=? WHERE chat_id=?", (msg, utcnow(), cm.chat.id))
    log_op("security", cm.chat.id, ch_label(row), success=False, error=msg, mode="system")
    await notify(ctx.bot, "access", f"🚫 القناة {h(ch_label(row))}: {h(msg)}. التفاعل التلقائي لن يعمل عليها.")


async def on_error(update, ctx: ContextTypes.DEFAULT_TYPE):
    global LAST_ERR_NOTIFY
    err = ctx.error
    log.error("خطأ غير معالج: %s", redact(repr(err)), exc_info=err if not isinstance(err, TelegramError) else None)
    if isinstance(err, (TimedOut, NetworkError)) and not isinstance(err, BadRequest):
        return
    try:
        if isinstance(update, Update) and update.effective_message and update.effective_chat.type == ChatType.PRIVATE:
            await update.effective_message.reply_text("⚠️ حدث خطأ غير متوقع أثناء تنفيذ الطلب. لم يتوقف البوت.")
    except TelegramError:
        pass
    if time.time() - LAST_ERR_NOTIFY > 300:
        LAST_ERR_NOTIFY = time.time()
        await notify(ctx.bot, "fail", f"🚨 خطأ داخلي: <code>{h(err.__class__.__name__)}</code> — {h(redact(str(err))[:150])}")


async def post_init(app: Application):
    global BOT_ID
    me = await app.bot.get_me()
    BOT_ID = me.id
    log.info("البوت يعمل: @%s", me.username)
    await notify(app.bot, "restart", f"🔄 تم تشغيل البوت @{h(me.username or '')} ({utcnow()} UTC).")


def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()
    private = filters.ChatType.PRIVATE
    app.add_handler(CommandHandler(["start", "panel"], cmd_start, filters=private))
    app.add_handler(CommandHandler("cancel", cmd_cancel, filters=private))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POST, on_channel_post, block=False))
    app.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(MessageHandler(private & filters.TEXT & ~filters.COMMAND, on_text))
    app.add_error_handler(on_error)
    if app.job_queue:
        app.job_queue.run_repeating(periodic_backup, interval=600, first=60)
    else:
        log.warning("JobQueue غير متاح: ثبّت python-telegram-bot[job-queue] لتفعيل النسخ الدوري.")
    return app


def main() -> None:
    missing = [n for n, v in (("BOT_TOKEN", BOT_TOKEN), ("ADMIN_SECRET", ADMIN_SECRET), ("ADMIN_IDS", ADMIN_IDS)) if not v]
    if missing:
        print("❌ متغيرات البيئة التالية مفقودة: " + ", ".join(missing) + "\nراجع ملف .env.example")
        sys.exit(1)
    setup_logging()
    init_db(DB_PATH)
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    build_app().run_polling(allowed_updates=["message", "callback_query", "channel_post", "my_chat_member"])


if __name__ == "__main__":
    main()
