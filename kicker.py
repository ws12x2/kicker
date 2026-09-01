"""
Kanal Ulash + Vaqtinchalik A'zolik + Hamkorlar Boti — bitta faylda.

Ishga tushirish:
    pip install aiogram aiosqlite aiohttp_socks
    python3 kicker.py

Sozlash uchun pastdagi BOT_TOKEN qatorini to'ldiring
(yoki BOT_TOKEN muhit o'zgaruvchisini bering).

MUHIM: "A'zo qo'shish" funksiyasi ishlashi uchun kanalingizda
"so'rov orqali qo'shilish" (join request) yoqilgan bo'lishi kerak:
Kanal -> Administrators -> Invite Links -> "Require Admin Approval".

MUDDAT: kun soni kiritilgan zahoti boshlanadi (foydalanuvchi hali kanalga
kirmagan bo'lsa ham). Shu muddat davomida foydalanuvchi istalgancha kanalga
kirib-chiqishi mumkin - har safar so'rov yuborganda, muddati tugamagan bo'lsa,
bot avtomatik qabul qiladi.

HAMKORLAR: narx kiritilgan zahoti hamkorning balansiga qo'shiladi.
Agar a'zo muddatidan oldin qo'lda chiqarib yuborilsa, kiritilgan narx
balansdan ayirilsinmi deb so'raladi (narx 0 bo'lsa so'ralmaydi).
"""

import asyncio
import http.server
import logging
import os
import sqlite3
import threading
import time
from collections import defaultdict
from datetime import datetime

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.session.middlewares.base import BaseRequestMiddleware
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ChatJoinRequest,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    FSInputFile,
)
from aiogram.exceptions import TelegramBadRequest
try:
    from aiogram.exceptions import TelegramMigrateToChat
except ImportError:
    class TelegramMigrateToChat(Exception):
        """aiogram versiyasida bu klass topilmasa, hech qachon ushlanmaydigan bo'sh o'rinbosar."""
        migrate_to_chat_id = None

# ==================== SOZLAMALAR ====================

BOT_TOKEN = os.getenv("BOT_TOKEN", "6011700872:AAFwlE59GqI04UeHgWkBnl5RwExjKI5RSl0")
DB_PATH = os.getenv("DB_PATH", "kicker.db")

# BOT EGASI (owner) - bu botning umumiy /admin paneliga (baza zaxirasi va
# tiklash) kirish huquqiga ega bo'lgan shaxs(lar). Bu botning har bir
# foydalanuvchisi o'z kanalini ulab, o'sha kanal uchun "admin" bo'lishi
# mumkin (added_by orqali) - lekin BAZANI BOSHQARISH faqat OWNER_IDS
# ro'yxatidagilar uchun.
# Sozlash: Render "Environment" bo'limida OWNER_IDS="123456789,987654321"
# kabi (vergul bilan ajratib, bir nechta bo'lishi mumkin) yozing, yoki
# pastdagi ro'yxatga to'g'ridan-to'g'ri o'z Telegram ID'ingizni yozing.
_owner_ids_raw = os.getenv("OWNER_IDS", "").strip()
OWNER_IDS = [int(x.strip()) for x in _owner_ids_raw.split(",") if x.strip().lstrip("-").isdigit()]
if not OWNER_IDS:
    OWNER_IDS = [5393636771]  # <-- agar OWNER_IDS muhit o'zgaruvchisi bo'lmasa, shu yerga o'z ID'ingizni yozing


def is_owner(user_id: int) -> bool:
    return user_id in OWNER_IDS


# BAZA ZAXIRASI SAQLANADIGAN GURUH (Render kabi platformalarda doimiy fayl
# xotirasi bo'lmagani uchun ZARUR - aks holda qayta deploy qilinganda barcha
# ma'lumot yo'qolib ketadi). Guruh yaratib, botni admin qilib qo'shing
# (Pin Messages huquqi bilan), so'ng guruh ID'sini shu yerga (yoki Render
# "Environment" bo'limida STORAGE_CHAT_ID nomi bilan) yozing.
_storage_chat_id_raw = os.getenv("STORAGE_CHAT_ID", "").strip()
STORAGE_CHAT_ID = int(_storage_chat_id_raw) if _storage_chat_id_raw.lstrip("-").isdigit() else None
CHECK_INTERVAL_SECONDS = 60  # muddati tugaganlarni necha soniyada tekshirish

# Botning ASOSIY EGASI (admin panel - /admin - faqat shu ID(lar)ga ochiq).
# Render "Environment" bo'limida ADMIN_IDS="123456789,987654321" (vergul
# bilan, bir nechta bo'lishi mumkin) sifatida bering.
_admin_ids_raw = os.getenv("ADMIN_IDS", "").strip()
ADMIN_IDS = [int(x) for x in _admin_ids_raw.split(",") if x.strip().lstrip("-").isdigit()]


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

PROXY_URL = (
    os.getenv("BOT_PROXY")
    or os.getenv("https_proxy")
    or os.getenv("HTTPS_PROXY")
    or os.getenv("http_proxy")
    or os.getenv("HTTP_PROXY")
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if PROXY_URL:
    logger.info(f"Proksi orqali ulanilmoqda: {PROXY_URL}")
    bot = Bot(token=BOT_TOKEN, session=AiohttpSession(proxy=PROXY_URL))
else:
    bot = Bot(token=BOT_TOKEN)

dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

# ==================== ESKI XABARLARNI AVTOMATIK O'CHIRISH ====================
# Har bir chatda faqat oxirgi 3 ta xabar (bot va foydalanuvchining ikkalasiniki
# ham) qolib, undan oldingilari avtomatik o'chiriladi.

HISTORY_LIMIT = 3
chat_history: dict[int, list[int]] = defaultdict(list)


async def track_message(chat_id: int, message_id: int):
    history = chat_history[chat_id]
    if message_id in history:
        return
    history.append(message_id)
    while len(history) > HISTORY_LIMIT:
        old_id = history.pop(0)
        try:
            await bot.delete_message(chat_id, old_id)
        except TelegramBadRequest:
            pass  # allaqachon o'chirilgan yoki juda eski bo'lishi mumkin


class TrackSentMessages(BaseRequestMiddleware):
    """Bot yuborgan har qanday xabarni (message.answer, edit_text va h.k.)
    avtomatik kuzatuvga qo'shadi."""

    async def __call__(self, make_request, bot_obj, method):
        response = await make_request(bot_obj, method)
        try:
            if isinstance(response, Message):
                await track_message(response.chat.id, response.message_id)
        except Exception:
            pass
        return response


bot.session.middleware(TrackSentMessages())


@router.message.outer_middleware()
async def track_incoming_middleware(handler, event: Message, data):
    """Foydalanuvchi yuborgan har bir xabarni ham kuzatuvga qo'shadi."""
    await track_message(event.chat.id, event.message_id)
    return await handler(event, data)


MAIN_MENU = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📢 Kanalim"), KeyboardButton(text="➕ A'zo qo'shish")],
        [KeyboardButton(text="🤝 Hamkorlar"), KeyboardButton(text="💰 Balansim")],
    ],
    resize_keyboard=True,
)

CANCEL_KB = InlineKeyboardMarkup(
    inline_keyboard=[[InlineKeyboardButton(text="✖️ Bosh menyu", callback_data="cancel")]]
)


def with_cancel_row(kb: InlineKeyboardMarkup) -> InlineKeyboardMarkup:
    rows = list(kb.inline_keyboard) + [
        [InlineKeyboardButton(text="✖️ Bosh menyu", callback_data="cancel")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


class ConnectChannelFSM(StatesGroup):
    waiting_for_channel = State()


class AddMemberFSM(StatesGroup):
    choosing_channel = State()
    entering_user = State()
    entering_days = State()
    entering_price = State()


class ExtendMemberFSM(StatesGroup):
    entering_days = State()


class PartnerFSM(StatesGroup):
    entering_user = State()
    choosing_channel = State()


class BalanceEditFSM(StatesGroup):
    entering_amount = State()


class DbUploadFSM(StatesGroup):
    waiting_file = State()
    confirming = State()


# ==================== MA'LUMOTLAR BAZASI ====================

def db_init():
    con = sqlite3.connect(DB_PATH)
    con.execute(
        """CREATE TABLE IF NOT EXISTS channels (
            channel_id INTEGER PRIMARY KEY,
            title TEXT,
            added_by INTEGER
        )"""
    )
    con.execute(
        """CREATE TABLE IF NOT EXISTS access (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER,
            user_id INTEGER,
            username TEXT,
            days INTEGER,
            price INTEGER DEFAULT 0,
            added_by INTEGER,
            partner_id INTEGER,
            created_at INTEGER,
            expire_at INTEGER,
            status TEXT DEFAULT 'active'
        )"""
    )
    con.execute(
        """CREATE TABLE IF NOT EXISTS partners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER,
            user_id INTEGER,
            username TEXT,
            admin_id INTEGER,
            balance INTEGER DEFAULT 0,
            created_at INTEGER
        )"""
    )
    con.execute(
        """CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )"""
    )
    con.commit()
    con.close()


def db_add_channel(channel_id: int, title: str, added_by: int):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT OR REPLACE INTO channels (channel_id, title, added_by) VALUES (?, ?, ?)",
        (channel_id, title, added_by),
    )
    con.commit()
    con.close()


def db_get_channels_by_admin(admin_id: int):
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT channel_id, title FROM channels WHERE added_by = ?", (admin_id,)
    ).fetchall()
    con.close()
    return rows


def db_get_channel_title(channel_id: int):
    con = sqlite3.connect(DB_PATH)
    row = con.execute(
        "SELECT title FROM channels WHERE channel_id = ?", (channel_id,)
    ).fetchone()
    con.close()
    return row[0] if row else str(channel_id)


# ---------- A'zolik (access) ----------

def db_add_access(channel_id, user_id, username, days, price, added_by, partner_id):
    now = int(time.time())
    expire_at = now + days * 24 * 60 * 60
    con = sqlite3.connect(DB_PATH)
    cur = con.execute(
        """INSERT INTO access
           (channel_id, user_id, username, days, price, added_by, partner_id, created_at, expire_at, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')""",
        (channel_id, user_id, username, days, price, added_by, partner_id, now, expire_at),
    )
    con.commit()
    new_id = cur.lastrowid
    con.close()
    return new_id, expire_at


def db_find_active_access(channel_id, user_id, username):
    """Faol (status='active') yozuvni user_id, topilmasa username bo'yicha qidiradi."""
    con = sqlite3.connect(DB_PATH)
    row = con.execute(
        """SELECT id, expire_at, added_by, price, partner_id FROM access
           WHERE channel_id = ? AND user_id = ? AND status = 'active'
           ORDER BY expire_at DESC LIMIT 1""",
        (channel_id, user_id),
    ).fetchone()

    if not row and username:
        uname = username.lstrip("@")
        row = con.execute(
            """SELECT id, expire_at, added_by, price, partner_id FROM access
               WHERE channel_id = ? AND username = ? AND status = 'active'
               ORDER BY expire_at DESC LIMIT 1""",
            (channel_id, uname),
        ).fetchone()
        if row:
            con.execute("UPDATE access SET user_id = ? WHERE id = ?", (user_id, row[0]))
            con.commit()

    con.close()
    return row  # (id, expire_at, added_by, price, partner_id) yoki None


def db_get_active_access_list(channel_id, added_by):
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        """SELECT id, user_id, username, expire_at, price FROM access
           WHERE channel_id = ? AND added_by = ? AND status = 'active'
           ORDER BY expire_at ASC""",
        (channel_id, added_by),
    ).fetchall()
    con.close()
    return rows


def db_get_access_by_id(access_id: int):
    con = sqlite3.connect(DB_PATH)
    row = con.execute(
        """SELECT id, channel_id, user_id, username, expire_at, price, added_by, partner_id, status
           FROM access WHERE id = ?""",
        (access_id,),
    ).fetchone()
    con.close()
    return row


def db_set_status(access_id: int, status: str):
    con = sqlite3.connect(DB_PATH)
    con.execute("UPDATE access SET status = ? WHERE id = ?", (status, access_id))
    con.commit()
    con.close()


def db_extend_access(access_id: int, extra_days: int):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "UPDATE access SET expire_at = expire_at + ?, status = 'active' WHERE id = ?",
        (extra_days * 24 * 60 * 60, access_id),
    )
    con.commit()
    row = con.execute("SELECT expire_at FROM access WHERE id = ?", (access_id,)).fetchone()
    con.close()
    return row[0]


def db_get_expired_access():
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT id, channel_id, user_id, added_by FROM access WHERE status = 'active' AND expire_at <= ?",
        (int(time.time()),),
    ).fetchall()
    con.close()
    return rows


# ---------- Hamkorlar ----------

def db_add_partner(channel_id, user_id, username, admin_id):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        """INSERT INTO partners (channel_id, user_id, username, admin_id, balance, created_at)
           VALUES (?, ?, ?, ?, 0, ?)""",
        (channel_id, user_id, username, admin_id, int(time.time())),
    )
    con.commit()
    con.close()


def db_get_partners_by_admin(admin_id: int):
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        """SELECT p.id, p.channel_id, c.title, p.user_id, p.username, p.balance
           FROM partners p LEFT JOIN channels c ON c.channel_id = p.channel_id
           WHERE p.admin_id = ?
           ORDER BY p.id DESC""",
        (admin_id,),
    ).fetchall()
    con.close()
    return rows


def db_remove_partner(partner_id: int):
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM partners WHERE id = ?", (partner_id,))
    con.commit()
    con.close()


def db_get_partner_channels_for_user(user_id: int, username):
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        """SELECT p.id, p.channel_id, c.title, p.balance
           FROM partners p LEFT JOIN channels c ON c.channel_id = p.channel_id
           WHERE p.user_id = ?""",
        (user_id,),
    ).fetchall()

    if not rows and username:
        uname = username.lstrip("@")
        rows = con.execute(
            """SELECT p.id, p.channel_id, c.title, p.balance
               FROM partners p LEFT JOIN channels c ON c.channel_id = p.channel_id
               WHERE p.username = ?""",
            (uname,),
        ).fetchall()
        if rows:
            con.execute("UPDATE partners SET user_id = ? WHERE username = ?", (user_id, uname))
            con.commit()

    con.close()
    return rows


def db_credit_partner(partner_id: int, amount: int):
    """Musbat son - qo'shadi, manfiy son - ayiradi."""
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "UPDATE partners SET balance = balance + ? WHERE id = ?", (amount, partner_id)
    )
    con.commit()
    con.close()


# ==================== "KANALIM" (ULASH) ====================

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Salom! 👋\n\nPastdagi menyudan foydalaning.",
        reply_markup=MAIN_MENU,
    )


@router.message(F.text == "📢 Kanalim")
async def my_channel(message: Message, state: FSMContext):
    channels = db_get_channels_by_admin(message.from_user.id)

    if channels:
        text = "Sizning ulangan kanal(lar)ingiz:\n\n"
        for ch_id, title in channels:
            text += f"✅ <b>{title}</b>\n(ID: <code>{ch_id}</code>)\n\n"
        text += "Yangi kanal ulamoqchi bo'lsangiz, pastdagi yo'riqnoma bo'yicha davom eting."
        await message.answer(text, parse_mode="HTML")

    await state.set_state(ConnectChannelFSM.waiting_for_channel)
    await message.answer(
        "📌 Kanal ulash uchun:\n\n"
        "1️⃣ Botni o'zingizning kanalingizga <b>admin</b> qilib qo'shing\n"
        "(kamida \"Foydalanuvchilarni taklif qilish\" va \"Foydalanuvchilarni bloklash\" "
        "huquqlari bilan)\n\n"
        "2️⃣ Shundan so'ng quyidagilardan birini qiling:\n"
        "• Kanalingizdan istalgan postni shu yerga <b>forward</b> qiling\n"
        "yoki\n"
        "• Kanal ID raqamini yuboring (masalan: <code>-1001234567890</code>)\n\n"
        "Men kanalni tekshirib, sizga bog'lab qo'yaman.",
        parse_mode="HTML",
        reply_markup=CANCEL_KB,
    )


async def verify_and_save_channel(message: Message, chat_id: int) -> bool:
    user_id = message.from_user.id

    try:
        bot_member = await bot.get_chat_member(chat_id, bot.id)
    except TelegramBadRequest:
        await message.answer(
            "❌ Bu kanalni topa olmadim yoki bot u yerga qo'shilmagan.\n"
            "Avval botni kanalingizga admin qilib qo'shing, so'ng qayta urinib ko'ring."
        )
        return False

    if bot_member.status != "administrator":
        await message.answer(
            "❌ Bot bu kanalda topildi, lekin <b>admin emas</b>.\n"
            "Iltimos, botga kanalda admin huquqini bering va qayta urinib ko'ring.",
            parse_mode="HTML",
        )
        return False

    try:
        user_member = await bot.get_chat_member(chat_id, user_id)
    except TelegramBadRequest:
        await message.answer("❌ Sizni bu kanal a'zosi sifatida topa olmadim.")
        return False

    if user_member.status not in ("creator", "administrator"):
        await message.answer(
            "❌ Siz bu kanalda admin yoki egasi emassiz. "
            "Faqat kanal admini uni botga ulashi mumkin."
        )
        return False

    chat = await bot.get_chat(chat_id)
    db_add_channel(chat_id, chat.title or str(chat_id), user_id)

    await message.answer(
        f"✅ Kanal muvaffaqiyatli ulandi!\n\n"
        f"<b>{chat.title}</b>\n(ID: <code>{chat_id}</code>)",
        parse_mode="HTML",
        reply_markup=MAIN_MENU,
    )
    return True


@router.message(ConnectChannelFSM.waiting_for_channel, F.forward_from_chat)
async def connect_via_forward(message: Message, state: FSMContext):
    if message.forward_from_chat.type != "channel":
        await message.answer("Bu kanal posti emas. Iltimos, kanalingizdan postni forward qiling.")
        return
    if await verify_and_save_channel(message, message.forward_from_chat.id):
        await state.clear()


@router.message(ConnectChannelFSM.waiting_for_channel, F.text)
async def connect_via_id(message: Message, state: FSMContext):
    text = message.text.strip()

    if not text.lstrip("-").isdigit():
        await message.answer(
            "Iltimos, kanaldan post forward qiling yoki to'g'ri kanal ID yuboring "
            "(masalan: <code>-1001234567890</code>).",
            parse_mode="HTML",
        )
        return

    if await verify_and_save_channel(message, int(text)):
        await state.clear()


# ==================== "A'ZO QO'SHISH" (BOSHQARUV PANELI) ====================

def format_days_left(expire_at: int) -> int:
    return max(0, (expire_at - int(time.time())) // 86400)


def dashboard_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Yangi a'zo qo'shish", callback_data="am_new")],
            [InlineKeyboardButton(text="❌ A'zo chiqarish", callback_data="am_remove_menu")],
            [InlineKeyboardButton(text="⏳ Muddatni uzaytirish", callback_data="am_extend_menu")],
            [InlineKeyboardButton(text="✖️ Bosh menyu", callback_data="cancel")],
        ]
    )


async def show_dashboard(answer_target, state: FSMContext):
    """answer_target - Message yoki CallbackQuery bo'lishi mumkin."""
    data = await state.get_data()
    channel_id = data.get("channel_id")
    channel_title = data.get("channel_title", str(channel_id))
    admin_id = answer_target.from_user.id

    rows = db_get_active_access_list(channel_id, admin_id)
    text = f"Kanal: <b>{channel_title}</b>\n\n"
    if rows:
        text += "Qo'shilgan a'zolar:\n\n"
        for _id, user_id, username, expire_at, price in rows:
            who = f"@{username}" if username else str(user_id)
            remain = format_days_left(expire_at)
            price_part = f" — 💰 {price} so'm" if price else ""
            text += f"• {who} — {remain} kun qoldi{price_part}\n"
    else:
        text += "Hozircha a'zo qo'shilmagan."

    if isinstance(answer_target, CallbackQuery):
        await answer_target.message.answer(text, parse_mode="HTML", reply_markup=dashboard_kb())
    else:
        await answer_target.answer(text, parse_mode="HTML", reply_markup=dashboard_kb())


@router.message(F.text == "➕ A'zo qo'shish")
async def add_member_start(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    username = message.from_user.username

    owned_channels = db_get_channels_by_admin(user_id)
    if owned_channels:
        if len(owned_channels) == 1:
            await state.update_data(
                channel_id=owned_channels[0][0],
                channel_title=owned_channels[0][1],
                requires_price=False,
                partner_id=None,
            )
            await show_dashboard(message, state)
            return

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=title, callback_data=f"selch:{ch_id}")]
                for ch_id, title in owned_channels
            ]
        )
        await message.answer("Qaysi kanal bilan ishlaysiz?", reply_markup=with_cancel_row(kb))
        return

    partner_channels = db_get_partner_channels_for_user(user_id, username)
    if not partner_channels:
        await message.answer(
            "❌ Sizda a'zo qo'shish huquqi yo'q.\n"
            "Kanal egasi bo'lsangiz - avval \"📢 Kanalim\" orqali kanalingizni ulang.\n"
            "Hamkor bo'lsangiz - kanal egasi sizni hamkor qilib tayinlashi kerak."
        )
        return

    if len(partner_channels) == 1:
        p_id, ch_id, ch_title, _balance = partner_channels[0]
        await state.update_data(
            requires_price=True, partner_id=p_id, channel_id=ch_id, channel_title=ch_title
        )
        await show_dashboard(message, state)
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=ch_title, callback_data=f"selpch:{p_id}:{ch_id}")]
            for p_id, ch_id, ch_title, _balance in partner_channels
        ]
    )
    await message.answer("Qaysi kanal bilan ishlaysiz?", reply_markup=with_cancel_row(kb))


@router.callback_query(F.data.startswith("selch:"))
async def choose_channel_cb(callback: CallbackQuery, state: FSMContext):
    channel_id = int(callback.data.split(":")[1])
    title = db_get_channel_title(channel_id)
    await state.update_data(
        channel_id=channel_id, channel_title=title, requires_price=False, partner_id=None
    )
    await callback.message.edit_text(f"Kanal: {title}")
    await show_dashboard(callback, state)
    await callback.answer()


@router.callback_query(F.data.startswith("selpch:"))
async def choose_partner_channel_cb(callback: CallbackQuery, state: FSMContext):
    _, p_id, ch_id = callback.data.split(":")
    title = db_get_channel_title(int(ch_id))
    await state.update_data(
        channel_id=int(ch_id), channel_title=title, requires_price=True, partner_id=int(p_id)
    )
    await callback.message.edit_text(f"Kanal: {title}")
    await show_dashboard(callback, state)
    await callback.answer()


# ---------- Yangi a'zo qo'shish ----------

@router.callback_query(F.data == "am_new")
async def am_new_cb(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not data.get("channel_id"):
        await callback.answer("Xatolik: kanal tanlanmagan. Qaytadan urinib ko'ring.", show_alert=True)
        return
    await state.set_state(AddMemberFSM.entering_user)
    await callback.message.answer(
        "Foydalanuvchining Telegram ID raqamini yoki @username'ini yuboring.",
        reply_markup=CANCEL_KB,
    )
    await callback.answer()


@router.message(AddMemberFSM.entering_user)
async def add_member_enter_user(message: Message, state: FSMContext):
    text = message.text.strip()
    user_id = None
    username = None

    if text.startswith("@"):
        username = text.lstrip("@")
    elif text.lstrip("-").isdigit():
        user_id = int(text)
    else:
        await message.answer(
            "Noto'g'ri format. ID raqami (masalan 123456789) yoki @username kiriting.",
            reply_markup=CANCEL_KB,
        )
        return

    await state.update_data(target_user_id=user_id, target_username=username)
    await state.set_state(AddMemberFSM.entering_days)
    await message.answer(
        "Necha kunga a'zo qilinsin? Faqat raqam yuboring (masalan: 30)\n\n"
        "Muddat hozirdan boshlab hisoblanadi va shu muddat davomida "
        "foydalanuvchi kanalga istalgancha kirib-chiqishi mumkin.",
        reply_markup=CANCEL_KB,
    )


@router.message(AddMemberFSM.entering_days)
async def add_member_enter_days(message: Message, state: FSMContext):
    text = message.text.strip()
    if not text.isdigit() or int(text) <= 0:
        await message.answer("Iltimos, musbat butun son kiriting (masalan: 30)", reply_markup=CANCEL_KB)
        return

    await state.update_data(days=int(text))
    data = await state.get_data()

    if data.get("requires_price"):
        await state.set_state(AddMemberFSM.entering_price)
        await message.answer("Narxini kiriting (so'mda, faqat raqam, masalan: 10)", reply_markup=CANCEL_KB)
    else:
        await finish_add_member(message, state, price=0)


@router.message(AddMemberFSM.entering_price)
async def add_member_enter_price(message: Message, state: FSMContext):
    text = message.text.strip().replace(" ", "")
    if not text.isdigit() or int(text) <= 0:
        await message.answer("Iltimos, musbat butun son kiriting (masalan: 10)", reply_markup=CANCEL_KB)
        return
    await finish_add_member(message, state, price=int(text))


async def finish_add_member(message: Message, state: FSMContext, price: int):
    data = await state.get_data()
    channel_id = data["channel_id"]
    channel_title = data["channel_title"]
    user_id = data.get("target_user_id")
    username = data.get("target_username")
    days = data["days"]
    partner_id = data.get("partner_id")

    _id, expire_at = db_add_access(
        channel_id, user_id, username, days, price, message.from_user.id, partner_id
    )

    if price and partner_id:
        db_credit_partner(partner_id, price)

    expire_str = datetime.fromtimestamp(expire_at).strftime("%Y-%m-%d %H:%M")
    who = f"@{username}" if username else str(user_id)
    price_line = f"\nNarx: {price} so'm (balansga qo'shildi)" if price else ""
    await message.answer(
        f"✅ Ruxsat berildi!\n\n"
        f"Kanal: {channel_title}\n"
        f"Foydalanuvchi: {who}\n"
        f"Muddat: {days} kun (tugash sanasi: {expire_str}){price_line}\n\n"
        f"Endi ushbu foydalanuvchi kanalga so'rov yuborsa, bot avtomatik qabul qiladi. "
        f"Muddat davomida u xohlagancha kirib-chiqishi mumkin.",
    )
    await state.set_state(None)
    await show_dashboard(message, state)


# ---------- A'zo chiqarish ----------

@router.callback_query(F.data == "am_remove_menu")
async def am_remove_menu_cb(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    channel_id = data.get("channel_id")
    if not channel_id:
        await callback.answer("Xatolik: kanal tanlanmagan.", show_alert=True)
        return

    rows = db_get_active_access_list(channel_id, callback.from_user.id)
    if not rows:
        await callback.answer("Hozircha a'zo yo'q", show_alert=True)
        return

    kb_rows = []
    for access_id, user_id, username, expire_at, price in rows:
        who = f"@{username}" if username else str(user_id)
        kb_rows.append(
            [InlineKeyboardButton(text=f"❌ {who}", callback_data=f"rm_am:{access_id}")]
        )
    await callback.message.answer(
        "Qaysi a'zoni chiqarmoqchisiz?",
        reply_markup=with_cancel_row(InlineKeyboardMarkup(inline_keyboard=kb_rows)),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("rm_am:"))
async def rm_am_cb(callback: CallbackQuery, state: FSMContext):
    access_id = int(callback.data.split(":")[1])
    row = db_get_access_by_id(access_id)
    if not row:
        await callback.answer("Topilmadi", show_alert=True)
        return

    _id, channel_id, user_id, username, expire_at, price, added_by, partner_id, status = row
    if added_by != callback.from_user.id:
        await callback.answer("Bu sizning a'zoingiz emas", show_alert=True)
        return

    if price and price > 0:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Ha, ayirilsin", callback_data=f"rm_am_conf:{access_id}:yes")],
                [InlineKeyboardButton(text="Yo'q, ayirilmasin", callback_data=f"rm_am_conf:{access_id}:no")],
            ]
        )
        await callback.message.edit_text(
            f"Ushbu a'zo uchun {price} so'm kiritilgan edi.\n"
            f"Balansdan {price} so'm ayirilsinmi?",
            reply_markup=kb,
        )
        await callback.answer()
    else:
        await perform_removal(callback, access_id, deduct=False, state=state)


@router.callback_query(F.data.startswith("rm_am_conf:"))
async def rm_am_conf_cb(callback: CallbackQuery, state: FSMContext):
    _, access_id, choice = callback.data.split(":")
    await perform_removal(callback, int(access_id), deduct=(choice == "yes"), state=state)


async def perform_removal(callback: CallbackQuery, access_id: int, deduct: bool, state: FSMContext):
    row = db_get_access_by_id(access_id)
    if not row:
        await callback.answer("Topilmadi", show_alert=True)
        return

    _id, channel_id, user_id, username, expire_at, price, added_by, partner_id, status = row

    if user_id:
        try:
            await bot.ban_chat_member(channel_id, user_id)
            await bot.unban_chat_member(channel_id, user_id)
        except TelegramBadRequest:
            pass

    db_set_status(access_id, "removed_manually")

    if deduct and price and partner_id:
        db_credit_partner(partner_id, -price)

    who = f"@{username}" if username else str(user_id)
    msg = f"✅ {who} kanaldan chiqarildi."
    if deduct and price:
        msg += f"\nBalansdan {price} so'm ayirildi."
    await callback.message.edit_text(msg)
    await callback.answer()

    await show_dashboard(callback, state)


# ---------- Muddatni uzaytirish ----------

@router.callback_query(F.data == "am_extend_menu")
async def am_extend_menu_cb(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    channel_id = data.get("channel_id")
    if not channel_id:
        await callback.answer("Xatolik: kanal tanlanmagan.", show_alert=True)
        return

    rows = db_get_active_access_list(channel_id, callback.from_user.id)
    if not rows:
        await callback.answer("Hozircha a'zo yo'q", show_alert=True)
        return

    kb_rows = []
    for access_id, user_id, username, expire_at, price in rows:
        who = f"@{username}" if username else str(user_id)
        remain = format_days_left(expire_at)
        kb_rows.append(
            [InlineKeyboardButton(
                text=f"⏳ {who} ({remain} kun qoldi)", callback_data=f"ext_am:{access_id}"
            )]
        )
    await callback.message.answer(
        "Qaysi a'zoning muddatini uzaytirmoqchisiz?",
        reply_markup=with_cancel_row(InlineKeyboardMarkup(inline_keyboard=kb_rows)),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("ext_am:"))
async def ext_am_cb(callback: CallbackQuery, state: FSMContext):
    access_id = int(callback.data.split(":")[1])
    row = db_get_access_by_id(access_id)
    if not row:
        await callback.answer("Topilmadi", show_alert=True)
        return
    if row[6] != callback.from_user.id:  # added_by
        await callback.answer("Bu sizning a'zoingiz emas", show_alert=True)
        return

    await state.update_data(extend_access_id=access_id)
    await state.set_state(ExtendMemberFSM.entering_days)
    await callback.message.edit_text("Necha kunga uzaytirilsin? Faqat raqam yuboring (masalan: 30)")
    await callback.answer()


@router.message(ExtendMemberFSM.entering_days)
async def extend_enter_days(message: Message, state: FSMContext):
    text = message.text.strip()
    if not text.isdigit() or int(text) <= 0:
        await message.answer("Iltimos, musbat butun son kiriting (masalan: 30)", reply_markup=CANCEL_KB)
        return

    data = await state.get_data()
    access_id = data["extend_access_id"]
    extra_days = int(text)

    new_expire = db_extend_access(access_id, extra_days)
    row = db_get_access_by_id(access_id)
    _id, channel_id, user_id, username, expire_at, price, added_by, partner_id, status = row
    who = f"@{username}" if username else str(user_id)
    expire_str = datetime.fromtimestamp(new_expire).strftime("%Y-%m-%d %H:%M")

    await message.answer(
        f"✅ Muddat uzaytirildi!\n\n"
        f"Foydalanuvchi: {who}\n"
        f"Yangi tugash sanasi: {expire_str}"
    )
    await state.set_state(None)
    await show_dashboard(message, state)


# ==================== "HAMKORLAR" ====================

def format_partners_text(admin_id: int) -> str:
    partners = db_get_partners_by_admin(admin_id)
    if not partners:
        return "Hozircha hamkorlar yo'q."

    text = "🤝 Hamkorlar:\n\n"
    for _p_id, _ch_id, ch_title, u_id, uname, balance in partners:
        who = f"@{uname}" if uname else str(u_id)
        bal = f"{balance:,}".replace(",", " ")
        text += f"• {who} — {ch_title or '?'} — 💰 {bal} so'm\n"
    return text


def partners_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Hamkor qo'shish", callback_data="addpartner")],
            [InlineKeyboardButton(text="➖ Hamkor chiqarish", callback_data="rmpartner_menu")],
            [InlineKeyboardButton(text="✏️ Balansni o'zgartirish", callback_data="editbalance_menu")],
            [InlineKeyboardButton(text="✖️ Bosh menyu", callback_data="cancel")],
        ]
    )


@router.message(F.text == "🤝 Hamkorlar")
async def partners_view(message: Message, state: FSMContext):
    await state.clear()
    channels = db_get_channels_by_admin(message.from_user.id)
    if not channels:
        await message.answer("Bu bo'lim faqat kanal egalari uchun. Avval \"📢 Kanalim\" orqali kanal ulang.")
        return

    await message.answer(format_partners_text(message.from_user.id), reply_markup=partners_menu_kb())


@router.callback_query(F.data == "addpartner")
async def add_partner_cb(callback: CallbackQuery, state: FSMContext):
    channels = db_get_channels_by_admin(callback.from_user.id)
    if not channels:
        await callback.answer("Sizda kanal yo'q", show_alert=True)
        return

    await state.set_state(PartnerFSM.entering_user)
    await callback.message.answer(
        "Hamkor qilinadigan foydalanuvchining Telegram ID raqamini yoki @username'ini yuboring.",
        reply_markup=CANCEL_KB,
    )
    await callback.answer()


@router.message(PartnerFSM.entering_user)
async def partner_enter_user(message: Message, state: FSMContext):
    text = message.text.strip()
    user_id = None
    username = None

    if text.startswith("@"):
        username = text.lstrip("@")
    elif text.lstrip("-").isdigit():
        user_id = int(text)
    else:
        await message.answer(
            "Noto'g'ri format. ID raqami (masalan 123456789) yoki @username kiriting.",
            reply_markup=CANCEL_KB,
        )
        return

    await state.update_data(partner_user_id=user_id, partner_username=username)

    channels = db_get_channels_by_admin(message.from_user.id)
    if len(channels) == 1:
        ch_id, ch_title = channels[0]
        db_add_partner(ch_id, user_id, username, message.from_user.id)
        who = f"@{username}" if username else str(user_id)
        await message.answer(
            f"✅ Hamkor qo'shildi!\n\nHamkor: {who}\nKanal: {ch_title}",
            reply_markup=MAIN_MENU,
        )
        await state.clear()
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=title, callback_data=f"partnerch:{ch_id}")]
            for ch_id, title in channels
        ]
    )
    await state.set_state(PartnerFSM.choosing_channel)
    await message.answer("Qaysi kanalga hamkor qilinsin?", reply_markup=with_cancel_row(kb))


@router.callback_query(PartnerFSM.choosing_channel, F.data.startswith("partnerch:"))
async def partner_choose_channel_cb(callback: CallbackQuery, state: FSMContext):
    channel_id = int(callback.data.split(":")[1])
    title = db_get_channel_title(channel_id)
    data = await state.get_data()
    user_id = data.get("partner_user_id")
    username = data.get("partner_username")

    db_add_partner(channel_id, user_id, username, callback.from_user.id)

    who = f"@{username}" if username else str(user_id)
    await callback.message.edit_text(f"✅ Hamkor qo'shildi!\n\nHamkor: {who}\nKanal: {title}")
    await callback.answer()
    await state.clear()


@router.callback_query(F.data == "rmpartner_menu")
async def rm_partner_menu_cb(callback: CallbackQuery):
    partners = db_get_partners_by_admin(callback.from_user.id)
    if not partners:
        await callback.answer("Hamkorlar yo'q", show_alert=True)
        return

    kb_rows = []
    for p_id, _ch_id, ch_title, u_id, uname, _balance in partners:
        who = f"@{uname}" if uname else str(u_id)
        kb_rows.append(
            [InlineKeyboardButton(text=f"❌ {who} ({ch_title})", callback_data=f"rmpartner:{p_id}")]
        )
    await callback.message.answer(
        "Qaysi hamkorni chiqarib yubormoqchisiz?",
        reply_markup=with_cancel_row(InlineKeyboardMarkup(inline_keyboard=kb_rows)),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("rmpartner:"))
async def rm_partner_cb(callback: CallbackQuery):
    partner_id = int(callback.data.split(":")[1])
    db_remove_partner(partner_id)
    await callback.message.edit_text("✅ Hamkor chiqarib yuborildi.")
    await callback.answer()


# ---------- Balansni o'zgartirish ----------

@router.callback_query(F.data == "editbalance_menu")
async def edit_balance_menu_cb(callback: CallbackQuery):
    partners = db_get_partners_by_admin(callback.from_user.id)
    if not partners:
        await callback.answer("Hamkorlar yo'q", show_alert=True)
        return

    kb_rows = []
    for p_id, _ch_id, ch_title, u_id, uname, balance in partners:
        who = f"@{uname}" if uname else str(u_id)
        kb_rows.append(
            [InlineKeyboardButton(
                text=f"✏️ {who} — 💰 {balance} so'm", callback_data=f"editbal:{p_id}"
            )]
        )
    await callback.message.answer(
        "Qaysi hamkorning balansini o'zgartirmoqchisiz?",
        reply_markup=with_cancel_row(InlineKeyboardMarkup(inline_keyboard=kb_rows)),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("editbal:"))
async def edit_balance_choose_cb(callback: CallbackQuery, state: FSMContext):
    partner_id = int(callback.data.split(":")[1])
    await state.update_data(edit_partner_id=partner_id)
    await state.set_state(BalanceEditFSM.entering_amount)
    await callback.message.edit_text(
        "Qo'shish uchun musbat son, ayirish uchun oldiga \"-\" qo'yib son kiriting.\n"
        "Masalan: <code>50000</code> (qo'shadi) yoki <code>-20000</code> (ayiradi)",
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(BalanceEditFSM.entering_amount)
async def edit_balance_enter_amount(message: Message, state: FSMContext):
    text = message.text.strip().replace(" ", "")
    is_negative = text.startswith("-")
    digits = text.lstrip("+-")

    if not digits.isdigit() or int(digits) == 0:
        await message.answer(
            "Iltimos, to'g'ri son kiriting (masalan: 50000 yoki -20000)",
            reply_markup=CANCEL_KB,
        )
        return

    amount = -int(digits) if is_negative else int(digits)
    data = await state.get_data()
    partner_id = data["edit_partner_id"]

    db_credit_partner(partner_id, amount)

    partners = db_get_partners_by_admin(message.from_user.id)
    new_balance = None
    who = str(partner_id)
    for p_id, _ch_id, _ch_title, u_id, uname, balance in partners:
        if p_id == partner_id:
            new_balance = balance
            who = f"@{uname}" if uname else str(u_id)
            break

    sign = "qo'shildi" if amount > 0 else "ayirildi"
    await message.answer(
        f"✅ {abs(amount)} so'm balansdan {sign}.\n\n"
        f"Hamkor: {who}\nYangi balans: {new_balance} so'm",
    )
    await state.clear()
    await message.answer(format_partners_text(message.from_user.id), reply_markup=partners_menu_kb())


# ==================== "BALANSIM" ====================

@router.message(F.text == "💰 Balansim")
async def my_balance(message: Message, state: FSMContext):
    await state.clear()
    rows = db_get_partner_channels_for_user(message.from_user.id, message.from_user.username)

    if not rows:
        await message.answer("Sizda hozircha hamkorlik balansi yo'q.")
        return

    text = "💰 Sizning balansingiz:\n\n"
    total = 0
    for _p_id, _ch_id, ch_title, balance in rows:
        text += f"• {ch_title or '?'} — {balance} so'm\n"
        total += balance
    if len(rows) > 1:
        text += f"\nJami: {total} so'm"

    await message.answer(text)


# ==================== BEKOR QILISH (universal) ====================

@router.callback_query(F.data == "cancel")
async def cancel_cb(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await callback.message.edit_text("❌ Bekor qilindi.")
    except TelegramBadRequest:
        pass
    await callback.message.answer("Asosiy menyu:", reply_markup=MAIN_MENU)
    await callback.answer()


# ==================== /admin PANELI (BAZA BOSHQARUVI) ====================

def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Hozir zaxiralash / tekshirish", callback_data="adm:backupnow")],
        [InlineKeyboardButton(text="♻️ Zaxiradan hozir tiklash", callback_data="adm:restorenow")],
        [InlineKeyboardButton(text="📥 DB faylni qo'lda yuklash", callback_data="adm:dbupload")],
    ])


_BACKUP_REASON_LABELS = {
    "success": "Muvaffaqiyatli yuklandi",
    "no_chat": "Saqlash chati topilmadi (STORAGE_CHAT_ID sozlanmagan)",
    "no_file": "Mahalliy baza fayli topilmadi",
    "empty_skip": "Baza bo'sh - xavfsizlik uchun o'tkazib yuborildi (eski zaxira saqlanib qoldi)",
    "upload_failed": "Yuklashda xatolik yuz berdi",
}

_RESTORE_REASON_LABELS = {
    "success": "Muvaffaqiyatli tiklandi",
    "no_chat": "Saqlash chati topilmadi (STORAGE_CHAT_ID sozlanmagan)",
    "get_chat_failed": "Saqlash chatini olib bo'lmadi",
    "no_pinned": "PIN qilingan zaxira topilmadi",
    "download_failed": "Zaxirani yuklab olishda xatolik",
    "invalid_file": "Yuklab olingan fayl noto'g'ri/buzilgan",
}


@router.message(Command("admin"))
async def admin_command(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("⛔️ Sizda admin panelidan foydalanish huquqi yo'q.")
        return
    await message.answer("⚙️ Admin panel:", reply_markup=admin_menu_kb())


@router.callback_query(F.data == "adm:menu")
async def adm_menu_cb(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return
    await state.clear()
    await callback.message.edit_text("⚙️ Admin panel:", reply_markup=admin_menu_kb())
    await callback.answer()


@router.callback_query(F.data == "adm:backupnow")
async def adm_backupnow_cb(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return
    await callback.message.edit_text("🔄 Zaxiralanmoqda, biroz kuting...")
    result = await backup_database()

    lines = ["🔄 Zaxiralash natijasi:\n"]
    lines.append(f"Holat: {'✅ Muvaffaqiyatli' if result['ok'] else '❌ Muvaffaqiyatsiz'}")
    lines.append(f"Sabab: {_BACKUP_REASON_LABELS.get(result['reason'], result['reason'])}")
    lines.append(f"Obuna yozuvlari soni (mahalliy bazada): {result['access_count']}")
    lines.append(f"Saqlash chati ID: {result['chat_id']}")
    if result["ok"]:
        pin_text = "✅ Ha" if result["pinned"] else "❌ Yo'q (MUAMMO - tiklash ishlamaydi!)"
        lines.append(f"PIN qilindi: {pin_text}")
    if result.get("error"):
        lines.append(f"\n⚠️ Texnik tafsilot:\n{result['error']}")

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Orqaga", callback_data="adm:menu")]])
    await callback.message.edit_text("\n".join(lines), reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data == "adm:restorenow")
async def adm_restorenow_cb(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return
    await callback.message.edit_text("♻️ Tiklanmoqda, biroz kuting...")
    result = await _restore_from_pinned_backup()

    lines = ["♻️ Tiklash natijasi:\n"]
    lines.append(f"Holat: {'✅ Muvaffaqiyatli' if result['ok'] else '❌ Muvaffaqiyatsiz'}")
    lines.append(f"Sabab: {_RESTORE_REASON_LABELS.get(result['reason'], result['reason'])}")
    if result.get("access_count") is not None:
        lines.append(f"Topilgan obuna yozuvlari soni: {result['access_count']}")
    if result.get("error"):
        lines.append(f"\n⚠️ Texnik tafsilot:\n{result['error']}")

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Orqaga", callback_data="adm:menu")]])
    await callback.message.edit_text("\n".join(lines), reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data == "adm:dbupload")
async def adm_dbupload_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return
    await state.set_state(DbUploadFSM.waiting_file)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✖️ Bekor qilish", callback_data="adm:dbuploadcancel")]
    ])
    await callback.message.edit_text(
        "📥 Bazani qo'lda yuklash\n\n"
        "Iltimos, .db faylni HUJJAT sifatida yuboring (masalan, guruhdagi "
        "pin qilingan zaxirani qayta yuklab, shu yerga jo'nating).",
        reply_markup=kb,
    )
    await callback.answer()


@router.message(DbUploadFSM.waiting_file, F.document)
async def adm_dbupload_receive(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    tmp_path = DB_PATH + ".upload_tmp"
    try:
        file = await bot.get_file(message.document.file_id)
        await bot.download_file(file.file_path, destination=tmp_path)
    except Exception as e:
        await message.answer(
            f"❌ Faylni yuklab olishda xatolik: {e}\n\nQaytadan urinib ko'ring, yoki /cancel yozing."
        )
        return

    ok, err = _validate_backup_file(tmp_path)
    if not ok:
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        await message.answer(
            f"❌ Bu fayl to'g'ri baza emasga o'xshaydi ({err}).\n\n"
            "Boshqa faylni yuboring, yoki /cancel bilan bekor qiling."
        )
        return

    con = sqlite3.connect(tmp_path)
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM access")
    found_count = cur.fetchone()[0]
    con.close()

    await state.update_data(tmp_path=tmp_path)
    await state.set_state(DbUploadFSM.confirming)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Ha, almashtirish", callback_data="adm:dbuploadconfirm")],
        [InlineKeyboardButton(text="❌ Yo'q, bekor qilish", callback_data="adm:dbuploadcancel")],
    ])
    await message.answer(
        f"📄 Fayl tekshirildi.\nTopilgan obuna yozuvlari soni: {found_count}\n\n"
        "⚠️ Joriy bazangiz shu fayl bilan BUTUNLAY ALMASHTIRILADI. Davom etasizmi?",
        reply_markup=kb,
    )


@router.callback_query(DbUploadFSM.confirming, F.data == "adm:dbuploadconfirm")
async def adm_dbupload_confirm(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer()
        return

    data = await state.get_data()
    tmp_path = data.get("tmp_path")
    await state.clear()

    if not tmp_path or not os.path.exists(tmp_path):
        await callback.message.edit_text("⚠️ Fayl topilmadi. /admin orqali qaytadan boshlang.")
        await callback.answer()
        return

    os.replace(tmp_path, DB_PATH)
    db_init()
    access_count = _count_access_rows()

    await callback.message.edit_text(
        f"✅ Baza muvaffaqiyatli almashtirildi!\nTopilgan obuna yozuvlari soni: {access_count}\n\n"
        "Tavsiya: endi shu yangi bazani darhol zaxiralab qo'ying - /admin → "
        "🔄 Hozir zaxiralash / tekshirish tugmasini bosing."
    )
    await callback.answer()


@router.callback_query(F.data == "adm:dbuploadcancel")
async def adm_dbupload_cancel(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    tmp_path = data.get("tmp_path")
    if tmp_path:
        try:
            os.remove(tmp_path)
        except Exception:
            pass
    await state.clear()
    try:
        await callback.message.edit_text("Bekor qilindi.")
    except TelegramBadRequest:
        pass
    await callback.message.answer("Asosiy menyu:", reply_markup=MAIN_MENU)
    await callback.answer()


# ==================== KANALGA SO'ROV KELGANDA ====================

@router.chat_join_request()
async def on_join_request(request: ChatJoinRequest):
    channel_id = request.chat.id
    user_id = request.from_user.id
    username = request.from_user.username

    row = db_find_active_access(channel_id, user_id, username)

    if not row:
        try:
            await bot.decline_chat_join_request(channel_id, user_id)
        except TelegramBadRequest:
            pass
        logger.info(f"Rad etildi: user={user_id} channel={channel_id} (ruxsat topilmadi)")
        return

    access_id, expire_at, added_by, price, partner_id = row

    if expire_at <= int(time.time()):
        db_set_status(access_id, "expired")
        try:
            await bot.decline_chat_join_request(channel_id, user_id)
        except TelegramBadRequest:
            pass
        logger.info(f"Rad etildi: user={user_id} channel={channel_id} (muddati tugagan)")
        return

    try:
        await bot.approve_chat_join_request(channel_id, user_id)
    except TelegramBadRequest as e:
        logger.warning(f"Qabul qilib bo'lmadi: {e}")
        return

    expire_str = datetime.fromtimestamp(expire_at).strftime("%Y-%m-%d %H:%M")
    try:
        await bot.send_message(
            added_by,
            f"✅ Foydalanuvchi kanalga qo'shildi!\n"
            f"Kanal: {db_get_channel_title(channel_id)}\n"
            f"Foydalanuvchi: @{username or user_id}\n"
            f"Muddati: {expire_str} gacha (shu vaqtgacha kirib-chiqishi mumkin)",
        )
    except TelegramBadRequest:
        pass


# ==================== MUDDATI TUGAGANLARNI AVTOMATIK CHIQARISH ====================

async def check_expired_members():
    while True:
        try:
            expired = db_get_expired_access()
            for access_id, channel_id, user_id, added_by in expired:
                try:
                    if user_id:
                        await bot.ban_chat_member(channel_id, user_id)
                        await bot.unban_chat_member(channel_id, user_id)
                    db_set_status(access_id, "expired")
                    logger.info(f"Muddati tugadi: user={user_id} channel={channel_id}")
                    if added_by:
                        try:
                            await bot.send_message(
                                added_by,
                                f"⏰ Foydalanuvchi (ID: {user_id}) muddati tugagani sababli "
                                f"kanaldan avtomatik chiqarildi.",
                            )
                        except TelegramBadRequest:
                            pass
                except TelegramBadRequest as e:
                    logger.warning(f"Chiqarib bo'lmadi: user={user_id} channel={channel_id} xato={e}")
                    db_set_status(access_id, "expired")
        except Exception as e:
            logger.exception(f"Tekshiruvda xato: {e}")

        await asyncio.sleep(CHECK_INTERVAL_SECONDS)


# ==================== ISHGA TUSHIRISH ====================

class _HealthCheckHandler(http.server.BaseHTTPRequestHandler):
    """Render (yoki shunga o'xshash platformalar) 'portni tinglayapsizmi'
    tekshiruvini qanoatlantirish uchun har qanday so'rovga 200 OK bilan
    javob beradi. Botning asosiy ishiga (Telegram bilan gaplashish) hech
    qanday aloqasi yo'q - faqat platforma buni talab qilgani uchun kerak."""

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write("Bot ishlamoqda ✅".encode("utf-8"))

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        pass  # standart HTTP loglarini o'chirib qo'yamiz (keraksiz shovqin)


def start_health_check_server():
    """PORT muhit o'zgaruvchisi berilgan bo'lsa (Render kabi platformalarda
    avtomatik beriladi), fon rejimida (alohida thread'da) mayda HTTP server
    ishga tushiradi. Agar PORT berilmagan bo'lsa (masalan kompyuterda sinab
    ko'rayotganda), umuman hech narsa qilmaydi."""
    port_raw = os.environ.get("PORT")
    if not port_raw:
        return
    try:
        port = int(port_raw)
    except ValueError:
        logger.warning(f"PORT muhit o'zgaruvchisi noto'g'ri qiymatga ega: {port_raw}")
        return

    def _serve():
        try:
            server = http.server.HTTPServer(("0.0.0.0", port), _HealthCheckHandler)
            logger.info(f"Health-check HTTP server {port} portda ishga tushdi.")
            server.serve_forever()
        except Exception:
            logger.exception("Health-check HTTP serverni ishga tushirishda xatolik.")

    thread = threading.Thread(target=_serve, daemon=True)
    thread.start()


def _get_setting(key: str, default=None):
    """Sozlamani o'qiydi. Jadval hali mavjud bo'lmasa ham xatoga uchramaydi."""
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cur.fetchone()
        con.close()
    except sqlite3.OperationalError:
        row = None
    return row[0] if row else default


def _set_setting(key: str, value: str):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )
    con.commit()
    con.close()


def _count_access_rows() -> int:
    """Mahalliy bazadagi 'access' (obuna) yozuvlari sonini hisoblaydi
    (xavfsizlik tekshiruvi uchun - bo'sh bazani zaxira ustidan yozib
    yubormaslik uchun)."""
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM access")
        count = cur.fetchone()[0]
        con.close()
        return count
    except Exception:
        return 0


async def backup_database() -> dict:
    """
    kicker.db faylini hujjat sifatida saqlash guruhiga yuklaydi va PIN
    qiladi. Har safar yangi zaxira muvaffaqiyatli yuklanganidan KEYIN,
    undan OLDINGI zaxira xabari avtomatik o'chiriladi - shunda guruhda
    doim faqat BITTA (eng so'nggi) zaxira fayli saqlanadi.

    ⚠️ Agar mahalliy baza BO'SH (0 ta obuna yozuvi) bo'lsa-yu, oldin zaxira
    mavjud bo'lsa - bu zaxiralanmaydi (yaxshi zaxira yo'q qilib yuborilishining
    oldini olish uchun).

    Natija lug'ati /admin paneldagi "Hozir zaxiralash" tugmasi uchun
    to'liq diagnostika ko'rsatishga xizmat qiladi.
    """
    global STORAGE_CHAT_ID

    if not STORAGE_CHAT_ID:
        logger.warning("STORAGE_CHAT_ID sozlanmagan - baza zaxiralanmaydi.")
        return {"ok": False, "reason": "no_chat", "pinned": False, "access_count": 0, "chat_id": None, "error": None}

    if not os.path.exists(DB_PATH):
        return {"ok": False, "reason": "no_file", "pinned": False, "access_count": 0, "chat_id": STORAGE_CHAT_ID, "error": None}

    previous_backup_message_id = _get_setting("last_backup_message_id")
    access_count = _count_access_rows()

    if access_count == 0 and previous_backup_message_id:
        logger.warning(
            "⚠️ Mahalliy baza BO'SH ko'rinadi, lekin oldin zaxira mavjud - "
            "xavfsizlik uchun zaxiralash o'tkazib yuborildi."
        )
        return {
            "ok": False, "reason": "empty_skip", "pinned": False,
            "access_count": access_count, "chat_id": STORAGE_CHAT_ID, "error": None,
        }

    sent = None
    last_error = None
    attempts_left = 2
    attempt = 0
    while attempt < attempts_left and sent is None:
        try:
            sent = await bot.send_document(
                chat_id=STORAGE_CHAT_ID,
                document=FSInputFile(DB_PATH, filename="kicker_backup.db"),
                caption="🗄 Avtomatik zaxira nusxa (backup) - bu xabarni O'CHIRMANG.",
            )
        except TelegramMigrateToChat as e:
            new_id = e.migrate_to_chat_id
            if new_id:
                logger.warning("Guruh supergroup'ga aylangan. Yangi ID: %s", new_id)
                STORAGE_CHAT_ID = new_id
                _set_setting("storage_chat_id", str(new_id))
                attempts_left += 1
            last_error = e
            attempt += 1
        except Exception as e:
            logger.exception("Bazani zaxiralashda xatolik yuz berdi.")
            last_error = e
            attempt += 1

    if sent is None:
        return {
            "ok": False, "reason": "upload_failed", "pinned": False,
            "access_count": access_count, "chat_id": STORAGE_CHAT_ID,
            "error": str(last_error) if last_error else None,
        }

    pinned_ok = True
    try:
        await bot.pin_chat_message(chat_id=STORAGE_CHAT_ID, message_id=sent.message_id, disable_notification=True)
    except Exception:
        pinned_ok = False
        logger.warning(
            "Zaxira xabarini PIN qilib bo'lmadi - botda 'Pin messages' huquqi "
            "borligini tekshiring (aks holda tiklash ishlamaydi)."
        )

    _set_setting("last_backup_message_id", str(sent.message_id))

    if previous_backup_message_id:
        try:
            await bot.delete_message(chat_id=STORAGE_CHAT_ID, message_id=int(previous_backup_message_id))
        except Exception:
            pass  # allaqachon o'chirilgan yoki topilmagan bo'lishi mumkin

    return {
        "ok": True, "reason": "success", "pinned": pinned_ok,
        "access_count": access_count, "chat_id": STORAGE_CHAT_ID, "error": None,
    }


def _validate_backup_file(path: str):
    """
    Berilgan fayl haqiqatan ham to'g'ri SQLite baza ekanligini va kerakli
    jadvallar (access, channels) mavjudligini tekshiradi. Qaytaradi:
    (ok: bool, error_message: str yoki None)
    """
    try:
        con = sqlite3.connect(path)
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
        con.close()
    except Exception as e:
        return False, str(e)

    if "access" not in tables or "channels" not in tables:
        return False, "kerakli jadvallar (access, channels) topilmadi"

    return True, None


async def _restore_from_pinned_backup() -> dict:
    """
    Saqlash guruhida PIN qilingan eng so'nggi zaxirani qidirib, topilsa
    yuklab, mahalliy faylni ALMASHTIRADI (mavjud fayl bo'lsa ham). Bu
    funksiya HAM bot ishga tushganda (restore_database_if_needed orqali,
    faqat mahalliy fayl bo'sh/yo'q bo'lsagina), HAM /admin paneldagi
    "Zaxiradan hozir tiklash" tugmasi orqali (har doim, majburiy) chaqiriladi.
    """
    if not STORAGE_CHAT_ID:
        return {"ok": False, "reason": "no_chat"}

    try:
        chat = await bot.get_chat(STORAGE_CHAT_ID)
    except Exception as e:
        logger.exception("Saqlash chatini olishda xatolik.")
        return {"ok": False, "reason": "get_chat_failed", "error": str(e)}

    pinned = chat.pinned_message
    if pinned is None or pinned.document is None:
        return {"ok": False, "reason": "no_pinned"}

    try:
        file = await bot.get_file(pinned.document.file_id)
        tmp_path = DB_PATH + ".restore_tmp"
        await bot.download_file(file.file_path, destination=tmp_path)
    except Exception as e:
        logger.exception("Zaxirani yuklab olishda xatolik.")
        return {"ok": False, "reason": "download_failed", "error": str(e)}

    # Yuklab olingan faylni tekshiramiz (haqiqatan ham to'g'ri baza ekanligini)
    ok, err = _validate_backup_file(tmp_path)
    if not ok:
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        return {"ok": False, "reason": "invalid_file", "error": err}

    os.replace(tmp_path, DB_PATH)
    db_init()
    access_count = _count_access_rows()
    logger.info("✅ Baza muvaffaqiyatli zaxiradan tiklandi (%s).", DB_PATH)
    return {"ok": True, "reason": "success", "access_count": access_count}


async def restore_database_if_needed():
    """
    Bot ishga tushganda (main() ichida) chaqiriladi. FAQAT mahalliy baza
    fayli MAVJUD BO'LMASA yoki BO'SH bo'lsa, saqlash guruhida PIN qilingan
    eng so'nggi zaxiradan tiklaydi (_restore_from_pinned_backup orqali).
    Agar mahalliy fayl allaqachon bor va bo'sh bo'lmasa - tegilmaydi.
    """
    if os.path.exists(DB_PATH) and os.path.getsize(DB_PATH) > 0:
        return

    if not STORAGE_CHAT_ID:
        logger.info("STORAGE_CHAT_ID sozlanmagan - yangi (bo'sh) baza bilan boshlanadi.")
        return

    result = await _restore_from_pinned_backup()
    if result.get("ok"):
        logger.info(
            "✅ Baza muvaffaqiyatli zaxiradan tiklandi (%s ta obuna yozuvi).",
            result.get("access_count"),
        )
    else:
        logger.info(
            "Zaxiradan tiklab bo'lmadi (sabab: %s) - yangi (bo'sh) baza bilan boshlanadi.",
            result.get("reason"),
        )


async def backup_loop():
    """Har 1 soatda bir marta avtomatik zaxiralab turadigan fon vazifasi."""
    await asyncio.sleep(60)  # birinchi zaxira 1 daqiqadan keyin
    while True:
        try:
            await backup_database()
        except Exception:
            logger.exception("backup_loop ichida kutilmagan xatolik.")
        await asyncio.sleep(3600)  # keyingi zaxiralar - har soatda


async def main():
    await restore_database_if_needed()  # avval - mahalliy baza yo'q/bo'sh bo'lsa, zaxiradan tiklaydi
    db_init()  # keyin - jadvallarni yaratadi/migratsiya qiladi (tiklangan yoki yangi fayl ustida)
    start_health_check_server()
    asyncio.create_task(check_expired_members())
    asyncio.create_task(backup_loop())  # har soatda avtomatik zaxiralab turadi
    logger.info("Bot ishga tushdi...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
