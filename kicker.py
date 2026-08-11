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
import logging
import os
import sqlite3
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
)
from aiogram.exceptions import TelegramBadRequest

# ==================== SOZLAMALAR ====================

BOT_TOKEN = os.getenv("BOT_TOKEN", "6011700872:AAFwlE59GqI04UeHgWkBnl5RwExjKI5RSl0")
DB_PATH = os.getenv("DB_PATH", "kicker.db")
CHECK_INTERVAL_SECONDS = 60  # muddati tugaganlarni necha soniyada tekshirish

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

async def main():
    db_init()
    asyncio.create_task(check_expired_members())
    logger.info("Bot ishga tushdi...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
