import os
import io
import csv
import random
from datetime import datetime, timedelta, time
import pytz
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

# ---- Telegram & scheduling ----
from telegram import Update, ForceReply, InputFile
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from dotenv import load_dotenv

# ---- DB backends ----
import sqlite3
IS_PG = False
try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:
    psycopg2 = None

load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DEFAULT_TZ = "Europe/Kyiv"
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_PATH = os.getenv("DB_PATH", "vocabcoach.db")  # локально

if DATABASE_URL and DATABASE_URL.startswith(("postgres://", "postgresql://")) and psycopg2 is not None:
    IS_PG = True

PROMPTS = [
    "How was your day? Use **{item}** in a short sentence.",
    "Write one line about your life using **{item}**.",
    "Make a simple daily sentence with **{item}**.",
]

HELP = (
    "Команди:\n"
    "/add слово або фраза — додати\n"
    "/list — список перших 50\n"
    "/remove <id> — видалити запис\n"
    "/settings <кількість> <початок> <кінець> [timezone]\n"
    "/when — сьогоднішні часи\n"
    "/export — відповіді у CSV\n"
    "/help — ця довідка"
)

# ===================== DB LAYER =====================

def pg_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def sqlite_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

if IS_PG:
    def db_exec(query: str, params: tuple = ()):  # write
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
    def db_fetchone(query: str, params: tuple = ()):  # one row
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchone()
    def db_fetchall(query: str, params: tuple = ()):  # many rows
        with pg_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchall()
else:
    _sqlite = sqlite_conn()
    def db_exec(query: str, params: tuple = ()):  # write
        with _sqlite:
            _sqlite.execute(query, params)
    def db_fetchone(query: str, params: tuple = ()):  # one row
        cur = _sqlite.execute(query, params)
        return cur.fetchone()
    def db_fetchall(query: str, params: tuple = ()):  # many rows
        cur = _sqlite.execute(query, params)
        return cur.fetchall()

def init_schema():
    if IS_PG:
        db_exec(
            """
            CREATE TABLE IF NOT EXISTS users (
              chat_id BIGINT PRIMARY KEY,
              tz TEXT DEFAULT 'Europe/Kyiv',
              start_hour INT DEFAULT 10,
              end_hour INT DEFAULT 21,
              daily_count INT DEFAULT 3
            );
            """
        )
        db_exec(
            """
            CREATE TABLE IF NOT EXISTS vocab (
              id SERIAL PRIMARY KEY,
              chat_id BIGINT,
              text TEXT,
              last_seen TIMESTAMPTZ,
              strength INT DEFAULT 0,
              active INT DEFAULT 1
            );
            """
        )
        db_exec(
            """
            CREATE TABLE IF NOT EXISTS answers (
              id SERIAL PRIMARY KEY,
              chat_id BIGINT,
              vocab_id INT,
              text TEXT,
              answered_at TIMESTAMPTZ
            );
            """
        )
    else:
        db_exec(
            """
            CREATE TABLE IF NOT EXISTS users (
              chat_id INTEGER PRIMARY KEY,
              tz TEXT DEFAULT 'Europe/Kyiv',
              start_hour INTEGER DEFAULT 10,
              end_hour INTEGER DEFAULT 21,
              daily_count INTEGER DEFAULT 3
            );
            """
        )
        db_exec(
            """
            CREATE TABLE IF NOT EXISTS vocab (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              chat_id INTEGER,
              text TEXT,
              last_seen TEXT,
              strength INTEGER DEFAULT 0,
              active INTEGER DEFAULT 1
            );
            """
        )
        db_exec(
            """
            CREATE TABLE IF NOT EXISTS answers (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              chat_id INTEGER,
              vocab_id INTEGER,
              text TEXT,
              answered_at TEXT
            );
            """
        )

def get_user(chat_id: int):
    row = db_fetchone((
        "SELECT * FROM users WHERE chat_id = %s" if IS_PG else "SELECT * FROM users WHERE chat_id = ?"
    ), (chat_id,))
    if row:
        return row
    db_exec((
        "INSERT INTO users(chat_id) VALUES(%s)" if IS_PG else "INSERT INTO users(chat_id) VALUES(?)"
    ), (chat_id,))
    return get_user(chat_id)

def set_user(chat_id: int, **fields):
    if not fields:
        return
    cols = []
    vals = []
    for k, v in fields.items():
        cols.append(f"{k} = %s" if IS_PG else f"{k} = ?")
        vals.append(v)
    vals.append(chat_id)
    db_exec((
        f"UPDATE users SET {', '.join(cols)} WHERE chat_id = %s" if IS_PG else f"UPDATE users SET {', '.join(cols)} WHERE chat_id = ?"
    ), tuple(vals))

def add_vocab(chat_id: int, text: str):
    text = (text or "").strip()
    if not text:
        return
    db_exec((
        "INSERT INTO vocab(chat_id, text) VALUES(%s,%s)" if IS_PG else "INSERT INTO vocab(chat_id, text) VALUES(?,?)"
    ), (chat_id, text))

def list_vocab(chat_id: int, limit: int = 50):
    return db_fetchall((
        "SELECT id, text, last_seen, strength FROM vocab WHERE chat_id=%s AND active=1 ORDER BY id LIMIT %s"
        if IS_PG else
        "SELECT id, text, last_seen, strength FROM vocab WHERE chat_id=? AND active=1 ORDER BY id LIMIT ?"
    ), (chat_id, limit))

def remove_vocab(chat_id: int, vid: int):
    db_exec((
        "UPDATE vocab SET active=0 WHERE chat_id=%s AND id=%s" if IS_PG else "UPDATE vocab SET active=0 WHERE chat_id=? AND id=?"
    ), (chat_id, vid))

def pick_vocab(chat_id: int):
    if IS_PG:
        return db_fetchone(
            """
            SELECT id, text, last_seen, strength FROM vocab
            WHERE chat_id=%s AND active=1
            ORDER BY last_seen NULLS FIRST, strength ASC, random() LIMIT 1
            """,
            (chat_id,)
        )
    else:
        return db_fetchone(
            """
            SELECT id, text, last_seen, strength FROM vocab
            WHERE chat_id=? AND active=1
            ORDER BY COALESCE(last_seen,'0000'), strength ASC, RANDOM() LIMIT 1
            """,
            (chat_id,)
        )

def record_answer(chat_id: int, vocab_id: int, text: str):
    now = datetime.utcnow().isoformat()
    db_exec((
        "INSERT INTO answers(chat_id, vocab_id, text, answered_at) VALUES(%s,%s,%s,%s)"
        if IS_PG else
        "INSERT INTO answers(chat_id, vocab_id, text, answered_at) VALUES(?,?,?,?)"
    ), (chat_id, vocab_id, text, now))
    db_exec((
        "UPDATE vocab SET last_seen=%s, strength=strength+1 WHERE id=%s"
        if IS_PG else
        "UPDATE vocab SET last_seen=?, strength=strength+1 WHERE id=?"
    ), (now, vocab_id))

# ===================== SCHEDULER =====================

scheduler = AsyncIOScheduler()

async def schedule_today(app, chat_id: int):
    u = get_user(chat_id)
    tz = pytz.timezone(u.get("tz") if IS_PG else u["tz"])
    now = datetime.now(tz)

    start_h = int(u.get("start_hour") if IS_PG else u["start_hour"])
    end_h = int(u.get("end_hour") if IS_PG else u["end_hour"])
    count = int(u.get("daily_count") if IS_PG else u["daily_count"])

    planned = set()
    while len(planned) < max(1, min(12, count)):
        hour = random.randint(start_h, max(start_h, end_h - 1))
        minute = random.choice([5, 15, 25, 35, 45])
        run_at = tz.localize(datetime.combine(now.date(), time(hour, minute)))
        if run_at > now:
            planned.add(run_at)

    for t in sorted(planned):
        scheduler.add_job(send_prompt, trigger=DateTrigger(run_date=t), args=[app, chat_id])

async def send_prompt(app, chat_id: int):
    v = pick_vocab(chat_id)
    if not v:
        await app.bot.send_message(chat_id, "Додай слова через /add ✍️")
        return
    text = v["text"] if IS_PG else v["text"]
    msg = random.choice(PROMPTS).format(item=text)
    await app.bot.send_message(chat_id, msg, reply_markup=ForceReply())

# ===================== HANDLERS =====================

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    get_user(chat_id)
    await update.message.reply_text(
        "Привіт! Я кілька разів на день нагадаю попрактикувати твої слова.\n" + HELP
    )
    await schedule_today(ctx.application, chat_id)

async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP)

async def add_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    txt = " ".join(ctx.args).strip()
    if not txt:
        await update.message.reply_text("Напиши: /add твоє_слово_або_фраза")
        return
    add_vocab(update.effective_chat.id, txt)
    await update.message.reply_text(f"Додано: {txt}")

async def list_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    rows = list_vocab(update.effective_chat.id)
    if not rows:
        await update.message.reply_text("Список пустий")
        return
    out = []
    for r in rows:
        if IS_PG:
            out.append(f"{r['id']}. {r['text']} (seen: {r.get('last_seen') or '—'})")
        else:
            out.append(f"{r['id']}. {r['text']} (seen: {r['last_seen'] or '—'})")
    await update.message.reply_text("\n".join(out))

async def remove_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args or not ctx.args[0].isdigit():
        await update.message.reply_text("Використання: /remove <id>  (див. /list)")
        return
    remove_vocab(update.effective_chat.id, int(ctx.args[0]))
    await update.message.reply_text("Видалено")

async def settings_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    u = get_user(chat_id)
    if len(ctx.args) >= 3:
        try:
            count = int(ctx.args[0]); start_h = int(ctx.args[1]); end_h = int(ctx.args[2])
            tz_name = ctx.args[3] if len(ctx.args) >= 4 else (u.get("tz") if IS_PG else u["tz"])
            set_user(chat_id, daily_count=count, start_hour=start_h, end_hour=end_h, tz=tz_name)
            await update.message.reply_text(
                f"Оновлено: {count} раз/день, {start_h}:00–{end_h}:00, tz: {tz_name}"
            )
            await schedule_today(ctx.application, chat_id)
            return
        except Exception:
            pass
    cur_line = (
        f"Поточні: {(u.get('daily_count') if IS_PG else u['daily_count'])} раз/день, "
        f"{(u.get('start_hour') if IS_PG else u['start_hour'])}:00–{(u.get('end_hour') if IS_PG else u['end_hour'])}:00, "
        f"tz: {(u.get('tz') if IS_PG else u['tz'])}"
    )
    await update.message.reply_text(
        "Використання: /settings <кількість> <початок> <кінець> [timezone]\n" + cur_line
    )

async def when_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    u = get_user(chat_id)
    tz = pytz.timezone(u.get("tz") if IS_PG else u["tz"])
    today = datetime.now(tz).date()
    times = []
    for job in scheduler.get_jobs():
        nxt = job.next_run_time
        if nxt and nxt.astimezone(tz).date() == today:
            times.append(nxt.astimezone(tz).strftime("%H:%M"))
    times.sort()
    await update.message.reply_text("Сьогодні: " + (", ".join(times) if times else "поки порожньо"))

async def reply_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        return
    v = pick_vocab(update.effective_chat.id)
    if not v:
        return
    vid = v["id"] if IS_PG else v["id"]
    record_answer(update.effective_chat.id, vid, update.message.text)
    await update.message.reply_text("Записано ✔️")

async def export_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    rows = db_fetchall((
        "SELECT answered_at, text FROM answers WHERE chat_id=%s ORDER BY answered_at DESC"
        if IS_PG else
        "SELECT answered_at, text FROM answers WHERE chat_id=? ORDER BY answered_at DESC"
    ), (chat_id,))
    if not rows:
        await update.message.reply_text("Немає відповідей 💾")
        return
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["answered_at_UTC", "answer"])
    for r in rows:
        writer.writerow([r["answered_at"] if IS_PG else r["answered_at"], r["text"] if IS_PG else r["text"]])
    output.seek(0)
    await update.message.reply_document(InputFile(io.BytesIO(output.getvalue().encode("utf-8")), filename="answers.csv"))

# ===================== tiny HTTP server for Render =====================

class _Health(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/healthz"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()

def _run_http():
    # Render передає порт у змінній середовища PORT
    port = int(os.environ.get("PORT", "10000"))
    HTTPServer(("0.0.0.0", port), _Health).serve_forever()

# ===================== MAIN =====================

def main():
    if not BOT_TOKEN:
        raise RuntimeError("У .env немає TELEGRAM_BOT_TOKEN")

    # 1) Підняти схему БД
    init_schema()

    # 2) Telegram app
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("add", add_cmd))
    app.add_handler(CommandHandler("list", list_cmd))
    app.add_handler(CommandHandler("remove", remove_cmd))
    app.add_handler(CommandHandler("settings", settings_cmd))
    app.add_handler(CommandHandler("when", when_cmd))
    app.add_handler(CommandHandler("export", export_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, reply_handler))

    # ——— запуск легкого вебсерверу, щоб Render бачив відкритий порт
    threading.Thread(target=_run_http, daemon=True).start()

    scheduler.start()
    print("Bot started. Press Ctrl+C to stop.")
    app.run_polling()

if __name__ == "__main__":
    main()