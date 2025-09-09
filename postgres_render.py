import os
import random
import logging
from decimal import Decimal
from hashlib import sha256
from base58 import b58encode, b58decode
from dotenv import load_dotenv

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters
)

import asyncio
import sys
import threading
from typing import Optional
from datetime import datetime, timezone, date

from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.transaction import Transaction
from solders.instruction import Instruction, AccountMeta
from solders.message import Message

from solana.rpc.api import Client
from solana.rpc.types import TxOpts

# ---------- Postgres ----------
import psycopg2
from psycopg2.pool import SimpleConnectionPool

# Windows asyncio fix
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# === CONFIG & LOGGING ===
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# === ENV & CONSTANTS ===
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
SOLANA_RPC = os.getenv("SOLANA_RPC", "https://api.devnet.solana.com")
BOT_PRIVATE_KEY = os.getenv("BOT_PRIVATE_KEY")
HOUSE_WALLET = os.getenv("HOUSE_WALLET")
HOUSE_PRIVATE_KEY = os.getenv("HOUSE_PRIVATE_KEY")  # OPTIONAL: payouts/refunds from house
DATABASE_URL = os.getenv("DATABASE_URL")

SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "YourSupportHandle")  # no '@'
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN")
if not BOT_PRIVATE_KEY:
    raise RuntimeError("Missing BOT_PRIVATE_KEY (base58-encoded keypair bytes)")
if not HOUSE_WALLET:
    raise RuntimeError("Missing HOUSE_WALLET (Solana address)")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL (Postgres URI)")

HOUSE_FEE_RATE = Decimal("0.05")          # 5% house fee (user-facing text never shows this)
INACTIVITY_TIMEOUT_SECS = 5 * 60          # 5 minutes
FEE_BUFFER_LAMPORTS = 100_000             # fee cushion for payouts (~0.0001 SOL)

games = {}        # gid -> game data (in-memory, active games only)
user_games = {}   # uid -> gid (in-memory, active games only)
used_txs = set()  # fast in-memory cache, persisted in Postgres
game_locks = {}   # gid -> asyncio.Lock()

# serialize payouts to avoid double-spends/nonce issues under load
_payout_lock = asyncio.Lock()

EMOJI_MAP = {
    "basketball": "🏀",
    "darts": "🎯",
    "dice": "🎲",
}

# === BLOCKCHAIN SETUP ===
solana = Client(SOLANA_RPC)
signer = Keypair.from_bytes(b58decode(BOT_PRIVATE_KEY))
house_signer = Keypair.from_bytes(b58decode(HOUSE_PRIVATE_KEY)) if HOUSE_PRIVATE_KEY else None

def _get_payout_signer() -> Keypair:
    """Prefer house wallet for payouts/refunds if provided; otherwise use bot signer."""
    return house_signer or signer

# === Postgres helpers & migrations ===
_pg_pool: Optional[SimpleConnectionPool] = None
_pg_lock = threading.Lock()  # guard pool init

def pg() -> SimpleConnectionPool:
    global _pg_pool
    with _pg_lock:
        if _pg_pool is None:
            _pg_pool = SimpleConnectionPool(minconn=1, maxconn=5, dsn=DATABASE_URL)
        return _pg_pool

def pg_exec(sql: str, params: tuple = (), fetch: str = "none"):
    pool = pg()
    conn = pool.getconn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
            if fetch == "one":
                return cur.fetchone()
            elif fetch == "all":
                return cur.fetchall()
            return None
    finally:
        pool.putconn(conn)

def init_db():
    # Create core tables
    pg_exec("""
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT PRIMARY KEY,
        username TEXT,
        first TEXT,
        last TEXT
    );
    """)
    pg_exec("""
    CREATE TABLE IF NOT EXISTS user_stats (
        user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
        wins INTEGER NOT NULL DEFAULT 0,
        losses INTEGER NOT NULL DEFAULT 0,
        draws INTEGER NOT NULL DEFAULT 0,
        net_profit_sol NUMERIC NOT NULL DEFAULT 0,
        last_played_ts BIGINT NOT NULL DEFAULT 0
    );
    """)
    pg_exec("""
    CREATE TABLE IF NOT EXISTS used_txs (
        tx TEXT PRIMARY KEY
    );
    """)
    pg_exec("""
    CREATE TABLE IF NOT EXISTS game_keys (
        gid INTEGER PRIMARY KEY,
        priv58 TEXT NOT NULL
    );
    """)
    # Metrics tables
    pg_exec("""
    CREATE TABLE IF NOT EXISTS metrics_daily (
        day DATE PRIMARY KEY,
        games_created INTEGER NOT NULL DEFAULT 0,
        games_started INTEGER NOT NULL DEFAULT 0,
        games_completed INTEGER NOT NULL DEFAULT 0,
        games_drawn INTEGER NOT NULL DEFAULT 0,
        games_cancelled INTEGER NOT NULL DEFAULT 0,
        total_entries_sol NUMERIC NOT NULL DEFAULT 0,
        total_prize_gross_sol NUMERIC NOT NULL DEFAULT 0,
        total_payouts_net_sol NUMERIC NOT NULL DEFAULT 0,
        total_refunds_net_sol NUMERIC NOT NULL DEFAULT 0,
        total_fees_sol NUMERIC NOT NULL DEFAULT 0
    );
    """)
    pg_exec("""
    CREATE TABLE IF NOT EXISTS metrics_players (
        day DATE NOT NULL,
        user_id BIGINT NOT NULL,
        PRIMARY KEY (day, user_id)
    );
    """)

    # prime used_txs cache
    rows = pg_exec("SELECT tx FROM used_txs", fetch="all") or []
    used_txs.clear()
    used_txs.update(tx for (tx,) in rows)

# === Metrics helpers ===
def _today_utc() -> date:
    return datetime.now(timezone.utc).date()

def _metrics_touch_day(d: date | None = None):
    d = d or _today_utc()
    pg_exec("INSERT INTO metrics_daily(day) VALUES (%s) ON CONFLICT DO NOTHING;", (d,))

def metrics_inc(d: date | None = None, **fields):
    """
    Increment numeric columns in metrics_daily atomically.
    Example: metrics_inc(total_entries_sol=Decimal('0.3'), games_created=1)
    """
    d = d or _today_utc()
    if not fields:
        return
    _metrics_touch_day(d)
    sets = []
    params = []
    for col, val in fields.items():
        sets.append(f"{col} = {col} + %s")
        params.append(str(val))
    params.append(d)
    sql = f"UPDATE metrics_daily SET {', '.join(sets)} WHERE day = %s;"
    pg_exec(sql, tuple(params))

def metrics_mark_player(user_id: int, d: date | None = None):
    d = d or _today_utc()
    try:
        pg_exec("INSERT INTO metrics_players(day, user_id) VALUES (%s, %s) ON CONFLICT DO NOTHING;", (d, user_id))
    except Exception as e:
        logger.warning(f"metrics player insert failed: {e}")

def metrics_report(days: int = 7):
    """Return (rows_by_day, totals_all_time, totals_window, unique_all, unique_window)"""
    rows = pg_exec("""
        SELECT day, games_created, games_started, games_completed, games_drawn, games_cancelled,
               total_entries_sol, total_prize_gross_sol, total_payouts_net_sol, total_refunds_net_sol, total_fees_sol
        FROM metrics_daily
        WHERE day >= CURRENT_DATE - (%s::INT - 1)
        ORDER BY day ASC;
    """, (days,), fetch="all") or []

    totals_all = pg_exec("""
        SELECT
          SUM(games_created), SUM(games_started), SUM(games_completed), SUM(games_drawn), SUM(games_cancelled),
          SUM(total_entries_sol), SUM(total_prize_gross_sol), SUM(total_payouts_net_sol), SUM(total_refunds_net_sol), SUM(total_fees_sol)
        FROM metrics_daily;
    """, fetch="one")

    totals_win = pg_exec("""
        SELECT
          SUM(games_created), SUM(games_started), SUM(games_completed), SUM(games_drawn), SUM(games_cancelled),
          SUM(total_entries_sol), SUM(total_prize_gross_sol), SUM(total_payouts_net_sol), SUM(total_refunds_net_sol), SUM(total_fees_sol)
        FROM metrics_daily
        WHERE day >= CURRENT_DATE - (%s::INT - 1);
    """, (days,), fetch="one")

    unique_win = pg_exec("""
        SELECT COUNT(DISTINCT user_id) FROM metrics_players
        WHERE day >= CURRENT_DATE - (%s::INT - 1);
    """, (days,), fetch="one")[0] or 0
    unique_all = pg_exec("SELECT COUNT(DISTINCT user_id) FROM metrics_players;", fetch="one")[0] or 0

    return rows, totals_all, totals_win, unique_all, unique_win

# === DATABASE (was JSON; now Postgres) ===
class DB:
    # profile snapshot/upsert
    def snapshot_username(self, user_id: int, username: Optional[str], first: Optional[str], last: Optional[str]):
        pg_exec("""
            INSERT INTO users (user_id, username, first, last)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE
            SET username = EXCLUDED.username,
                first = EXCLUDED.first,
                last = EXCLUDED.last;
        """, (user_id, username, first, last))
        pg_exec("""
            INSERT INTO user_stats (user_id) VALUES (%s)
            ON CONFLICT (user_id) DO NOTHING;
        """, (user_id,))

    # stats updates (atomic)
    def update_stats(self, user_id: int, *, delta_win=0, delta_loss=0, delta_draw=0, delta_profit_sol: Decimal = Decimal("0")):
        pg_exec("INSERT INTO users (user_id) VALUES (%s) ON CONFLICT DO NOTHING;", (user_id,))
        pg_exec("INSERT INTO user_stats (user_id) VALUES (%s) ON CONFLICT DO NOTHING;", (user_id,))
        pg_exec("""
            UPDATE user_stats
            SET wins = wins + %s,
                losses = losses + %s,
                draws = draws + %s,
                net_profit_sol = net_profit_sol + %s,
                last_played_ts = GREATEST(last_played_ts, %s)
            WHERE user_id = %s;
        """, (
            int(delta_win), int(delta_loss), int(delta_draw),
            str(delta_profit_sol), int(asyncio.get_event_loop().time()), user_id
        ))

    # leaderboard snapshot (top N)
    def top_players(self, limit: int = 10):
        rows = pg_exec("""
            SELECT u.user_id,
                   COALESCE(NULLIF(u.username,''), 'user' || u.user_id::text) AS username,
                   s.wins, s.losses, s.draws, s.net_profit_sol
            FROM user_stats s
            JOIN users u ON u.user_id = s.user_id
            WHERE (s.wins + s.losses + s.draws) > 0
            ORDER BY s.net_profit_sol DESC, s.wins DESC
            LIMIT %s;
        """, (limit,), fetch="all") or []
        return rows

db = DB()

# === UTILS (moved from JSON to Postgres) ===
def store_key(gid: int, kp: Keypair):
    priv = b58encode(bytes(kp)).decode()
    pg_exec("""
        INSERT INTO game_keys (gid, priv58)
        VALUES (%s, %s)
        ON CONFLICT (gid) DO UPDATE SET priv58 = EXCLUDED.priv58;
    """, (gid, priv))
    return priv

def _load_game_keypair(gid: int) -> Keypair:
    row = pg_exec("SELECT priv58 FROM game_keys WHERE gid = %s;", (gid,), fetch="one")
    if not row:
        raise RuntimeError(f"Missing key for game {gid}")
    priv58 = row[0]
    return Keypair.from_bytes(b58decode(priv58))

def mark_tx_used(tx: str):
    try:
        pg_exec("INSERT INTO used_txs (tx) VALUES (%s) ON CONFLICT DO NOTHING;", (tx,))
        used_txs.add(tx)
    except Exception as e:
        logger.warning(f"Failed to persist used tx {tx}: {e}")

def solscan(sig):
    return f"https://solscan.io/tx/{sig}?cluster=devnet"

def _lamports_from_sol(amt: float | Decimal) -> int:
    return int(Decimal(str(amt)) * Decimal(1_000_000_000))

def _get_signer_balance_lamports(kp: Optional[Keypair] = None) -> int:
    kp = kp or _get_payout_signer()
    try:
        resp = solana.get_balance(kp.pubkey())
        return int(resp.value)
    except Exception:
        return 0

# === PAYOUTS ===
def payout(to_addr, amt_sol, *, from_signer: Optional[Keypair] = None) -> str:
    """Send a SystemProgram transfer from 'from_signer' (or payout signer) to 'to_addr'."""
    kp = from_signer or _get_payout_signer()
    lam = _lamports_from_sol(amt_sol)
    fpk = kp.pubkey()
    tpk = Pubkey.from_string(to_addr)

    # SystemProgram::Transfer discriminator index 2 (u32 LE) + lamports (u64 LE)
    data = (2).to_bytes(4, "little") + lam.to_bytes(8, "little")

    ix = Instruction(
        program_id=Pubkey.from_string("11111111111111111111111111111111"),
        accounts=[
            AccountMeta(fpk, is_signer=True, is_writable=True),
            AccountMeta(tpk, is_signer=False, is_writable=True),
        ],
        data=data,
    )

    msg = Message([ix], payer=fpk)
    txn = Transaction.new_unsigned(msg)

    blk = solana.get_latest_blockhash(commitment="confirmed").value.blockhash
    txn.sign([kp], blk)

    raw = bytes(txn)
    res = solana.send_raw_transaction(raw, opts=TxOpts(skip_preflight=True))
    return str(res.value)

async def _safe_distribute(recipient_wallet: str, amount: float):
    # serialize payouts; run blocking call in thread
    async with _payout_lock:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: distribute_winnings(recipient_wallet, amount))

def distribute_winnings(recipient_wallet: str, amount: float):
    """
    Sends the house fee to HOUSE_WALLET (unless payout signer IS the house, then it's retained),
    and sends net winnings/refund to recipient_wallet.
    USER-FACING MESSAGES MUST NOT MENTION FEES; FEES ARE LOGGED ONLY.
    """
    fee = (Decimal(str(amount)) * HOUSE_FEE_RATE).quantize(Decimal("0.00000001"))
    net = (Decimal(str(amount)) - fee).quantize(Decimal("0.00000001"))

    kp = _get_payout_signer()
    paying_from_house = (str(kp.pubkey()) == HOUSE_WALLET)

    # require enough to cover net (+ fee if not retained) + buffer
    needed = _lamports_from_sol(net) + (0 if paying_from_house else _lamports_from_sol(fee)) + FEE_BUFFER_LAMPORTS
    if _get_signer_balance_lamports(kp) < needed:
        raise RuntimeError("Insufficient payout balance in payout wallet.")

    # fee transfer/retention
    if paying_from_house:
        fee_sig = "retained"
    else:
        fee_sig = payout(HOUSE_WALLET, float(fee), from_signer=kp)

    win_sig = payout(recipient_wallet, float(net), from_signer=kp)

    # LOG fees (not shown to users)
    logger.info(f"[payout] gross={amount} net={net} fee={fee} fee_sig={fee_sig} to={recipient_wallet} win_sig={win_sig}")

    logger.info(f"[+] Paid {net} to winner/refund: {recipient_wallet} ({win_sig})")
    if paying_from_house:
        logger.info(f"[+] Fee {fee} retained in house wallet: {HOUSE_WALLET}")
    else:
        logger.info(f"[+] Fee {fee} sent to house wallet: {HOUSE_WALLET} ({fee_sig})")

    return win_sig, fee_sig, float(net), float(fee)

# === GAME WALLET SWEEP (silent to users) ===
def _estimate_fee_for_message(msg: Message) -> int:
    try:
        res = solana.get_fee_for_message(msg)
        fee = getattr(res, "value", None)
        if isinstance(fee, int):
            return fee
        return int(fee or 0)
    except Exception:
        return 80_000  # conservative fallback

def _is_plain_system_account(pubkey: Pubkey) -> bool:
    try:
        ai = solana.get_account_info(pubkey).value
        if not ai:
            return True
        owner = str(ai.owner)
        return owner == "11111111111111111111111111111111"
    except Exception:
        return True

def sweep_game_wallet_to_house(gid: int) -> Optional[str]:
    kp = _load_game_keypair(gid)
    src_pk = kp.pubkey()
    dst_pk = Pubkey.from_string(HOUSE_WALLET)

    if not _is_plain_system_account(src_pk):
        logger.warning(f"[sweep] Game #{gid} source is not a plain System account; skipping sweep.")
        return None

    try:
        bal = int(solana.get_balance(src_pk).value)
    except Exception as e:
        logger.error(f"[sweep] Game #{gid} failed to get balance: {e}")
        return None

    SWEEP_KEEP_LAMPORTS = 50_000
    if bal <= SWEEP_KEEP_LAMPORTS:
        return None

    placeholder_lamports = 1
    data = (2).to_bytes(4, "little") + placeholder_lamports.to_bytes(8, "little")
    draft_ix = Instruction(
        program_id=Pubkey.from_string("11111111111111111111111111111111"),
        accounts=[AccountMeta(src_pk, True, True), AccountMeta(dst_pk, False, True)],
        data=data,
    )
    draft_msg = Message([draft_ix], payer=src_pk)
    fee_est = _estimate_fee_for_message(draft_msg)

    keep_min = max(SWEEP_KEEP_LAMPORTS, fee_est + 10_000)
    if bal <= keep_min:
        return None

    lamports_to_send = bal - keep_min
    if lamports_to_send < 5_000:
        return None

    real_data = (2).to_bytes(4, "little") + lamports_to_send.to_bytes(8, "little")
    ix = Instruction(
        program_id=Pubkey.from_string("11111111111111111111111111111111"),
        accounts=[AccountMeta(src_pk, True, True), AccountMeta(dst_pk, False, True)],
        data=real_data,
    )

    msg = Message([ix], payer=src_pk)
    txn = Transaction.new_unsigned(msg)
    try:
        blk = solana.get_latest_blockhash(commitment="confirmed").value.blockhash
        txn.sign([kp], blk)
        raw = bytes(txn)
        res = solana.send_raw_transaction(raw, opts=TxOpts(skip_preflight=False))
        sig = str(res.value)
        logger.info(f"[sweep] Game #{gid} → HOUSE {HOUSE_WALLET}: {lamports_to_send} lamports ({solscan(sig)})")
        return sig
    except Exception as e:
        logger.error(f"[sweep] Game #{gid} sweep failed: {e}")
        return None

async def _sweep_game_wallet_to_house_async(gid: int) -> Optional[str]:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: sweep_game_wallet_to_house(gid))

# === MISC HELPERS ===
async def safe_send(bot, chat_id, *args, **kwargs):
    try:
        return await bot.send_message(chat_id, *args, **kwargs)
    except Exception as e:
        logger.warning(f"Failed to send message to {chat_id}: {e}")
        return None

def _clear_user_games(gid: int):
    for uid in list(user_games.keys()):
        if user_games.get(uid) == gid:
            user_games.pop(uid, None)

def _reschedule_timeout(context: ContextTypes.DEFAULT_TYPE, gid: int, seconds: int = INACTIVITY_TIMEOUT_SECS):
    g = games.get(gid)
    if not g:
        return
    jq = getattr(context.application, "job_queue", None)
    if jq is None:
        logger.warning("JobQueue not available; timeouts/cancel-on-inactivity will not run.")
        return
    job = g.get("timeout_job")
    if job:
        try:
            job.schedule_removal()
        except Exception:
            pass
    g["timeout_job"] = jq.run_once(_timeout_cb, when=seconds, data={"gid": gid}, name=f"timeout-{gid}")

async def _timeout_cb(ctx: ContextTypes.DEFAULT_TYPE):
    try:
        gid = ctx.job.data.get("gid") if getattr(ctx, "job", None) else None
    except Exception:
        gid = None
    if gid is None:
        return
    g = games.get(gid)
    if not g or g["state"] == "ended":
        return
    await _finish_with_draw_and_refund(ctx, gid, "inactivity timeout")

# === HELPERS: user touch/logging ===
async def _touch_user(update: Update):
    u = update.effective_user
    if not u:
        return
    db.snapshot_username(u.id, u.username, u.first_name, u.last_name)

def _is_owner(uid: int) -> bool:
    return OWNER_ID and uid == OWNER_ID

# === COMMAND HANDLERS ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _touch_user(update)
    m = update.effective_message
    if not m:
        return
    await m.reply_text(
        "👋 Welcome to MojiBet.\n\n"
        "How it works:\n"
        "• Start a game with: /play [game] [players] [amount in SOL]\n"
        "• Other users can join with: /join [game ID] [tx]\n"
        "• Payouts and refunds are automatically sent back to the same wallet that funded the game.\n\n"
        "Examples:\n"
        "• /play basketball 2 0.1\n"
        "• /play darts 3 0.2\n"
        "• /play dice 2 0.1\n\n"
        "Need help? /support",
        parse_mode="Markdown"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _touch_user(update)
    m = update.effective_message
    if not m:
        return
    help_text = (
        "/play [game] [players] [amount] – Create a game\n"
        "/join [id] [tx] - Join a game (use your funding tx)\n"
        "/cancel - Cancel your active game\n"
        "/status - List active games\n"
        "/leaderboard – Top players by net profit\n"
        "/metrics – Admin metrics (owner only)\n"
        "/support – DM support\n"
        "/help – Show this message\n\n"
        "Notes:\n"
        "• No in-app wallet setup or withdrawals needed; everything is on-chain.\n"
        "• If something fails, tell support your game # and funding tx."
    )
    await m.reply_text(help_text)

async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _touch_user(update)
    m = update.effective_message
    if not m:
        return
    url = f"https://t.me/{SUPPORT_USERNAME}"
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Message Support", url=url)]])
    await m.reply_text("Need help? Tap below to DM support.", reply_markup=kb, disable_web_page_preview=True)

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _touch_user(update)
    m = update.effective_message
    rows = db.top_players(limit=10)
    if not rows:
        return await m.reply_text("No stats yet. Play a game with /play!")

    lines = ["🏆 Leaderboard (by net profit in SOL):"]
    for i, (uid, uname, wins, losses, draws, net) in enumerate(rows, 1):
        lines.append(f"{i}. @{uname} — net {net} | W:{wins} L:{losses} D:{draws}")
    await m.reply_text("\n".join(lines))

async def users_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _touch_user(update)
    if not _is_owner(update.effective_user.id):
        return await update.effective_message.reply_text("🚫")
    rows = pg_exec("""
        SELECT u.user_id, COALESCE(NULLIF(u.username,''), 'user' || u.user_id::text) AS uname,
               s.wins, s.losses, s.draws, s.net_profit_sol
        FROM users u
        LEFT JOIN user_stats s ON s.user_id = u.user_id
        ORDER BY u.user_id DESC
        LIMIT 50;
    """, fetch="all") or []
    total_users = pg_exec('SELECT COUNT(*) FROM users;', fetch='one')[0]
    lines = [f"Total users: {total_users}"]
    for uid, uname, w, l, d, net in rows:
        lines.append(f"• {uid} @{uname} W:{w or 0} L:{l or 0} D:{d or 0} net:{net or 0}")
    await update.effective_message.reply_text("\n".join(lines))

async def metrics_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _touch_user(update)
    if not _is_owner(update.effective_user.id):
        return await update.effective_message.reply_text("🚫")

    try:
        days = 7
        if context.args:
            try:
                days = max(1, min(31, int(context.args[0])))
            except Exception:
                pass

        rows, totals_all, totals_win, unique_all, unique_win = metrics_report(days)

        def fmt_num(x):
            return "0" if x is None else str(x)

        (a_gc, a_gs, a_gcomp, a_gdraw, a_gcan,
         a_entries, a_prize, a_payouts, a_refunds, a_fees) = totals_all or (0,)*10
        (w_gc, w_gs, w_gcomp, w_gdraw, w_gcan,
         w_entries, w_prize, w_payouts, w_refunds, w_fees) = totals_win or (0,)*10

        per_day_lines = []
        for (d, gc, gs, gcomp, gdraw, gcan, entries, prize, payouts, refunds, fees) in rows:
            per_day_lines.append(
                f"{d}: created {fmt_num(gc)}, started {fmt_num(gs)}, "
                f"completed {fmt_num(gcomp)}, drawn {fmt_num(gdraw)}, cancelled {fmt_num(gcan)} | "
                f"entries {entries or 0} SOL, prize {prize or 0} SOL, "
                f"payouts {payouts or 0} SOL, refunds {refunds or 0} SOL, fees {fees or 0} SOL"
            )

        text = (
            f"📊 Metrics (last {days} days)\n"
            f"Unique players: {unique_win} (window) | {unique_all} (all-time)\n\n"
            f"Window totals:\n"
            f"• Created: {fmt_num(w_gc)}, Started: {fmt_num(w_gs)}, Completed: {fmt_num(w_gcomp)}, "
            f"Drawn: {fmt_num(w_gdraw)}, Cancelled: {fmt_num(w_gcan)}\n"
            f"• Entries: {w_entries or 0} SOL, Prize (gross): {w_prize or 0} SOL\n"
            f"• Net payouts: {w_payouts or 0} SOL, Net refunds: {w_refunds or 0} SOL\n"
            f"• Fees: {w_fees or 0} SOL\n\n"
            f"All-time totals:\n"
            f"• Created: {fmt_num(a_gc)}, Started: {fmt_num(a_gs)}, Completed: {fmt_num(a_gcomp)}, "
            f"Drawn: {fmt_num(a_gdraw)}, Cancelled: {fmt_num(a_gcan)}\n"
            f"• Entries: {a_entries or 0} SOL, Prize (gross): {a_prize or 0} SOL\n"
            f"• Net payouts: {a_payouts or 0} SOL, Net refunds: {a_refunds or 0} SOL\n"
            f"• Fees: {a_fees or 0} SOL\n\n"
            f"Daily breakdown:\n" +
            ("\n".join(per_day_lines) if per_day_lines else "No data yet.")
        )

        await update.effective_message.reply_text(text)
    except Exception as e:
        logger.error(f"[metrics] command failed: {e}")
        await update.effective_message.reply_text("⚠️ Metrics unavailable right now.")

# === GAME COMMANDS ===
async def play(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _touch_user(update)
    m = update.effective_message
    if not m:
        return
    uid = update.effective_user.id
    chat = update.effective_chat
    cid = chat.id
    chat_type = getattr(chat, "type", None) or "private"

    if uid in user_games:
        return await m.reply_text("⚠️ Finish your active game with /cancel first.")

    parts = context.args
    if len(parts) != 3:
        return await m.reply_text("⚠️ Usage: /play [type] [players] [amount]")

    gtype = parts[0].lower()
    if gtype not in EMOJI_MAP:
        return await m.reply_text("⚠️ Type must be basketball, darts, or dice.")
    try:
        num = int(parts[1])
        amt = float(parts[2])
        if num < 2:
            return await m.reply_text("⚠️ You need at least 2 players for this game. Try `/play [type] 2 [amount]`.", parse_mode="Markdown")
        if amt <= 0:
            return await m.reply_text("⚠️ Amount must be greater than 0.")
    except Exception:
        return await m.reply_text("⚠️ [players]=int, [amount]=float")

    gid = random.randint(1000, 9999)
    while gid in games:
        gid = random.randint(1000, 9999)
    kp = mk_wallet(gid)
    addr = str(kp.pubkey())
    store_key(gid, kp)

    # per-game settings
    if gtype == "basketball":
        max_tries = 3
    elif gtype == "darts":
        max_tries = 3
    else:  # dice
        max_tries = None

    games[gid] = {
        "creator": uid,
        "type": gtype,
        "num": num,
        "amt": amt,
        "emoji": EMOJI_MAP[gtype],
        "wallet": addr,
        "chat_id": cid,
        "chat_type": chat_type,
        "players": {},
        "tries": {},
        "scores": {},
        "rolls": {},
        "max": max_tries,
        "state": "waiting",
        "order": [],
        "cur": 0,
        "winner": None,
        "accepted_txs": set(),
        "source_wallets": set(),
        "allow_only_origin_chat": True,
        "timeout_job": None,
    }
    user_games[uid] = gid
    game_locks[gid] = asyncio.Lock()

    # METRICS: game created (entries volume is amt * num)
    try:
        metrics_inc(total_entries_sol=Decimal(str(amt)) * Decimal(str(num)), games_created=1)
    except Exception as e:
        logger.warning(f"[metrics] play/create failed: {e}")

    await m.reply_text(
        f"🎮 Game #{gid} [{gtype}]\n"
        f"💰 {amt} SOL each • 👥 {num}\n\n"
        f"Deposit to <code>{addr}</code> then:\n"
        f"/join {gid} &lt;tx&gt;\n\n"
        f"ℹ️ Payouts will go back to your funding wallet.",
        parse_mode="HTML"
    )

def mk_wallet(gid):
    seed = f"mojibet-game-{gid}".encode()
    h = sha256(seed).digest()[:32]
    return Keypair.from_seed(h)

async def join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _touch_user(update)
    m = update.effective_message
    if not m:
        return
    args = context.args
    if len(args) != 2:
        return await m.reply_text("⚠️ Usage: /join [id] [tx]")

    try:
        gid = int(args[0])
        tx = args[1].strip()
    except Exception:
        return await m.reply_text("⚠️ Usage: /join [id] [tx]")

    if gid not in games:
        return await m.reply_text("❌ Game not found.")
    g = games[gid]
    uid = update.effective_user.id
    chat_id = update.effective_chat.id

    origin_type = g.get("chat_type", "private")
    if g.get("allow_only_origin_chat", True):
        if origin_type in ("group", "supergroup"):
            if chat_id != g["chat_id"]:
                return await m.reply_text("🚫 Join in the original group where the game was created.")

    if uid in g["players"]:
        return await m.reply_text("⚠️ You already joined.")

    try:
        sig = Signature.from_string(tx)
    except ValueError:
        return await m.reply_text("❌ Invalid transaction signature format.")

    lock = game_locks.get(gid)
    if lock is None:
        lock = asyncio.Lock()
        game_locks[gid] = lock

    async with lock:
        if tx in used_txs or tx in g["accepted_txs"]:
            return await m.reply_text("🚫 This transaction has already been used. Provide a fresh payment.")

        try:
            res = solana.get_transaction(sig, encoding="jsonParsed")
        except Exception as e:
            logger.warning(f"RPC error fetching tx {tx}: {e}")
            return await m.reply_text("❌ Could not verify the transaction (RPC error). Please try again shortly.")

        if not res or not res.value:
            return await m.reply_text("❌ Transaction not found or not confirmed yet.")

        ok = False
        src = None
        try:
            if hasattr(res.value, "meta") and getattr(res.value.meta, "err", None):
                return await m.reply_text("❌ Transaction failed on-chain (meta error).")

            for ix in res.value.transaction.transaction.message.instructions:
                if hasattr(ix, "parsed"):
                    p, info = ix.parsed, ix.parsed.get("info", {})
                    if (p.get("type") == "transfer"
                        and info.get("destination") == g["wallet"]
                        and int(info.get("lamports", 0)) >= int(g["amt"] * 1e9)):
                        ok, src = True, info.get("source")
                        break
        except Exception as e:
            logger.warning(f"TX parse error for {tx}: {e}")
            return await m.reply_text("❌ Could not parse the transaction. Ensure it's a plain SOL transfer to the game address.")

        if not ok:
            return await m.reply_text("❌ TX does not match this game’s deposit (wrong destination or amount).")

        if src in g["source_wallets"]:
            return await m.reply_text("🚫 That funding wallet has already been used to join this game.")

        g["accepted_txs"].add(tx)
        g["source_wallets"].add(src)
        mark_tx_used(tx)

        g["players"][uid] = {
            "username": update.effective_user.username or f"user{uid}",
            "wallet": src
        }

        if g["type"] == "basketball":
            g["tries"][uid] = g["max"]
            g["scores"][uid] = 0
        elif g["type"] == "darts":
            g["tries"][uid] = g["max"]
            g["rolls"][uid] = None
        else:  # dice
            g["tries"][uid] = None

        user_games[uid] = gid

        # METRICS: unique participant today
        try:
            metrics_mark_player(uid)
        except Exception as e:
            logger.warning(f"[metrics] mark player failed: {e}")

    await safe_send(
        context.bot,
        g["chat_id"],
        f"✅ @{g['players'][uid]['username']} joined Game #{gid} ({len(g['players'])}/{g['num']})"
    )
    await safe_send(context.bot, uid, f"✅ You’ve joined Game #{gid} ({g['type']}).")

    if len(g["players"]) < g["num"]:
        _reschedule_timeout(context, gid)
        return

    # Start the game
    g["state"] = "playing"
    lst = list(g["players"])
    random.shuffle(lst)
    g["order"], g["cur"] = lst, 0
    first = lst[0]

    # METRICS: game started
    try:
        metrics_inc(games_started=1)
    except Exception as e:
        logger.warning(f"[metrics] game start failed: {e}")

    if g["type"] == "basketball":
        start_line = (
            f"🔥 Game #{gid} (🏀 best-of-3). "
            f"@{g['players'][first]['username']} — tap the `🏀` to shoot."
        )
    elif g["type"] == "dice":
        start_line = (
            f"🔥 Game #{gid} (🎲 first to roll a 6 wins). "
            f"@{g['players'][first]['username']} — tap the `🎲` to roll."
        )
    else:
        start_line = (
            f"🔥 Game #{gid} (🎯 three throws each; hit a bullseye = instant win; "
            f"no bullseyes after 3 throws each = draw). "
            f"@{g['players'][first]['username']} — tap the `🎯` to throw (3 tries to hit a bullseye)."
        )

    await safe_send(context.bot, g["chat_id"], start_line, parse_mode="Markdown")
    await safe_send(context.bot, first, start_line.replace(f"@{g['players'][first]['username']}", "It's your turn!"), parse_mode="Markdown")

    _reschedule_timeout(context, gid)

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _touch_user(update)
    m = update.effective_message
    if not m:
        return
    uid = update.effective_user.id
    gid = user_games.get(uid)
    if not gid or gid not in games:
        return await m.reply_text("⚠️ You have no active game.")

    g = games[gid]
    if g["creator"] != uid:
        return await m.reply_text("🚫 Only the creator can cancel the game.")

    # If funded/started, treat as draw/refund
    if g["state"] in ("waiting", "playing") and len(g["players"]) > 0:
        await _finish_with_draw_and_refund(context, gid, "cancelled by creator")
        return

    # Otherwise just remove (no participants)
    user_games.pop(uid, None)
    if g.get("timeout_job"):
        try:
            g["timeout_job"].schedule_removal()
        except Exception:
            pass
    games.pop(gid, None)
    game_locks.pop(gid, None)

    # METRICS: cancelled game with no players
    try:
        metrics_inc(games_cancelled=1)
    except Exception as e:
        logger.warning(f"[metrics] cancel metrics failed: {e}")

    await m.reply_text(f"🗑 Cancelled Game #{gid}.")

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _touch_user(update)
    m = update.effective_message
    if not m:
        return
    if not games:
        return await m.reply_text("📭 No active games.")
    msg = "🎮 Active games:\n"
    for gid, g in games.items():
        msg += f"- #{gid}: {g['type']} ({len(g['players'])}/{g['num']})\n"
    await m.reply_text(msg)

# === GAME PLAY ===
def _advance_to_next_playable(g):
    n = len(g["order"])
    for _ in range(n):
        g["cur"] = (g["cur"] + 1) % n
        nxt = g["order"][g["cur"]]
        if g["tries"][nxt] is None or g["tries"][nxt] > 0:
            return True
    return False

async def _finish_with_winner(context, gid, winner_uid):
    g = games.get(gid)
    if not g:
        return

    try:
        await _sweep_game_wallet_to_house_async(gid)
    except Exception as e:
        logger.error(f"[sweep] Game #{gid} sweep failed: {e}")

    prize = g["amt"] * g["num"]
    wall = g["players"][winner_uid]["wallet"]
    uname = g["players"][winner_uid]["username"]

    try:
        win_sig, fee_sig, net_amount, fee = await _safe_distribute(wall, prize)
        prize_line = f"Prize paid: {net_amount} SOL → {wall}\n{solscan(win_sig)}"

        # METRICS: completed game (win)
        try:
            metrics_inc(
                games_completed=1,
                total_prize_gross_sol=Decimal(str(prize)),
                total_payouts_net_sol=Decimal(str(net_amount)),
                total_fees_sol=Decimal(str(fee))
            )
        except Exception as me:
            logger.warning(f"[metrics] winner finalize failed: {me}")

    except Exception as e:
        logger.error(f"Payout failed for Game #{gid}: {e}")
        prize_line = f"⚠️ Payout could not be sent automatically. Support will contact you.\nReason: {e}"

    try:
        for pu in g["players"].keys():
            if pu == winner_uid:
                db.update_stats(pu, delta_win=1, delta_profit_sol=Decimal(str(prize - g["amt"])))
            else:
                db.update_stats(pu, delta_loss=1, delta_profit_sol=Decimal(str(-g["amt"])))
    except Exception as e:
        logger.warning(f"Stats update failed: {e}")

    await safe_send(
        context.bot, g["chat_id"],
        f"🏆 @{uname} wins Game #{gid}!\n{prize_line}\n\n"
        f"➕ Want a rematch? Start a new one with /play"
    )

    for pu, pdata in g["players"].items():
        if pu == winner_uid:
            await safe_send(context.bot, pu, f"✅ You WON Game #{gid}!\n{prize_line}")
        else:
            await safe_send(context.bot, pu, f"❌ You lost Game #{gid}.\nWinner: @{uname}\n{prize_line}")

    g["state"] = "ended"
    if g.get("timeout_job"):
        try:
            g["timeout_job"].schedule_removal()
        except Exception:
            pass
    _clear_user_games(gid)
    games.pop(gid, None)
    game_locks.pop(gid, None)

async def _finish_with_draw_and_refund(context, gid, reason: str):
    g = games.get(gid)
    if not g:
        return

    try:
        await _sweep_game_wallet_to_house_async(gid)
    except Exception as e:
        logger.error(f"[sweep] Game #{gid} sweep failed: {e}")

    lines = [f"🤝 Game #{gid} ended in a draw ({reason}).", "All entries refunded:"]

    # for metrics:
    total_net_refunds = Decimal("0")
    total_fees = Decimal("0")

    for pu, pdata in g["players"].items():
        ref = g["amt"]
        try:
            win_sig, fee_sig, net_amount, fee = await _safe_distribute(pdata["wallet"], ref)
            lines.append(f"@{pdata['username']} → {net_amount} SOL ({pdata['wallet']})")

            # metrics accumulation
            total_net_refunds += Decimal(str(net_amount))
            total_fees += Decimal(str(fee))

            await safe_send(context.bot, pu, f"🔄 Game #{gid} draw/refund sent to your wallet.\n{solscan(win_sig)}")
            db.update_stats(pu, delta_draw=1, delta_profit_sol=Decimal("0"))
        except Exception as e:
            logger.error(f"Refund payout failed for Game #{gid}, user {pu}: {e}")
            lines.append(f"@{pdata['username']} → refund pending (payout error).")

    # METRICS: drawn game
    try:
        metrics_inc(
            games_drawn=1,
            total_refunds_net_sol=total_net_refunds,
            total_fees_sol=total_fees
        )
    except Exception as me:
        logger.warning(f"[metrics] draw finalize failed: {me}")

    lines.append("\n➕ Start a new one with /play")
    await safe_send(context.bot, g["chat_id"], "\n".join(lines))

    g["state"] = "ended"
    if g.get("timeout_job"):
        try:
            g["timeout_job"].schedule_removal()
        except Exception:
            pass
    _clear_user_games(gid)
    games.pop(gid, None)
    game_locks.pop(gid, None)

async def _prompt_next_turn(context, g, gid, text_for_group: str, player_uid: int, emoji: str, action_word: str):
    """Monospaced emoji for all types to ensure tappable surface."""
    mono = f"`{emoji}`"
    tg = text_for_group.replace(f" {emoji} ", f" {mono} ").replace(f" {emoji}", f" {mono}")
    await safe_send(context.bot, g["chat_id"], tg, parse_mode="Markdown")
    await safe_send(context.bot, player_uid, f"Your turn in Game #{gid}! Tap the {mono} to {action_word}.", parse_mode="Markdown")

async def on_roll(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _touch_user(update)
    msg = update.effective_message
    if not msg:
        return

    val = None
    emoji = None

    if msg.dice:
        val = msg.dice.value
        emoji = msg.dice.emoji
    else:
        txt = (msg.text or "").strip()
        if txt in EMOJI_MAP.values():
            emoji = txt
            val = random.randint(1, 6)
        else:
            return

    uid = update.effective_user.id

    # find the game where it's this user's turn and emoji matches
    g = None
    gid_match = None
    for gid, gg in games.items():
        if gg["state"] == "playing" and gg["order"][gg["cur"]] == uid and gg["emoji"] == emoji:
            g = gg
            gid_match = gid
            break
    if not g:
        return

    gtype = g["type"]

    if gtype == "basketball":
        scored = (val in (4, 5))  # Telegram 🏀 hoop when 4 or 5
        if scored:
            g["scores"][uid] += 1
        g["tries"][uid] -= 1

        if g["scores"][uid] >= 2:
            await _finish_with_winner(context, gid_match, uid)
            return

        any_left = any((t is None) or (t > 0) for t in g["tries"].values())
        if not any_left:
            ms = max(g["scores"].values(), default=0)
            winners = [u for u, s in g["scores"].items() if s == ms]
            if ms == 0 or len(winners) != 1:
                await _finish_with_draw_and_refund(context, gid_match, "no decisive score")
                return
            await _finish_with_winner(context, gid_match, winners[0])
            return

        _advance_to_next_playable(g)
        nxt = g["order"][g["cur"]]
        await _prompt_next_turn(
            context, g, gid_match,
            f"@{g['players'][nxt]['username']} — tap the `🏀` to shoot.",
            nxt, g['emoji'], "shoot"
        )
        _reschedule_timeout(context, gid_match)
        return

    elif gtype == "dice":
        if val == 6:
            await _finish_with_winner(context, gid_match, uid)
            return
        g["cur"] = (g["cur"] + 1) % len(g["order"])
        nxt = g["order"][g["cur"]]
        await _prompt_next_turn(
            context, g, gid_match,
            f"@{g['players'][nxt]['username']} — tap the `🎲` to roll.",
            nxt, g['emoji'], "roll"
        )
        _reschedule_timeout(context, gid_match)
        return

    else:  # darts
        if val == 6:
            await _finish_with_winner(context, gid_match, uid)
            return

        g["tries"][uid] -= 1

        any_left = any(t > 0 for t in g["tries"].values())
        if not any_left:
            await _finish_with_draw_and_refund(context, gid_match, "no bullseyes in 3 throws each")
            return

        _advance_to_next_playable(g)
        nxt = g["order"][g["cur"]]
        await _prompt_next_turn(
            context, g, gid_match,
            f"@{g['players'][nxt]['username']} — tap the `🎯` to throw (3 tries to hit a bullseye).",
            nxt, g['emoji'], "throw"
        )
        _reschedule_timeout(context, gid_match)
        return

# === Callback query handler (legacy UI; not used) ===
async def cq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _touch_user(update)
    q = update.callback_query
    await q.answer()
    await q.edit_message_text("ℹ️ Buttons are not used in this version. Payouts are automatic.")

# === Passive logger (captures any interaction) ===
async def _any_update_logger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user:
        await _touch_user(update)
    # no reply; keep it transparent

# === Commands list ===
async def _set_my_commands(app):
    cmds = [
        ("start", "Start"),
        ("help", "How to play"),
        ("play", "Create a game"),
        ("join", "Join a game"),
        ("status", "List active games"),
        ("cancel", "Cancel your game"),
        ("leaderboard", "Top players"),
        ("metrics", "Admin metrics (owner only)"),
        ("support", "DM support"),
    ]
    await app.bot.set_my_commands([BotCommand(k, v) for k, v in cmds])

# === BOOT ===
def main():
    # Init DB and cache
    init_db()

    async def _post_init(app_):
        await _set_my_commands(app_)

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(_post_init)
        .build()
    )

    if getattr(app, "job_queue", None) is None:
        logger.warning("JobQueue is None; timeouts/cancel-on-inactivity will not run.")

    # Passive logger (early group to not steal updates)
    app.add_handler(MessageHandler(filters.ALL, _any_update_logger), group=-1)

    # Basic commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("support", support))

    # Game commands
    app.add_handler(CommandHandler("play", play))
    app.add_handler(CommandHandler("join", join))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("status", status))

    # Leaderboard/Admin
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("users", users_cmd))
    app.add_handler(CommandHandler("metrics", metrics_cmd))

    # Game interactions
    app.add_handler(MessageHandler(filters.Dice() | filters.Regex(r'^[🏀🎯🎲]$'), on_roll))
    app.add_handler(CallbackQueryHandler(cq))

    logger.info('🚀 MojiBet bot started (Postgres + Metrics)')
    app.run_polling()

if __name__ == "__main__":
    main()
