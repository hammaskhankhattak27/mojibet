import os  
import random
import json
import logging
import time
from decimal import Decimal
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
from pathlib import Path

from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.transaction import Transaction
from solders.instruction import Instruction, AccountMeta
from solders.message import Message

from solana.rpc.api import Client
from solana.rpc.types import TxOpts

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
SOLANA_RPC = os.getenv("SOLANA_RPC", "https://api.mainnet-beta.solana.com")

BOT_PRIVATE_KEY = os.getenv("BOT_PRIVATE_KEY")
HOUSE_WALLET = os.getenv("HOUSE_WALLET")
HOUSE_PRIVATE_KEY = os.getenv("HOUSE_PRIVATE_KEY")

SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "YourSupportHandle")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN")
if not BOT_PRIVATE_KEY:
    raise RuntimeError("Missing BOT_PRIVATE_KEY (base58-encoded keypair bytes)")
if not HOUSE_WALLET:
    raise RuntimeError("Missing HOUSE_WALLET (Solana address)")

try:
    _tmp_signer = Keypair.from_bytes(b58decode(BOT_PRIVATE_KEY))
except Exception:
    raise RuntimeError("BOT_PRIVATE_KEY must be base58 of the raw secret key bytes (64 bytes).")
try:
    _ = Pubkey.from_string(HOUSE_WALLET)
except Exception:
    raise RuntimeError("HOUSE_WALLET is not a valid Solana public key.")
if HOUSE_PRIVATE_KEY:
    try:
        _ = Keypair.from_bytes(b58decode(HOUSE_PRIVATE_KEY))
    except Exception:
        raise RuntimeError("HOUSE_PRIVATE_KEY must be base58 of the raw secret key bytes.")

HOUSE_FEE_RATE = Decimal("0.05")
INACTIVITY_TIMEOUT_SECS = 5 * 60
FEE_BUFFER_LAMPORTS = 200_000

GAMES_FILE = "game_keys.json"
USED_TX_FILE = "used_txs.json"
FAILED_PAYOUTS_FILE = "failed_payouts.json"

games = {}
user_games = {}
used_txs = set()
game_locks = {}
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
    return house_signer or signer

# === RETRY WRAPPER ===
def rpc_call_with_retry(fn, *args, retries=5, **kwargs):
    """Retry Solana RPC calls with exponential backoff on 429/connection errors."""
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            msg = str(e)
            if ("429" in msg or "Too Many Requests" in msg or "HTTP error" in msg) and i < retries - 1:
                wait = 2 ** i + random.random()
                logger.warning(f"[rpc] Retry {i+1}/{retries} after error: {msg} (waiting {wait:.1f}s)")
                time.sleep(wait)
                continue
            raise

# === DATABASE ===
DB_FILE = Path("db.json")
_db_lock = threading.Lock()

def _load_db() -> dict:
    if not DB_FILE.exists():
        return {"users": {}}
    try:
        with DB_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"DB load error: {e}")
        return {"users": {}}

def _save_db(data: dict):
    try:
        with DB_FILE.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"DB save error: {e}")

def _ensure_user(data: dict, user_id: int) -> dict:
    users = data.setdefault("users", {})
    u = users.get(str(user_id))
    if not u:
        u = {
            "stats": {"wins": 0, "losses": 0, "draws": 0, "net_profit_sol": "0", "last_played_ts": 0},
            "profile": {},
        }
        users[str(user_id)] = u
    u.setdefault("stats", {"wins": 0, "losses": 0, "draws": 0, "net_profit_sol": "0", "last_played_ts": 0})
    u.setdefault("profile", {})
    return u

class DB:
    def get_user(self, user_id: int) -> dict:
        with _db_lock:
            data = _load_db()
            u = _ensure_user(data, user_id)
            return u

    def update_stats(self, user_id: int, *, delta_win=0, delta_loss=0, delta_draw=0, delta_profit_sol=Decimal("0")):
        with _db_lock:
            data = _load_db()
            u = _ensure_user(data, user_id)
            s = u["stats"]
            s["wins"] = int(s.get("wins", 0)) + delta_win
            s["losses"] = int(s.get("losses", 0)) + delta_loss
            s["draws"] = int(s.get("draws", 0)) + delta_draw
            cur = Decimal(s.get("net_profit_sol", "0"))
            s["net_profit_sol"] = str((cur + delta_profit_sol).normalize())
            s["last_played_ts"] = int(time.time())
            _save_db(data)

    def snapshot_username(self, user_id: int, username: Optional[str], first: Optional[str], last: Optional[str]):
        with _db_lock:
            data = _load_db()
            u = _ensure_user(data, user_id)
            u["profile"] = {"username": username, "first": first, "last": last}
            _save_db(data)

db = DB()

# === UTILS ===
def _load_used_txs():
    try:
        return set(json.load(open(USED_TX_FILE)))
    except Exception:
        return set()

def _persist_used_txs():
    try:
        json.dump(sorted(list(used_txs)), open(USED_TX_FILE, "w"), indent=2)
    except Exception as e:
        logger.warning(f"Failed to persist used txs: {e}")

def _store_failed_payout(game_id: int, recipient: str, amount: float, reason: str):
    try:
        try:
            data = json.load(open(FAILED_PAYOUTS_FILE))
        except Exception:
            data = []
        data.append({
            "game_id": game_id,
            "recipient": recipient,
            "amount": amount,
            "reason": reason,
            "ts": int(time.time())
        })
        json.dump(data, open(FAILED_PAYOUTS_FILE, "w"), indent=2)
    except Exception as e:
        logger.error(f"Failed to persist failed payout: {e}")

def store_key(gid, kp):
    priv = b58encode(bytes(kp)).decode()
    try:
        data = json.load(open(GAMES_FILE))
    except Exception:
        data = {}
    data[str(gid)] = priv
    with open(GAMES_FILE, "w") as f:
        json.dump(data, f, indent=2)
    return priv

def mk_wallet(gid: int) -> Keypair:
    seed = os.urandom(32)
    return Keypair.from_seed(seed)

def solscan(sig: str) -> str:
    return f"https://solscan.io/tx/{sig}"

def _lamports_from_sol(amt: float | Decimal) -> int:
    return int(Decimal(str(amt)) * Decimal(1_000_000_000))

def _get_signer_balance_lamports(kp: Optional[Keypair] = None) -> int:
    kp = kp or _get_payout_signer()
    try:
        resp = rpc_call_with_retry(solana.get_balance, kp.pubkey())
        return int(resp.value)
    except Exception:
        return 0

# === PAYOUTS ===
def payout(to_addr, amt_sol, *, from_signer: Optional[Keypair] = None) -> str:
    kp = from_signer or _get_payout_signer()
    lam = _lamports_from_sol(amt_sol)
    fpk = kp.pubkey()
    tpk = Pubkey.from_string(to_addr)

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

    blk = rpc_call_with_retry(solana.get_latest_blockhash, commitment="finalized").value.blockhash
    txn.sign([kp], blk)

    raw = bytes(txn)
    res = rpc_call_with_retry(
        solana.send_raw_transaction,
        raw,
        opts=TxOpts(skip_preflight=False, preflight_commitment="confirmed", max_retries=15),
    )
    return str(res.value)

async def _safe_distribute(recipient_wallet: str, amount: float):
    async with _payout_lock:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: distribute_winnings(recipient_wallet, amount))

def distribute_winnings(recipient_wallet: str, amount: float):
    fee = (Decimal(str(amount)) * HOUSE_FEE_RATE).quantize(Decimal("0.00000001"))
    net = (Decimal(str(amount)) - fee).quantize(Decimal("0.00000001"))

    kp = _get_payout_signer()
    paying_from_house = (str(kp.pubkey()) == HOUSE_WALLET)

    needed = _lamports_from_sol(net) + (0 if paying_from_house else _lamports_from_sol(fee)) + FEE_BUFFER_LAMPORTS
    bal = _get_signer_balance_lamports(kp)
    if bal < needed:
        raise RuntimeError(f"Insufficient payout balance (have {bal} lamports, need {needed}).")

    if paying_from_house:
        fee_sig = "retained"
    else:
        fee_sig = payout(HOUSE_WALLET, float(fee), from_signer=kp)

    win_sig = payout(recipient_wallet, float(net), from_signer=kp)

    logger.info(f"[payout] gross={amount} net={net} fee={fee} fee_sig={fee_sig} to={recipient_wallet} win_sig={win_sig}")

    logger.info(f"[+] Paid {net} to winner/refund: {recipient_wallet} ({win_sig})")
    if paying_from_house:
        logger.info(f"[+] Fee {fee} retained in house wallet: {HOUSE_WALLET}")
    else:
        logger.info(f"[+] Fee {fee} sent to house wallet: {HOUSE_WALLET} ({fee_sig})")

    return win_sig, fee_sig, float(net), float(fee)

# === GAME WALLET SWEEP (DYNAMIC RENT FIX) ===
def _estimate_fee_for_message(msg: Message) -> int:
    try:
        res = rpc_call_with_retry(solana.get_fee_for_message, msg)
        fee = getattr(res, "value", None)
        if isinstance(fee, int):
            return max(fee, 5_000)
        return 10_000
    except Exception:
        return 10_000

def _is_plain_system_account(pubkey: Pubkey) -> bool:
    try:
        ai = rpc_call_with_retry(solana.get_account_info, pubkey).value
        if not ai:
            return True
        owner = str(ai.owner)
        return owner == "11111111111111111111111111111111"
    except Exception:
        return True

def _load_game_keypair(gid: int) -> Keypair:
    try:
        with open(GAMES_FILE, "r") as f:
            data = json.load(f)
        priv58 = data[str(gid)]
        return Keypair.from_bytes(b58decode(priv58))
    except Exception as e:
        raise RuntimeError(f"Missing or unreadable key for game {gid}: {e}")

def _get_rent_exempt_minimum() -> int:
    try:
        return rpc_call_with_retry(solana.get_minimum_balance_for_rent_exemption, 0).value
    except Exception:
        return 2_500_000  # fallback

def sweep_game_wallet_to_house(gid: int) -> Optional[str]:
    kp = _load_game_keypair(gid)
    src_pk = kp.pubkey()
    dst_pk = Pubkey.from_string(HOUSE_WALLET)

    if not _is_plain_system_account(src_pk):
        logger.warning(f"[sweep] Game #{gid} source is not a plain System account; skipping sweep.")
        return None

    try:
        bal = int(rpc_call_with_retry(solana.get_balance, src_pk).value)
    except Exception as e:
        logger.error(f"[sweep] Game #{gid} failed to get balance: {e}")
        return None

    if bal <= 0:
        return None

    placeholder_lamports = 1
    data = (2).to_bytes(4, "little") + placeholder_lamports.to_bytes(8, "little")
    draft_ix = Instruction(
        program_id=Pubkey.from_string("11111111111111111111111111111111"),
        accounts=[
            AccountMeta(src_pk, is_signer=True, is_writable=True),
            AccountMeta(dst_pk, is_signer=False, is_writable=True),
        ],
        data=data,
    )
    draft_msg = Message([draft_ix], payer=src_pk)

    fee_est = _estimate_fee_for_message(draft_msg)
    rent_min = _get_rent_exempt_minimum()

    keep_min = max(rent_min, fee_est + 20_000)
    if bal <= keep_min:
        return None

    lamports_to_send = bal - keep_min
    if lamports_to_send < 20_000:
        return None

    real_data = (2).to_bytes(4, "little") + lamports_to_send.to_bytes(8, "little")
    ix = Instruction(
        program_id=Pubkey.from_string("11111111111111111111111111111111"),
        accounts=[
            AccountMeta(src_pk, is_signer=True, is_writable=True),
            AccountMeta(dst_pk, is_signer=False, is_writable=True),
        ],
        data=real_data,
    )

    msg = Message([ix], payer=src_pk)
    txn = Transaction.new_unsigned(msg)

    try:
        blk = rpc_call_with_retry(solana.get_latest_blockhash, commitment="confirmed").value.blockhash
        txn.sign([kp], blk)
        raw = bytes(txn)
        res = rpc_call_with_retry(
            solana.send_raw_transaction,
            raw, 
            opts=TxOpts(skip_preflight=False, preflight_commitment="confirmed", max_retries=15)
        )
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
    """Schedule (or reschedule) the inactivity timeout via Application JobQueue."""
    g = games.get(gid)
    if not g:
        return

    jq = getattr(context.application, "job_queue", None)
    if jq is None:
        logger.warning("JobQueue not available; timeouts disabled for this run.")
        return

    job = g.get("timeout_job")
    if job:
        try:
            job.schedule_removal()
        except Exception:
            pass

    g["timeout_job"] = jq.run_once(
        _timeout_cb,
        when=seconds,
        data={"gid": gid},
        name=f"timeout-{gid}",
    )

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
    with _db_lock:
        data = _load_db()
        users = data.get("users", {})
        rows = []
        for uid, rec in users.items():
            s = rec.get("stats", {})
            played = int(s.get("wins", 0)) + int(s.get("losses", 0)) + int(s.get("draws", 0))
            if played == 0:
                continue
            p = rec.get("profile", {})
            uname = p.get("username") or f"user{uid}"
            try:
                net = Decimal(s.get("net_profit_sol", "0"))
            except Exception:
                net = Decimal("0")
            rows.append({
                "uid": int(uid),
                "username": uname,
                "wins": int(s.get("wins", 0)),
                "losses": int(s.get("losses", 0)),
                "draws": int(s.get("draws", 0)),
                "net": net,
            })
    if not rows:
        return await m.reply_text("No stats yet. Play a game with /play!")

    rows.sort(key=lambda r: (r["net"], r["wins"]), reverse=True)
    top = rows[:10]
    lines = ["🏆 Leaderboard (by net profit in SOL):"]
    for i, r in enumerate(top, 1):
        lines.append(f"{i}. @{r['username']} — net {r['net']} | W:{r['wins']} L:{r['losses']} D:{r['draws']}")
    await m.reply_text("\n".join(lines))

async def users_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _touch_user(update)
    if not _is_owner(update.effective_user.id):
        return await update.effective_message.reply_text("🚫")
    with _db_lock:
        data = _load_db()
        users = data.get("users", {})
        lines = [f"Total users: {len(users)}"]
        for uid, rec in list(users.items())[:50]:
            p = rec.get("profile", {})
            s = rec.get("stats", {})
            uname = p.get("username") or f"user{uid}"
            lines.append(f"• {uid} @{uname} W:{s.get('wins',0)} L:{s.get('losses',0)} D:{s.get('draws',0)} net:{s.get('net_profit_sol','0')}")
    await update.effective_message.reply_text("\n".join(lines))

async def balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner-only: show payout signer and current SOL balance."""
    await _touch_user(update)
    if not _is_owner(update.effective_user.id):
        return await update.effective_message.reply_text("🚫")
    kp = _get_payout_signer()
    lam = _get_signer_balance_lamports(kp)
    await update.effective_message.reply_text(
        f"Payout signer: {str(kp.pubkey())}\n"
        f"Balance: {lam} lamports (~{lam/1_000_000_000:.6f} SOL)\n"
        f"HOUSE_WALLET: {HOUSE_WALLET}"
    )

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

    # REMOVED: payout signer pre-check so games can start regardless of signer balance

    gid = random.randint(1000, 9999)
    while gid in games:
        gid = random.randint(1000, 9999)
    kp = mk_wallet(gid)
    addr = str(kp.pubkey())
    store_key(gid, kp)

    if gtype == "basketball":
        max_tries = 3
    elif gtype == "darts":
        max_tries = 3
    else:
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

    await m.reply_text(
        f"🎮 Game #{gid} [{gtype}]\n"
        f"💰 {amt} SOL each • 👥 {num}\n\n"
        f"Deposit to <code>{addr}</code> then:\n"
        f"/join {gid} &lt;tx&gt;\n\n"
        f"ℹ️ Payouts will go back to your funding wallet.",
        parse_mode="HTML"
    )

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

    # Prevent a user from being in multiple concurrent games
    if uid in user_games and user_games[uid] != gid:
        return await m.reply_text("⚠️ Finish your other active game with /cancel first.")

    # Enforce: must join in the same chat ONLY for group-origin games.
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
        # Reject recycled signatures
        if tx in used_txs or tx in g["accepted_txs"]:
            return await m.reply_text("🚫 This transaction has already been used. Provide a fresh payment.")

        # Guard: don't allow joining if already full or started
        if g["state"] != "waiting":
            return await m.reply_text("❌ Game already started.")
        if len(g["players"]) >= g["num"]:
            return await m.reply_text("❌ Game is already full.")

        # Fetch tx
        try:
            res = solana.get_transaction(sig, encoding="jsonParsed", commitment="confirmed")
        except Exception as e:
            logger.warning(f"RPC error fetching tx {tx}: {e}")
            return await m.reply_text("❌ Could not verify the transaction (RPC error). Please try again shortly.")

        if not res or not res.value:
            return await m.reply_text("❌ Transaction not found or not confirmed yet.")

        # Must be a system transfer → escrow
        ok = False
        src = None
        try:
            if hasattr(res.value, "meta") and getattr(res.value.meta, "err", None):
                return await m.reply_text("❌ Transaction failed on-chain (meta error).")

            # Walk parsed instructions and find a transfer to the game wallet with sufficient lamports
            min_lamports = _lamports_from_sol(g["amt"])
            for ix in res.value.transaction.transaction.message.instructions:
                if hasattr(ix, "parsed"):
                    p, info = ix.parsed, ix.parsed.get("info", {})
                    if (
                        p.get("type") == "transfer"
                        and info.get("destination") == g["wallet"]
                        and int(info.get("lamports", 0)) >= min_lamports
                    ):
                        ok, src = True, info.get("source")
                        break
        except Exception as e:
            logger.warning(f"TX parse error for {tx}: {e}")
            return await m.reply_text("❌ Could not parse the transaction. Ensure it's a plain SOL transfer to the game address.")

        if not ok:
            return await m.reply_text("❌ TX does not match this game’s deposit (wrong destination or amount).")

        # Prevent same funding wallet twice in this game
        if src in g["source_wallets"]:
            return await m.reply_text("🚫 That funding wallet has already been used to join this game.")

        # Mark as used (atomic under the lock)
        g["accepted_txs"].add(tx)
        g["source_wallets"].add(src)
        used_txs.add(tx)
        _persist_used_txs()

        # Last guard before registering: capacity check (avoid overfill under concurrent joins)
        if len(g["players"]) >= g["num"]:
            return await m.reply_text("❌ Game just filled up.")

        # Register player
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
        else:
            g["tries"][uid] = None

        user_games[uid] = gid

    # Notify
    await safe_send(
        context.bot,
        g["chat_id"],
        f"✅ @{g['players'][uid]['username']} joined Game #{gid} ({len(g['players'])}/{g['num']})"
    )
    await safe_send(context.bot, uid, f"✅ You’ve joined Game #{gid} ({g['type']}).")

    # If game not full yet, just reschedule timeout (activity)
    if len(g["players"]) < g["num"]:
        _reschedule_timeout(context, gid)
        return

    # Start the game
    g["state"] = "playing"
    lst = list(g["players"])
    random.shuffle(lst)
    g["order"], g["cur"] = lst, 0
    first = lst[0]

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

    user_games.pop(uid, None)
    if g.get("timeout_job"):
        try:
            g["timeout_job"].schedule_removal()
        except Exception:
            pass
    games.pop(gid, None)
    game_locks.pop(gid, None)
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
    start = g["cur"]
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
    except Exception as e:
        logger.error(f"Payout failed for Game #{gid}: {e}")
        _store_failed_payout(gid, wall, prize, str(e))
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
            await safe_send(
                context.bot, pu,
                f"✅ You WON Game #{gid}!\n{prize_line}"
            )
        else:
            await safe_send(
                context.bot, pu,
                f"❌ You lost Game #{gid}.\nWinner: @{uname}\n{prize_line}"
            )

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

    for pu, pdata in g["players"].items():
        ref = g["amt"]
        try:
            win_sig, fee_sig, net_amount, fee = await _safe_distribute(pdata["wallet"], ref)
            lines.append(f"@{pdata['username']} → {net_amount} SOL ({pdata['wallet']})")
            await safe_send(
                context.bot, pu,
                f"🔄 Game #{gid} draw/refund sent to your wallet.\n{solscan(win_sig)}"
            )
            db.update_stats(pu, delta_draw=1, delta_profit_sol=Decimal("0"))
        except Exception as e:
            logger.error(f"Refund payout failed for Game #{gid}, user {pu}: {e}")
            _store_failed_payout(gid, pdata["wallet"], ref, str(e))
            lines.append(f"@{pdata['username']} → refund pending (payout error).")

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
            val = random.randint(1, 6)  # fallback
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
        scored = (val in (4, 5))
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
    # no reply

# === Commands list (wallet commands removed) ===
async def _set_my_commands(app):
    cmds = [
        ("start", "Start"),
        ("help", "How to play"),
        ("play", "Create a game"),
        ("join", "Join a game"),
        ("status", "List active games"),
        ("cancel", "Cancel your game"),
        ("leaderboard", "Top players"),
        ("support", "DM support"),
        ("balance", "Admin: payout balance"),
    ]
    await app.bot.set_my_commands([BotCommand(k, v) for k, v in cmds])

# === FAILED PAYOUTS RETRY (runs every hour) ===
async def retry_failed_payouts(context: ContextTypes.DEFAULT_TYPE):
    try:
        data = json.load(open(FAILED_PAYOUTS_FILE))
    except Exception:
        return
    if not data:
        return
    logger.info(f"[retry] Attempting {len(data)} failed payouts...")
    still_failed = []
    for rec in data:
        recipient = rec.get("recipient")
        amount = rec.get("amount")
        try:
            sig = payout(recipient, amount)
            logger.info(f"[retry] ✅ Retried payout {amount} SOL → {recipient} ({sig})")
        except Exception as e:
            logger.error(f"[retry] ❌ Retry failed for {recipient}: {e}")
            rec["reason"] = str(e)
            rec["ts"] = int(time.time())
            still_failed.append(rec)
    json.dump(still_failed, open(FAILED_PAYOUTS_FILE, "w"), indent=2)

# === BOOT ===
def main():
    global used_txs
    used_txs = _load_used_txs()

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
    else:
        # Auto-retry failed payouts every hour (starts after 60s)
        app.job_queue.run_repeating(retry_failed_payouts, interval=3600, first=60)
        logger.info("⏳ Failed payout retry scheduled every 1h")

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
    app.add_handler(CommandHandler("balance", balance))

    # Game interactions
    app.add_handler(MessageHandler(filters.Dice() | filters.Regex(r'^[🏀🎯🎲]$'), on_roll))
    app.add_handler(CallbackQueryHandler(cq))

    logger.info('🚀 MojiBet bot started (MAINNET)')
    app.run_polling()

if __name__ == "__main__":
    main()
