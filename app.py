import os
import io
import json
import logging
import asyncio
import functools
import sys
import argparse
from datetime import datetime, date, timedelta
import requests

# Import chart functions
from charts import (
    generate_sales_chart,
    generate_products_chart,
    generate_ingredients_chart,
    generate_stats_chart
)

# Check if running in CLI mode before importing telegram
CLI_MODE = '--cli' in sys.argv

if not CLI_MODE:
    from telegram import Update, Bot, InputFile
    from telegram.ext import Application, CommandHandler, ContextTypes
    from telegram.constants import ParseMode
    from telegram.error import Conflict, TimedOut, NetworkError, RetryAfter
    from telegram.request import HTTPXRequest
    import pytz
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
else:
    # Mock classes for CLI mode
    class Update:
        pass
    class Bot:
        def __init__(self, token=None):
            self.token = token
    InputFile = None
    Application = None
    CommandHandler = None
    class ContextTypes:
        DEFAULT_TYPE = None
    class ParseMode:
        HTML = 'HTML'
    Conflict = Exception
    TimedOut = Exception
    NetworkError = Exception
    RetryAfter = Exception
    HTTPXRequest = None
    import pytz
    AsyncIOScheduler = None
    CronTrigger = None
    plt = None

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)

# Silence noisy third-party loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
POSTER_API_URL = "https://joinposter.com/api"

# Import config module
import config
from config import (
    CONFIG_FILE, load_config, save_config, mask_api_key,
    set_api_key, delete_api_key, get_config_data,
    subscribed_chats, theft_alert_chats, admin_chat_ids,
    approved_users, pending_requests, last_alerted_transaction_id, last_alerted_expense_id,
    notified_transaction_ids, notified_transaction_date, last_seen_void_id, last_cash_balance,
    check_agent_rate_limit, record_agent_usage, get_agent_usage,
    get_agent_limits, set_agent_limit,
    agent_conversations, AGENT_HISTORY_LIMIT,
    set_log_level
)

# Import agent module (optional dependency)
try:
    from agent import run_agent
    AGENT_AVAILABLE = True
except ImportError:
    AGENT_AVAILABLE = False

# Thailand timezone
THAI_TZ = pytz.timezone('Asia/Bangkok')

# Global scheduler
scheduler = None

# Async lock to prevent concurrent API operations causing race conditions
api_lock = asyncio.Lock()

# Request configuration
REQUEST_TIMEOUT = 30  # seconds
REQUEST_READ_TIMEOUT = 30
REQUEST_WRITE_TIMEOUT = 30
REQUEST_CONNECT_TIMEOUT = 15
REQUEST_POOL_TIMEOUT = 10
MAX_RETRIES = 3
RETRY_DELAY = 1  # Base delay in seconds for exponential backoff



def require_auth(func):
    """Decorator to require user authentication."""
    @functools.wraps(func)
    async def wrapper(update, context):
        chat_id = str(update.effective_chat.id)
        if not admin_chat_ids:
            await update.message.reply_text("No admin configured. Send /setup to become admin.")
            return
        if chat_id not in admin_chat_ids and chat_id not in approved_users:
            if chat_id in pending_requests:
                await update.message.reply_text("Your request is pending approval.")
            else:
                await update.message.reply_text("Access required. Send /request to request access.")
            return
        user = update.effective_user
        username = f"@{user.username}" if user and user.username else f"id:{chat_id}"
        text = update.message.text if update.message and update.message.text else func.__name__
        logger.info(f"{username}: {text}")
        return await func(update, context)
    return wrapper


def require_admin(func):
    """Decorator to require admin privileges."""
    @functools.wraps(func)
    async def wrapper(update, context):
        chat_id = str(update.effective_chat.id)
        if not admin_chat_ids:
            await update.message.reply_text("No admin configured. Send /setup to become admin.")
            return
        if chat_id not in admin_chat_ids:
            await update.message.reply_text("Admin privileges required.")
            return
        user = update.effective_user
        username = f"@{user.username}" if user and user.username else f"id:{chat_id}"
        text = update.message.text if update.message and update.message.text else func.__name__
        logger.info(f"{username} (admin): {text}")
        return await func(update, context)
    return wrapper


async def clear_webhook():
    """Clear any existing webhook before starting polling."""
    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        webhook_info = await bot.get_webhook_info()
        if webhook_info.url:
            logger.info(f"Clearing existing webhook: {webhook_info.url}")
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("Webhook cleared successfully")
        else:
            logger.info("No webhook configured")
    except Exception as e:
        logger.warning(f"Failed to clear webhook: {e}")


async def retry_async(coro_func, *args, max_retries=MAX_RETRIES, **kwargs):
    """Retry an async operation with exponential backoff."""
    last_exception = None

    for attempt in range(max_retries):
        try:
            async with api_lock:
                return await coro_func(*args, **kwargs)
        except RetryAfter as e:
            wait_time = e.retry_after + 1
            logger.warning(f"Rate limited, waiting {wait_time}s before retry")
            await asyncio.sleep(wait_time)
            last_exception = e
        except TimedOut as e:
            wait_time = RETRY_DELAY * (2 ** attempt)
            logger.warning(f"Request timed out (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s")
            await asyncio.sleep(wait_time)
            last_exception = e
        except NetworkError as e:
            wait_time = RETRY_DELAY * (2 ** attempt)
            logger.warning(f"Network error (attempt {attempt + 1}/{max_retries}): {e}, retrying in {wait_time}s")
            await asyncio.sleep(wait_time)
            last_exception = e
        except Conflict as e:
            # Don't retry conflicts - this means another instance is running
            logger.error(f"Bot conflict detected: {e}")
            logger.error("Another bot instance is running. Please stop other instances.")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in retry_async: {e}")
            raise

    # All retries exhausted
    logger.error(f"All {max_retries} retries exhausted")
    if last_exception:
        raise last_exception


async def safe_send_message(bot, chat_id, text, parse_mode=None, **kwargs):
    """Send a message with retry logic and error handling."""
    async def _send():
        return await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=parse_mode,
            read_timeout=REQUEST_READ_TIMEOUT,
            write_timeout=REQUEST_WRITE_TIMEOUT,
            connect_timeout=REQUEST_CONNECT_TIMEOUT,
            **kwargs
        )

    try:
        return await retry_async(_send)
    except Conflict:
        raise  # Re-raise conflict errors
    except Exception as e:
        logger.error(f"Failed to send message to {chat_id}: {e}")
        return None


# Theft detection thresholds
LARGE_DISCOUNT_THRESHOLD = 20  # Alert if discount > 20%
LARGE_REFUND_THRESHOLD = 50000  # Alert if refund > 500 THB (in cents)
LARGE_EXPENSE_THRESHOLD = 300000  # Alert if single expense > 3000 THB (in cents)


def format_currency(amount_in_cents, short=False):
    """Format amount from cents to THB."""
    try:
        amount = float(amount_in_cents) / 100
        if short:
            abs_amount = abs(amount)
            sign = "-" if amount < 0 else ""
            if abs_amount >= 1_000_000:
                return f"฿{sign}{abs_amount / 1_000_000:.1f}M"
            elif abs_amount >= 100_000:
                return f"฿{sign}{abs_amount / 1_000:.1f}k"
            else:
                return f"฿{sign}{abs_amount:,.0f}"
        return f"฿{amount:,.2f}"
    except (ValueError, TypeError):
        return "฿0.00"


# Business day cutoff hour (4am) - "today" means yesterday until this hour
BUSINESS_DAY_CUTOFF_HOUR = 4


def adjust_poster_time(timestamp_str):
    """Add 4-hour offset to Poster API timestamp (API returns 4h behind local time)."""
    try:
        dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        dt = dt + timedelta(hours=4)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return timestamp_str


def get_business_date():
    """Get the current business date in Bangkok time.

    For bars/restaurants that operate late, the business day doesn't end at midnight.
    If current time is before BUSINESS_DAY_CUTOFF_HOUR (4am), return yesterday's date.
    """
    now = datetime.now(THAI_TZ)
    if now.hour < BUSINESS_DAY_CUTOFF_HOUR:
        return (now - timedelta(days=1)).date()
    return now.date()


def fetch_cash_shifts():
    """Fetch cash shift data from Poster API."""
    url = f"{POSTER_API_URL}/finance.getCashShifts"
    params = {"token": config.POSTER_ACCESS_TOKEN}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("response", [])
    except requests.RequestException as e:
        logger.error(f"Failed to fetch cash shifts: {e}")
        return []


def fetch_finance_transactions(date_from, date_to=None):
    """Fetch finance transactions (expenses/income) from Poster API."""
    url = f"{POSTER_API_URL}/finance.getTransactions"
    params = {
        "token": config.POSTER_ACCESS_TOKEN,
        "dateFrom": date_from,
        "dateTo": date_to or date_from
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("response", [])
    except requests.RequestException as e:
        logger.error(f"Failed to fetch finance transactions: {e}")
        return []


def calculate_expenses(finance_transactions):
    """Calculate expense totals from finance transactions."""
    expenses = []
    total_expenses = 0

    for txn in finance_transactions:
        # type "0" = expense/outgoing, amount is negative
        txn_type = txn.get('type', '')
        amount = int(txn.get('amount', 0) or 0)
        comment = txn.get('comment', '')
        category = txn.get('category_name', '')

        # Skip cash payments (sales income) and positive adjustments
        if 'Cash payments' in comment:
            continue

        # Only count actual expenses (negative amounts or type 0 expenses)
        if amount < 0:
            expense_amount = abs(amount)
            total_expenses += expense_amount
            expenses.append({
                'amount': expense_amount,
                'comment': comment,
                'category': category,
                'date': txn.get('date', ''),
                'transaction_id': txn.get('transaction_id', '')
            })

    return {
        'total_expenses': total_expenses,
        'expense_list': expenses
    }


def fetch_transactions(date_from, date_to=None):
    """Fetch transactions for a date or date range from Poster API."""
    url = f"{POSTER_API_URL}/dash.getTransactions"
    params = {
        "token": config.POSTER_ACCESS_TOKEN,
        "dateFrom": date_from,
        "dateTo": date_to or date_from
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("response", [])
    except requests.RequestException as e:
        logger.error(f"Failed to fetch transactions: {e}")
        return []


def fetch_product_sales(date_from, date_to=None):
    """Fetch product-level sales data from Poster API."""
    url = f"{POSTER_API_URL}/dash.getProductsSales"
    params = {
        "token": config.POSTER_ACCESS_TOKEN,
        "dateFrom": date_from,
        "dateTo": date_to or date_from
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        return data.get("response", [])
    except requests.RequestException as e:
        logger.error(f"Failed to fetch product sales: {e}")
        return []


def fetch_product_catalog():
    """Fetch the full product catalog from Poster to get category mappings.

    Returns a dict mapping product_id (str) -> category_name (str).
    """
    url = f"{POSTER_API_URL}/menu.getProducts"
    params = {"token": config.POSTER_ACCESS_TOKEN}

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        products = data.get("response", [])
        return {
            str(p.get("product_id", "")): p.get("category_name", "Uncategorized") or "Uncategorized"
            for p in products
        }
    except requests.RequestException as e:
        logger.error(f"Failed to fetch product catalog: {e}")
        return {}


def fetch_stock_levels():
    """Fetch current stock/inventory levels from Poster API."""
    url = f"{POSTER_API_URL}/storage.getStorageLeftovers"
    params = {"token": config.POSTER_ACCESS_TOKEN}

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        return data.get("response", [])
    except requests.RequestException as e:
        logger.error(f"Failed to fetch stock levels: {e}")
        return []


def fetch_transaction_products(transaction_id):
    """Fetch products for a specific transaction from Poster API."""
    url = f"{POSTER_API_URL}/dash.getTransactionProducts"
    params = {
        "token": config.POSTER_ACCESS_TOKEN,
        "transaction_id": transaction_id
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("response", [])
    except requests.RequestException as e:
        logger.error(f"Failed to fetch transaction products: {e}")
        return []


def fetch_ingredient_usage(date_from, date_to=None):
    """Fetch ingredient usage/movement from Poster API."""
    url = f"{POSTER_API_URL}/storage.getReportMovement"
    params = {
        "token": config.POSTER_ACCESS_TOKEN,
        "dateFrom": date_from,
        "dateTo": date_to or date_from
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        return data.get("response", [])
    except requests.RequestException as e:
        logger.error(f"Failed to fetch ingredient usage: {e}")
        return []


def calculate_summary(transactions):
    """Calculate summary statistics from transactions."""
    total_sales = 0
    total_profit = 0
    cash_sales = 0
    card_sales = 0

    for txn in transactions:
        total_sales += int(txn.get('sum', 0) or 0)
        total_profit += int(txn.get('total_profit', 0) or 0)
        cash_sales += int(txn.get('payed_cash', 0) or 0)
        card_sales += int(txn.get('payed_card', 0) or 0)

    return {
        "transaction_count": len(transactions),
        "total_sales": total_sales,
        "total_profit": total_profit,
        "cash_sales": cash_sales,
        "card_sales": card_sales
    }


def format_summary_message(date_display, summary, expenses=None):
    """Format the summary into a Telegram message."""
    if summary["transaction_count"] == 0:
        return f"📊 <b>Summary for {date_display}</b>\n\nNo transactions found."

    message = (
        f"📊 <b>Summary for {date_display}</b>\n\n"
        f"<b>Transactions:</b> {summary['transaction_count']}\n"
        f"<b>Total Sales:</b> {format_currency(summary['total_sales'])}\n"
        f"<b>Gross Profit:</b> {format_currency(summary['total_profit'])}\n\n"
        f"<b>💵 Cash:</b> {format_currency(summary['cash_sales'])}\n"
        f"<b>💳 Card:</b> {format_currency(summary['card_sales'])}"
    )

    # Add expenses if provided
    if expenses and expenses['total_expenses'] > 0:
        net_profit = summary['total_sales'] - expenses['total_expenses']
        message += (
            f"\n\n<b>💸 Expenses:</b> -{format_currency(expenses['total_expenses'])}\n"
            f"<b>💰 Net Profit:</b> {format_currency(net_profit)}"
        )

    return message


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command."""
    chat_id = str(update.effective_chat.id)

    # No admin configured yet
    if not admin_chat_ids:
        await update.message.reply_text(
            "🍺 <b>Ban Sabai POS Bot</b>\n\n"
            "No admin configured.\n"
            "Send /setup to become admin.",
            parse_mode=ParseMode.HTML
        )
        return

    # Check if user is approved
    is_admin = chat_id in admin_chat_ids
    is_approved = chat_id in approved_users
    is_pending = chat_id in pending_requests

    if not is_admin and not is_approved:
        if is_pending:
            await update.message.reply_text(
                "🍺 <b>Ban Sabai POS Bot</b>\n\n"
                "Your access request is pending approval.\n"
                "Please wait for admin to approve.",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text(
                "🍺 <b>Ban Sabai POS Bot</b>\n\n"
                "Access required.\n"
                "Send /request to request access.",
                parse_mode=ParseMode.HTML
            )
        return

    # User has access - show full menu
    message = (
        "🍺 <b>Ban Sabai POS Bot</b>\n\n"
        "<b>📊 Reports:</b>\n"
        "/today - Today's sales summary\n"
        "/week - This week's summary\n"
        "/month - This month's summary\n"
        "/summary DATE [DATE] - Custom date/range\n"
        "/sales [N] - Last N sales with items\n"
        "/products [today|week|month] - Product sales\n"
        "/stats [today|week|month] - Sales statistics\n"
        "/expenses [DATE] [DATE] - Expense breakdown\n\n"
        "<b>📦 Inventory:</b>\n"
        "/stock - Current stock levels\n"
        "/ingredients [today|week|month] - Ingredient usage\n\n"
        "<b>💵 Cash:</b>\n"
        "/cash - Cash register balance\n\n"
        "<b>🔔 Real-time:</b>\n"
        "/subscribe - Get notified on each sale\n"
        "/unsubscribe - Stop sale notifications\n\n"
        "<b>🚨 Security:</b>\n"
        "/alerts - Enable theft detection\n"
        "/alerts_off - Disable theft alerts\n\n"
        "<b>🤖 AI Assistant:</b>\n"
        "/agent &lt;question&gt; - Ask AI about your data\n\n"
        "<b>📊 Dashboard:</b>\n"
        "/dashboard - Open web dashboard\n"
        "/setpassword &lt;pw&gt; - Set dashboard password\n"
        "/setgoal &lt;THB&gt; - Set monthly profit goal\n\n"
    )

    # Add admin commands if user is admin
    if is_admin:
        message += (
            "<b>👑 Admin:</b>\n"
            "/approve - Approve user access\n"
            "/reject ID - Reject user request\n"
            "/users - List approved users\n"
            "/promote ID - Promote user to admin\n"
            "/demote ID - Remove admin privileges\n"
            "/config - View bot configuration\n"
            "/reset - Reset all configuration\n\n"
            "<b>🔧 Debug:</b>\n"
            "/debug - Show raw transaction data\n"
            "/resend - Resend last 2 notifications\n"
            "/loglevel [LEVEL] - Set logging level\n\n"
        )

    message += "/help - Show this message"

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /help command."""
    await start(update, context)


async def setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /setup command - first user becomes admin."""
    global admin_chat_ids, approved_users
    chat_id = str(update.effective_chat.id)

    if admin_chat_ids:
        if chat_id in admin_chat_ids:
            await update.message.reply_text("You are already an admin.")
        else:
            await update.message.reply_text("Admin already configured. Send /request to request access.")
        return

    # Set this user as first admin
    admin_chat_ids.add(chat_id)
    user = update.effective_user
    approved_users[chat_id] = {
        'name': user.full_name if user else 'Admin',
        'username': user.username if user else None,
        'approved_at': datetime.now().isoformat()
    }
    save_config()

    await update.message.reply_text(
        "✅ <b>Admin Setup Complete!</b>\n\n"
        "You are now an admin with full access.\n\n"
        "Other users can request access with /request\n"
        "Manage users with /approve, /reject, /users",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Admin configured: chat_id={chat_id}")


async def request_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /request command - request access from admin."""
    global pending_requests
    chat_id = str(update.effective_chat.id)

    if not admin_chat_ids:
        await update.message.reply_text("No admin configured yet. Send /setup to become admin.")
        return

    if chat_id in admin_chat_ids:
        await update.message.reply_text("You are an admin. You already have full access.")
        return

    if chat_id in approved_users:
        await update.message.reply_text("You already have access.")
        return

    if chat_id in pending_requests:
        await update.message.reply_text("Your request is already pending. Please wait for admin approval.")
        return

    # Add to pending requests
    user = update.effective_user
    pending_requests[chat_id] = {
        'name': user.full_name if user else 'Unknown',
        'username': user.username if user else None,
        'requested_at': datetime.now().isoformat()
    }
    save_config()

    await update.message.reply_text(
        "📤 <b>Access Requested!</b>\n\n"
        "Your request has been sent to the admins.\n"
        "You'll be notified when it's approved.",
        parse_mode=ParseMode.HTML
    )

    # Notify all admins
    if TELEGRAM_BOT_TOKEN and admin_chat_ids:
        try:
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            username_str = f"@{user.username}" if user and user.username else "No username"
            for admin_id in admin_chat_ids:
                await safe_send_message(
                    bot, admin_id,
                    (
                        f"🔔 <b>New Access Request</b>\n\n"
                        f"<b>Name:</b> {user.full_name if user else 'Unknown'}\n"
                        f"<b>Username:</b> {username_str}\n"
                        f"<b>Chat ID:</b> <code>{chat_id}</code>\n\n"
                        f"Use /approve {chat_id} to approve\n"
                        f"Use /reject {chat_id} to reject"
                    ),
                    parse_mode=ParseMode.HTML
                )
        except Exception as e:
            logger.error(f"Failed to notify admins of access request: {e}")

    logger.info(f"Access request from chat_id={chat_id}")


@require_admin
async def approve(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /approve command - approve user access."""
    global approved_users, pending_requests

    if not context.args:
        # List pending requests
        if not pending_requests:
            await update.message.reply_text("No pending access requests.")
            return

        message = "📋 <b>Pending Requests</b>\n\n"
        for chat_id, info in pending_requests.items():
            username_str = f"@{info['username']}" if info.get('username') else "No username"
            message += (
                f"<b>{info['name']}</b>\n"
                f"Username: {username_str}\n"
                f"Chat ID: <code>{chat_id}</code>\n"
                f"/approve {chat_id}\n\n"
            )
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)
        return

    target_chat_id = context.args[0]

    if target_chat_id not in pending_requests:
        await update.message.reply_text(
            f"Chat ID {target_chat_id} not found in pending requests.\n"
            "Use /approve without arguments to see pending requests."
        )
        return

    # Move from pending to approved
    user_info = pending_requests.pop(target_chat_id)
    approved_users[target_chat_id] = {
        'name': user_info['name'],
        'username': user_info.get('username'),
        'approved_at': datetime.now().isoformat()
    }
    save_config()

    await update.message.reply_text(
        f"✅ <b>User Approved</b>\n\n"
        f"<b>{user_info['name']}</b> now has access.",
        parse_mode=ParseMode.HTML
    )

    # Notify the user
    if TELEGRAM_BOT_TOKEN:
        try:
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            await safe_send_message(
                bot, target_chat_id,
                (
                    "✅ <b>Access Granted!</b>\n\n"
                    "Your access request has been approved.\n"
                    "Send /help to see available commands."
                ),
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Failed to notify user of approval: {e}")

    logger.info(f"User approved: chat_id={target_chat_id}")


@require_admin
async def reject(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /reject command - reject user access request."""
    global pending_requests

    if not context.args:
        await update.message.reply_text(
            "Usage: /reject <chat_id>\n\n"
            "Use /approve to see pending requests with chat IDs."
        )
        return

    target_chat_id = context.args[0]

    if target_chat_id not in pending_requests:
        await update.message.reply_text(
            f"Chat ID {target_chat_id} not found in pending requests."
        )
        return

    # Remove from pending
    user_info = pending_requests.pop(target_chat_id)
    save_config()

    await update.message.reply_text(
        f"❌ <b>Request Rejected</b>\n\n"
        f"<b>{user_info['name']}</b>'s request has been rejected.",
        parse_mode=ParseMode.HTML
    )

    # Notify the user
    if TELEGRAM_BOT_TOKEN:
        try:
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            await safe_send_message(
                bot, target_chat_id,
                "❌ Your access request has been denied.",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Failed to notify user of rejection: {e}")

    logger.info(f"User rejected: chat_id={target_chat_id}")


@require_admin
async def users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /users command - list approved users."""
    if not approved_users:
        await update.message.reply_text("No approved users.")
        return

    message = "👥 <b>Approved Users</b>\n\n"
    for chat_id, info in approved_users.items():
        username_str = f"@{info['username']}" if info.get('username') else "No username"
        is_admin = " (Admin)" if chat_id in admin_chat_ids else ""
        message += (
            f"<b>{info['name']}</b>{is_admin}\n"
            f"Username: {username_str}\n"
            f"Chat ID: <code>{chat_id}</code>\n\n"
        )

    pending_count = len(pending_requests)
    if pending_count > 0:
        message += f"<i>{pending_count} pending request(s) - use /approve to view</i>"

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


@require_admin
async def promote(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /promote command - promote an approved user to admin."""
    global admin_chat_ids

    if not context.args:
        # List promotable users (approved users who are not already admins)
        promotable = {cid: info for cid, info in approved_users.items() if cid not in admin_chat_ids}
        if not promotable:
            await update.message.reply_text("No approved users available to promote.")
            return

        message = "👑 <b>Promote User to Admin</b>\n\n"
        message += "Select a user to promote:\n\n"
        for chat_id, info in promotable.items():
            username_str = f"@{info['username']}" if info.get('username') else "no username"
            message += (
                f"<b>{info['name']}</b> - {username_str}\n"
                f"/promote {chat_id}\n\n"
            )
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)
        return

    target_chat_id = context.args[0]

    # Check if target is already an admin
    if target_chat_id in admin_chat_ids:
        await update.message.reply_text("This user is already an admin.")
        return

    # Check if target is an approved user
    if target_chat_id not in approved_users:
        await update.message.reply_text(
            f"Chat ID {target_chat_id} is not an approved user.\n"
            "Use /users to see approved users."
        )
        return

    # Promote the user
    admin_chat_ids.add(target_chat_id)
    save_config()

    user_info = approved_users[target_chat_id]
    await update.message.reply_text(
        f"👑 <b>Admin Promoted</b>\n\n"
        f"<b>{user_info['name']}</b> is now an admin.",
        parse_mode=ParseMode.HTML
    )

    # Notify the new admin
    if TELEGRAM_BOT_TOKEN:
        try:
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            await safe_send_message(
                bot, target_chat_id,
                (
                    "👑 <b>You are now an Admin!</b>\n\n"
                    "You have been promoted to admin.\n"
                    "Use /help to see available commands."
                ),
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Failed to notify new admin: {e}")

    logger.info(f"User promoted to admin: {target_chat_id}")


@require_admin
async def demote(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /demote command - remove admin privileges from a user."""
    global admin_chat_ids
    chat_id = str(update.effective_chat.id)

    if not context.args:
        # List demotable admins (other admins, not self)
        demotable = {cid: approved_users.get(cid, {'name': 'Unknown'})
                     for cid in admin_chat_ids if cid != chat_id and cid in approved_users}
        if not demotable:
            await update.message.reply_text("No other admins to demote.")
            return

        message = "👑 <b>Demote Admin</b>\n\n"
        message += "Select an admin to demote:\n\n"
        for admin_id, info in demotable.items():
            username_str = f"@{info.get('username')}" if info.get('username') else "no username"
            message += (
                f"<b>{info.get('name', 'Unknown')}</b> - {username_str}\n"
                f"/demote {admin_id}\n\n"
            )
        await update.message.reply_text(message, parse_mode=ParseMode.HTML)
        return

    target_chat_id = context.args[0]

    # Cannot demote yourself
    if target_chat_id == chat_id:
        await update.message.reply_text("You cannot demote yourself.")
        return

    # Check if target is an admin
    if target_chat_id not in admin_chat_ids:
        await update.message.reply_text("This user is not an admin.")
        return

    # Ensure at least one admin remains
    if len(admin_chat_ids) <= 1:
        await update.message.reply_text("Cannot demote the last admin. Promote someone else first.")
        return

    # Demote the user
    admin_chat_ids.discard(target_chat_id)
    save_config()

    user_info = approved_users.get(target_chat_id, {'name': 'Unknown'})
    await update.message.reply_text(
        f"👑 <b>Admin Demoted</b>\n\n"
        f"<b>{user_info.get('name', 'Unknown')}</b> is no longer an admin.",
        parse_mode=ParseMode.HTML
    )

    # Notify the demoted user
    if TELEGRAM_BOT_TOKEN:
        try:
            bot = Bot(token=TELEGRAM_BOT_TOKEN)
            await safe_send_message(
                bot, target_chat_id,
                "ℹ️ Your admin privileges have been removed.",
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Failed to notify demoted admin: {e}")

    logger.info(f"User demoted from admin: {target_chat_id}")


@require_admin
async def config_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /config command - show or set configuration."""
    allowed_vars = ["ANTHROPIC_API_KEY", "POSTER_ACCESS_TOKEN"]

    # Handle /config set <VAR> <VALUE>
    if context.args and len(context.args) >= 3 and context.args[0].lower() == "set":
        var_name = context.args[1].upper()
        var_value = " ".join(context.args[2:])

        if var_name not in allowed_vars:
            await update.message.reply_text(
                f"Unknown variable: {var_name}\n\n"
                f"Allowed variables:\n"
                f"• ANTHROPIC_API_KEY\n"
                f"• POSTER_ACCESS_TOKEN"
            )
            return

        set_api_key(var_name, var_value)
        await update.message.reply_text(
            f"✅ <b>{var_name}</b> has been set.\n"
            f"Value: <code>{mask_api_key(var_value)}</code>",
            parse_mode=ParseMode.HTML
        )
        logger.info(f"Config variable {var_name} updated by admin")
        return

    # Handle /config del <VAR>
    if context.args and len(context.args) >= 2 and context.args[0].lower() == "del":
        var_name = context.args[1].upper()

        if var_name not in allowed_vars:
            await update.message.reply_text(
                f"Unknown variable: {var_name}\n\n"
                f"Allowed variables:\n"
                f"• ANTHROPIC_API_KEY\n"
                f"• POSTER_ACCESS_TOKEN"
            )
            return

        if delete_api_key(var_name):
            await update.message.reply_text(
                f"✅ <b>{var_name}</b> has been deleted.",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text(
                f"Variable <b>{var_name}</b> was not set.",
                parse_mode=ParseMode.HTML
            )
        return

    try:
        config_data = get_config_data()
        if not config_data:
            await update.message.reply_text("No configuration file exists yet.")
            return

        # Format the config nicely
        message = "⚙️ <b>Bot Configuration</b>\n\n"

        # API Keys section
        message += "<b>API Keys:</b>\n"
        anthropic_key = config_data.get('ANTHROPIC_API_KEY') or config.ANTHROPIC_API_KEY
        poster_key = config_data.get('POSTER_ACCESS_TOKEN') or config.POSTER_ACCESS_TOKEN
        message += f"  • ANTHROPIC_API_KEY: <code>{mask_api_key(anthropic_key) if anthropic_key else 'Not set'}</code>\n"
        message += f"  • POSTER_ACCESS_TOKEN: <code>{mask_api_key(poster_key) if poster_key else 'Not set'}</code>\n\n"

        # Admin info - handle both old and new format
        admin_ids = set(config_data.get('admin_chat_ids', []))
        old_admin = config_data.get('admin_chat_id')
        if old_admin:
            admin_ids.add(old_admin)
        message += f"<b>Admins:</b> {len(admin_ids)}\n"
        for admin_id in admin_ids:
            message += f"  • <code>{admin_id}</code>\n"
        message += "\n"

        # Approved users
        users_data = config_data.get('approved_users', {})
        message += f"<b>Approved Users:</b> {len(users_data)}\n"
        for chat_id, info in users_data.items():
            username = f"@{info.get('username')}" if info.get('username') else "no username"
            is_admin = " (Admin)" if chat_id in admin_ids else ""
            message += f"  • {info.get('name', 'Unknown')}{is_admin} - {username}\n"

        # Pending requests
        pending = config_data.get('pending_requests', {})
        message += f"\n<b>Pending Requests:</b> {len(pending)}\n"
        for chat_id, info in pending.items():
            username = f"@{info.get('username')}" if info.get('username') else "no username"
            message += f"  • {info.get('name', 'Unknown')} - {username}\n"

        # Subscribed chats
        subs = config_data.get('subscribed_chats', [])
        message += f"\n<b>Sale Notifications:</b> {len(subs)} chat(s)\n"

        # Theft alert chats
        alerts = config_data.get('theft_alert_chats', [])
        message += f"<b>Theft Alerts:</b> {len(alerts)} chat(s)\n"

        # Config file path and usage hint
        message += f"\n<i>File: {CONFIG_FILE}</i>\n"
        message += "<i>Use /config set VAR VALUE to set API keys</i>"

        await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        # Send raw JSON as a separate message (with sensitive fields masked)
        display_config = config_data.copy()
        if display_config.get('ANTHROPIC_API_KEY'):
            display_config['ANTHROPIC_API_KEY'] = mask_api_key(display_config['ANTHROPIC_API_KEY'])
        if display_config.get('POSTER_ACCESS_TOKEN'):
            display_config['POSTER_ACCESS_TOKEN'] = mask_api_key(display_config['POSTER_ACCESS_TOKEN'])
        # Strip password hashes from approved_users
        if 'approved_users' in display_config:
            display_config['approved_users'] = {
                k: {kk: vv for kk, vv in v.items() if kk != 'password_hash'}
                for k, v in display_config['approved_users'].items()
            }
        raw_json = json.dumps(display_config, indent=2, ensure_ascii=False)
        # Telegram message limit is 4096 chars, truncate if needed
        if len(raw_json) > 4000:
            raw_json = raw_json[:4000] + "\n... (truncated)"
        await update.message.reply_text(
            f"<b>Raw Config:</b>\n<pre>{raw_json}</pre>",
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        logger.error(f"Error reading config: {e}")
        await update.message.reply_text(f"Error reading config: {e}")


@require_admin
async def reset(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /reset command - delete configuration and reset bot."""
    global admin_chat_ids, approved_users, pending_requests, subscribed_chats, theft_alert_chats
    global notified_transaction_ids, notified_transaction_date, last_seen_void_id, last_cash_balance, last_alerted_transaction_id, last_alerted_expense_id

    # Check for confirmation argument
    if not context.args or context.args[0] != "CONFIRM":
        await update.message.reply_text(
            "⚠️ <b>Warning: This will delete all bot data!</b>\n\n"
            "This includes:\n"
            "• Admin configuration\n"
            "• All approved users\n"
            "• Pending requests\n"
            "• Subscription settings\n"
            "• Alert settings\n"
            "• Theft detection state\n\n"
            "To confirm, send:\n"
            "<code>/reset CONFIRM</code>",
            parse_mode=ParseMode.HTML
        )
        return

    try:
        # Delete config file
        if os.path.exists(CONFIG_FILE):
            os.remove(CONFIG_FILE)

        # Reset all global state
        admin_chat_ids = set()
        approved_users = {}
        pending_requests = {}
        subscribed_chats = set()
        theft_alert_chats = set()
        notified_transaction_ids = set()
        notified_transaction_date = None
        last_seen_void_id = None
        last_cash_balance = None
        last_alerted_transaction_id = 0
        last_alerted_expense_id = 0

        await update.message.reply_text(
            "✅ <b>Configuration Reset</b>\n\n"
            "All data has been deleted.\n"
            "Send /setup to configure a new admin.",
            parse_mode=ParseMode.HTML
        )
        logger.info(f"Configuration reset by chat_id={update.effective_chat.id}")

    except Exception as e:
        logger.error(f"Error resetting config: {e}")
        await update.message.reply_text(f"Error resetting config: {e}")


@require_admin
async def agent(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /agent command - AI agent to query POS data (admin only)."""
    if not AGENT_AVAILABLE:
        await update.message.reply_text(
            "The AI agent is not available.\n\n"
            "To enable it, install the anthropic package:\n"
            "<code>pip install anthropic</code>",
            parse_mode=ParseMode.HTML
        )
        return

    if not config.ANTHROPIC_API_KEY:
        await update.message.reply_text(
            "ANTHROPIC_API_KEY is not configured.\n\n"
            "Ask an admin to set it with:\n"
            "<code>/config set ANTHROPIC_API_KEY sk-...</code>",
            parse_mode=ParseMode.HTML
        )
        return

    if not config.POSTER_ACCESS_TOKEN:
        await update.message.reply_text(
            "POSTER_ACCESS_TOKEN is not configured.\n\n"
            "Ask an admin to set it with:\n"
            "<code>/config set POSTER_ACCESS_TOKEN ...</code>",
            parse_mode=ParseMode.HTML
        )
        return

    # Use user_id for per-user conversation history and rate limiting
    user_id = str(update.effective_user.id)
    chat_id = str(update.effective_chat.id)

    # Handle /agent limit <user_id> <key> <value> - admin command to set per-user limits
    if context.args and context.args[0].lower() == "limit":
        if len(context.args) != 4:
            await update.message.reply_text(
                "Usage: <code>/agent limit &lt;user_id&gt; &lt;key&gt; &lt;value&gt;</code>\n\n"
                "Keys:\n"
                "• daily_limit - Max requests per day\n"
                "• max_iterations - Max tool iterations per request\n\n"
                "Examples:\n"
                "• /agent limit 123456 daily_limit 10\n"
                "• /agent limit 123456 max_iterations 3",
                parse_mode=ParseMode.HTML
            )
            return

        target_user_id = context.args[1]
        limit_key = context.args[2].lower()
        try:
            limit_value = int(context.args[3])
        except ValueError:
            await update.message.reply_text(
                "Error: value must be an integer.",
                parse_mode=ParseMode.HTML
            )
            return

        if set_agent_limit(target_user_id, limit_key, limit_value):
            limits = get_agent_limits(target_user_id)
            await update.message.reply_text(
                f"Updated limits for user {target_user_id}:\n"
                f"• daily_limit: {limits['daily_limit']}\n"
                f"• max_iterations: {limits['max_iterations']}",
                parse_mode=ParseMode.HTML
            )
        else:
            await update.message.reply_text(
                f"Error: invalid key '{limit_key}'. Valid keys: daily_limit, max_iterations",
                parse_mode=ParseMode.HTML
            )
        return

    # Handle /agent clear - reset conversation history
    if context.args and context.args[0].lower() == "clear":
        if user_id in agent_conversations:
            del agent_conversations[user_id]
        await update.message.reply_text(
            "Conversation history cleared.",
            parse_mode=ParseMode.HTML
        )
        return

    # Get per-user limits
    user_limits = get_agent_limits(user_id)

    # Check rate limit
    allowed, remaining = check_agent_rate_limit(user_id)
    if not allowed:
        await update.message.reply_text(
            f"Daily limit reached ({user_limits['daily_limit']} requests/day).\n"
            "Try again tomorrow.",
            parse_mode=ParseMode.HTML
        )
        return

    if not context.args:
        used, limit = get_agent_usage(user_id)
        await update.message.reply_text(
            "Usage: <code>/agent &lt;your question&gt;</code>\n\n"
            "Examples:\n"
            "• /agent What were total sales today?\n"
            "• /agent Which products sold the most this week?\n"
            "• /agent What's the current stock level of beer?\n"
            "• /agent Show me expenses for this month\n"
            "• /agent clear - Reset conversation history\n\n"
            f"<i>Daily usage: {used}/{limit}</i>",
            parse_mode=ParseMode.HTML
        )
        return

    prompt = " ".join(context.args)

    # Record usage before making request
    record_agent_usage(user_id)
    used, limit = get_agent_usage(user_id)

    # Send "thinking" message
    thinking_msg = await update.message.reply_text("🤔 Thinking...")

    try:
        # Get existing conversation history for this user
        history = agent_conversations.get(user_id, [])

        response, updated_history, charts = await run_agent(
            prompt, config.ANTHROPIC_API_KEY, config.POSTER_ACCESS_TOKEN,
            history=history, max_iterations=user_limits['max_iterations']
        )

        # Store updated history (already trimmed to last 10 messages)
        agent_conversations[user_id] = updated_history

        # Delete thinking message
        await thinking_msg.delete()

        # Add usage footer
        usage_footer = f"\n\n<i>({used}/{limit} today)</i>"

        # Handle long responses (Telegram limit is 4096 chars)
        if len(response) + len(usage_footer) <= 4000:
            await update.message.reply_text(response + usage_footer, parse_mode=ParseMode.HTML)
        else:
            # Split into multiple messages
            chunks = [response[i:i+4000] for i in range(0, len(response), 4000)]
            for i, chunk in enumerate(chunks):
                if i == len(chunks) - 1:  # Last chunk
                    chunk += usage_footer
                await update.message.reply_text(chunk, parse_mode=ParseMode.HTML)

        # Send any generated charts
        for chart in charts:
            await update.message.reply_photo(photo=chart)

    except Exception as e:
        logger.error(f"Agent error: {e}")
        await thinking_msg.edit_text(f"Error: {str(e)}")


@require_admin
async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /debug command - show raw API transaction data."""
    today_str = get_business_date().strftime('%Y%m%d')

    await update.message.reply_text("⏳ Fetching raw transaction data...")

    transactions = [t for t in fetch_transactions(today_str)
                    if str(t.get('status')) in ('1', '2')]

    if not transactions:
        await update.message.reply_text("No transactions found for today.")
        return

    # Sort by transaction_id descending and take last 3
    transactions.sort(key=lambda x: int(x.get('transaction_id', 0)), reverse=True)
    recent = transactions[:3]

    message = f"<b>🔍 Debug: Last closed {len(recent)} transactions</b>\n\n"

    for txn in recent:
        txn_id = txn.get('transaction_id', 'N/A')
        message += f"<b>Transaction ID:</b> {txn_id}\n"
        message += f"<pre>{json.dumps(txn, indent=2, ensure_ascii=False)[:1000]}</pre>\n\n"

    # Also show notified transaction set info
    message += f"<b>notified_transaction_ids:</b> {len(notified_transaction_ids)} tracked\n"
    message += f"<b>notified_transaction_date:</b> {notified_transaction_date}"

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


@require_auth
async def sales(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /sales command - show N most recent sales."""
    # Default to 5, allow override with argument
    count = 5
    if context.args:
        try:
            count = min(int(context.args[0]), 20)  # Max 20
        except ValueError:
            pass

    today_str = get_business_date().strftime('%Y%m%d')
    today_display = get_business_date().strftime('%d %b %Y')

    await update.message.reply_text(f"⏳ Fetching last {count} sales...")

    transactions = fetch_transactions(today_str)

    if not transactions:
        await update.message.reply_text("No transactions found for today.")
        return

    # Filter for open and closed transactions with actual sales
    valid_sales = [
        t for t in transactions
        if str(t.get('status')) in ('1', '2') and int(t.get('sum', 0) or 0) > 0
    ]

    if not valid_sales:
        await update.message.reply_text("No sales found for today.")
        return

    # Sort by transaction_id descending (most recent first)
    valid_sales.sort(key=lambda x: int(x.get('transaction_id', 0)), reverse=True)

    # Take requested count
    recent_sales = valid_sales[:count]

    message = f"🧾 <b>Last {len(recent_sales)} Sales - {today_display}</b>\n\n"

    for txn in recent_sales:
        txn_id = txn.get('transaction_id')
        total = int(txn.get('sum', 0) or 0)
        profit = int(txn.get('total_profit', 0) or 0)
        payed_cash = int(txn.get('payed_cash', 0) or 0)
        payed_card = int(txn.get('payed_card', 0) or 0)
        table_name = txn.get('table_name', '-')
        close_time = adjust_poster_time(txn.get('date_close_date', '') or txn.get('date', ''))

        # Format time
        time_str = close_time.split(' ')[1][:5] if ' ' in close_time else '-'

        # Payment icon
        if payed_card > 0 and payed_cash > 0:
            pay_icon = "💳💵"
        elif payed_card > 0:
            pay_icon = "💳"
        else:
            pay_icon = "💵"

        # Fetch items
        items_str = ""
        try:
            products = fetch_transaction_products(txn_id)
            if products:
                items_list = []
                for p in products[:5]:  # Limit to 5 items per sale
                    qty = float(p.get('num', 1))
                    name = p.get('product_name', 'Unknown')
                    if len(name) > 15:
                        name = name[:12] + "..."
                    if qty == 1:
                        items_list.append(name)
                    else:
                        items_list.append(f"{qty:.0f}x {name}")
                items_str = ", ".join(items_list)
                if len(products) > 5:
                    items_str += f" +{len(products)-5} more"
        except Exception as e:
            logger.error(f"Failed to fetch products for txn {txn_id}: {e}")

        message += f"<code>{time_str}</code> {pay_icon} {format_currency(total)} 📍{table_name}\n"
        if items_str:
            message += f"   <i>{items_str}</i>\n"

    message += f"\n<i>Usage: /sales [count]</i>"

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


@require_admin
async def resend(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /resend command - resend last N closed transactions as notifications."""
    # Default to 2, allow override with argument
    count = 2
    if context.args:
        try:
            count = min(int(context.args[0]), 10)  # Max 10
        except ValueError:
            pass

    today_str = get_business_date().strftime('%Y%m%d')

    await update.message.reply_text(f"⏳ Fetching and resending last {count} transactions...")

    transactions = fetch_transactions(today_str)

    if not transactions:
        await update.message.reply_text("No transactions found for today.")
        return

    # Filter for open and closed transactions with actual sales (exclude voided with sum=0)
    closed_txns = [t for t in transactions if str(t.get('status')) in ('1', '2') and int(t.get('sum', 0) or 0) > 0]
    closed_txns.sort(key=lambda x: int(x.get('transaction_id', 0)), reverse=True)

    if not closed_txns:
        await update.message.reply_text("No transactions found for today.")
        return

    # Take requested count
    recent = closed_txns[:count]

    if not subscribed_chats:
        await update.message.reply_text("No subscribed chats to send to.")
        return

    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    sent_count = 0

    for txn in reversed(recent):  # Send oldest first
        logging.debug(f"Raw txn: {txn}")
        txn_id = txn.get('transaction_id')
        total = int(txn.get('sum', 0) or 0)
        profit = int(txn.get('total_profit', 0) or 0)
        payed_cash = int(txn.get('payed_cash', 0) or 0)
        payed_card = int(txn.get('payed_card', 0) or 0)
        table_name = txn.get('table_name', '')

        if payed_card > 0 and payed_cash > 0:
            payment = "💳+💵"
        elif payed_card > 0:
            payment = "💳 Card"
        else:
            payment = "💵 Cash"

        # Fetch items sold in this transaction
        items_str = ""
        try:
            products = fetch_transaction_products(txn_id)
            if products:
                items_list = []
                for p in products:
                    qty = float(p.get('num', 1))
                    name = p.get('product_name', 'Unknown')
                    if qty == 1:
                        items_list.append(name)
                    else:
                        items_list.append(f"{qty:.0f}x {name}")
                items_str = "\n<b>Items:</b> " + ", ".join(items_list)
        except Exception as e:
            logger.error(f"Failed to fetch products for txn {txn_id}: {e}")

        message = (
            f"🔄 <b>Resend Test - Sale #{txn_id}</b>\n\n"
            f"<b>Amount:</b> {format_currency(total)}\n"
            f"<b>Profit:</b> {format_currency(profit)}\n"
            f"<b>Payment:</b> {payment}\n"
            f"<b>Table:</b> {table_name}"
            f"{items_str}"
        )

        for chat_id in subscribed_chats.copy():
            try:
                result = await safe_send_message(bot, chat_id, message, parse_mode=ParseMode.HTML)
                if result:
                    sent_count += 1
            except Exception as e:
                logger.error(f"Failed to resend to {chat_id}: {e}")

    await update.message.reply_text(f"✅ Resent {len(recent)} transactions to {len(subscribed_chats)} chats ({sent_count} messages sent).")


@require_admin
async def loglevel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /loglevel command - view or set logging level."""
    valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR']

    if not context.args:
        # Show current level
        current_level = logging.getLevelName(logger.getEffectiveLevel())
        await update.message.reply_text(
            f"📋 <b>Current log level:</b> {current_level}\n\n"
            f"<b>Usage:</b> <code>/loglevel LEVEL</code>\n"
            f"<b>Valid levels:</b> {', '.join(valid_levels)}",
            parse_mode=ParseMode.HTML
        )
        return

    level_name = context.args[0].upper()

    if level_name not in valid_levels:
        await update.message.reply_text(
            f"❌ Invalid level: {level_name}\n"
            f"<b>Valid levels:</b> {', '.join(valid_levels)}",
            parse_mode=ParseMode.HTML
        )
        return

    # Set and persist log level
    if not set_log_level(level_name):
        await update.message.reply_text(
            f"❌ Failed to set log level: {level_name}",
            parse_mode=ParseMode.HTML
        )
        return

    # Apply to loggers
    level = getattr(logging, level_name)
    logger.setLevel(level)
    logging.getLogger().setLevel(level)  # Also set root logger

    await update.message.reply_text(
        f"✅ Log level set to <b>{level_name}</b> (saved)",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Log level changed to {level_name} by chat_id={update.effective_chat.id}")


@require_auth
async def products(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /products command - show products sold with quantities."""
    # Default to today, allow 'week' or 'month' as argument
    period = context.args[0].lower() if context.args else 'today'

    today_date = get_business_date()

    if period == 'week':
        monday = today_date - timedelta(days=today_date.weekday())
        date_from = monday.strftime('%Y%m%d')
        date_to = today_date.strftime('%Y%m%d')
        period_display = f"This Week ({monday.strftime('%d %b')} - {today_date.strftime('%d %b')})"
    elif period == 'month':
        first_day = today_date.replace(day=1)
        date_from = first_day.strftime('%Y%m%d')
        date_to = today_date.strftime('%Y%m%d')
        period_display = today_date.strftime('%B %Y')
    else:
        date_from = today_date.strftime('%Y%m%d')
        date_to = date_from
        period_display = today_date.strftime('%d %b %Y')

    await update.message.reply_text(f"⏳ Fetching product sales for {period_display}...")

    product_sales = fetch_product_sales(date_from, date_to)

    if not product_sales:
        await update.message.reply_text("No product sales found for this period.")
        return

    # Sort by quantity sold (descending)
    product_sales.sort(key=lambda x: float(x.get('count', 0)), reverse=True)

    # Calculate totals
    total_items = sum(float(p.get('count', 0)) for p in product_sales)
    total_revenue = sum(int(p.get('payed_sum', 0) or 0) for p in product_sales)
    total_profit = sum(int(p.get('product_profit', 0) or 0) for p in product_sales)

    message = f"🛒 <b>Products Sold - {period_display}</b>\n\n"
    message += f"<b>Total Items:</b> {total_items:.0f}\n"
    message += f"<b>Revenue:</b> {format_currency(total_revenue)}\n"
    message += f"<b>Profit:</b> {format_currency(total_profit)}\n"
    message += "\n<b>Top Products:</b>\n"
    message += "─" * 25 + "\n"

    # Show top 15 products
    for p in product_sales[:15]:
        name = p.get('product_name', 'Unknown')
        count = float(p.get('count', 0))
        revenue = int(p.get('payed_sum', 0) or 0)
        profit = int(p.get('product_profit', 0) or 0)

        # Truncate long names
        if len(name) > 18:
            name = name[:15] + "..."

        message += f"<code>{count:>4.0f}x</code> {name}\n"
        message += f"      {format_currency(revenue)} (P: {format_currency(profit)})\n"

    if len(product_sales) > 15:
        message += f"\n<i>... and {len(product_sales) - 15} more products</i>"

    message += f"\n\n<i>Usage: /products [today|week|month]</i>"

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

    # Generate and send chart
    try:
        chart = generate_products_chart(product_sales, f"Top Products - {period_display}")
        if chart:
            await update.message.reply_photo(photo=InputFile(chart, filename='products.png'))
    except Exception as e:
        logger.error(f"Failed to generate products chart: {e}")


@require_auth
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /stats command - show product sales statistics with comparisons."""
    period = context.args[0].lower() if context.args else 'today'

    today_date = get_business_date()

    # Calculate current and previous periods
    if period == 'week':
        monday = today_date - timedelta(days=today_date.weekday())
        current_from = monday.strftime('%Y%m%d')
        current_to = today_date.strftime('%Y%m%d')
        prev_monday = monday - timedelta(days=7)
        prev_sunday = monday - timedelta(days=1)
        prev_from = prev_monday.strftime('%Y%m%d')
        prev_to = prev_sunday.strftime('%Y%m%d')
        period_display = "This Week"
        prev_display = "Last Week"
        days_in_period = (today_date - monday).days + 1
    elif period == 'month':
        first_day = today_date.replace(day=1)
        current_from = first_day.strftime('%Y%m%d')
        current_to = today_date.strftime('%Y%m%d')
        last_month_end = first_day - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)
        prev_from = last_month_start.strftime('%Y%m%d')
        prev_to = last_month_end.strftime('%Y%m%d')
        period_display = today_date.strftime('%B')
        prev_display = last_month_end.strftime('%B')
        days_in_period = today_date.day
    else:
        current_from = today_date.strftime('%Y%m%d')
        current_to = current_from
        yesterday = today_date - timedelta(days=1)
        prev_from = yesterday.strftime('%Y%m%d')
        prev_to = prev_from
        period_display = "Today"
        prev_display = "Yesterday"
        days_in_period = 1

    await update.message.reply_text(f"⏳ Calculating statistics for {period_display}...")

    # Fetch current and previous period data
    current_sales = fetch_product_sales(current_from, current_to)
    prev_sales = fetch_product_sales(prev_from, prev_to)

    if not current_sales:
        await update.message.reply_text("No product sales found for this period.")
        return

    # Calculate totals
    total_items = sum(float(p.get('count', 0)) for p in current_sales)
    total_revenue = sum(int(p.get('payed_sum', 0) or 0) for p in current_sales)
    total_profit = sum(int(p.get('product_profit', 0) or 0) for p in current_sales)

    prev_items = sum(float(p.get('count', 0)) for p in prev_sales) if prev_sales else 0
    prev_revenue = sum(int(p.get('payed_sum', 0) or 0) for p in prev_sales) if prev_sales else 0

    # Calculate changes
    def calc_change(current, previous):
        if previous == 0:
            return "+∞%" if current > 0 else "0%"
        change = ((current - previous) / previous) * 100
        return f"+{change:.0f}%" if change >= 0 else f"{change:.0f}%"

    items_change = calc_change(total_items, prev_items)
    revenue_change = calc_change(total_revenue, prev_revenue)

    # Sort for different rankings
    by_quantity = sorted(current_sales, key=lambda x: float(x.get('count', 0)), reverse=True)
    by_revenue = sorted(current_sales, key=lambda x: int(x.get('payed_sum', 0) or 0), reverse=True)
    by_profit = sorted(current_sales, key=lambda x: int(x.get('product_profit', 0) or 0), reverse=True)

    # Calculate profit margins and sort
    for p in current_sales:
        revenue = int(p.get('payed_sum', 0) or 0)
        profit = int(p.get('product_profit', 0) or 0)
        p['margin'] = (profit / revenue * 100) if revenue > 0 else 0
    by_margin = sorted(current_sales, key=lambda x: x.get('margin', 0), reverse=True)

    message = f"📈 <b>Product Statistics - {period_display}</b>\n\n"

    # Summary with comparison
    message += f"<b>📊 Summary vs {prev_display}:</b>\n"
    message += f"Items: {total_items:.0f} ({items_change})\n"
    message += f"Revenue: {format_currency(total_revenue)} ({revenue_change})\n"
    message += f"Profit: {format_currency(total_profit)}\n"
    if days_in_period > 1:
        message += f"Avg/day: {format_currency(total_revenue // days_in_period)}\n"
    message += "\n"

    # Top 5 by quantity
    message += "<b>🏆 Top Sellers (qty):</b>\n"
    for p in by_quantity[:5]:
        name = p.get('product_name', 'Unknown')[:15]
        count = float(p.get('count', 0))
        message += f"  {count:.0f}x {name}\n"
    message += "\n"

    # Top 5 by revenue
    message += "<b>💰 Top Revenue:</b>\n"
    for p in by_revenue[:5]:
        name = p.get('product_name', 'Unknown')[:15]
        revenue = int(p.get('payed_sum', 0) or 0)
        message += f"  {format_currency(revenue)} {name}\n"
    message += "\n"

    # Top 5 by profit margin (only products with significant sales)
    significant = [p for p in by_margin if float(p.get('count', 0)) >= 2]
    if significant:
        message += "<b>📊 Best Margins:</b>\n"
        for p in significant[:5]:
            name = p.get('product_name', 'Unknown')[:15]
            margin = p.get('margin', 0)
            message += f"  {margin:.0f}% {name}\n"

    message += f"\n<i>Usage: /stats [today|week|month]</i>"

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

    # Generate and send comparison chart
    try:
        chart = generate_stats_chart(
            current_sales, prev_sales,
            f"Product Comparison - {period_display} vs {prev_display}",
            period_display, prev_display
        )
        if chart:
            await update.message.reply_photo(photo=InputFile(chart, filename='stats.png'))
    except Exception as e:
        logger.error(f"Failed to generate stats chart: {e}")


@require_auth
async def stock(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /stock command - show current inventory levels."""
    await update.message.reply_text("⏳ Fetching stock levels...")

    stock_data = fetch_stock_levels()

    if not stock_data:
        await update.message.reply_text("No stock data available.")
        return

    # Separate into low/negative stock and normal stock
    low_stock = []
    negative_stock = []
    normal_stock = []

    for item in stock_data:
        name = item.get('ingredient_name', 'Unknown')
        left = float(item.get('ingredient_left', 0))
        unit = item.get('ingredient_unit', '')
        limit = float(item.get('limit_value', 0))
        hidden = item.get('hidden', '0') == '1'

        if hidden:
            continue

        if left < 0:
            negative_stock.append((name, left, unit))
        elif limit > 0 and left <= limit:
            low_stock.append((name, left, unit, limit))
        elif left > 0:
            normal_stock.append((name, left, unit))

    message = "📦 <b>Stock Levels</b>\n\n"

    # Show negative stock (critical)
    if negative_stock:
        message += "🔴 <b>NEGATIVE STOCK (needs restock!):</b>\n"
        for name, left, unit in sorted(negative_stock, key=lambda x: x[1])[:10]:
            message += f"  ⚠️ {name}: {left:.2f} {unit}\n"
        message += "\n"

    # Show low stock (warning)
    if low_stock:
        message += "🟡 <b>LOW STOCK (below limit):</b>\n"
        for name, left, unit, limit in sorted(low_stock, key=lambda x: x[1])[:10]:
            message += f"  ⚠️ {name}: {left:.2f}/{limit:.0f} {unit}\n"
        message += "\n"

    if not negative_stock and not low_stock:
        message += "✅ All items are well stocked!\n\n"

    # Summary stats
    total_items = len([s for s in stock_data if s.get('hidden', '0') != '1'])
    items_with_stock = len([s for s in stock_data if float(s.get('ingredient_left', 0)) > 0 and s.get('hidden', '0') != '1'])
    message += f"<b>Summary:</b> {items_with_stock}/{total_items} items in stock"

    if len(negative_stock) > 0:
        message += f"\n⚠️ {len(negative_stock)} items need immediate restock!"

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


@require_auth
async def ingredients(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /ingredients command - show most used ingredients."""
    period = context.args[0].lower() if context.args else 'week'

    today_date = get_business_date()

    if period == 'month':
        first_day = today_date.replace(day=1)
        date_from = first_day.strftime('%Y%m%d')
        date_to = today_date.strftime('%Y%m%d')
        period_display = today_date.strftime('%B')
    elif period == 'today':
        date_from = today_date.strftime('%Y%m%d')
        date_to = date_from
        period_display = "Today"
    else:  # week
        monday = today_date - timedelta(days=today_date.weekday())
        date_from = monday.strftime('%Y%m%d')
        date_to = today_date.strftime('%Y%m%d')
        period_display = "This Week"

    await update.message.reply_text(f"⏳ Fetching ingredient usage for {period_display}...")

    usage_data = fetch_ingredient_usage(date_from, date_to)

    if not usage_data:
        await update.message.reply_text("No ingredient usage data available.")
        return

    # Filter to items with actual usage (write_offs > 0)
    used_items = [
        item for item in usage_data
        if float(item.get('write_offs', 0)) > 0
    ]

    if not used_items:
        await update.message.reply_text(f"No ingredients used during {period_display}.")
        return

    # Sort by usage (write_offs) descending
    used_items.sort(key=lambda x: float(x.get('write_offs', 0)), reverse=True)

    message = f"🧪 <b>Ingredient Usage - {period_display}</b>\n\n"
    message += f"<b>Total ingredients used:</b> {len(used_items)}\n\n"
    message += "<b>Top Used Ingredients:</b>\n"
    message += "─" * 25 + "\n"

    for item in used_items[:20]:
        name = item.get('ingredient_name', 'Unknown')
        usage = float(item.get('write_offs', 0))
        # Try to determine unit from the data or default
        # The API returns units based on ingredient type

        # Truncate long names
        if len(name) > 20:
            name = name[:17] + "..."

        # Format usage nicely
        if usage >= 1:
            usage_str = f"{usage:.1f}"
        else:
            usage_str = f"{usage:.3f}"

        message += f"  <code>{usage_str:>8}</code> {name}\n"

    if len(used_items) > 20:
        message += f"\n<i>... and {len(used_items) - 20} more ingredients</i>"

    message += f"\n\n<i>Usage: /ingredients [today|week|month]</i>"

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

    # Generate and send chart
    try:
        chart = generate_ingredients_chart(used_items, f"Ingredient Usage - {period_display}")
        if chart:
            await update.message.reply_photo(photo=InputFile(chart, filename='ingredients.png'))
    except Exception as e:
        logger.error(f"Failed to generate ingredients chart: {e}")


@require_auth
async def today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /today command - get today's summary."""
    today_str = get_business_date().strftime('%Y%m%d')
    today_display = get_business_date().strftime('%d %b %Y')

    await update.message.reply_text("⏳ Fetching today's data...")

    transactions = fetch_transactions(today_str)
    finance_txns = fetch_finance_transactions(today_str)

    active_txns = [t for t in transactions
                   if str(t.get('status', '')) in ('1', '2') and int(t.get('sum', 0) or 0) > 0]
    summary = calculate_summary(active_txns)
    expenses = calculate_expenses(finance_txns)
    message = format_summary_message(today_display, summary, expenses)

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


@require_auth
async def week(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /week command - get this week's summary."""
    today_date = get_business_date()
    monday = today_date - timedelta(days=today_date.weekday())

    date_from = monday.strftime('%Y%m%d')
    date_to = today_date.strftime('%Y%m%d')
    week_display = f"{monday.strftime('%d %b')} - {today_date.strftime('%d %b %Y')}"

    await update.message.reply_text("⏳ Fetching data for this week...")

    transactions = fetch_transactions(date_from, date_to)
    finance_txns = fetch_finance_transactions(date_from, date_to)

    summary_data = calculate_summary(transactions)
    expenses_data = calculate_expenses(finance_txns)

    days_count = (today_date - monday).days + 1
    avg_sales = summary_data['total_sales'] // days_count if days_count > 0 else 0
    avg_profit = summary_data['total_profit'] // days_count if days_count > 0 else 0
    net_profit = summary_data['total_sales'] - expenses_data['total_expenses']

    message = (
        f"📅 <b>Weekly Report</b>\n"
        f"<i>{week_display}</i>\n\n"
        f"<b>Transactions:</b> {summary_data['transaction_count']}\n"
        f"<b>Total Sales:</b> {format_currency(summary_data['total_sales'])}\n"
        f"<b>Gross Profit:</b> {format_currency(summary_data['total_profit'])}\n\n"
        f"<b>💵 Cash:</b> {format_currency(summary_data['cash_sales'])}\n"
        f"<b>💳 Card:</b> {format_currency(summary_data['card_sales'])}\n\n"
        f"<b>💸 Expenses:</b> -{format_currency(expenses_data['total_expenses'])}\n"
        f"<b>💰 Net Profit:</b> {format_currency(net_profit)}\n\n"
        f"<b>📊 Daily Average:</b>\n"
        f"• Sales: {format_currency(avg_sales)}\n"
        f"• Gross Profit: {format_currency(avg_profit)}"
    )

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

    # Generate and send chart
    if transactions:
        chart = generate_sales_chart(transactions, monday, today_date, f"Weekly Profit & Expenses ({week_display})", finance_txns)
        await update.message.reply_photo(photo=chart, caption="📊 Daily breakdown")


@require_auth
async def month(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /month command - get this month's summary."""
    today_date = get_business_date()
    first_of_month = today_date.replace(day=1)

    date_from = first_of_month.strftime('%Y%m%d')
    date_to = today_date.strftime('%Y%m%d')
    month_display = today_date.strftime('%B %Y')

    await update.message.reply_text(f"⏳ Fetching data for {month_display}...")

    transactions = fetch_transactions(date_from, date_to)
    finance_txns = fetch_finance_transactions(date_from, date_to)

    summary_data = calculate_summary(transactions)
    expenses_data = calculate_expenses(finance_txns)

    days_count = today_date.day
    avg_sales = summary_data['total_sales'] // days_count if days_count > 0 else 0
    avg_profit = summary_data['total_profit'] // days_count if days_count > 0 else 0
    net_profit = summary_data['total_sales'] - expenses_data['total_expenses']

    message = (
        f"📆 <b>Monthly Report</b>\n"
        f"<i>{month_display}</i>\n\n"
        f"<b>Transactions:</b> {summary_data['transaction_count']}\n"
        f"<b>Total Sales:</b> {format_currency(summary_data['total_sales'])}\n"
        f"<b>Gross Profit:</b> {format_currency(summary_data['total_profit'])}\n\n"
        f"<b>💵 Cash:</b> {format_currency(summary_data['cash_sales'])}\n"
        f"<b>💳 Card:</b> {format_currency(summary_data['card_sales'])}\n\n"
        f"<b>💸 Expenses:</b> -{format_currency(expenses_data['total_expenses'])}\n"
        f"<b>💰 Net Profit:</b> {format_currency(net_profit)}\n\n"
        f"<b>📊 Daily Average:</b>\n"
        f"• Sales: {format_currency(avg_sales)}\n"
        f"• Gross Profit: {format_currency(avg_profit)}"
    )

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)

    # Generate and send chart
    if transactions:
        chart = generate_sales_chart(transactions, first_of_month, today_date, f"Monthly Profit & Expenses ({month_display})", finance_txns)
        await update.message.reply_photo(photo=chart, caption="📊 Daily breakdown")


@require_auth
async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /summary command - get summary for a specific date or date range."""
    if not context.args:
        await update.message.reply_text(
            "Please provide a date or date range.\n"
            "Usage:\n"
            "/summary YYYYMMDD - Single date\n"
            "/summary YYYYMMDD YYYYMMDD - Date range\n\n"
            "Examples:\n"
            "/summary 20260120\n"
            "/summary 20260115 20260120"
        )
        return

    # Parse first date
    try:
        date_from = datetime.strptime(context.args[0], '%Y%m%d')
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid date format.\n"
            "Use YYYYMMDD format.\n"
            "Example: /summary 20260120"
        )
        return

    # Check if second date provided
    if len(context.args) >= 2:
        try:
            date_to = datetime.strptime(context.args[1], '%Y%m%d')
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid end date format.\n"
                "Use YYYYMMDD format.\n"
                "Example: /summary 20260115 20260120"
            )
            return

        # Ensure date_from is before date_to
        if date_from > date_to:
            date_from, date_to = date_to, date_from

        date_from_str = date_from.strftime('%Y%m%d')
        date_to_str = date_to.strftime('%Y%m%d')
        date_display = f"{date_from.strftime('%d %b')} - {date_to.strftime('%d %b %Y')}"

        await update.message.reply_text(f"⏳ Fetching data for {date_display}...")

        transactions = fetch_transactions(date_from_str, date_to_str)
        finance_txns = fetch_finance_transactions(date_from_str, date_to_str)

        summary_data = calculate_summary(transactions)
        expenses_data = calculate_expenses(finance_txns)

        # Calculate daily average for range
        days_count = (date_to - date_from).days + 1
        avg_sales = summary_data['total_sales'] // days_count if days_count > 0 else 0
        avg_profit = summary_data['total_profit'] // days_count if days_count > 0 else 0
        net_profit = summary_data['total_profit'] - expenses_data['total_expenses']

        message = (
            f"📊 <b>Summary for {date_display}</b>\n\n"
            f"<b>Transactions:</b> {summary_data['transaction_count']}\n"
            f"<b>Total Sales:</b> {format_currency(summary_data['total_sales'])}\n"
            f"<b>Gross Profit:</b> {format_currency(summary_data['total_profit'])}\n\n"
            f"<b>💵 Cash:</b> {format_currency(summary_data['cash_sales'])}\n"
            f"<b>💳 Card:</b> {format_currency(summary_data['card_sales'])}\n\n"
            f"<b>💸 Expenses:</b> -{format_currency(expenses_data['total_expenses'])}\n"
            f"<b>💰 Net Profit:</b> {format_currency(net_profit)}\n\n"
            f"<b>📊 Daily Average ({days_count} days):</b>\n"
            f"• Sales: {format_currency(avg_sales)}\n"
            f"• Gross Profit: {format_currency(avg_profit)}"
        )

        await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        # Generate and send chart for date range
        if transactions and days_count > 1:
            chart = generate_sales_chart(transactions, date_from.date(), date_to.date(), f"Profit & Expenses ({date_display})", finance_txns)
            await update.message.reply_photo(photo=chart, caption="📊 Daily breakdown")
        return

    # Single date
    date_str = date_from.strftime('%Y%m%d')
    date_display = date_from.strftime('%d %b %Y')

    await update.message.reply_text(f"⏳ Fetching data for {date_display}...")

    transactions = fetch_transactions(date_str)
    finance_txns = fetch_finance_transactions(date_str)

    summary_data = calculate_summary(transactions)
    expenses_data = calculate_expenses(finance_txns)
    message = format_summary_message(date_display, summary_data, expenses_data)

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


@require_auth
async def cash(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /cash command - get current cash register status."""
    await update.message.reply_text("⏳ Fetching cash register data...")

    shifts = fetch_cash_shifts()

    if not shifts:
        await update.message.reply_text("❌ Could not fetch cash register data.")
        return

    latest_shift = shifts[0]

    shift_start = adjust_poster_time(latest_shift.get('date_start', 'Unknown'))
    shift_end = adjust_poster_time(latest_shift.get('date_end', ''))
    amount_start = int(latest_shift.get('amount_start', 0) or 0)
    amount_end = int(latest_shift.get('amount_end', 0) or 0)
    cash_sales = int(latest_shift.get('amount_sell_cash', 0) or 0)
    cash_out = int(latest_shift.get('amount_credit', 0) or 0)
    staff = latest_shift.get('comment', 'Unknown')

    if shift_end:
        status = "🔴 Closed"
        current_cash = amount_end
        shift_info = f"<b>Ended:</b> {shift_end}"
    else:
        status = "🟢 Open"
        current_cash = amount_start + cash_sales - cash_out
        shift_info = f"<b>Started:</b> {shift_start}"

    message = (
        f"💵 <b>Cash Register</b>\n\n"
        f"<b>Status:</b> {status}\n"
        f"<b>Staff:</b> {staff}\n"
        f"{shift_info}\n\n"
        f"<b>Current Cash:</b> {format_currency(current_cash)}\n\n"
        f"<b>Shift Details:</b>\n"
        f"• Opening: {format_currency(amount_start)}\n"
        f"• Cash Sales: +{format_currency(cash_sales)}\n"
        f"• Cash Out: -{format_currency(cash_out)}\n"
        f"• <b>Net: {format_currency(cash_sales - cash_out)}</b>"
    )

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


@require_auth
async def expenses(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /expenses command - get detailed expense breakdown."""
    # Default to today if no date specified
    if not context.args:
        date_from = get_business_date()
        date_to = date_from
        date_display = date_from.strftime('%d %b %Y')
    elif len(context.args) == 1:
        try:
            date_from = datetime.strptime(context.args[0], '%Y%m%d').date()
            date_to = date_from
            date_display = date_from.strftime('%d %b %Y')
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid date format.\n"
                "Use YYYYMMDD format.\n"
                "Examples:\n"
                "/expenses - Today's expenses\n"
                "/expenses 20260120 - Specific date\n"
                "/expenses 20260115 20260120 - Date range"
            )
            return
    else:
        try:
            date_from = datetime.strptime(context.args[0], '%Y%m%d').date()
            date_to = datetime.strptime(context.args[1], '%Y%m%d').date()
            if date_from > date_to:
                date_from, date_to = date_to, date_from
            date_display = f"{date_from.strftime('%d %b')} - {date_to.strftime('%d %b %Y')}"
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid date format.\n"
                "Use YYYYMMDD format.\n"
                "Example: /expenses 20260115 20260120"
            )
            return

    await update.message.reply_text(f"⏳ Fetching expenses for {date_display}...")

    date_from_str = date_from.strftime('%Y%m%d')
    date_to_str = date_to.strftime('%Y%m%d')

    finance_txns = fetch_finance_transactions(date_from_str, date_to_str)
    expenses_data = calculate_expenses(finance_txns)

    if not expenses_data['expense_list']:
        await update.message.reply_text(
            f"💸 <b>Expenses for {date_display}</b>\n\n"
            "No expenses recorded.",
            parse_mode=ParseMode.HTML
        )
        return

    # Group expenses by category
    by_category = {}
    for exp in expenses_data['expense_list']:
        cat = exp['category'] or 'Uncategorized'
        if cat not in by_category:
            by_category[cat] = {'total': 0, 'items': []}
        by_category[cat]['total'] += exp['amount']
        by_category[cat]['items'].append(exp)

    message = f"💸 <b>Expenses for {date_display}</b>\n\n"
    message += f"<b>Total:</b> -{format_currency(expenses_data['total_expenses'])}\n\n"

    for category, data in sorted(by_category.items(), key=lambda x: x[1]['total'], reverse=True):
        message += f"<b>{category}:</b> {format_currency(data['total'])}\n"
        for item in data['items'][:5]:  # Show top 5 per category
            comment = item['comment'][:30] + '...' if len(item['comment']) > 30 else item['comment']
            if comment:
                message += f"  • {comment}: {format_currency(item['amount'])}\n"
            else:
                message += f"  • {format_currency(item['amount'])}\n"
        if len(data['items']) > 5:
            message += f"  <i>... and {len(data['items']) - 5} more</i>\n"
        message += "\n"

    await update.message.reply_text(message.strip(), parse_mode=ParseMode.HTML)


def fetch_removed_transactions(date_from, date_to=None):
    """Fetch removed/voided transactions from Poster API."""
    url = f"{POSTER_API_URL}/dash.getTransactions"
    params = {
        "token": config.POSTER_ACCESS_TOKEN,
        "dateFrom": date_from,
        "dateTo": date_to or date_from,
        "status": "3"  # Status 3 = removed/voided
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("response", [])
    except requests.RequestException as e:
        logger.error(f"Failed to fetch removed transactions: {e}")
        return []


@require_auth
async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /subscribe command - enable real-time sale notifications."""
    chat_id = str(update.effective_chat.id)

    if chat_id in subscribed_chats:
        await update.message.reply_text("✅ You're already subscribed to real-time updates.")
        return

    subscribed_chats.add(chat_id)
    save_config()
    await update.message.reply_text(
        "🔔 <b>Subscribed!</b>\n\n"
        "You'll now receive notifications for each new sale.\n"
        "Use /unsubscribe to stop.",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Chat {chat_id} subscribed to real-time updates")


@require_auth
async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /unsubscribe command - disable real-time sale notifications."""
    chat_id = str(update.effective_chat.id)

    if chat_id not in subscribed_chats:
        await update.message.reply_text("ℹ️ You're not subscribed to real-time updates.")
        return

    subscribed_chats.discard(chat_id)
    save_config()
    await update.message.reply_text(
        "🔕 <b>Unsubscribed!</b>\n\n"
        "You'll no longer receive real-time sale notifications.",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Chat {chat_id} unsubscribed from real-time updates")


@require_auth
async def alerts_on(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /alerts command - enable theft detection alerts."""
    chat_id = str(update.effective_chat.id)

    if chat_id in theft_alert_chats:
        await update.message.reply_text("✅ Theft detection alerts are already enabled.")
        return

    theft_alert_chats.add(chat_id)
    save_config()
    await update.message.reply_text(
        "🚨 <b>Theft Detection Enabled!</b>\n\n"
        "You'll receive alerts for:\n"
        "• Voided/cancelled transactions\n"
        "• Large discounts (>20%)\n"
        "• Orders closed without payment\n"
        "• Large expenses (>฿1,000)\n"
        "• Cash register discrepancies\n\n"
        "Use /alerts_off to disable.",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Chat {chat_id} enabled theft detection alerts")


@require_auth
async def alerts_off(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /alerts_off command - disable theft detection alerts."""
    chat_id = str(update.effective_chat.id)

    if chat_id not in theft_alert_chats:
        await update.message.reply_text("ℹ️ Theft detection alerts are not enabled.")
        return

    theft_alert_chats.discard(chat_id)
    save_config()
    await update.message.reply_text(
        "🔕 <b>Theft Detection Disabled!</b>\n\n"
        "You'll no longer receive theft alerts.",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Chat {chat_id} disabled theft detection alerts")


async def send_theft_alert(alert_type, message):
    """Send theft alert to all subscribed chats."""
    if not theft_alert_chats or not TELEGRAM_BOT_TOKEN:
        return

    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    for chat_id in theft_alert_chats.copy():
        try:
            result = await safe_send_message(bot, chat_id, message, parse_mode=ParseMode.HTML)
            if result is None:
                logger.warning(f"Failed to send theft alert to {chat_id}")
        except Conflict:
            logger.error("Bot conflict detected in send_theft_alert")
            return  # Stop sending, another instance is running
        except Exception as e:
            logger.error(f"Failed to send theft alert to {chat_id}: {e}")
            if "chat not found" in str(e).lower() or "bot was blocked" in str(e).lower():
                theft_alert_chats.discard(chat_id)
                save_config()


async def check_theft_indicators():
    """Check for potential theft indicators."""
    global last_seen_void_id, last_cash_balance, last_alerted_transaction_id, last_alerted_expense_id

    if not theft_alert_chats:
        return

    today_str = get_business_date().strftime('%Y%m%d')

    try:
        # Check for voided transactions
        voided = fetch_removed_transactions(today_str)
        if voided:
            voided.sort(key=lambda x: int(x.get('transaction_id', 0)), reverse=True)
            latest_void = voided[0]
            latest_void_id = latest_void.get('transaction_id')

            if last_seen_void_id is None:
                last_seen_void_id = latest_void_id
                config.last_seen_void_id = last_seen_void_id
                save_config()
            elif latest_void_id != last_seen_void_id:
                # New void detected
                new_voids = [
                    v for v in voided
                    if int(v.get('transaction_id', 0)) > int(last_seen_void_id or 0)
                ]
                last_seen_void_id = latest_void_id

                for void_txn in new_voids:
                    amount = int(void_txn.get('sum', 0) or 0)
                    reason = void_txn.get('reason', 'No reason given')
                    staff = void_txn.get('name', 'Unknown')
                    table = void_txn.get('table_name', 'N/A')

                    alert_msg = (
                        f"🚨 <b>VOID ALERT</b>\n\n"
                        f"<b>Amount:</b> {format_currency(amount)}\n"
                        f"<b>Staff:</b> {staff}\n"
                        f"<b>Table:</b> {table}\n"
                        f"<b>Reason:</b> {reason}\n\n"
                        f"⚠️ Please verify this void was legitimate."
                    )
                    await send_theft_alert("void", alert_msg)

        # Check for suspicious transactions
        transactions = fetch_transactions(today_str)
        # Sort by transaction ID ascending to process in order
        transactions.sort(key=lambda x: int(x.get('transaction_id', 0) or 0))
        for txn in transactions:
            txn_id = int(txn.get('transaction_id', 0) or 0)

            # Skip if we've already checked this transaction
            if txn_id <= last_alerted_transaction_id:
                continue

            total = int(txn.get('sum', 0) or 0)
            payed_sum = int(txn.get('payed_sum', 0) or 0)
            discount = int(txn.get('discount', 0) or 0)
            status = txn.get('status', '')
            staff = txn.get('name', 'Unknown')
            table = txn.get('table_name', 'N/A')

            # Check for closed order without payment (or underpayment)
            if status == '2' and total > 0:  # Status 2 = closed
                if payed_sum == 0:
                    # Closed with NO payment - high alert!
                    alert_msg = (
                        f"🚨 <b>NO PAYMENT ALERT</b>\n\n"
                        f"<b>Order closed without payment!</b>\n\n"
                        f"<b>Order Amount:</b> {format_currency(total)}\n"
                        f"<b>Paid:</b> {format_currency(0)}\n"
                        f"<b>Staff:</b> {staff}\n"
                        f"<b>Table:</b> {table}\n"
                        f"<b>Transaction:</b> #{txn_id}\n\n"
                        f"🚨 This requires immediate investigation!"
                    )
                    await send_theft_alert("no_payment", alert_msg)
                elif payed_sum < total:
                    # Partial payment - also suspicious
                    shortage = total - payed_sum
                    alert_msg = (
                        f"⚠️ <b>UNDERPAYMENT ALERT</b>\n\n"
                        f"<b>Order Amount:</b> {format_currency(total)}\n"
                        f"<b>Paid:</b> {format_currency(payed_sum)}\n"
                        f"<b>Shortage:</b> {format_currency(shortage)}\n"
                        f"<b>Staff:</b> {staff}\n"
                        f"<b>Table:</b> {table}\n"
                        f"<b>Transaction:</b> #{txn_id}\n\n"
                        f"⚠️ Please verify this was authorized."
                    )
                    await send_theft_alert("underpayment", alert_msg)

            # Check for large discounts
            if total > 0 and discount > 0:
                original = total + discount
                discount_pct = (discount / original) * 100

                if discount_pct > LARGE_DISCOUNT_THRESHOLD:
                    alert_msg = (
                        f"⚠️ <b>LARGE DISCOUNT ALERT</b>\n\n"
                        f"<b>Discount:</b> {discount_pct:.1f}% ({format_currency(discount)})\n"
                        f"<b>Final Amount:</b> {format_currency(total)}\n"
                        f"<b>Staff:</b> {staff}\n"
                        f"<b>Table:</b> {table}\n"
                        f"<b>Transaction:</b> #{txn_id}\n\n"
                        f"⚠️ Please verify this discount was authorized."
                    )
                    await send_theft_alert("discount", alert_msg)

            # Update after processing each transaction (sorted ascending)
            last_alerted_transaction_id = txn_id

        # Check cash register discrepancies
        shifts = fetch_cash_shifts()
        if shifts:
            latest_shift = shifts[0]
            if latest_shift.get('date_end'):  # Shift is closed
                expected = int(latest_shift.get('amount_start', 0) or 0) + \
                          int(latest_shift.get('amount_sell_cash', 0) or 0) - \
                          int(latest_shift.get('amount_credit', 0) or 0)
                actual = int(latest_shift.get('amount_end', 0) or 0)

                discrepancy = actual - expected

                if last_cash_balance != actual and abs(discrepancy) > 10000:  # > 100 THB
                    last_cash_balance = actual
                    staff = latest_shift.get('comment', 'Unknown')

                    if discrepancy < 0:
                        alert_msg = (
                            f"🚨 <b>CASH SHORTAGE ALERT</b>\n\n"
                            f"<b>Missing:</b> {format_currency(abs(discrepancy))}\n"
                            f"<b>Expected:</b> {format_currency(expected)}\n"
                            f"<b>Actual:</b> {format_currency(actual)}\n"
                            f"<b>Staff:</b> {staff}\n\n"
                            f"⚠️ Cash drawer is short!"
                        )
                        await send_theft_alert("shortage", alert_msg)
                    else:
                        alert_msg = (
                            f"⚠️ <b>CASH OVERAGE ALERT</b>\n\n"
                            f"<b>Extra:</b> {format_currency(discrepancy)}\n"
                            f"<b>Expected:</b> {format_currency(expected)}\n"
                            f"<b>Actual:</b> {format_currency(actual)}\n"
                            f"<b>Staff:</b> {staff}\n\n"
                            f"ℹ️ Cash drawer has extra money (possible missed sale)."
                        )
                        await send_theft_alert("overage", alert_msg)

        # Check for large expenses
        finance_txns = fetch_finance_transactions(today_str)
        expenses_data = calculate_expenses(finance_txns)
        expense_list = expenses_data['expense_list']
        # Sort by transaction ID ascending to process in order
        expense_list.sort(key=lambda x: int(x.get('transaction_id', 0) or 0))

        for expense in expense_list:
            expense_id = int(expense.get('transaction_id', 0) or 0)
            if expense_id <= last_alerted_expense_id:
                continue

            if expense['amount'] >= LARGE_EXPENSE_THRESHOLD:
                comment = expense['comment'] or 'No description'
                category = expense['category'] or 'Uncategorized'

                alert_msg = (
                    f"⚠️ <b>LARGE EXPENSE ALERT</b>\n\n"
                    f"<b>Amount:</b> {format_currency(expense['amount'])}\n"
                    f"<b>Category:</b> {category}\n"
                    f"<b>Description:</b> {comment}\n"
                    f"<b>Date:</b> {expense['date']}\n\n"
                    f"⚠️ Please verify this expense was authorized."
                )
                await send_theft_alert("large_expense", alert_msg)

            # Update after processing each expense (sorted ascending)
            last_alerted_expense_id = expense_id

        # Sync state back to config module before saving
        config.last_seen_void_id = last_seen_void_id
        config.last_cash_balance = last_cash_balance
        config.last_alerted_transaction_id = last_alerted_transaction_id
        config.last_alerted_expense_id = last_alerted_expense_id
        # Save state after checking to persist alerted items
        save_config()

    except Exception as e:
        logger.error(f"Error in theft detection: {e}")


async def check_new_transactions():
    """Poll for new transactions and notify subscribed chats."""
    global notified_transaction_ids, notified_transaction_date

    if not subscribed_chats:
        return

    if not TELEGRAM_BOT_TOKEN:
        return

    try:
        # Check for business date rollover — clear the set when the day changes
        current_business_date = get_business_date().isoformat()
        if notified_transaction_date != current_business_date:
            notified_transaction_ids = set()
            notified_transaction_date = current_business_date
            config.notified_transaction_ids = notified_transaction_ids
            config.notified_transaction_date = notified_transaction_date
            save_config()
            logger.info(f"Business date changed to {current_business_date}, cleared notified set")

        # Fetch today's transactions
        today_str = get_business_date().strftime('%Y%m%d')
        transactions = fetch_transactions(today_str)

        if not transactions:
            logger.debug("No transactions found for today")
            return

        # First run — seed the set with all currently closed transaction IDs (don't spam)
        if not notified_transaction_ids:
            for txn in transactions:
                status = str(txn.get('status', ''))
                total = int(txn.get('sum', 0) or 0)
                if status == '2' and total > 0:
                    notified_transaction_ids.add(str(txn.get('transaction_id', '')))
            config.notified_transaction_ids = notified_transaction_ids
            save_config()
            logger.info(f"Seeded notified set with {len(notified_transaction_ids)} existing transactions")
            return

        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        notifications_sent = 0
        new_count = 0

        for txn in transactions:
            txn_id_str = str(txn.get('transaction_id', ''))
            status = str(txn.get('status', ''))
            total = int(txn.get('sum', 0) or 0)

            # Only notify for closed transactions with actual sales, not yet notified
            if status != '2' or total <= 0 or txn_id_str in notified_transaction_ids:
                continue

            new_count += 1
            txn_id = int(txn.get('transaction_id', 0) or 0)
            # Debug: log raw transaction data
            logger.debug(f"Raw transaction data for {txn_id}: {txn}")
            profit = int(txn.get('total_profit', 0) or 0)
            logger.debug(f"Parsed values - total: {total}, profit: {profit}")
            payed_cash = int(txn.get('payed_cash', 0) or 0)
            payed_card = int(txn.get('payed_card', 0) or 0)
            table_name = txn.get('table_name', '')
            close_time = adjust_poster_time(txn.get('date_close_date', ''))
            time_str = close_time.split(' ')[1][:5] if ' ' in close_time else ''

            if payed_card > 0 and payed_cash > 0:
                payment = "💳+💵"
            elif payed_card > 0:
                payment = "💳 Card"
            else:
                payment = "💵 Cash"

            # Fetch items sold in this transaction
            items_str = ""
            try:
                products = fetch_transaction_products(txn_id)
                if products:
                    items_list = []
                    for p in products:
                        qty = float(p.get('num', 1))
                        name = p.get('product_name', 'Unknown')
                        if qty == 1:
                            items_list.append(name)
                        else:
                            items_list.append(f"{qty:.0f}x {name}")
                    items_str = "\n<b>Items:</b> " + ", ".join(items_list)
            except Exception as e:
                logger.error(f"Failed to fetch products for txn {txn_id}: {e}")

            message = (
                f"💵 <b>Cha-ching!</b>\n\n"
                f"<b>Time:</b> {time_str}\n"
                f"<b>Amount:</b> {format_currency(total)}\n"
                f"<b>Profit:</b> {format_currency(profit)}\n"
                f"<b>Payment:</b> {payment}\n"
                f"<b>Table:</b> {table_name}"
                f"{items_str}"
            )

            for chat_id in subscribed_chats.copy():
                try:
                    result = await safe_send_message(bot, chat_id, message, parse_mode=ParseMode.HTML)
                    if result is None:
                        logger.warning(f"Failed to send notification for txn {txn_id} to {chat_id}")
                    else:
                        notifications_sent += 1
                except Conflict:
                    logger.error("Bot conflict detected in check_new_transactions")
                    return  # Stop, another instance is running
                except Exception as e:
                    logger.error(f"Failed to send to {chat_id}: {e}")
                    # Remove invalid chats
                    if "chat not found" in str(e).lower() or "bot was blocked" in str(e).lower():
                        subscribed_chats.discard(chat_id)
                        save_config()

            # Broadcast to WebSocket dashboard clients
            try:
                from dashboard import broadcast_sale
                await broadcast_sale({
                    "transaction_id": txn_id,
                    "sum": total,
                    "total_profit": profit,
                    "payed_cash": payed_cash,
                    "payed_card": payed_card,
                    "table_name": table_name,
                    "close_time": close_time,
                    "items": items_str,
                })
            except Exception as e:
                logger.debug(f"Dashboard broadcast failed: {e}")

            # Mark as notified after successful processing
            notified_transaction_ids.add(txn_id_str)
            config.notified_transaction_ids = notified_transaction_ids
            save_config()

        if new_count > 0:
            logger.info(f"Sent {notifications_sent} notifications for {new_count} new transactions")
        else:
            logger.debug(f"No new transactions (notified set size: {len(notified_transaction_ids)})")

    except Conflict:
        logger.error("Bot conflict detected - another instance may be running")
    except Exception as e:
        logger.error(f"Error checking new transactions: {e}", exc_info=True)


async def send_daily_summary():
    """Send daily summary at midnight."""
    if not TELEGRAM_CHAT_ID or not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_CHAT_ID or BOT_TOKEN not set, skipping scheduled summary")
        return

    today_str = get_business_date().strftime('%Y%m%d')
    today_display = get_business_date().strftime('%d %b %Y')

    transactions = fetch_transactions(today_str)
    summary_data = calculate_summary(transactions)

    message = f"🌙 <b>End of Day Report</b>\n\n" + format_summary_message(today_display, summary_data)[3:]

    try:
        bot = Bot(token=TELEGRAM_BOT_TOKEN)
        result = await safe_send_message(bot, TELEGRAM_CHAT_ID, message, parse_mode=ParseMode.HTML)
        if result:
            logger.info("Daily summary sent successfully")
        else:
            logger.error("Failed to send daily summary")
    except Conflict:
        logger.error("Bot conflict detected in send_daily_summary")
    except Exception as e:
        logger.error(f"Failed to send daily summary: {e}")


@require_auth
async def dashboard_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /dashboard command - show the dashboard URL."""
    from dashboard import get_dashboard_url

    chat_id = str(update.effective_chat.id)
    dashboard_url = get_dashboard_url()

    has_password = chat_id in config.approved_users and config.approved_users[chat_id].get("password_hash")

    user = update.effective_user
    username = f"@{user.username}" if user and user.username else f"id:{chat_id}"

    if has_password:
        await update.message.reply_text(
            f"<b>Web Dashboard</b>\n\n"
            f'<a href="{dashboard_url}">Open Dashboard</a>\n\n'
            f"Username: <code>{username}</code>\n"
            f"Password: the one you set via /setpassword",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )
    else:
        await update.message.reply_text(
            f"<b>Web Dashboard</b>\n\n"
            f"You need to set a password first.\n"
            f"Use: /setpassword &lt;your_password&gt;\n\n"
            f"Your login username will be: <code>{username}</code>\n"
            f"Dashboard: {dashboard_url}",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True
        )


@require_auth
async def setpassword_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /setpassword command - set dashboard login password."""
    import hashlib as _hashlib

    chat_id = str(update.effective_chat.id)
    user = update.effective_user
    username = f"@{user.username}" if user and user.username else f"id:{chat_id}"

    if not context.args:
        await update.message.reply_text(
            "Usage: /setpassword &lt;password&gt;\n\n"
            "This sets your password for the web dashboard. "
            "You'll log in with your Telegram username and this password.",
            parse_mode=ParseMode.HTML
        )
        return

    password = " ".join(context.args)
    if len(password) < 4:
        await update.message.reply_text("Password must be at least 4 characters.")
        return

    # Hash with salt
    salt = os.urandom(16).hex()
    password_hash = _hashlib.sha256(f"{salt}{password}".encode()).hexdigest()

    # Ensure user has an approved_users entry (admins may not have one yet)
    if chat_id not in config.approved_users:
        config.approved_users[chat_id] = {
            'name': user.full_name if user else username,
            'username': user.username if user else None,
            'approved_at': datetime.now().isoformat(),
        }
    config.approved_users[chat_id]["password_hash"] = f"{salt}${password_hash}"
    save_config()

    from dashboard import get_dashboard_url
    dashboard_url = get_dashboard_url()

    await update.message.reply_text(
        f"Dashboard password set.\n\n"
        f"Username: <code>{username}</code>\n"
        f"Dashboard: {dashboard_url}",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True
    )


@require_auth
async def setgoal_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /setgoal command - set monthly profit goal."""
    if not context.args:
        current = format_currency(config.monthly_goal) if config.monthly_goal else "not set"
        await update.message.reply_text(
            f"Usage: /setgoal &lt;amount_in_THB&gt;\n\n"
            f"Sets your monthly gross profit goal.\n"
            f"Example: <code>/setgoal 200000</code> for ฿200,000\n\n"
            f"Current goal: {current}",
            parse_mode=ParseMode.HTML
        )
        return

    try:
        amount_thb = float(context.args[0].replace(',', ''))
        if amount_thb <= 0:
            await update.message.reply_text("Goal must be greater than 0.")
            return
    except ValueError:
        await update.message.reply_text("Invalid amount. Example: /setgoal 200000")
        return

    config.monthly_goal = int(amount_thb * 100)  # Convert THB to satang
    save_config()

    await update.message.reply_text(
        f"Monthly goal set to <b>{format_currency(config.monthly_goal)}</b>",
        parse_mode=ParseMode.HTML
    )


async def startup(application):
    """Run startup tasks before polling begins."""
    logger.info("Running startup tasks...")
    await clear_webhook()

    # Start the dashboard web server
    from dashboard import start_dashboard_server
    asyncio.create_task(start_dashboard_server())

    logger.info("Startup complete")


async def shutdown(application):
    """Run cleanup tasks when bot stops."""
    global scheduler
    logger.info("Shutting down...")

    # Stop the dashboard web server
    from dashboard import stop_dashboard_server
    await stop_dashboard_server()

    if scheduler:
        scheduler.shutdown(wait=False)
    logger.info("Shutdown complete")


# ============================================================
# CLI Test Mode - for local testing without Telegram
# ============================================================

class MockMessage:
    """Mock Telegram message for CLI testing."""
    def __init__(self, chat_id="cli_test_user"):
        self.chat_id = chat_id
        self.responses = []

    async def reply_text(self, text, parse_mode=None, **kwargs):
        # Strip HTML tags for cleaner CLI output
        import re
        clean_text = re.sub(r'<[^>]+>', '', text)
        print(f"\n{clean_text}")
        self.responses.append(text)
        return self  # Return self so delete/edit can be called on it

    async def delete(self):
        """Mock delete - just clears the last line in CLI."""
        pass

    async def edit_text(self, text, parse_mode=None, **kwargs):
        """Mock edit - prints the new text."""
        import re
        clean_text = re.sub(r'<[^>]+>', '', text)
        print(f"\n{clean_text}")


class MockChat:
    """Mock Telegram chat for CLI testing."""
    def __init__(self, chat_id="cli_test_user"):
        self.id = chat_id


class MockUpdate:
    """Mock Telegram Update for CLI testing."""
    def __init__(self, chat_id="cli_test_user"):
        self.message = MockMessage(chat_id)
        self.effective_chat = MockChat(chat_id)


class MockContext:
    """Mock Telegram Context for CLI testing."""
    def __init__(self, args=None):
        self.args = args or []


async def cli_mode():
    """Run bot in CLI test mode - accepts commands from stdin."""
    print("=" * 60)
    print("CLI Test Mode - Type commands like /today, /agent <question>")
    print("Type 'exit' or 'quit' to stop")
    print("=" * 60)

    # Load config
    load_config()

    # Sync theft detection state from config module
    global notified_transaction_ids, notified_transaction_date, last_seen_void_id, last_cash_balance
    global last_alerted_transaction_id, last_alerted_expense_id
    notified_transaction_ids = config.notified_transaction_ids
    notified_transaction_date = config.notified_transaction_date
    last_seen_void_id = config.last_seen_void_id
    last_cash_balance = config.last_cash_balance
    last_alerted_transaction_id = config.last_alerted_transaction_id
    last_alerted_expense_id = config.last_alerted_expense_id

    # Apply configured log level
    log_level = getattr(logging, config.LOG_LEVEL, logging.INFO)
    logger.setLevel(log_level)
    logging.getLogger().setLevel(log_level)

    # Make CLI user an admin for testing
    global admin_chat_ids, approved_users, subscribed_chats
    cli_chat_id = "cli_test_user"
    admin_chat_ids.add(cli_chat_id)
    approved_users[cli_chat_id] = {"name": "CLI Tester", "username": "cli"}
    subscribed_chats.add(cli_chat_id)
    print(f"\nCLI user '{cli_chat_id}' added as admin and subscriber")

    # Map commands to handlers
    commands = {
        '/debug': debug,
        '/resend': resend,
        '/loglevel': loglevel,
        '/config': config_cmd,
        '/products': products,
        '/stats': stats,
        '/sales': sales,
        '/stock': stock,
        '/ingredients': ingredients,
        '/today': today,
        '/week': week,
        '/month': month,
        '/summary': summary,
        '/cash': cash,
        '/expenses': expenses,
        '/users': users,
        '/help': help_command,
        '/agent': agent,
        '/check_transactions': lambda u, c: check_new_transactions(),
        '/check_theft': lambda u, c: check_theft_indicators(),
    }

    while True:
        try:
            user_input = input("\n> ").strip()
        except EOFError:
            break

        if not user_input:
            continue

        if user_input.lower() in ('exit', 'quit'):
            print("Exiting CLI mode...")
            break

        # Parse command and args
        parts = user_input.split()
        cmd = parts[0].lower()
        args = parts[1:] if len(parts) > 1 else []

        # Add leading slash if missing
        if not cmd.startswith('/'):
            cmd = '/' + cmd

        if cmd in commands:
            update = MockUpdate(cli_chat_id)
            context = MockContext(args)
            try:
                handler = commands[cmd]
                result = handler(update, context)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                print(f"Error: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"Unknown command: {cmd}")
            print(f"Available: {', '.join(sorted(commands.keys()))}")


def main():
    """Start the bot."""
    global scheduler

    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set")
        return

    # Load persisted state (may contain POSTER_ACCESS_TOKEN)
    load_config()

    # Sync theft detection state from config module
    global notified_transaction_ids, notified_transaction_date, last_seen_void_id, last_cash_balance
    global last_alerted_transaction_id, last_alerted_expense_id
    notified_transaction_ids = config.notified_transaction_ids
    notified_transaction_date = config.notified_transaction_date
    last_seen_void_id = config.last_seen_void_id
    last_cash_balance = config.last_cash_balance
    last_alerted_transaction_id = config.last_alerted_transaction_id
    last_alerted_expense_id = config.last_alerted_expense_id

    # Apply configured log level
    log_level = getattr(logging, config.LOG_LEVEL, logging.INFO)
    logger.setLevel(log_level)
    logging.getLogger().setLevel(log_level)

    if not config.POSTER_ACCESS_TOKEN:
        logger.error("POSTER_ACCESS_TOKEN not set (set via env var or /config set)")
        return

    # Configure request with proper timeouts and connection pooling
    request = HTTPXRequest(
        connection_pool_size=8,
        read_timeout=REQUEST_READ_TIMEOUT,
        write_timeout=REQUEST_WRITE_TIMEOUT,
        connect_timeout=REQUEST_CONNECT_TIMEOUT,
        pool_timeout=REQUEST_POOL_TIMEOUT,
    )

    # Create application with custom request configuration
    application = (
        Application.builder()
        .token(TELEGRAM_BOT_TOKEN)
        .request(request)
        .get_updates_request(HTTPXRequest(
            connection_pool_size=2,
            read_timeout=60,  # Long polling timeout
            write_timeout=REQUEST_WRITE_TIMEOUT,
            connect_timeout=REQUEST_CONNECT_TIMEOUT,
            pool_timeout=REQUEST_POOL_TIMEOUT,
        ))
        .post_init(startup)
        .post_shutdown(shutdown)
        .build()
    )

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("setup", setup))
    application.add_handler(CommandHandler("request", request_access))
    application.add_handler(CommandHandler("approve", approve))
    application.add_handler(CommandHandler("reject", reject))
    application.add_handler(CommandHandler("users", users))
    application.add_handler(CommandHandler("promote", promote))
    application.add_handler(CommandHandler("demote", demote))
    application.add_handler(CommandHandler("config", config_cmd))
    application.add_handler(CommandHandler("reset", reset))
    application.add_handler(CommandHandler("debug", debug))
    application.add_handler(CommandHandler("resend", resend))
    application.add_handler(CommandHandler("loglevel", loglevel))
    application.add_handler(CommandHandler("today", today))
    application.add_handler(CommandHandler("products", products))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(CommandHandler("sales", sales))
    application.add_handler(CommandHandler("stock", stock))
    application.add_handler(CommandHandler("ingredients", ingredients))
    application.add_handler(CommandHandler("week", week))
    application.add_handler(CommandHandler("month", month))
    application.add_handler(CommandHandler("summary", summary))
    application.add_handler(CommandHandler("cash", cash))
    application.add_handler(CommandHandler("expenses", expenses))
    application.add_handler(CommandHandler("subscribe", subscribe))
    application.add_handler(CommandHandler("unsubscribe", unsubscribe))
    application.add_handler(CommandHandler("alerts", alerts_on))
    application.add_handler(CommandHandler("alerts_off", alerts_off))
    application.add_handler(CommandHandler("agent", agent))
    application.add_handler(CommandHandler("dashboard", dashboard_cmd))
    application.add_handler(CommandHandler("setpassword", setpassword_cmd))
    application.add_handler(CommandHandler("setgoal", setgoal_cmd))

    # Set up scheduler for background jobs
    scheduler = AsyncIOScheduler(timezone=THAI_TZ)

    # Poll for new transactions every 30 seconds
    # coalesce=True: If job is delayed, run once instead of catching up
    # max_instances=1: Prevent overlapping executions
    scheduler.add_job(
        check_new_transactions,
        'interval',
        seconds=30,
        id="check_transactions",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=30
    )

    # Check for theft indicators every 60 seconds
    scheduler.add_job(
        check_theft_indicators,
        'interval',
        seconds=60,
        id="check_theft",
        coalesce=True,
        max_instances=1,
        misfire_grace_time=60
    )

    # Schedule daily summary at 23:59 Bangkok time
    if TELEGRAM_CHAT_ID:
        scheduler.add_job(
            send_daily_summary,
            CronTrigger(hour=23, minute=59, timezone=THAI_TZ),
            id="daily_summary",
            coalesce=True,
            max_instances=1
        )
        logger.info(f"Scheduled daily summary at 23:59 Bangkok time to chat {TELEGRAM_CHAT_ID}")
    else:
        logger.warning("TELEGRAM_CHAT_ID not set - daily summary disabled")

    scheduler.start()
    logger.info("Started transaction polling (every 30 seconds)")

    # Start the bot with error handling
    logger.info("Starting bot...")
    try:
        application.run_polling(
            drop_pending_updates=True,  # Ignore updates that arrived while bot was offline
            allowed_updates=Update.ALL_TYPES,
            close_loop=False
        )
    except Conflict as e:
        logger.error(f"Bot conflict error: {e}")
        logger.error("Another instance is running. Please stop other instances and try again.")
        if scheduler:
            scheduler.shutdown(wait=False)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        if scheduler:
            scheduler.shutdown(wait=False)
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ban Sabai POS Telegram Bot')
    parser.add_argument('--cli', action='store_true', help='Run in CLI test mode (no Telegram)')
    args = parser.parse_args()

    if args.cli:
        asyncio.run(cli_mode())
    else:
        main()
