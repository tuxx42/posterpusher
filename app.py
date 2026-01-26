import os
import io
import json
import logging
import asyncio
import functools
import sys
from datetime import datetime, date, timedelta
from telegram import Update, Bot, InputFile
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode
from telegram.error import Conflict, TimedOut, NetworkError, RetryAfter
from telegram.request import HTTPXRequest
import requests
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
POSTER_ACCESS_TOKEN = os.environ.get('POSTER_ACCESS_TOKEN')
POSTER_API_URL = "https://joinposter.com/api"

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

# Config file for persisting state
CONFIG_FILE = os.environ.get('CONFIG_FILE', 'bot_config.json')

# Track subscribed chats and last seen transaction
subscribed_chats = set()
theft_alert_chats = set()
last_seen_transaction_id = None
last_seen_void_id = None
last_cash_balance = None

# Authentication state
admin_chat_ids = set()  # Set of admin chat IDs
approved_users = {}   # {chat_id: {name, username, approved_at}}
pending_requests = {} # {chat_id: {name, username, requested_at}}


def load_config():
    """Load persisted state from config file."""
    global subscribed_chats, theft_alert_chats, admin_chat_ids, approved_users, pending_requests
    global last_seen_transaction_id, last_seen_void_id, last_cash_balance
    global alerted_transactions, alerted_expenses
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
                subscribed_chats = set(config.get('subscribed_chats', []))
                theft_alert_chats = set(config.get('theft_alert_chats', []))
                # Handle both old single admin and new multiple admins format
                admin_chat_ids = set(config.get('admin_chat_ids', []))
                # Backwards compatibility: migrate old admin_chat_id to new format
                old_admin = config.get('admin_chat_id')
                if old_admin and old_admin not in admin_chat_ids:
                    admin_chat_ids.add(old_admin)
                approved_users = config.get('approved_users', {})
                pending_requests = config.get('pending_requests', {})
                # Load theft detection state
                last_seen_transaction_id = config.get('last_seen_transaction_id')
                last_seen_void_id = config.get('last_seen_void_id')
                last_cash_balance = config.get('last_cash_balance')
                alerted_transactions = set(config.get('alerted_transactions', []))
                alerted_expenses = set(config.get('alerted_expenses', []))
                logger.info(f"Loaded config: {len(subscribed_chats)} subscribed, {len(theft_alert_chats)} alert chats, {len(admin_chat_ids)} admins")
                logger.info(f"Loaded theft state: {len(alerted_transactions)} alerted txns, {len(alerted_expenses)} alerted expenses")
    except Exception as e:
        logger.error(f"Failed to load config: {e}")


def save_config():
    """Save state to config file."""
    try:
        config = {
            'subscribed_chats': list(subscribed_chats),
            'theft_alert_chats': list(theft_alert_chats),
            'admin_chat_ids': list(admin_chat_ids),
            'approved_users': approved_users,
            'pending_requests': pending_requests,
            # Theft detection state
            'last_seen_transaction_id': last_seen_transaction_id,
            'last_seen_void_id': last_seen_void_id,
            'last_cash_balance': last_cash_balance,
            'alerted_transactions': list(alerted_transactions),
            'alerted_expenses': list(alerted_expenses)
        }
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        logger.info("Config saved")
    except Exception as e:
        logger.error(f"Failed to save config: {e}")


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
LARGE_EXPENSE_THRESHOLD = 100000  # Alert if single expense > 1000 THB (in cents)

# Track expenses we've already alerted on
alerted_expenses = set()

# Track transactions we've already alerted on (to avoid duplicates)
alerted_transactions = set()


def format_currency(amount_in_cents):
    """Format amount from cents to THB."""
    try:
        amount = float(amount_in_cents) / 100
        return f"฿{amount:,.2f}"
    except (ValueError, TypeError):
        return "฿0.00"


def generate_sales_chart(transactions, date_from, date_to, title, finance_transactions=None):
    """Generate a bar chart showing daily gross profit, net profit, and expenses."""
    # Group transactions by date
    daily_data = {}
    current = date_from
    while current <= date_to:
        daily_data[current] = {'sales': 0, 'gross_profit': 0, 'expenses': 0}
        current += timedelta(days=1)

    for txn in transactions:
        txn_date = txn.get('date_close_date', '')[:10]  # Get YYYY-MM-DD
        if txn_date:
            try:
                d = datetime.strptime(txn_date, '%Y-%m-%d').date()
                if d in daily_data:
                    daily_data[d]['sales'] += int(txn.get('sum', 0) or 0)
                    daily_data[d]['gross_profit'] += int(txn.get('total_profit', 0) or 0)
            except ValueError:
                continue

    # Process expenses by date
    if finance_transactions:
        for txn in finance_transactions:
            amount = int(txn.get('amount', 0) or 0)
            comment = txn.get('comment', '')

            # Skip cash payments (sales income)
            if 'Cash payments' in comment:
                continue

            # Only count expenses (negative amounts)
            if amount < 0:
                txn_date = txn.get('date', '')[:10]
                if txn_date:
                    try:
                        d = datetime.strptime(txn_date, '%Y-%m-%d').date()
                        if d in daily_data:
                            daily_data[d]['expenses'] += abs(amount)
                    except ValueError:
                        continue

    # Prepare data for plotting
    dates = sorted(daily_data.keys())
    gross_profits = [daily_data[d]['gross_profit'] / 100 for d in dates]  # Convert to THB
    expenses = [-(daily_data[d]['expenses'] / 100) for d in dates]  # Negative for display
    net_profits = [(daily_data[d]['gross_profit'] - daily_data[d]['expenses']) / 100 for d in dates]

    # Create chart
    fig, ax = plt.subplots(figsize=(10, 5))
    x = range(len(dates))
    width = 0.27

    # Three bars: Gross Profit, Net Profit, Expenses (negative)
    ax.bar([i - width for i in x], gross_profits, width, label='Gross Profit', color='#4CAF50')
    ax.bar([i for i in x], net_profits, width, label='Net Profit', color='#2196F3')
    ax.bar([i + width for i in x], expenses, width, label='Expenses', color='#F44336')

    # Add horizontal line at y=0
    ax.axhline(y=0, color='black', linewidth=0.5)

    ax.set_xlabel('Date')
    ax.set_ylabel('Amount (฿)')
    ax.set_title(title)
    ax.set_xticks(list(x))
    ax.set_xticklabels([d.strftime('%d %b') for d in dates], rotation=45, ha='right')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)

    # Format y-axis with thousands separator
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:,.0f}'))

    plt.tight_layout()

    # Save to BytesIO
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)

    return buf


def fetch_cash_shifts():
    """Fetch cash shift data from Poster API."""
    url = f"{POSTER_API_URL}/finance.getCashShifts"
    params = {"token": POSTER_ACCESS_TOKEN}

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
        "token": POSTER_ACCESS_TOKEN,
        "date_from": date_from,
        "date_to": date_to or date_from
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
        "token": POSTER_ACCESS_TOKEN,
        "date_from": date_from,
        "date_to": date_to or date_from
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("response", [])
    except requests.RequestException as e:
        logger.error(f"Failed to fetch transactions: {e}")
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
        "<b>Reports:</b>\n"
        "/today - Today's sales summary\n"
        "/week - This week's summary\n"
        "/month - This month's summary\n"
        "/summary DATE [DATE] - Custom date/range\n"
        "/expenses [DATE] [DATE] - Expense breakdown\n\n"
        "<b>Cash:</b>\n"
        "/cash - Cash register balance\n\n"
        "<b>Real-time:</b>\n"
        "/subscribe - Get notified on each sale\n"
        "/unsubscribe - Stop sale notifications\n\n"
        "<b>Security:</b>\n"
        "/alerts - Enable theft detection\n"
        "/alerts_off - Disable theft alerts\n\n"
    )

    # Add admin commands if user is admin
    if is_admin:
        message += (
            "<b>Admin:</b>\n"
            "/approve - Approve user access\n"
            "/reject ID - Reject user request\n"
            "/users - List approved users\n"
            "/promote ID - Promote user to admin\n"
            "/demote ID - Remove admin privileges\n"
            "/config - View bot configuration\n"
            "/reset - Reset all configuration\n\n"
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
async def config(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /config command - show current configuration."""
    try:
        if not os.path.exists(CONFIG_FILE):
            await update.message.reply_text("No configuration file exists yet.")
            return

        with open(CONFIG_FILE, 'r') as f:
            config_data = json.load(f)

        # Format the config nicely
        message = "⚙️ <b>Bot Configuration</b>\n\n"

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

        # Config file path
        message += f"\n<i>File: {CONFIG_FILE}</i>"

        await update.message.reply_text(message, parse_mode=ParseMode.HTML)

        # Send raw JSON as a separate message
        raw_json = json.dumps(config_data, indent=2, ensure_ascii=False)
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
    global last_seen_transaction_id, last_seen_void_id, last_cash_balance, alerted_transactions, alerted_expenses

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
        last_seen_transaction_id = None
        last_seen_void_id = None
        last_cash_balance = None
        alerted_transactions = set()
        alerted_expenses = set()

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
async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /debug command - show raw API transaction data."""
    today_str = date.today().strftime('%Y%m%d')

    await update.message.reply_text("⏳ Fetching raw transaction data...")

    transactions = [t for t in fetch_transactions(today_str)
                    if str(t.get('status')) == '2']

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

    # Also show last_seen_transaction_id
    message += f"<b>last_seen_transaction_id:</b> {last_seen_transaction_id}\n"
    message += f"<b>Type:</b> {type(last_seen_transaction_id).__name__}"

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


@require_admin
async def resend(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /resend command - resend last 2 closed transactions as notifications."""
    today_str = date.today().strftime('%Y%m%d')

    await update.message.reply_text("⏳ Fetching and resending last 2 transactions...")

    transactions = fetch_transactions(today_str)

    if not transactions:
        await update.message.reply_text("No transactions found for today.")
        return

    # Filter to closed transactions only and sort by transaction_id descending
    closed_txns = [t for t in transactions if str(t.get('status')) == '2']
    closed_txns.sort(key=lambda x: int(x.get('transaction_id', 0)), reverse=True)

    if not closed_txns:
        await update.message.reply_text("No closed transactions found for today.")
        return

    # Take last 2
    recent = closed_txns[:2]

    if not subscribed_chats:
        await update.message.reply_text("No subscribed chats to send to.")
        return

    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    sent_count = 0

    for txn in reversed(recent):  # Send oldest first
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

        message = (
            f"🔄 <b>Resend Test - Sale #{txn_id}</b>\n\n"
            f"<b>Amount:</b> {format_currency(total)}\n"
            f"<b>Profit:</b> {format_currency(profit)}\n"
            f"<b>Payment:</b> {payment}\n"
            f"<b>Table:</b> {table_name}"
        )

        for chat_id in subscribed_chats.copy():
            try:
                result = await safe_send_message(bot, chat_id, message, parse_mode=ParseMode.HTML)
                if result:
                    sent_count += 1
            except Exception as e:
                logger.error(f"Failed to resend to {chat_id}: {e}")

    await update.message.reply_text(f"✅ Resent {len(recent)} transactions to {len(subscribed_chats)} chats ({sent_count} messages sent).")


@require_auth
async def today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /today command - get today's summary."""
    today_str = date.today().strftime('%Y%m%d')
    today_display = date.today().strftime('%d %b %Y')

    await update.message.reply_text("⏳ Fetching today's data...")

    transactions = fetch_transactions(today_str)
    finance_txns = fetch_finance_transactions(today_str)

    summary = calculate_summary(transactions)
    expenses = calculate_expenses(finance_txns)
    message = format_summary_message(today_display, summary, expenses)

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


@require_auth
async def week(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /week command - get this week's summary."""
    today_date = date.today()
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
    today_date = date.today()
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

    shift_start = latest_shift.get('date_start', 'Unknown')
    shift_end = latest_shift.get('date_end', '')
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
        f"• Cash Out: -{format_currency(cash_out)}"
    )

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


@require_auth
async def expenses(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /expenses command - get detailed expense breakdown."""
    # Default to today if no date specified
    if not context.args:
        date_from = date.today()
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
        "token": POSTER_ACCESS_TOKEN,
        "date_from": date_from,
        "date_to": date_to or date_from,
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
    global last_seen_void_id, last_cash_balance

    if not theft_alert_chats:
        return

    today_str = date.today().strftime('%Y%m%d')

    try:
        # Check for voided transactions
        voided = fetch_removed_transactions(today_str)
        if voided:
            voided.sort(key=lambda x: int(x.get('transaction_id', 0)), reverse=True)
            latest_void = voided[0]
            latest_void_id = latest_void.get('transaction_id')

            if last_seen_void_id is None:
                last_seen_void_id = latest_void_id
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
        for txn in transactions:
            txn_id = txn.get('transaction_id')
            alert_key = f"txn_{txn_id}"

            # Skip if we've already alerted on this transaction
            if alert_key in alerted_transactions:
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
                    alerted_transactions.add(alert_key)
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
                    alerted_transactions.add(alert_key)
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
                    discount_key = f"discount_{txn_id}"
                    if discount_key not in alerted_transactions:
                        alerted_transactions.add(discount_key)
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

        for expense in expenses_data['expense_list']:
            expense_id = expense.get('transaction_id', '')
            if expense_id in alerted_expenses:
                continue

            if expense['amount'] >= LARGE_EXPENSE_THRESHOLD:
                alerted_expenses.add(expense_id)
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

        # Save state after checking to persist alerted items
        save_config()

    except Exception as e:
        logger.error(f"Error in theft detection: {e}")


async def check_new_transactions():
    """Poll for new transactions and notify subscribed chats."""
    global last_seen_transaction_id

    if not subscribed_chats:
        return

    if not TELEGRAM_BOT_TOKEN:
        return

    try:
        # Fetch today's transactions
        today_str = date.today().strftime('%Y%m%d')
        transactions = fetch_transactions(today_str)

        if not transactions:
            logger.debug("No transactions found for today")
            return

        # Sort by transaction_id to get the latest
        transactions.sort(key=lambda x: int(x.get('transaction_id', 0)), reverse=True)
        latest_txn = transactions[0]
        latest_id = latest_txn.get('transaction_id')

        # Convert to int for consistent comparison (API may return string or int)
        try:
            latest_id_int = int(latest_id) if latest_id else 0
        except (ValueError, TypeError):
            logger.error(f"Invalid latest transaction ID: {latest_id}")
            return

        # First run - just set the last seen ID
        if last_seen_transaction_id is None:
            last_seen_transaction_id = latest_id_int
            save_config()
            logger.info(f"Initialized last_seen_transaction_id to {latest_id_int}")
            return

        # Convert last seen to int for comparison
        try:
            last_seen_int = int(last_seen_transaction_id) if last_seen_transaction_id else 0
        except (ValueError, TypeError):
            logger.error(f"Invalid last_seen_transaction_id: {last_seen_transaction_id}, resetting")
            last_seen_transaction_id = latest_id_int
            save_config()
            return

        # Check if there are newer transactions (using numeric comparison)
        if latest_id_int > last_seen_int:
            # Find all new closed transactions (status 0 = open, 1 = closed)
            new_transactions = [
                t for t in transactions
                if int(t.get('transaction_id', 0)) > last_seen_int
                and str(t.get('status')) == '1'  # Only closed transactions
            ]

            logger.info(f"Found {len(new_transactions)} new closed transactions (latest: {latest_id_int}, last seen: {last_seen_int})")

            if new_transactions:
                bot = Bot(token=TELEGRAM_BOT_TOKEN)
                notifications_sent = 0

                for txn in reversed(new_transactions):  # Send oldest first
                    txn_id = txn.get('transaction_id')
                    # Debug: log raw transaction data
                    logger.debug(f"Raw transaction data for {txn_id}: {txn}")
                    total = int(txn.get('sum', 0) or 0)
                    profit = int(txn.get('total_profit', 0) or 0)
                    logger.debug(f"Parsed values - total: {total}, profit: {profit}")
                    payed_cash = int(txn.get('payed_cash', 0) or 0)
                    payed_card = int(txn.get('payed_card', 0) or 0)
                    table_name = txn.get('table_name', '')

                    if payed_card > 0 and payed_cash > 0:
                        payment = "💳+💵"
                    elif payed_card > 0:
                        payment = "💳 Card"
                    else:
                        payment = "💵 Cash"

                    message = (
                        f"💰 <b>New Sale!</b>\n\n"
                        f"<b>Amount:</b> {format_currency(total)}\n"
                        f"<b>Profit:</b> {format_currency(profit)}\n"
                        f"<b>Payment:</b> {payment}\n"
                        f"<b>Table:</b> {table_name}"
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

                logger.info(f"Sent {notifications_sent} notifications for {len(new_transactions)} new transactions")

            # Update last seen ID only after successful processing
            last_seen_transaction_id = latest_id_int
            save_config()
        else:
            logger.debug(f"No new transactions (latest: {latest_id_int}, last seen: {last_seen_int})")

    except Conflict:
        logger.error("Bot conflict detected - another instance may be running")
    except Exception as e:
        logger.error(f"Error checking new transactions: {e}", exc_info=True)


async def send_daily_summary():
    """Send daily summary at midnight."""
    if not TELEGRAM_CHAT_ID or not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_CHAT_ID or BOT_TOKEN not set, skipping scheduled summary")
        return

    today_str = date.today().strftime('%Y%m%d')
    today_display = date.today().strftime('%d %b %Y')

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


async def startup(application):
    """Run startup tasks before polling begins."""
    logger.info("Running startup tasks...")
    await clear_webhook()
    logger.info("Startup complete")


async def shutdown(application):
    """Run cleanup tasks when bot stops."""
    global scheduler
    logger.info("Shutting down...")
    if scheduler:
        scheduler.shutdown(wait=False)
    logger.info("Shutdown complete")


def main():
    """Start the bot."""
    global scheduler

    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set")
        return

    if not POSTER_ACCESS_TOKEN:
        logger.error("POSTER_ACCESS_TOKEN not set")
        return

    # Load persisted state
    load_config()

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
    application.add_handler(CommandHandler("config", config))
    application.add_handler(CommandHandler("reset", reset))
    application.add_handler(CommandHandler("debug", debug))
    application.add_handler(CommandHandler("resend", resend))
    application.add_handler(CommandHandler("today", today))
    application.add_handler(CommandHandler("week", week))
    application.add_handler(CommandHandler("month", month))
    application.add_handler(CommandHandler("summary", summary))
    application.add_handler(CommandHandler("cash", cash))
    application.add_handler(CommandHandler("expenses", expenses))
    application.add_handler(CommandHandler("subscribe", subscribe))
    application.add_handler(CommandHandler("unsubscribe", unsubscribe))
    application.add_handler(CommandHandler("alerts", alerts_on))
    application.add_handler(CommandHandler("alerts_off", alerts_off))

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
    main()
