import os
import logging
import asyncio
from datetime import datetime, date, timedelta
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode
import requests
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
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

# Track subscribed chats and last seen transaction
subscribed_chats = set()
theft_alert_chats = set()
last_seen_transaction_id = None
last_seen_void_id = None
last_cash_balance = None

# Theft detection thresholds
LARGE_DISCOUNT_THRESHOLD = 20  # Alert if discount > 20%
LARGE_REFUND_THRESHOLD = 50000  # Alert if refund > 500 THB (in cents)

# Track transactions we've already alerted on (to avoid duplicates)
alerted_transactions = set()


def format_currency(amount_in_cents):
    """Format amount from cents to THB."""
    try:
        amount = float(amount_in_cents) / 100
        return f"฿{amount:,.2f}"
    except (ValueError, TypeError):
        return "฿0.00"


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


def format_summary_message(date_display, summary):
    """Format the summary into a Telegram message."""
    if summary["transaction_count"] == 0:
        return f"📊 <b>Summary for {date_display}</b>\n\nNo transactions found."

    return (
        f"📊 <b>Summary for {date_display}</b>\n\n"
        f"<b>Transactions:</b> {summary['transaction_count']}\n"
        f"<b>Total Sales:</b> {format_currency(summary['total_sales'])}\n"
        f"<b>Total Profit:</b> {format_currency(summary['total_profit'])}\n\n"
        f"<b>💵 Cash:</b> {format_currency(summary['cash_sales'])}\n"
        f"<b>💳 Card:</b> {format_currency(summary['card_sales'])}"
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command."""
    await update.message.reply_text(
        "🍺 <b>Ban Sabai POS Bot</b>\n\n"
        "<b>Reports:</b>\n"
        "/today - Today's sales summary\n"
        "/week - This week's summary\n"
        "/month - This month's summary\n"
        "/summary DATE [DATE] - Custom date/range\n\n"
        "<b>Cash:</b>\n"
        "/cash - Cash register balance\n\n"
        "<b>Real-time:</b>\n"
        "/subscribe - Get notified on each sale\n"
        "/unsubscribe - Stop sale notifications\n\n"
        "<b>Security:</b>\n"
        "/alerts - Enable theft detection\n"
        "/alerts_off - Disable theft alerts\n\n"
        "/help - Show this message",
        parse_mode=ParseMode.HTML
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /help command."""
    await start(update, context)


async def today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /today command - get today's summary."""
    today_str = date.today().strftime('%Y%m%d')
    today_display = date.today().strftime('%d %b %Y')

    await update.message.reply_text("⏳ Fetching today's data...")

    transactions = fetch_transactions(today_str)
    summary = calculate_summary(transactions)
    message = format_summary_message(today_display, summary)

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


async def week(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /week command - get this week's summary."""
    today_date = date.today()
    monday = today_date - timedelta(days=today_date.weekday())

    date_from = monday.strftime('%Y%m%d')
    date_to = today_date.strftime('%Y%m%d')
    week_display = f"{monday.strftime('%d %b')} - {today_date.strftime('%d %b %Y')}"

    await update.message.reply_text("⏳ Fetching data for this week...")

    transactions = fetch_transactions(date_from, date_to)
    summary_data = calculate_summary(transactions)

    days_count = (today_date - monday).days + 1
    avg_sales = summary_data['total_sales'] // days_count if days_count > 0 else 0
    avg_profit = summary_data['total_profit'] // days_count if days_count > 0 else 0

    message = (
        f"📅 <b>Weekly Report</b>\n"
        f"<i>{week_display}</i>\n\n"
        f"<b>Transactions:</b> {summary_data['transaction_count']}\n"
        f"<b>Total Sales:</b> {format_currency(summary_data['total_sales'])}\n"
        f"<b>Total Profit:</b> {format_currency(summary_data['total_profit'])}\n\n"
        f"<b>💵 Cash:</b> {format_currency(summary_data['cash_sales'])}\n"
        f"<b>💳 Card:</b> {format_currency(summary_data['card_sales'])}\n\n"
        f"<b>📊 Daily Average:</b>\n"
        f"• Sales: {format_currency(avg_sales)}\n"
        f"• Profit: {format_currency(avg_profit)}"
    )

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


async def month(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /month command - get this month's summary."""
    today_date = date.today()
    first_of_month = today_date.replace(day=1)

    date_from = first_of_month.strftime('%Y%m%d')
    date_to = today_date.strftime('%Y%m%d')
    month_display = today_date.strftime('%B %Y')

    await update.message.reply_text(f"⏳ Fetching data for {month_display}...")

    transactions = fetch_transactions(date_from, date_to)
    summary_data = calculate_summary(transactions)

    days_count = today_date.day
    avg_sales = summary_data['total_sales'] // days_count if days_count > 0 else 0
    avg_profit = summary_data['total_profit'] // days_count if days_count > 0 else 0

    message = (
        f"📆 <b>Monthly Report</b>\n"
        f"<i>{month_display}</i>\n\n"
        f"<b>Transactions:</b> {summary_data['transaction_count']}\n"
        f"<b>Total Sales:</b> {format_currency(summary_data['total_sales'])}\n"
        f"<b>Total Profit:</b> {format_currency(summary_data['total_profit'])}\n\n"
        f"<b>💵 Cash:</b> {format_currency(summary_data['cash_sales'])}\n"
        f"<b>💳 Card:</b> {format_currency(summary_data['card_sales'])}\n\n"
        f"<b>📊 Daily Average:</b>\n"
        f"• Sales: {format_currency(avg_sales)}\n"
        f"• Profit: {format_currency(avg_profit)}"
    )

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


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
        summary_data = calculate_summary(transactions)

        # Calculate daily average for range
        days_count = (date_to - date_from).days + 1
        avg_sales = summary_data['total_sales'] // days_count if days_count > 0 else 0
        avg_profit = summary_data['total_profit'] // days_count if days_count > 0 else 0

        message = (
            f"📊 <b>Summary for {date_display}</b>\n\n"
            f"<b>Transactions:</b> {summary_data['transaction_count']}\n"
            f"<b>Total Sales:</b> {format_currency(summary_data['total_sales'])}\n"
            f"<b>Total Profit:</b> {format_currency(summary_data['total_profit'])}\n\n"
            f"<b>💵 Cash:</b> {format_currency(summary_data['cash_sales'])}\n"
            f"<b>💳 Card:</b> {format_currency(summary_data['card_sales'])}\n\n"
            f"<b>📊 Daily Average ({days_count} days):</b>\n"
            f"• Sales: {format_currency(avg_sales)}\n"
            f"• Profit: {format_currency(avg_profit)}"
        )
    else:
        # Single date
        date_str = date_from.strftime('%Y%m%d')
        date_display = date_from.strftime('%d %b %Y')

        await update.message.reply_text(f"⏳ Fetching data for {date_display}...")

        transactions = fetch_transactions(date_str)
        summary_data = calculate_summary(transactions)
        message = format_summary_message(date_display, summary_data)

    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


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


async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /subscribe command - enable real-time sale notifications."""
    chat_id = str(update.effective_chat.id)

    if chat_id in subscribed_chats:
        await update.message.reply_text("✅ You're already subscribed to real-time updates.")
        return

    subscribed_chats.add(chat_id)
    await update.message.reply_text(
        "🔔 <b>Subscribed!</b>\n\n"
        "You'll now receive notifications for each new sale.\n"
        "Use /unsubscribe to stop.",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Chat {chat_id} subscribed to real-time updates")


async def unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /unsubscribe command - disable real-time sale notifications."""
    chat_id = str(update.effective_chat.id)

    if chat_id not in subscribed_chats:
        await update.message.reply_text("ℹ️ You're not subscribed to real-time updates.")
        return

    subscribed_chats.discard(chat_id)
    await update.message.reply_text(
        "🔕 <b>Unsubscribed!</b>\n\n"
        "You'll no longer receive real-time sale notifications.",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Chat {chat_id} unsubscribed from real-time updates")


async def alerts_on(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /alerts command - enable theft detection alerts."""
    chat_id = str(update.effective_chat.id)

    if chat_id in theft_alert_chats:
        await update.message.reply_text("✅ Theft detection alerts are already enabled.")
        return

    theft_alert_chats.add(chat_id)
    await update.message.reply_text(
        "🚨 <b>Theft Detection Enabled!</b>\n\n"
        "You'll receive alerts for:\n"
        "• Voided/cancelled transactions\n"
        "• Large discounts (>20%)\n"
        "• Suspicious refunds\n"
        "• Cash register discrepancies\n\n"
        "Use /alerts_off to disable.",
        parse_mode=ParseMode.HTML
    )
    logger.info(f"Chat {chat_id} enabled theft detection alerts")


async def alerts_off(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /alerts_off command - disable theft detection alerts."""
    chat_id = str(update.effective_chat.id)

    if chat_id not in theft_alert_chats:
        await update.message.reply_text("ℹ️ Theft detection alerts are not enabled.")
        return

    theft_alert_chats.discard(chat_id)
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
            await bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Failed to send theft alert to {chat_id}: {e}")
            if "chat not found" in str(e).lower():
                theft_alert_chats.discard(chat_id)


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
            return

        # Sort by transaction_id to get the latest
        transactions.sort(key=lambda x: int(x.get('transaction_id', 0)), reverse=True)
        latest_txn = transactions[0]
        latest_id = latest_txn.get('transaction_id')

        # First run - just set the last seen ID
        if last_seen_transaction_id is None:
            last_seen_transaction_id = latest_id
            logger.info(f"Initialized last_seen_transaction_id to {latest_id}")
            return

        # Check if there's a new transaction
        if latest_id != last_seen_transaction_id:
            # Find all new transactions
            new_transactions = [
                t for t in transactions
                if int(t.get('transaction_id', 0)) > int(last_seen_transaction_id)
            ]

            last_seen_transaction_id = latest_id

            if new_transactions:
                bot = Bot(token=TELEGRAM_BOT_TOKEN)

                for txn in reversed(new_transactions):  # Send oldest first
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
                        f"💰 <b>New Sale!</b>\n\n"
                        f"<b>Amount:</b> {format_currency(total)}\n"
                        f"<b>Profit:</b> {format_currency(profit)}\n"
                        f"<b>Payment:</b> {payment}\n"
                        f"<b>Table:</b> {table_name}"
                    )

                    for chat_id in subscribed_chats.copy():
                        try:
                            await bot.send_message(
                                chat_id=chat_id,
                                text=message,
                                parse_mode=ParseMode.HTML
                            )
                        except Exception as e:
                            logger.error(f"Failed to send to {chat_id}: {e}")
                            # Remove invalid chats
                            if "chat not found" in str(e).lower():
                                subscribed_chats.discard(chat_id)

                logger.info(f"Sent {len(new_transactions)} new transaction notifications")

    except Exception as e:
        logger.error(f"Error checking new transactions: {e}")


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
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=message,
            parse_mode=ParseMode.HTML
        )
        logger.info("Daily summary sent successfully")
    except Exception as e:
        logger.error(f"Failed to send daily summary: {e}")


def main():
    """Start the bot."""
    global scheduler

    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set")
        return

    if not POSTER_ACCESS_TOKEN:
        logger.error("POSTER_ACCESS_TOKEN not set")
        return

    # Create application without job queue
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("today", today))
    application.add_handler(CommandHandler("week", week))
    application.add_handler(CommandHandler("month", month))
    application.add_handler(CommandHandler("summary", summary))
    application.add_handler(CommandHandler("cash", cash))
    application.add_handler(CommandHandler("subscribe", subscribe))
    application.add_handler(CommandHandler("unsubscribe", unsubscribe))
    application.add_handler(CommandHandler("alerts", alerts_on))
    application.add_handler(CommandHandler("alerts_off", alerts_off))

    # Set up scheduler for background jobs
    scheduler = AsyncIOScheduler(timezone=THAI_TZ)

    # Poll for new transactions every 30 seconds
    scheduler.add_job(
        check_new_transactions,
        'interval',
        seconds=30,
        id="check_transactions"
    )

    # Check for theft indicators every 60 seconds
    scheduler.add_job(
        check_theft_indicators,
        'interval',
        seconds=60,
        id="check_theft"
    )

    # Schedule daily summary at 23:59 Bangkok time
    if TELEGRAM_CHAT_ID:
        scheduler.add_job(
            send_daily_summary,
            CronTrigger(hour=23, minute=59, timezone=THAI_TZ),
            id="daily_summary"
        )
        logger.info(f"Scheduled daily summary at 23:59 Bangkok time to chat {TELEGRAM_CHAT_ID}")
    else:
        logger.warning("TELEGRAM_CHAT_ID not set - daily summary disabled")

    scheduler.start()
    logger.info("Started transaction polling (every 30 seconds)")

    # Start the bot
    logger.info("Starting bot...")
    application.run_polling()


if __name__ == '__main__':
    main()
