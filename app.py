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

    # Schedule daily summary at 23:59 Bangkok time using APScheduler
    if TELEGRAM_CHAT_ID:
        scheduler = AsyncIOScheduler(timezone=THAI_TZ)
        scheduler.add_job(
            send_daily_summary,
            CronTrigger(hour=23, minute=59, timezone=THAI_TZ),
            id="daily_summary"
        )
        scheduler.start()
        logger.info(f"Scheduled daily summary at 23:59 Bangkok time to chat {TELEGRAM_CHAT_ID}")
    else:
        logger.warning("TELEGRAM_CHAT_ID not set - daily summary disabled")

    # Start the bot
    logger.info("Starting bot...")
    application.run_polling()


if __name__ == '__main__':
    main()
