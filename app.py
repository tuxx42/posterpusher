import os
import logging
from datetime import datetime, date
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import requests

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
POSTER_ACCESS_TOKEN = os.environ.get('POSTER_ACCESS_TOKEN')
POSTER_API_URL = "https://joinposter.com/api"


def format_currency(amount_in_cents):
    """Format amount from cents to THB."""
    try:
        amount = float(amount_in_cents) / 100
        return f"฿{amount:,.2f}"
    except (ValueError, TypeError):
        return f"฿0.00"


def fetch_transactions(date_str: str) -> list:
    """Fetch transactions for a specific date from Poster API."""
    url = f"{POSTER_API_URL}/dash.getTransactions"
    params = {
        "token": POSTER_ACCESS_TOKEN,
        "date_from": date_str,
        "date_to": date_str
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("response", [])
    except requests.RequestException as e:
        logger.error(f"Failed to fetch transactions: {e}")
        return []


def calculate_summary(transactions: list) -> dict:
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


def format_summary_message(date_display: str, summary: dict) -> str:
    """Format the summary into a Telegram message."""
    if summary["transaction_count"] == 0:
        return f"📊 <b>Summary for {date_display}</b>\n\nNo transactions found."

    message = (
        f"📊 <b>Summary for {date_display}</b>\n\n"
        f"<b>Transactions:</b> {summary['transaction_count']}\n"
        f"<b>Total Sales:</b> {format_currency(summary['total_sales'])}\n"
        f"<b>Total Profit:</b> {format_currency(summary['total_profit'])}\n\n"
        f"<b>💵 Cash:</b> {format_currency(summary['cash_sales'])}\n"
        f"<b>💳 Card:</b> {format_currency(summary['card_sales'])}"
    )

    return message


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command."""
    await update.message.reply_text(
        "🍺 <b>Ban Sabai POS Bot</b>\n\n"
        "Commands:\n"
        "/today - Get today's sales summary\n"
        "/summary YYYYMMDD - Get summary for a specific date\n"
        "/help - Show this help message",
        parse_mode='HTML'
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

    await update.message.reply_text(message, parse_mode='HTML')


async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /summary command - get summary for a specific date."""
    if not context.args:
        await update.message.reply_text(
            "Please provide a date.\n"
            "Usage: /summary YYYYMMDD\n"
            "Example: /summary 20260120"
        )
        return

    date_str = context.args[0]

    # Validate date format
    try:
        parsed_date = datetime.strptime(date_str, '%Y%m%d')
        date_display = parsed_date.strftime('%d %b %Y')
    except ValueError:
        await update.message.reply_text(
            "❌ Invalid date format.\n"
            "Use YYYYMMDD format.\n"
            "Example: /summary 20260120"
        )
        return

    await update.message.reply_text(f"⏳ Fetching data for {date_display}...")

    transactions = fetch_transactions(date_str)
    summary_data = calculate_summary(transactions)
    message = format_summary_message(date_display, summary_data)

    await update.message.reply_text(message, parse_mode='HTML')


def main() -> None:
    """Start the bot."""
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set")
        return

    if not POSTER_ACCESS_TOKEN:
        logger.error("POSTER_ACCESS_TOKEN not set")
        return

    # Create application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Add command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("today", today))
    application.add_handler(CommandHandler("summary", summary))

    # Start the bot
    logger.info("Starting bot...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()
