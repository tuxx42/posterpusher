import os
import json
import requests
from datetime import datetime, date
from flask import Flask, request, jsonify

app = Flask(__name__)

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
POSTER_ACCESS_TOKEN = os.environ.get('POSTER_ACCESS_TOKEN')
POSTER_API_URL = "https://joinposter.com/api"

# Simple in-memory daily totals (resets on server restart)
daily_stats = {
    "date": None,
    "total_sales": 0,
    "total_profit": 0,
    "transaction_count": 0
}


def reset_daily_stats_if_needed():
    """Reset daily stats if it's a new day."""
    today = date.today().isoformat()
    if daily_stats["date"] != today:
        daily_stats["date"] = today
        daily_stats["total_sales"] = 0
        daily_stats["total_profit"] = 0
        daily_stats["transaction_count"] = 0


def get_transaction_details(transaction_id):
    """Fetch transaction details from Poster API."""
    if not POSTER_ACCESS_TOKEN:
        print("Poster access token not configured")
        return None

    url = f"{POSTER_API_URL}/dash.getTransaction"
    params = {
        "token": POSTER_ACCESS_TOKEN,
        "transaction_id": transaction_id
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f"Transaction details: {json.dumps(data, indent=2)}")
        return data.get("response")
    except requests.RequestException as e:
        print(f"Failed to fetch transaction details: {e}")
        return None


def send_telegram_message(message):
    """Send a message to the configured Telegram chat."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram credentials not configured")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except requests.RequestException as e:
        print(f"Failed to send Telegram message: {e}")
        return False


def format_currency(amount_in_cents):
    """Format amount from cents to THB."""
    try:
        amount = float(amount_in_cents) / 100
        return f"฿{amount:,.2f}"
    except (ValueError, TypeError):
        return f"฿{amount_in_cents}"


def format_sale_message(webhook_data, transaction_details):
    """Format the closed sale into a readable Telegram message."""
    reset_daily_stats_if_needed()

    timestamp = webhook_data.get('time', '')
    if timestamp:
        try:
            dt = datetime.fromtimestamp(int(timestamp))
            timestamp = dt.strftime('%H:%M:%S')
        except (ValueError, TypeError):
            pass

    # Extract transaction info (adjust field names based on actual API response)
    if transaction_details:
        # Handle if API returns a list
        if isinstance(transaction_details, list) and len(transaction_details) > 0:
            transaction_details = transaction_details[0]

        # Poster API typically returns amounts in cents
        total = transaction_details.get('sum', 0) or transaction_details.get('payed_sum', 0) or 0
        profit = transaction_details.get('profit', 0) or 0

        # Update daily stats
        daily_stats["total_sales"] += int(total)
        daily_stats["total_profit"] += int(profit)
        daily_stats["transaction_count"] += 1

        message = (
            f"💰 <b>Sale Closed</b>\n\n"
            f"<b>Amount:</b> {format_currency(total)}\n"
            f"<b>Profit:</b> {format_currency(profit)}\n"
            f"<b>Time:</b> {timestamp}\n\n"
            f"📊 <b>Today's Totals</b>\n"
            f"<b>Sales:</b> {format_currency(daily_stats['total_sales'])}\n"
            f"<b>Profit:</b> {format_currency(daily_stats['total_profit'])}\n"
            f"<b>Transactions:</b> {daily_stats['transaction_count']}"
        )
    else:
        # Fallback if we couldn't fetch transaction details
        daily_stats["transaction_count"] += 1
        message = (
            f"💰 <b>Sale Closed</b>\n\n"
            f"<b>Transaction ID:</b> {webhook_data.get('object_id', 'N/A')}\n"
            f"<b>Time:</b> {timestamp}\n"
            f"<i>(Could not fetch sale details)</i>\n\n"
            f"📊 <b>Today's Transactions:</b> {daily_stats['transaction_count']}"
        )

    return message


@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle incoming webhooks from Poster POS."""
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 400

    data = request.get_json()
    print(f"Received webhook: {data}")

    # Only process closed transactions
    object_type = data.get('object', '')
    action = data.get('action', '')

    if object_type == 'transaction' and action == 'closed':
        transaction_id = data.get('object_id')
        transaction_details = get_transaction_details(transaction_id) if transaction_id else None
        message = format_sale_message(data, transaction_details)
        send_telegram_message(message)
    else:
        print(f"Ignoring webhook: object={object_type}, action={action}")

    return jsonify({"status": "ok"}), 200


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({"status": "healthy"}), 200


@app.route('/', methods=['GET'])
def index():
    """Root endpoint."""
    return jsonify({
        "service": "Poster POS Webhook Receiver",
        "endpoints": {
            "/webhook": "POST - Receive Poster POS webhooks",
            "/health": "GET - Health check"
        }
    }), 200


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
