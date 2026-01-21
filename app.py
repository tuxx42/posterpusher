import os
import requests
from datetime import datetime
from flask import Flask, request, jsonify

app = Flask(__name__)

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
POSTER_ACCESS_TOKEN = os.environ.get('POSTER_ACCESS_TOKEN')


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


def format_webhook_message(data):
    """Format the Poster POS webhook data into a readable message."""
    timestamp = data.get('time', '')
    if timestamp:
        try:
            dt = datetime.fromtimestamp(int(timestamp))
            timestamp = dt.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError):
            pass

    object_type = data.get('object', 'unknown')
    action = data.get('action', 'unknown')
    object_id = data.get('object_id', 'N/A')
    account = data.get('account', 'unknown')

    action_emoji = {
        'added': '➕',
        'changed': '✏️',
        'removed': '❌',
        'closed': '✅'
    }.get(action, '📋')

    message = (
        f"{action_emoji} <b>Poster POS Notification</b>\n\n"
        f"<b>Type:</b> {object_type.capitalize()}\n"
        f"<b>Action:</b> {action.capitalize()}\n"
        f"<b>ID:</b> {object_id}\n"
        f"<b>Account:</b> {account}\n"
        f"<b>Time:</b> {timestamp}"
    )

    return message


@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle incoming webhooks from Poster POS."""
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 400

    data = request.get_json()

    print(f"Received webhook: {data}")

    message = format_webhook_message(data)
    send_telegram_message(message)

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
