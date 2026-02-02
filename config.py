"""
Configuration management for the Telegram bot.
Handles loading, saving, and managing bot state.
"""
import os
import json
import logging

logger = logging.getLogger(__name__)

# Config file path
CONFIG_FILE = os.environ.get('CONFIG_FILE', 'bot_config.json')

# API Keys (can be set via env vars or config file)
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY')
POSTER_ACCESS_TOKEN = os.environ.get('POSTER_ACCESS_TOKEN')

# Subscription state
subscribed_chats = set()
theft_alert_chats = set()

# Theft detection state
last_seen_transaction_id = None
last_seen_void_id = None
last_cash_balance = None
alerted_transactions = set()
alerted_expenses = set()

# Authentication state
admin_chat_ids = set()
approved_users = {}
pending_requests = {}


def mask_api_key(key: str) -> str:
    """Mask an API key for display, showing only first 4 and last 4 characters."""
    if not key or len(key) < 12:
        return "****"
    return f"{key[:4]}...{key[-4:]}"


def load_config():
    """Load persisted state from config file."""
    global subscribed_chats, theft_alert_chats, admin_chat_ids, approved_users, pending_requests
    global last_seen_transaction_id, last_seen_void_id, last_cash_balance
    global alerted_transactions, alerted_expenses
    global ANTHROPIC_API_KEY, POSTER_ACCESS_TOKEN

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
                # Load API keys (config file overrides env vars)
                if config.get('ANTHROPIC_API_KEY'):
                    ANTHROPIC_API_KEY = config.get('ANTHROPIC_API_KEY')
                if config.get('POSTER_ACCESS_TOKEN'):
                    POSTER_ACCESS_TOKEN = config.get('POSTER_ACCESS_TOKEN')
                logger.info(f"Loaded config: {len(subscribed_chats)} subscribed, {len(theft_alert_chats)} alert chats, {len(admin_chat_ids)} admins")
                logger.info(f"Loaded theft state: {len(alerted_transactions)} alerted txns, {len(alerted_expenses)} alerted expenses")
    except Exception as e:
        logger.error(f"Failed to load config: {e}")


def save_config():
    """Save state to config file."""
    try:
        # Read existing config to preserve API keys
        existing_config = {}
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f:
                    existing_config = json.load(f)
            except Exception:
                pass

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
        # Preserve API keys from existing config
        if existing_config.get('ANTHROPIC_API_KEY'):
            config['ANTHROPIC_API_KEY'] = existing_config['ANTHROPIC_API_KEY']
        if existing_config.get('POSTER_ACCESS_TOKEN'):
            config['POSTER_ACCESS_TOKEN'] = existing_config['POSTER_ACCESS_TOKEN']
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=2)
        logger.info("Config saved")
    except Exception as e:
        logger.error(f"Failed to save config: {e}")


def set_api_key(var_name: str, value: str) -> bool:
    """Set an API key in config file and memory."""
    global ANTHROPIC_API_KEY, POSTER_ACCESS_TOKEN

    allowed_vars = ["ANTHROPIC_API_KEY", "POSTER_ACCESS_TOKEN"]
    if var_name not in allowed_vars:
        return False

    # Load existing config
    config_data = {}
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                config_data = json.load(f)
        except Exception:
            pass

    # Update the variable
    config_data[var_name] = value
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config_data, f, indent=2)

    # Update global variable
    if var_name == "ANTHROPIC_API_KEY":
        ANTHROPIC_API_KEY = value
    elif var_name == "POSTER_ACCESS_TOKEN":
        POSTER_ACCESS_TOKEN = value

    logger.info(f"Config variable {var_name} updated")
    return True


def delete_api_key(var_name: str) -> bool:
    """Delete an API key from config file and memory."""
    global ANTHROPIC_API_KEY, POSTER_ACCESS_TOKEN

    allowed_vars = ["ANTHROPIC_API_KEY", "POSTER_ACCESS_TOKEN"]
    if var_name not in allowed_vars:
        return False

    # Load existing config
    config_data = {}
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                config_data = json.load(f)
        except Exception:
            pass

    # Delete the variable if it exists
    if var_name in config_data:
        del config_data[var_name]
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config_data, f, indent=2)

        # Clear global variable
        if var_name == "ANTHROPIC_API_KEY":
            ANTHROPIC_API_KEY = None
        elif var_name == "POSTER_ACCESS_TOKEN":
            POSTER_ACCESS_TOKEN = None

        logger.info(f"Config variable {var_name} deleted")
        return True

    return False


def get_config_data() -> dict:
    """Get the current config file data."""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return {}
