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

# Logging configuration
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()

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

# Agent configuration defaults
AGENT_DEFAULTS = {
    'daily_limit': 20,
    'max_iterations': 5,
    'history_limit': 10
}

# Rate limiting for /agent command
AGENT_DAILY_LIMIT = AGENT_DEFAULTS['daily_limit']  # Default max requests per user per day
agent_usage = {}  # {user_id: {'date': 'YYYY-MM-DD', 'count': N}}

# Conversation memory for /agent command
AGENT_HISTORY_LIMIT = AGENT_DEFAULTS['history_limit']  # Keep last N messages per user
agent_conversations = {}  # {user_id: [messages]}

# Per-user agent limits (overrides defaults)
agent_user_limits = {}  # {user_id: {'daily_limit': N, 'max_iterations': N}}


def get_agent_limits(user_id: str) -> dict:
    """Get agent limits for a user.

    Returns:
        Dict with 'daily_limit', 'max_iterations', 'history_limit' keys
    """
    user_id = str(user_id)
    limits = dict(AGENT_DEFAULTS)  # Start with defaults
    if user_id in agent_user_limits:
        limits.update(agent_user_limits[user_id])
    return limits


def set_agent_limit(user_id: str, key: str, value: int) -> bool:
    """Set a limit for a specific user.

    Args:
        user_id: The user ID to set limit for
        key: Limit key ('daily_limit' or 'max_iterations')
        value: The limit value

    Returns:
        True if successful, False if invalid key
    """
    if key not in ('daily_limit', 'max_iterations'):
        return False

    user_id = str(user_id)
    if user_id not in agent_user_limits:
        agent_user_limits[user_id] = {}
    agent_user_limits[user_id][key] = value
    return True


def check_agent_rate_limit(user_id: str) -> tuple[bool, int]:
    """Check if user is within rate limit for /agent.

    Returns:
        (allowed, remaining): Whether request is allowed and remaining quota
    """
    from datetime import date
    today = date.today().isoformat()
    user_id = str(user_id)

    # Get per-user daily limit
    limits = get_agent_limits(user_id)
    daily_limit = limits['daily_limit']

    if user_id not in agent_usage or agent_usage[user_id]['date'] != today:
        # New day or new user
        agent_usage[user_id] = {'date': today, 'count': 0}

    usage = agent_usage[user_id]
    remaining = daily_limit - usage['count']

    if usage['count'] >= daily_limit:
        return False, 0

    return True, remaining


def record_agent_usage(user_id: str):
    """Record an /agent request for rate limiting."""
    from datetime import date
    today = date.today().isoformat()
    user_id = str(user_id)

    if user_id not in agent_usage or agent_usage[user_id]['date'] != today:
        agent_usage[user_id] = {'date': today, 'count': 0}

    agent_usage[user_id]['count'] += 1


def get_agent_usage(user_id: str) -> tuple[int, int]:
    """Get current usage for a user.

    Returns:
        (used, limit): Number used today and daily limit
    """
    from datetime import date
    today = date.today().isoformat()
    user_id = str(user_id)

    # Get per-user daily limit
    limits = get_agent_limits(user_id)
    daily_limit = limits['daily_limit']

    if user_id not in agent_usage or agent_usage[user_id]['date'] != today:
        return 0, daily_limit

    return agent_usage[user_id]['count'], daily_limit


def set_log_level(level: str) -> bool:
    """Set the log level and persist to config.

    Args:
        level: Log level name (DEBUG, INFO, WARNING, ERROR)

    Returns:
        True if successful, False if invalid level
    """
    global LOG_LEVEL

    valid_levels = ('DEBUG', 'INFO', 'WARNING', 'ERROR')
    level = level.upper()

    if level not in valid_levels:
        return False

    LOG_LEVEL = level

    # Persist to config file
    config_data = {}
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                config_data = json.load(f)
        except Exception:
            pass

    config_data['LOG_LEVEL'] = level
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config_data, f, indent=2)

    logger.info(f"Log level set to {level}")
    return True


def mask_api_key(key: str) -> str:
    """Mask an API key for display, showing only first 4 and last 4 characters."""
    if not key or len(key) < 12:
        return "****"
    return f"{key[:4]}...{key[-4:]}"


def load_config():
    """Load persisted state from config file."""
    global last_seen_transaction_id, last_seen_void_id, last_cash_balance
    global ANTHROPIC_API_KEY, POSTER_ACCESS_TOKEN, LOG_LEVEL

    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                cfg = json.load(f)
                # Update sets/dicts in place so imported references see changes
                # Convert all chat IDs to strings for consistent comparison
                subscribed_chats.clear()
                subscribed_chats.update(str(x) for x in cfg.get('subscribed_chats', []))

                theft_alert_chats.clear()
                theft_alert_chats.update(str(x) for x in cfg.get('theft_alert_chats', []))

                # Handle both old single admin and new multiple admins format
                admin_chat_ids.clear()
                admin_chat_ids.update(str(x) for x in cfg.get('admin_chat_ids', []))
                # Backwards compatibility: migrate old admin_chat_id to new format
                old_admin = cfg.get('admin_chat_id')
                if old_admin and str(old_admin) not in admin_chat_ids:
                    admin_chat_ids.add(str(old_admin))

                # Ensure approved_users keys are strings
                approved_users.clear()
                approved_users.update({str(k): v for k, v in cfg.get('approved_users', {}).items()})

                pending_requests.clear()
                pending_requests.update({str(k): v for k, v in cfg.get('pending_requests', {}).items()})

                # Load theft detection state
                last_seen_transaction_id = cfg.get('last_seen_transaction_id')
                last_seen_void_id = cfg.get('last_seen_void_id')
                last_cash_balance = cfg.get('last_cash_balance')

                alerted_transactions.clear()
                alerted_transactions.update(cfg.get('alerted_transactions', []))

                alerted_expenses.clear()
                alerted_expenses.update(cfg.get('alerted_expenses', []))

                # Load API keys (config file overrides env vars)
                if cfg.get('ANTHROPIC_API_KEY'):
                    ANTHROPIC_API_KEY = cfg.get('ANTHROPIC_API_KEY')
                if cfg.get('POSTER_ACCESS_TOKEN'):
                    POSTER_ACCESS_TOKEN = cfg.get('POSTER_ACCESS_TOKEN')

                # Load log level (config file overrides env var)
                if cfg.get('LOG_LEVEL'):
                    LOG_LEVEL = cfg.get('LOG_LEVEL').upper()

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
        # Preserve API keys and log level from existing config
        if existing_config.get('ANTHROPIC_API_KEY'):
            config['ANTHROPIC_API_KEY'] = existing_config['ANTHROPIC_API_KEY']
        if existing_config.get('POSTER_ACCESS_TOKEN'):
            config['POSTER_ACCESS_TOKEN'] = existing_config['POSTER_ACCESS_TOKEN']
        if existing_config.get('LOG_LEVEL'):
            config['LOG_LEVEL'] = existing_config['LOG_LEVEL']
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
