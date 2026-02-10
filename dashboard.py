"""
Web dashboard for the POS Telegram bot.
Provides interactive charts and a real-time sales feed via WebSocket.
"""
import os
import json
import asyncio
import secrets
import logging
from datetime import datetime, timedelta

import uvicorn
from fastapi import FastAPI, Request, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import config

logger = logging.getLogger(__name__)

DASHBOARD_HOST = os.environ.get("DASHBOARD_HOST", "0.0.0.0")
# Railway sets PORT env var; fall back to DASHBOARD_PORT or 8050 for local dev
DASHBOARD_PORT = int(os.environ.get("PORT", os.environ.get("DASHBOARD_PORT", "8050")))
SESSION_SECRET = os.environ.get("SESSION_SECRET", secrets.token_hex(32))
TOKEN_EXPIRY_MINUTES = 60
SESSION_EXPIRY_HOURS = 24
SESSION_COOKIE_NAME = "pos_session_id"


def get_dashboard_url() -> str:
    """Get the public dashboard URL. Auto-detects Railway."""
    if os.environ.get("DASHBOARD_URL"):
        return os.environ["DASHBOARD_URL"]
    railway_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN")
    if railway_domain:
        return f"https://{railway_domain}"
    return f"http://localhost:{DASHBOARD_PORT}"

# Token and session storage (in-memory, lost on restart)
# {token_str: {"user_id": str, "username": str, "created": datetime, "expires": datetime}}
dashboard_tokens = {}
# {session_id: {"user_id": str, "username": str, "created": datetime}}
dashboard_sessions = {}

# Connected WebSocket clients
connected_clients: set[WebSocket] = set()

# FastAPI app
dashboard_app = FastAPI(title="POS Dashboard", docs_url=None, redoc_url=None)

# Templates and static files
_base_dir = os.path.dirname(__file__)
templates = Jinja2Templates(directory=os.path.join(_base_dir, "templates"))
dashboard_app.mount("/static", StaticFiles(directory=os.path.join(_base_dir, "static")), name="static")


# ============================================================
# Auth helpers
# ============================================================

def generate_dashboard_token(user_id: str, username: str) -> str:
    """Generate a one-time login token. Called from the /dashboard Telegram command."""
    now = datetime.now()
    # Clean expired tokens
    expired = [t for t, data in dashboard_tokens.items() if data["expires"] < now]
    for t in expired:
        del dashboard_tokens[t]

    token = secrets.token_urlsafe(32)
    dashboard_tokens[token] = {
        "user_id": str(user_id),
        "username": username,
        "created": now,
        "expires": now + timedelta(minutes=TOKEN_EXPIRY_MINUTES),
    }
    return token


def get_session(request: Request) -> dict | None:
    """Get valid session from request cookie, or None."""
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_id or session_id not in dashboard_sessions:
        return None

    session = dashboard_sessions[session_id]
    user_id = session["user_id"]

    # Check session expiry
    age = datetime.now() - session["created"]
    if age.total_seconds() > SESSION_EXPIRY_HOURS * 3600:
        del dashboard_sessions[session_id]
        return None

    # Verify user is still authorized
    if user_id not in config.admin_chat_ids and user_id not in config.approved_users:
        del dashboard_sessions[session_id]
        return None

    return session


async def require_auth(request: Request) -> dict:
    """FastAPI dependency that checks for valid session."""
    session = get_session(request)
    if session is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return session


# ============================================================
# WebSocket — real-time sales feed
# ============================================================

async def broadcast_sale(sale_data: dict):
    """Broadcast a new sale to all connected WebSocket clients."""
    if not connected_clients:
        return
    message = json.dumps(sale_data)
    disconnected = set()
    for ws in connected_clients:
        try:
            await ws.send_text(message)
        except Exception:
            disconnected.add(ws)
    connected_clients.difference_update(disconnected)


@dashboard_app.websocket("/ws/sales")
async def websocket_sales(websocket: WebSocket):
    """WebSocket endpoint for real-time sales feed."""
    # Authenticate via session cookie
    session_id = websocket.cookies.get(SESSION_COOKIE_NAME)
    if not session_id or session_id not in dashboard_sessions:
        await websocket.close(code=4001, reason="Not authenticated")
        return

    session = dashboard_sessions[session_id]
    age = datetime.now() - session["created"]
    if age.total_seconds() > SESSION_EXPIRY_HOURS * 3600:
        await websocket.close(code=4001, reason="Session expired")
        return

    await websocket.accept()
    connected_clients.add(websocket)
    logger.info(f"WebSocket client connected ({len(connected_clients)} total)")

    try:
        # Keep connection alive — wait for client messages (pings) or disconnect
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        connected_clients.discard(websocket)
        logger.info(f"WebSocket client disconnected ({len(connected_clients)} total)")


# ============================================================
# Auth routes (public)
# ============================================================

@dashboard_app.get("/auth")
async def auth_token_exchange(request: Request, token: str = Query(...)):
    """Exchange a token for a session cookie. Token is reusable until it expires."""
    token_data = dashboard_tokens.get(token)
    if not token_data:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Invalid or expired link. Please run /dashboard in Telegram to get a new link."
        }, status_code=401)

    if token_data["expires"] < datetime.now():
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "This link has expired. Please run /dashboard in Telegram to get a new link."
        }, status_code=401)

    # Create session
    session_id = secrets.token_urlsafe(32)
    dashboard_sessions[session_id] = {
        "user_id": token_data["user_id"],
        "username": token_data["username"],
        "created": datetime.now(),
    }

    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_id,
        max_age=SESSION_EXPIRY_HOURS * 3600,
        httponly=True,
        samesite="lax",
    )
    return response


@dashboard_app.get("/logout")
async def logout(request: Request):
    """Clear session and redirect to login message."""
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if session_id:
        dashboard_sessions.pop(session_id, None)
    response = templates.TemplateResponse("login.html", {
        "request": request,
        "error": "You have been logged out. Run /dashboard in Telegram to log in again."
    })
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response


# ============================================================
# Data helpers (import from app.py, run sync calls in executor)
# ============================================================

async def _run_sync(func, *args):
    """Run a synchronous function in a thread executor."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, func, *args)


def _get_date_range(period: str):
    """Calculate date_from and date_to for a period. Returns (date_from_str, date_to_str, display_label)."""
    from app import get_business_date
    today = get_business_date()

    if period == "today":
        date_str = today.strftime('%Y%m%d')
        return date_str, date_str, today.strftime('%d %b %Y')
    elif period == "week":
        # Monday of this week
        monday = today - timedelta(days=today.weekday())
        date_from = monday.strftime('%Y%m%d')
        date_to = today.strftime('%Y%m%d')
        return date_from, date_to, f"{monday.strftime('%d %b')} - {today.strftime('%d %b %Y')}"
    elif period == "month":
        first_day = today.replace(day=1)
        date_from = first_day.strftime('%Y%m%d')
        date_to = today.strftime('%Y%m%d')
        return date_from, date_to, f"{first_day.strftime('%d %b')} - {today.strftime('%d %b %Y')}"
    else:
        date_str = today.strftime('%Y%m%d')
        return date_str, date_str, today.strftime('%d %b %Y')


def _filter_closed_sales(transactions):
    """Filter transactions to only closed sales with sum > 0."""
    return [t for t in transactions
            if str(t.get('status', '')) == '2' and int(t.get('sum', 0) or 0) > 0]


def _build_daily_breakdown(transactions):
    """Group transactions by date for Chart.js daily breakdown."""
    from app import adjust_poster_time
    from collections import defaultdict

    daily = defaultdict(lambda: {"sales": 0, "profit": 0, "count": 0})
    for txn in transactions:
        close_date = adjust_poster_time(txn.get('date_close_date', ''))
        day_key = close_date.split(' ')[0] if close_date else 'Unknown'
        daily[day_key]["sales"] += int(txn.get('sum', 0) or 0)
        daily[day_key]["profit"] += int(txn.get('total_profit', 0) or 0)
        daily[day_key]["count"] += 1

    # Sort by date
    sorted_days = sorted(daily.items())
    return {
        "labels": [d[0] for d in sorted_days],
        "sales": [d[1]["sales"] for d in sorted_days],
        "profit": [d[1]["profit"] for d in sorted_days],
        "count": [d[1]["count"] for d in sorted_days],
    }


def _build_hourly_breakdown(transactions):
    """Group transactions by hour for Chart.js hourly breakdown."""
    from app import adjust_poster_time

    hourly = {h: {"sales": 0, "profit": 0, "count": 0} for h in range(24)}
    for txn in transactions:
        close_date = adjust_poster_time(txn.get('date_close_date', ''))
        if ' ' in close_date:
            try:
                hour = int(close_date.split(' ')[1].split(':')[0])
                hourly[hour]["sales"] += int(txn.get('sum', 0) or 0)
                hourly[hour]["profit"] += int(txn.get('total_profit', 0) or 0)
                hourly[hour]["count"] += 1
            except (ValueError, IndexError):
                pass

    # Only include hours that have data, plus a bit of context
    labels = [f"{h:02d}:00" for h in range(24)]
    return {
        "labels": labels,
        "sales": [hourly[h]["sales"] for h in range(24)],
        "profit": [hourly[h]["profit"] for h in range(24)],
        "count": [hourly[h]["count"] for h in range(24)],
    }


# ============================================================
# API Endpoints (JSON)
# ============================================================

@dashboard_app.get("/api/sales/today")
async def api_sales_today(session: dict = Depends(require_auth)):
    """Return today's closed sales."""
    from app import fetch_transactions, get_business_date, adjust_poster_time, fetch_transaction_products

    today_str = get_business_date().strftime('%Y%m%d')
    transactions = await _run_sync(fetch_transactions, today_str)
    sales = _filter_closed_sales(transactions)
    sales.sort(key=lambda x: int(x.get('transaction_id', 0) or 0), reverse=True)

    result = []
    for txn in sales:
        txn_id = int(txn.get('transaction_id', 0) or 0)
        close_time = adjust_poster_time(txn.get('date_close_date', ''))

        # Fetch items for each transaction
        items = []
        try:
            products = await _run_sync(fetch_transaction_products, txn_id)
            for p in products:
                qty = float(p.get('num', 1))
                name = p.get('product_name', 'Unknown')
                items.append({"name": name, "qty": qty})
        except Exception:
            pass

        result.append({
            "transaction_id": txn_id,
            "sum": int(txn.get('sum', 0) or 0),
            "total_profit": int(txn.get('total_profit', 0) or 0),
            "payed_cash": int(txn.get('payed_cash', 0) or 0),
            "payed_card": int(txn.get('payed_card', 0) or 0),
            "table_name": txn.get('table_name', ''),
            "close_time": close_time,
            "items": items,
        })

    return result


@dashboard_app.get("/api/summary/{period}")
async def api_summary(period: str, session: dict = Depends(require_auth)):
    """Return summary metrics and chart data for a period."""
    from app import fetch_transactions, fetch_finance_transactions, calculate_summary, calculate_expenses

    if period not in ("today", "week", "month"):
        raise HTTPException(status_code=400, detail="Invalid period")

    date_from, date_to, display = _get_date_range(period)
    transactions = await _run_sync(fetch_transactions, date_from, date_to)
    finance_txns = await _run_sync(fetch_finance_transactions, date_from, date_to)

    closed = _filter_closed_sales(transactions)
    summary = calculate_summary(closed)
    expenses = calculate_expenses(finance_txns)

    result = {
        "period": period,
        "display": display,
        "metrics": {
            **summary,
            "total_expenses": expenses["total_expenses"],
            "net_profit": summary["total_sales"] - expenses["total_expenses"],
        },
        "daily_breakdown": _build_daily_breakdown(closed),
    }

    # Add hourly breakdown for single-day views
    if period == "today":
        result["hourly_breakdown"] = _build_hourly_breakdown(closed)

    return result


@dashboard_app.get("/api/summary/custom")
async def api_summary_custom(
    date_from: str = Query(..., alias="from"),
    date_to: str = Query(..., alias="to"),
    session: dict = Depends(require_auth),
):
    """Return summary for a custom date range."""
    from app import fetch_transactions, fetch_finance_transactions, calculate_summary, calculate_expenses

    transactions = await _run_sync(fetch_transactions, date_from, date_to)
    finance_txns = await _run_sync(fetch_finance_transactions, date_from, date_to)

    closed = _filter_closed_sales(transactions)
    summary = calculate_summary(closed)
    expenses = calculate_expenses(finance_txns)

    return {
        "period": "custom",
        "display": f"{date_from} - {date_to}",
        "metrics": {
            **summary,
            "total_expenses": expenses["total_expenses"],
            "net_profit": summary["total_sales"] - expenses["total_expenses"],
        },
        "daily_breakdown": _build_daily_breakdown(closed),
    }


@dashboard_app.get("/api/products/{period}")
async def api_products(period: str, session: dict = Depends(require_auth)):
    """Return product analytics data for a period."""
    from app import fetch_product_sales

    if period not in ("today", "week", "month"):
        raise HTTPException(status_code=400, detail="Invalid period")

    date_from, date_to, display = _get_date_range(period)
    products = await _run_sync(fetch_product_sales, date_from, date_to)

    # Process product data
    product_list = []
    total_revenue = 0
    total_profit = 0
    total_items = 0

    for p in products:
        revenue = int(p.get('payed_sum', 0) or 0)
        profit = int(p.get('product_profit', 0) or 0)
        count = float(p.get('count', 0) or 0)
        name = p.get('product_name', 'Unknown')

        if revenue > 0:
            product_list.append({
                "product_name": name,
                "count": count,
                "payed_sum": revenue,
                "product_profit": profit,
            })
            total_revenue += revenue
            total_profit += profit
            total_items += count

    # Sort by revenue descending
    product_list.sort(key=lambda x: x["payed_sum"], reverse=True)

    # Build chart data — top 10 for bar chart, top 8 + "Other" for pie
    top_10 = product_list[:10]
    top_8 = product_list[:8]
    other_revenue = sum(p["payed_sum"] for p in product_list[8:])

    pie_labels = [p["product_name"] for p in top_8]
    pie_values = [p["payed_sum"] for p in top_8]
    if other_revenue > 0:
        pie_labels.append("Other")
        pie_values.append(other_revenue)

    return {
        "period": period,
        "display": display,
        "totals": {
            "total_items": total_items,
            "total_revenue": total_revenue,
            "total_profit": total_profit,
        },
        "products": product_list,
        "chart_data": {
            "top_products": {
                "labels": [p["product_name"] for p in top_10],
                "revenue": [p["payed_sum"] for p in top_10],
                "profit": [p["product_profit"] for p in top_10],
            },
            "revenue_pie": {
                "labels": pie_labels,
                "values": pie_values,
            },
        },
    }


# ============================================================
# Page Routes (HTML)
# ============================================================

@dashboard_app.get("/", response_class=HTMLResponse)
async def page_dashboard(request: Request):
    """Live sales feed dashboard."""
    session = get_session(request)
    if session is None:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Please run /dashboard in Telegram to get a login link."
        }, status_code=401)

    from app import fetch_transactions, fetch_cash_shifts, get_business_date, adjust_poster_time, calculate_summary, format_currency

    today_str = get_business_date().strftime('%Y%m%d')
    transactions = await _run_sync(fetch_transactions, today_str)
    closed = _filter_closed_sales(transactions)
    closed.sort(key=lambda x: int(x.get('transaction_id', 0) or 0), reverse=True)
    summary = calculate_summary(closed)

    # Fetch cash register status
    shifts = await _run_sync(fetch_cash_shifts)
    cash_register = None
    if shifts:
        latest = shifts[0]
        shift_end = latest.get('date_end', '')
        amount_start = int(latest.get('amount_start', 0) or 0)
        cash_sales = int(latest.get('amount_sell_cash', 0) or 0)
        cash_out = int(latest.get('amount_credit', 0) or 0)
        if shift_end:
            current_cash = int(latest.get('amount_end', 0) or 0)
            cash_status = "Closed"
        else:
            current_cash = amount_start + cash_sales - cash_out
            cash_status = "Open"
        cash_register = {
            "status": cash_status,
            "current_cash": current_cash,
        }

    # Pre-process sales for template
    sales_display = []
    for txn in closed[:30]:
        close_time = adjust_poster_time(txn.get('date_close_date', ''))
        time_str = close_time.split(' ')[1][:5] if ' ' in close_time else ''
        payed_cash = int(txn.get('payed_cash', 0) or 0)
        payed_card = int(txn.get('payed_card', 0) or 0)

        if payed_card > 0 and payed_cash > 0:
            payment = "Cash+Card"
            payment_class = "badge-mixed"
        elif payed_card > 0:
            payment = "Card"
            payment_class = "badge-card"
        else:
            payment = "Cash"
            payment_class = "badge-cash"

        sales_display.append({
            "transaction_id": int(txn.get('transaction_id', 0) or 0),
            "time": time_str,
            "amount": format_currency(int(txn.get('sum', 0) or 0)),
            "profit": format_currency(int(txn.get('total_profit', 0) or 0)),
            "table_name": txn.get('table_name', ''),
            "payment": payment,
            "payment_class": payment_class,
        })

    ws_host = get_dashboard_url()
    ws_url = ws_host.replace("http://", "ws://").replace("https://", "wss://") + "/ws/sales"

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "active_page": "dashboard",
        "summary": summary,
        "cash_register": cash_register,
        "sales": sales_display,
        "format_currency": format_currency,
        "ws_url": ws_url,
        "username": session["username"],
    })


@dashboard_app.get("/summary", response_class=HTMLResponse)
async def page_summary(request: Request, period: str = "today"):
    """Summary dashboard page."""
    session = get_session(request)
    if session is None:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Please run /dashboard in Telegram to get a login link."
        }, status_code=401)

    from app import fetch_transactions, fetch_finance_transactions, calculate_summary, calculate_expenses, format_currency

    if period not in ("today", "week", "month"):
        period = "today"

    date_from, date_to, display = _get_date_range(period)
    transactions = await _run_sync(fetch_transactions, date_from, date_to)
    finance_txns = await _run_sync(fetch_finance_transactions, date_from, date_to)

    closed = _filter_closed_sales(transactions)
    summary = calculate_summary(closed)
    expenses = calculate_expenses(finance_txns)

    daily = _build_daily_breakdown(closed)
    hourly = _build_hourly_breakdown(closed) if period == "today" else None

    # Build expense-by-category pie chart data
    from collections import defaultdict
    expense_by_category = defaultdict(int)
    for exp in expenses["expense_list"]:
        cat = exp.get("category") or "Uncategorized"
        expense_by_category[cat] += exp["amount"]
    # Sort by amount descending
    sorted_cats = sorted(expense_by_category.items(), key=lambda x: x[1], reverse=True)
    expense_pie = {
        "labels": [c[0] for c in sorted_cats],
        "values": [c[1] for c in sorted_cats],
    } if sorted_cats else None

    return templates.TemplateResponse("summary.html", {
        "request": request,
        "active_page": "summary",
        "period": period,
        "display": display,
        "summary": summary,
        "expenses": expenses,
        "net_profit": summary["total_sales"] - expenses["total_expenses"],
        "daily_data": json.dumps(daily),
        "hourly_data": json.dumps(hourly) if hourly else "null",
        "expense_pie_data": json.dumps(expense_pie) if expense_pie else "null",
        "format_currency": format_currency,
        "username": session["username"],
    })


@dashboard_app.get("/products", response_class=HTMLResponse)
async def page_products(request: Request, period: str = "today"):
    """Product analytics page."""
    session = get_session(request)
    if session is None:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Please run /dashboard in Telegram to get a login link."
        }, status_code=401)

    from app import fetch_product_sales, format_currency

    if period not in ("today", "week", "month"):
        period = "today"

    date_from, date_to, display = _get_date_range(period)
    products_raw = await _run_sync(fetch_product_sales, date_from, date_to)

    # Process and sort
    product_list = []
    total_revenue = 0
    total_profit = 0
    total_items = 0

    for p in products_raw:
        revenue = int(p.get('payed_sum', 0) or 0)
        profit = int(p.get('product_profit', 0) or 0)
        count = float(p.get('count', 0) or 0)
        name = p.get('product_name', 'Unknown')

        if revenue > 0:
            product_list.append({
                "product_name": name,
                "count": count,
                "payed_sum": revenue,
                "product_profit": profit,
            })
            total_revenue += revenue
            total_profit += profit
            total_items += count

    product_list.sort(key=lambda x: x["payed_sum"], reverse=True)

    # Chart data
    top_10 = product_list[:10]
    top_8 = product_list[:8]
    other_revenue = sum(p["payed_sum"] for p in product_list[8:])

    pie_labels = [p["product_name"] for p in top_8]
    pie_values = [p["payed_sum"] for p in top_8]
    if other_revenue > 0:
        pie_labels.append("Other")
        pie_values.append(other_revenue)

    bar_data = {
        "labels": [p["product_name"] for p in top_10],
        "revenue": [p["payed_sum"] for p in top_10],
        "profit": [p["product_profit"] for p in top_10],
    }
    pie_data = {"labels": pie_labels, "values": pie_values}

    return templates.TemplateResponse("products.html", {
        "request": request,
        "active_page": "products",
        "period": period,
        "display": display,
        "products": product_list,
        "total_revenue": total_revenue,
        "total_profit": total_profit,
        "total_items": total_items,
        "bar_data": json.dumps(bar_data),
        "pie_data": json.dumps(pie_data),
        "format_currency": format_currency,
        "username": session["username"],
    })


# ============================================================
# Server lifecycle
# ============================================================

_server = None


async def start_dashboard_server():
    """Start the uvicorn server. Called as asyncio.create_task() from app.py startup."""
    global _server
    config_obj = uvicorn.Config(
        app=dashboard_app,
        host=DASHBOARD_HOST,
        port=DASHBOARD_PORT,
        log_level="info",
        loop="none",
    )
    _server = uvicorn.Server(config_obj)
    logger.info(f"Starting dashboard on http://{DASHBOARD_HOST}:{DASHBOARD_PORT}")
    await _server.serve()


async def stop_dashboard_server():
    """Stop the uvicorn server gracefully."""
    global _server
    if _server:
        _server.should_exit = True
        logger.info("Dashboard server stopping")
