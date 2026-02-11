"""
Web dashboard for the POS Telegram bot.
Provides interactive charts and a real-time sales feed via WebSocket.
"""
import os
import json
import asyncio
import base64
import hashlib
import logging
from datetime import datetime, timedelta

import uvicorn
from fastapi import FastAPI, Request, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import config

logger = logging.getLogger(__name__)

DASHBOARD_HOST = os.environ.get("DASHBOARD_HOST", "0.0.0.0")
# Railway sets PORT env var; fall back to DASHBOARD_PORT or 8050 for local dev
DASHBOARD_PORT = int(os.environ.get("PORT", os.environ.get("DASHBOARD_PORT", "8050")))


def get_dashboard_url() -> str:
    """Get the public dashboard URL. Auto-detects Railway."""
    if os.environ.get("DASHBOARD_URL"):
        return os.environ["DASHBOARD_URL"]
    railway_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN")
    if railway_domain:
        return f"https://{railway_domain}"
    return f"http://localhost:{DASHBOARD_PORT}"

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

def _verify_password(password: str, stored_hash: str) -> bool:
    """Verify a password against a stored salt$hash."""
    salt, hash_val = stored_hash.split('$', 1)
    return hashlib.sha256(f"{salt}{password}".encode()).hexdigest() == hash_val


def _unauthorized_response():
    """Return a 401 response that triggers the browser's basic auth dialog."""
    return Response(
        content="Unauthorized",
        status_code=401,
        headers={"WWW-Authenticate": 'Basic realm="POS Dashboard"'},
    )


def check_basic_auth(request: Request) -> dict | None:
    """Check HTTP Basic Auth credentials against dashboard_passwords.

    Returns {"user_id": str, "username": str} on success, None on failure.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Basic "):
        return None

    try:
        decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
        username, password = decoded.split(":", 1)
    except Exception:
        return None

    # Look up user by username in dashboard_passwords
    for chat_id, entry in config.dashboard_passwords.items():
        if entry["username"] == username and _verify_password(password, entry["password_hash"]):
            return {"user_id": chat_id, "username": username}

    return None


async def require_auth(request: Request) -> dict:
    """FastAPI dependency that checks HTTP Basic Auth."""
    session = check_basic_auth(request)
    if session is None:
        raise HTTPException(
            status_code=401,
            detail="Not authenticated",
            headers={"WWW-Authenticate": 'Basic realm="POS Dashboard"'},
        )
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
        start = today - timedelta(days=6)
        date_from = start.strftime('%Y%m%d')
        date_to = today.strftime('%Y%m%d')
        return date_from, date_to, f"{start.strftime('%d %b')} - {today.strftime('%d %b %Y')}"
    elif period == "month":
        start = today - timedelta(days=29)
        date_from = start.strftime('%Y%m%d')
        date_to = today.strftime('%Y%m%d')
        return date_from, date_to, f"{start.strftime('%d %b')} - {today.strftime('%d %b %Y')}"
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


def _build_cash_timeline(transactions, finance_txns):
    """Build cumulative cash timeline from sales and expenses."""
    from app import adjust_poster_time

    # Collect cash-in events from sales
    events = []
    for txn in transactions:
        payed_cash = int(txn.get('payed_cash', 0) or 0)
        if payed_cash > 0:
            close_time = adjust_poster_time(txn.get('date_close_date', ''))
            events.append({"time": close_time, "amount": payed_cash})

    # Collect cash-out events from expenses
    for txn in finance_txns:
        amount = int(txn.get('amount', 0) or 0)
        comment = txn.get('comment', '')
        if 'Cash payments' in comment:
            continue
        if amount < 0:
            date_str = txn.get('date', '')
            events.append({"time": date_str, "amount": amount})

    if not events:
        return None

    events.sort(key=lambda e: e["time"])

    balance = 0
    points = []
    for ev in events:
        balance += ev["amount"]
        # Use ISO timestamp for Chart.js time scale
        t = ev["time"]
        if ' ' in t:
            iso = t.replace(' ', 'T')
        else:
            iso = t + "T00:00:00"
        points.append({"x": iso, "y": balance})

    return {"points": points}


def _build_hourly_by_weekday(transactions):
    """Group transactions by day-of-week and hour for Chart.js."""
    from app import adjust_poster_time

    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    data = {day: {h: {"sales": 0, "profit": 0, "count": 0} for h in range(24)} for day in day_names}

    for txn in transactions:
        close_date = adjust_poster_time(txn.get('date_close_date', ''))
        if ' ' in close_date:
            try:
                dt = datetime.strptime(close_date, "%Y-%m-%d %H:%M:%S")
                day_name = day_names[dt.weekday()]
                hour = dt.hour
                data[day_name][hour]["sales"] += int(txn.get('sum', 0) or 0)
                data[day_name][hour]["profit"] += int(txn.get('total_profit', 0) or 0)
                data[day_name][hour]["count"] += 1
            except (ValueError, IndexError):
                pass

    labels = [f"{h:02d}:00" for h in range(24)]
    result = {}
    for day in day_names:
        result[day] = {
            "labels": labels,
            "sales": [data[day][h]["sales"] for h in range(24)],
            "profit": [data[day][h]["profit"] for h in range(24)],
            "count": [data[day][h]["count"] for h in range(24)],
        }
    return result


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

    # Build chart data — top 10 for bar chart, dynamic cutoff for pie
    top_10 = product_list[:10]

    # Dynamic pie cutoff: keep adding products until "Other" is ≤ 15% of total
    pie_cutoff = min(len(product_list), 8)
    if total_revenue > 0:
        for i in range(pie_cutoff, len(product_list)):
            remaining = sum(p["payed_sum"] for p in product_list[i:])
            if remaining / total_revenue <= 0.15:
                break
            pie_cutoff = i + 1

    pie_products = product_list[:pie_cutoff]
    other_revenue = sum(p["payed_sum"] for p in product_list[pie_cutoff:])

    pie_labels = [p["product_name"] for p in pie_products]
    pie_values = [p["payed_sum"] for p in pie_products]
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
    session = check_basic_auth(request)
    if session is None:
        return _unauthorized_response()

    from app import fetch_transactions, fetch_finance_transactions, fetch_cash_shifts, get_business_date, adjust_poster_time, calculate_summary, format_currency

    today_str = get_business_date().strftime('%Y%m%d')
    transactions = await _run_sync(fetch_transactions, today_str)
    finance_txns = await _run_sync(fetch_finance_transactions, today_str)
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

    # Build cash register timeline (running balance over time)
    cash_timeline = _build_cash_timeline(closed, finance_txns)
    if cash_timeline and cash_register:
        opening = int(shifts[0].get('amount_start', 0) or 0)
        # Offset all points by shift opening balance
        for p in cash_timeline["points"]:
            p["y"] += opening
        # Add opening point at shift start time
        shift_start = adjust_poster_time(latest.get('date_start', ''))
        open_iso = shift_start.replace(' ', 'T') if ' ' in shift_start else shift_start + "T00:00:00"
        cash_timeline["points"].insert(0, {"x": open_iso, "y": opening})
        if cash_register["status"] == "Closed":
            close_iso = shift_end.replace(' ', 'T') if ' ' in shift_end else shift_end + "T00:00:00"
            cash_timeline["points"].append({"x": close_iso, "y": cash_register["current_cash"]})
        cash_timeline["points"].sort(key=lambda p: p["x"])

    # Pre-process sales and expenses for merged feed
    from app import calculate_expenses
    expenses = calculate_expenses(finance_txns)

    feed_items = []
    for txn in closed:
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

        feed_items.append({
            "type": "sale",
            "sort_time": close_time,
            "transaction_id": int(txn.get('transaction_id', 0) or 0),
            "time": time_str,
            "amount": format_currency(int(txn.get('sum', 0) or 0)),
            "profit": format_currency(int(txn.get('total_profit', 0) or 0)),
            "table_name": txn.get('table_name', ''),
            "payment": payment,
            "payment_class": payment_class,
        })

    for exp in expenses["expense_list"]:
        exp_date = exp.get('date', '')
        time_str = exp_date.split(' ')[1][:5] if ' ' in exp_date else ''
        feed_items.append({
            "type": "expense",
            "sort_time": exp_date,
            "time": time_str,
            "amount": format_currency(exp["amount"]),
            "comment": exp.get("comment", ""),
            "category": exp.get("category", ""),
        })

    feed_items.sort(key=lambda x: x["sort_time"], reverse=True)
    feed_items = feed_items[:40]

    ws_host = get_dashboard_url()
    ws_url = ws_host.replace("http://", "ws://").replace("https://", "wss://") + "/ws/sales"

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "active_page": "dashboard",
        "summary": summary,
        "cash_register": cash_register,
        "cash_timeline": json.dumps(cash_timeline) if cash_timeline else "null",
        "feed_items": feed_items,
        "format_currency": format_currency,
        "ws_url": ws_url,
        "username": session["username"],
    })


@dashboard_app.get("/summary", response_class=HTMLResponse)
async def page_summary(
    request: Request,
    period: str = "today",
    date_from: str = Query(None),
    date_to: str = Query(None),
):
    """Summary dashboard page."""
    session = check_basic_auth(request)
    if session is None:
        return _unauthorized_response()

    from app import fetch_transactions, fetch_finance_transactions, fetch_cash_shifts, adjust_poster_time, calculate_summary, calculate_expenses, format_currency

    date_from_iso = ""
    date_to_iso = ""
    date_from_api = ""
    date_to_api = ""
    display = ""

    if period == "custom" and date_from and date_to:
        # Parse ISO dates (YYYY-MM-DD) to API format (YYYYMMDD)
        try:
            df = datetime.strptime(date_from, "%Y-%m-%d")
            dt = datetime.strptime(date_to, "%Y-%m-%d")
            date_from_iso = date_from
            date_to_iso = date_to
            date_from_api = df.strftime("%Y%m%d")
            date_to_api = dt.strftime("%Y%m%d")
            display = f"{df.strftime('%d %b')} - {dt.strftime('%d %b %Y')}"
        except ValueError:
            period = "today"
    else:
        if period not in ("today", "week", "month"):
            period = "today"

    if period != "custom":
        date_from_api, date_to_api, display = _get_date_range(period)

    transactions = await _run_sync(fetch_transactions, date_from_api, date_to_api)
    finance_txns = await _run_sync(fetch_finance_transactions, date_from_api, date_to_api)

    closed = _filter_closed_sales(transactions)
    summary = calculate_summary(closed)
    expenses = calculate_expenses(finance_txns)

    daily = _build_daily_breakdown(closed)
    hourly = _build_hourly_breakdown(closed)
    cash_timeline = _build_cash_timeline(closed, finance_txns)

    # Add shift opening balance to cash timeline
    shifts = await _run_sync(fetch_cash_shifts)
    if cash_timeline and shifts:
        # Find shifts that overlap with the selected date range
        for shift in shifts:
            shift_start = adjust_poster_time(shift.get('date_start', ''))
            opening = int(shift.get('amount_start', 0) or 0)
            if opening > 0:
                # Offset all points by opening balance
                for p in cash_timeline["points"]:
                    p["y"] += opening
                open_iso = shift_start.replace(' ', 'T') if ' ' in shift_start else shift_start + "T00:00:00"
                cash_timeline["points"].insert(0, {"x": open_iso, "y": opening})
                cash_timeline["points"].sort(key=lambda p: p["x"])
                break

    # Build expense-by-comment pie chart data
    from collections import defaultdict
    expense_by_comment = defaultdict(int)
    for exp in expenses["expense_list"]:
        label = exp.get("comment") or exp.get("category") or "Uncategorized"
        expense_by_comment[label] += exp["amount"]
    # Sort by amount descending
    sorted_cats = sorted(expense_by_comment.items(), key=lambda x: x[1], reverse=True)
    expense_pie = {
        "labels": [c[0] for c in sorted_cats],
        "values": [c[1] for c in sorted_cats],
    } if sorted_cats else None

    # Build merged transactions list (sales + expenses) sorted by date
    all_transactions = []
    for txn in closed:
        close_time = adjust_poster_time(txn.get('date_close_date', ''))
        all_transactions.append({
            "type": "sale",
            "date": close_time,
            "description": txn.get('table_name', '') or "Sale",
            "amount": int(txn.get('sum', 0) or 0),
        })
    for exp in expenses["expense_list"]:
        all_transactions.append({
            "type": "expense",
            "date": exp["date"],
            "description": exp.get("comment") or exp.get("category") or "Expense",
            "amount": exp["amount"],
        })
    all_transactions.sort(key=lambda x: x["date"], reverse=True)

    return templates.TemplateResponse("summary.html", {
        "request": request,
        "active_page": "summary",
        "period": period,
        "display": display,
        "summary": summary,
        "expenses": expenses,
        "all_transactions": all_transactions,
        "net_profit": summary["total_sales"] - expenses["total_expenses"],
        "daily_data": json.dumps(daily),
        "hourly_data": json.dumps(hourly) if hourly else "null",
        "expense_pie_data": json.dumps(expense_pie) if expense_pie else "null",
        "cash_timeline": json.dumps(cash_timeline) if cash_timeline else "null",
        "date_from_iso": date_from_iso,
        "date_to_iso": date_to_iso,
        "format_currency": format_currency,
        "username": session["username"],
    })


@dashboard_app.get("/hourly", response_class=HTMLResponse)
async def page_hourly(
    request: Request,
    period: str = "month",
    date_from: str = Query(None),
    date_to: str = Query(None),
):
    """Hourly summary page with charts grouped by day of week."""
    session = check_basic_auth(request)
    if session is None:
        return _unauthorized_response()

    from app import fetch_transactions, format_currency

    date_from_iso = ""
    date_to_iso = ""
    date_from_api = ""
    date_to_api = ""
    display = ""

    if period == "custom" and date_from and date_to:
        try:
            df = datetime.strptime(date_from, "%Y-%m-%d")
            dt = datetime.strptime(date_to, "%Y-%m-%d")
            date_from_iso = date_from
            date_to_iso = date_to
            date_from_api = df.strftime("%Y%m%d")
            date_to_api = dt.strftime("%Y%m%d")
            display = f"{df.strftime('%d %b')} - {dt.strftime('%d %b %Y')}"
        except ValueError:
            period = "month"
    else:
        if period not in ("today", "week", "month"):
            period = "month"

    if period != "custom":
        date_from_api, date_to_api, display = _get_date_range(period)

    transactions = await _run_sync(fetch_transactions, date_from_api, date_to_api)
    closed = _filter_closed_sales(transactions)
    weekday_data = _build_hourly_by_weekday(closed)

    return templates.TemplateResponse("hourly.html", {
        "request": request,
        "active_page": "hourly",
        "period": period,
        "display": display,
        "weekday_data": json.dumps(weekday_data),
        "date_from_iso": date_from_iso,
        "date_to_iso": date_to_iso,
        "format_currency": format_currency,
        "username": session["username"],
    })


@dashboard_app.get("/products", response_class=HTMLResponse)
async def page_products(request: Request, period: str = "today"):
    """Product analytics page."""
    session = check_basic_auth(request)
    if session is None:
        return _unauthorized_response()

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

    # Dynamic pie cutoff: keep adding products until "Other" is ≤ 15% of total
    pie_cutoff = min(len(product_list), 8)
    if total_revenue > 0:
        for i in range(pie_cutoff, len(product_list)):
            remaining = sum(p["payed_sum"] for p in product_list[i:])
            if remaining / total_revenue <= 0.15:
                break
            pie_cutoff = i + 1

    pie_products = product_list[:pie_cutoff]
    other_revenue = sum(p["payed_sum"] for p in product_list[pie_cutoff:])

    pie_labels = [p["product_name"] for p in pie_products]
    pie_values = [p["payed_sum"] for p in pie_products]
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
