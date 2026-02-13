"""
Web dashboard for the POS Telegram bot.
Provides interactive charts and a real-time sales feed via WebSocket.
"""
import os
import json
import asyncio
import base64
import calendar
import hashlib
import logging
from datetime import datetime, timedelta

import uvicorn
from fastapi import FastAPI, Request, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

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
    """Check HTTP Basic Auth credentials against approved_users.

    Returns {"user_id": str, "username": str, "is_admin": bool} on success, None on failure.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Basic "):
        return None

    try:
        decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
        login_name, password = decoded.split(":", 1)
    except Exception:
        return None

    # Look up user by username in approved_users
    # Login accepts "@username" (strip @) or "id:chatid"
    for chat_id, entry in config.approved_users.items():
        stored_hash = entry.get("password_hash")
        if not stored_hash:
            continue
        stored_username = entry.get("username")
        # Match "@tuxx" -> "tuxx", or "id:12345" -> chat_id
        if stored_username and login_name == f"@{stored_username}":
            if _verify_password(password, stored_hash):
                return {"user_id": chat_id, "username": f"@{stored_username}", "is_admin": chat_id in config.admin_chat_ids}
        elif login_name == f"id:{chat_id}":
            if _verify_password(password, stored_hash):
                return {"user_id": chat_id, "username": f"id:{chat_id}", "is_admin": chat_id in config.admin_chat_ids}

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
    """Filter transactions to open and closed sales with sum > 0."""
    return [t for t in transactions
            if str(t.get('status', '')) in ('1', '2') and int(t.get('sum', 0) or 0) > 0]


def _edit_distance(a, b):
    """Compute Levenshtein edit distance between two strings."""
    if len(a) < len(b):
        return _edit_distance(b, a)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (ca != cb)))
        prev = curr
    return prev[-1]


def _merge_similar_labels(label_amounts):
    """Merge expense labels that differ only by small typos.

    Uses case-insensitive comparison. Allowed edit distance scales with
    string length: 1 for short labels (<=6 chars), 2 for longer ones.
    The label with the highest total amount is kept as the canonical name.
    """
    canonical = {}  # lowered_label -> (display_label, total_amount)
    for label, amount in label_amounts.items():
        lower = label.lower()
        merged = False
        for key in list(canonical):
            max_dist = 1 if max(len(lower), len(key)) <= 6 else 2
            if _edit_distance(lower, key) <= max_dist:
                existing_label, existing_amount = canonical[key]
                new_total = existing_amount + amount
                # Keep the label that had more money as the canonical name
                if amount > existing_amount:
                    canonical[lower] = (label, new_total)
                    if key != lower:
                        del canonical[key]
                else:
                    canonical[key] = (existing_label, new_total)
                merged = True
                break
        if not merged:
            canonical[lower] = (label, amount)

    from collections import defaultdict
    result = defaultdict(int)
    for display_label, total in canonical.values():
        result[display_label] = total
    return result


def _build_daily_breakdown(transactions):
    """Group transactions by date for Chart.js daily breakdown."""
    from app import adjust_poster_time
    from collections import defaultdict

    daily = defaultdict(lambda: {"sales": 0, "profit": 0, "count": 0})
    for txn in transactions:
        close_date = adjust_poster_time(txn.get('date_close_date', '') or txn.get('date', ''))
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


def _build_cash_timeline(transactions, finance_txns, shifts):
    """Build cash timeline anchored to Poster shift data.

    For each shift, plots opening balance, incremental cash events, and
    closing balance (if closed).  This ensures the graph matches the
    cash-register values Poster reports.
    """
    from app import adjust_poster_time

    if not shifts:
        return None

    def _to_iso(ts):
        return ts.replace(' ', 'T') if ' ' in ts else ts + "T00:00:00"

    # Collect all cash events (using raw Poster times for shift matching)
    cash_events = []
    for txn in transactions:
        payed_cash = int(txn.get('payed_cash', 0) or 0)
        if payed_cash > 0:
            raw_time = txn.get('date_close_date', '')
            cash_events.append({"raw": raw_time, "amount": payed_cash})

    for txn in finance_txns:
        amount = int(txn.get('amount', 0) or 0)
        comment = txn.get('comment', '')
        if 'Cash payments' in comment:
            continue
        if amount < 0:
            raw_time = txn.get('date', '')
            cash_events.append({"raw": raw_time, "amount": amount})

    if not cash_events:
        return None

    # Determine the time range of our data
    earliest = min(ev["raw"] for ev in cash_events)
    latest_time = max(ev["raw"] for ev in cash_events)

    points = []

    # Process shifts in chronological order (API returns newest first)
    for shift in reversed(shifts):
        shift_start_raw = shift.get('date_start', '')
        shift_end_raw = shift.get('date_end', '')
        opening = int(shift.get('amount_start', 0) or 0)

        # Skip shifts that don't overlap with our data range
        if shift_end_raw and shift_end_raw < earliest:
            continue
        if shift_start_raw > latest_time:
            continue

        # Opening point
        start_iso = _to_iso(adjust_poster_time(shift_start_raw))
        points.append({"x": start_iso, "y": opening})

        # Find events within this shift's time window
        shift_events = []
        for ev in cash_events:
            if ev["raw"] >= shift_start_raw and (not shift_end_raw or ev["raw"] <= shift_end_raw):
                shift_events.append(ev)
        shift_events.sort(key=lambda e: e["raw"])

        # Plot incremental balance changes
        balance = opening
        for ev in shift_events:
            balance += ev["amount"]
            ev_iso = _to_iso(adjust_poster_time(ev["raw"]))
            points.append({"x": ev_iso, "y": balance})

        # Closing point from Poster (if shift is closed)
        if shift_end_raw:
            closing = int(shift.get('amount_end', 0) or 0)
            end_iso = _to_iso(adjust_poster_time(shift_end_raw))
            points.append({"x": end_iso, "y": closing})

    if len(points) <= 1:
        return None

    points.sort(key=lambda p: p["x"])
    return {"points": points}


def _build_hourly_by_weekday(transactions):
    """Group transactions by day-of-week and hour for Chart.js."""
    from app import adjust_poster_time

    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    data = {day: {h: {"sales": 0, "profit": 0, "count": 0} for h in range(24)} for day in day_names}

    for txn in transactions:
        close_date = adjust_poster_time(txn.get('date_close_date', '') or txn.get('date', ''))
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
        close_date = adjust_poster_time(txn.get('date_close_date', '') or txn.get('date', ''))
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
        close_time = adjust_poster_time(txn.get('date_close_date', '') or txn.get('date', ''))

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

    # Build cash register timeline anchored to shift data
    cash_timeline = _build_cash_timeline(closed, finance_txns, shifts)

    # Pre-process sales and expenses for merged feed
    from app import calculate_expenses
    expenses = calculate_expenses(finance_txns)

    feed_items = []
    for txn in closed:
        close_time = adjust_poster_time(txn.get('date_close_date', '') or txn.get('date', ''))
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

    # Goal progress — today
    goal_progress = 0
    goal_percent = 0
    goal_percent_adjusted = 0
    goal_adjusted = 0
    if config.monthly_goal > 0:
        today = get_business_date()
        goal_progress = summary["total_profit"]
        goal_percent = goal_progress / config.monthly_goal * 100
        days_in_month = calendar.monthrange(today.year, today.month)[1]
        goal_adjusted = config.monthly_goal / days_in_month
        goal_percent_adjusted = (goal_progress / goal_adjusted * 100) if goal_adjusted > 0 else 0

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
        "monthly_goal": config.monthly_goal,
        "goal_adjusted": goal_adjusted,
        "goal_progress": goal_progress,
        "goal_percent": goal_percent,
        "goal_percent_adjusted": goal_percent_adjusted,
        "username": session["username"],
        "is_admin": session.get("is_admin", False),
    })


@dashboard_app.get("/summary", response_class=HTMLResponse)
async def page_summary(
    request: Request,
    period: str = "month",
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

    shifts = await _run_sync(fetch_cash_shifts)
    cash_timeline = _build_cash_timeline(closed, finance_txns, shifts)

    # Build expense-by-comment pie chart data with fuzzy label merging
    from collections import defaultdict
    expense_by_comment = defaultdict(int)
    for exp in expenses["expense_list"]:
        label = exp.get("comment") or exp.get("category") or "Uncategorized"
        label = " ".join(label.split()).strip()  # normalize whitespace
        expense_by_comment[label] += exp["amount"]
    # Merge labels that are near-duplicates (small edit distance)
    expense_by_comment = _merge_similar_labels(expense_by_comment)
    # Sort by amount descending
    sorted_cats = sorted(expense_by_comment.items(), key=lambda x: x[1], reverse=True)
    expense_pie = {
        "labels": [c[0] for c in sorted_cats],
        "values": [c[1] for c in sorted_cats],
    } if sorted_cats else None

    # Build merged transactions list (sales + expenses) sorted by date
    all_transactions = []
    for txn in closed:
        close_time = adjust_poster_time(txn.get('date_close_date', '') or txn.get('date', ''))
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

    # Goal progress for viewed period
    goal_progress = summary["total_profit"]
    goal_percent = (goal_progress / config.monthly_goal * 100) if config.monthly_goal > 0 else 0
    goal_percent_adjusted = 0
    goal_adjusted = 0
    if config.monthly_goal > 0:
        from app import get_business_date
        today = get_business_date()
        days_in_month = calendar.monthrange(today.year, today.month)[1]
        if period == "today":
            num_days = 1
        elif period == "week":
            num_days = 7
        elif period == "month":
            num_days = 30
        elif period == "custom" and date_from_api and date_to_api:
            df = datetime.strptime(date_from_api, "%Y%m%d")
            dt = datetime.strptime(date_to_api, "%Y%m%d")
            num_days = (dt - df).days + 1
        else:
            num_days = 1
        goal_adjusted = min(config.monthly_goal, config.monthly_goal * num_days / days_in_month)
        goal_percent_adjusted = (goal_progress / goal_adjusted * 100) if goal_adjusted > 0 else 0

    # Prev/next day links for single-day custom view
    prev_day = ""
    next_day = ""
    if period == "custom" and date_from_iso and date_from_iso == date_to_iso:
        single = datetime.strptime(date_from_iso, "%Y-%m-%d")
        prev_day = (single - timedelta(days=1)).strftime("%Y-%m-%d")
        next_day = (single + timedelta(days=1)).strftime("%Y-%m-%d")

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
        "prev_day": prev_day,
        "next_day": next_day,
        "format_currency": format_currency,
        "monthly_goal": config.monthly_goal,
        "goal_adjusted": goal_adjusted,
        "goal_progress": goal_progress,
        "goal_percent": goal_percent,
        "goal_percent_adjusted": goal_percent_adjusted,
        "username": session["username"],
        "is_admin": session.get("is_admin", False),
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
        "is_admin": session.get("is_admin", False),
    })


@dashboard_app.get("/products", response_class=HTMLResponse)
async def page_products(request: Request, period: str = "month"):
    """Product analytics page."""
    session = check_basic_auth(request)
    if session is None:
        return _unauthorized_response()

    from app import fetch_product_sales, fetch_product_catalog, format_currency
    from collections import defaultdict

    if period not in ("today", "week", "month"):
        period = "today"

    date_from, date_to, display = _get_date_range(period)
    products_raw = await _run_sync(fetch_product_sales, date_from, date_to)
    catalog = await _run_sync(fetch_product_catalog)

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
            pid = str(p.get('product_id', ''))
            cat = catalog.get(pid, "Uncategorized")
            product_list.append({
                "product_name": name,
                "category_name": cat,
                "count": count,
                "payed_sum": revenue,
                "product_profit": profit,
            })
            total_revenue += revenue
            total_profit += profit
            total_items += count

    product_list.sort(key=lambda x: x["payed_sum"], reverse=True)

    # Collect unique categories for filter
    categories = sorted(set(p["category_name"] for p in product_list))

    # Chart data
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
        "labels": [p["product_name"] for p in product_list],
        "revenue": [p["payed_sum"] for p in product_list],
        "profit": [p["product_profit"] for p in product_list],
    }
    pie_data = {"labels": pie_labels, "values": pie_values}

    # Category breakdown from product catalog
    cat_revenue = defaultdict(int)
    cat_profit = defaultdict(int)
    for p in products_raw:
        revenue = int(p.get('payed_sum', 0) or 0)
        profit = int(p.get('product_profit', 0) or 0)
        if revenue > 0:
            pid = str(p.get('product_id', ''))
            cat = catalog.get(pid, "Uncategorized")
            cat_revenue[cat] += revenue
            cat_profit[cat] += profit

    sorted_cats = sorted(cat_revenue.items(), key=lambda x: x[1], reverse=True)
    category_data = {
        "labels": [c[0] for c in sorted_cats],
        "revenue": [c[1] for c in sorted_cats],
        "profit": [cat_profit[c[0]] for c in sorted_cats],
    } if sorted_cats else None

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
        "categories": categories,
        "category_data": json.dumps(category_data) if category_data else "null",
        "format_currency": format_currency,
        "username": session["username"],
        "is_admin": session.get("is_admin", False),
    })


@dashboard_app.get("/config", response_class=HTMLResponse)
async def page_config(request: Request):
    """Configuration viewer (admin only)."""
    session = check_basic_auth(request)
    if session is None:
        return _unauthorized_response()
    if not session.get("is_admin", False):
        raise HTTPException(status_code=403, detail="Admin access required")

    raw = config.get_config_data()

    # Mask sensitive keys
    for key in ("ANTHROPIC_API_KEY", "POSTER_ACCESS_TOKEN"):
        if key in raw:
            raw[key] = config.mask_api_key(raw[key])

    # Mask password hashes in approved_users
    for uid, entry in raw.get("approved_users", {}).items():
        if "password_hash" in entry:
            entry["password_hash"] = "****"

    config_json = json.dumps(raw, indent=2, ensure_ascii=False)

    return templates.TemplateResponse("config.html", {
        "request": request,
        "active_page": "config",
        "config_json": config_json,
        "username": session["username"],
        "is_admin": session.get("is_admin", False),
    })


class ConfigUpdateRequest(BaseModel):
    config: str


@dashboard_app.post("/api/config")
async def api_config_save(body: ConfigUpdateRequest, session: dict = Depends(require_auth)):
    """Save config edits (admin only). Preserves masked secrets."""
    if not session.get("is_admin", False):
        raise HTTPException(status_code=403, detail="Admin access required")

    # Parse submitted JSON
    try:
        submitted = json.loads(body.config)
    except (json.JSONDecodeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    if not isinstance(submitted, dict):
        raise HTTPException(status_code=400, detail="Config must be a JSON object")

    # Read real config to protect masked secrets
    real = config.get_config_data()

    # Protect API keys: keep real value if submitted looks masked
    for key in ("ANTHROPIC_API_KEY", "POSTER_ACCESS_TOKEN"):
        real_val = real.get(key, "")
        submitted_val = submitted.get(key, "")
        if isinstance(submitted_val, str) and ("..." in submitted_val or submitted_val == "****"):
            # Masked — preserve real value
            if real_val:
                submitted[key] = real_val
            else:
                submitted.pop(key, None)

    # Protect password hashes: keep real hash if submitted is "****"
    submitted_users = submitted.get("approved_users", {})
    real_users = real.get("approved_users", {})
    if isinstance(submitted_users, dict):
        for uid, entry in submitted_users.items():
            if isinstance(entry, dict) and entry.get("password_hash") == "****":
                real_entry = real_users.get(uid, {})
                if real_entry.get("password_hash"):
                    entry["password_hash"] = real_entry["password_hash"]

    # Write merged config
    with open(config.CONFIG_FILE, 'w') as f:
        json.dump(submitted, f, indent=2)

    # Refresh in-memory state
    config.load_config()

    return {"status": "ok"}


@dashboard_app.get("/voids", response_class=HTMLResponse)
async def page_voids_redirect(request: Request, period: str = "month", date_from: str = Query(None), date_to: str = Query(None)):
    """Redirect old /voids URL to /alerts."""
    from fastapi.responses import RedirectResponse
    params = request.query_params
    qs = f"?{params}" if params else ""
    return RedirectResponse(url=f"/alerts{qs}", status_code=301)


@dashboard_app.get("/alerts", response_class=HTMLResponse)
async def page_alerts(
    request: Request,
    period: str = "month",
    date_from: str = Query(None),
    date_to: str = Query(None),
):
    """Suspicious activity / alerts page."""
    session = check_basic_auth(request)
    if session is None:
        return _unauthorized_response()

    from app import (
        fetch_removed_transactions, fetch_transactions, fetch_finance_transactions,
        fetch_cash_shifts, calculate_expenses, adjust_poster_time, format_currency,
        LARGE_DISCOUNT_THRESHOLD, LARGE_EXPENSE_THRESHOLD,
    )
    from collections import defaultdict

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

    # Fetch all data sources in parallel
    removed = await _run_sync(fetch_removed_transactions, date_from_api, date_to_api)
    transactions = await _run_sync(fetch_transactions, date_from_api, date_to_api)
    finance_txns = await _run_sync(fetch_finance_transactions, date_from_api, date_to_api)
    shifts = await _run_sync(fetch_cash_shifts)

    # --- 1. Voided transactions ---
    void_list = []
    total_void_loss = 0
    for txn in removed:
        amount = int(txn.get('sum', 0) or 0)
        total_void_loss += amount
        close_time = adjust_poster_time(txn.get('date_close_date', ''))
        time_str = close_time.split(' ')[1][:5] if ' ' in close_time else ''
        void_list.append({
            "transaction_id": int(txn.get('transaction_id', 0) or 0),
            "date": close_time,
            "time": time_str,
            "amount": amount,
            "table_name": txn.get('table_name', ''),
            "staff": txn.get('name', ''),
        })
    void_list.sort(key=lambda x: x["date"], reverse=True)

    # --- 2. Zero-payment sales (closed with no payment) ---
    zero_payment_list = []
    for txn in transactions:
        status = str(txn.get('status', ''))
        total = int(txn.get('sum', 0) or 0)
        payed_sum = int(txn.get('payed_sum', 0) or 0)
        if status == '2' and total > 0 and payed_sum == 0:
            close_time = adjust_poster_time(txn.get('date_close_date', ''))
            time_str = close_time.split(' ')[1][:5] if ' ' in close_time else ''
            zero_payment_list.append({
                "transaction_id": int(txn.get('transaction_id', 0) or 0),
                "date": close_time,
                "time": time_str,
                "amount": total,
                "table_name": txn.get('table_name', ''),
                "staff": txn.get('name', ''),
            })
    zero_payment_list.sort(key=lambda x: x["date"], reverse=True)

    # --- 3. Underpayments (paid less than order total) ---
    underpayment_list = []
    for txn in transactions:
        status = str(txn.get('status', ''))
        total = int(txn.get('sum', 0) or 0)
        payed_sum = int(txn.get('payed_sum', 0) or 0)
        if status == '2' and total > 0 and 0 < payed_sum < total:
            close_time = adjust_poster_time(txn.get('date_close_date', ''))
            time_str = close_time.split(' ')[1][:5] if ' ' in close_time else ''
            shortage = total - payed_sum
            underpayment_list.append({
                "transaction_id": int(txn.get('transaction_id', 0) or 0),
                "date": close_time,
                "time": time_str,
                "amount": total,
                "paid": payed_sum,
                "shortage": shortage,
                "table_name": txn.get('table_name', ''),
                "staff": txn.get('name', ''),
            })
    underpayment_list.sort(key=lambda x: x["date"], reverse=True)

    # --- 4. Large discounts (>20%) ---
    discount_list = []
    for txn in transactions:
        total = int(txn.get('sum', 0) or 0)
        discount = int(txn.get('discount', 0) or 0)
        if total > 0 and discount > 0:
            original = total + discount
            discount_pct = (discount / original) * 100
            if discount_pct > LARGE_DISCOUNT_THRESHOLD:
                close_time = adjust_poster_time(txn.get('date_close_date', ''))
                time_str = close_time.split(' ')[1][:5] if ' ' in close_time else ''
                discount_list.append({
                    "transaction_id": int(txn.get('transaction_id', 0) or 0),
                    "date": close_time,
                    "time": time_str,
                    "original": original,
                    "discount": discount,
                    "discount_pct": discount_pct,
                    "final_amount": total,
                    "table_name": txn.get('table_name', ''),
                    "staff": txn.get('name', ''),
                })
    discount_list.sort(key=lambda x: x["date"], reverse=True)

    # --- 5. Cash register discrepancies (>100 THB) ---
    cash_discrepancy_list = []
    if shifts:
        for shift in shifts:
            if not shift.get('date_end'):
                continue
            amount_start = int(shift.get('amount_start', 0) or 0)
            cash_sales = int(shift.get('amount_sell_cash', 0) or 0)
            cash_out = int(shift.get('amount_credit', 0) or 0)
            expected = amount_start + cash_sales - cash_out
            actual = int(shift.get('amount_end', 0) or 0)
            discrepancy = actual - expected

            if abs(discrepancy) > 10000:  # > 100 THB
                shift_start = adjust_poster_time(shift.get('date_start', ''))
                shift_end = adjust_poster_time(shift.get('date_end', ''))
                cash_discrepancy_list.append({
                    "shift_start": shift_start,
                    "shift_end": shift_end,
                    "expected": expected,
                    "actual": actual,
                    "discrepancy": discrepancy,
                    "is_shortage": discrepancy < 0,
                    "staff": shift.get('comment', ''),
                })
    cash_discrepancy_list.sort(key=lambda x: x["shift_end"], reverse=True)

    # --- 6. Large expenses (>1000 THB) ---
    expenses_data = calculate_expenses(finance_txns)
    large_expense_list = []
    for exp in expenses_data["expense_list"]:
        if exp["amount"] >= LARGE_EXPENSE_THRESHOLD:
            large_expense_list.append({
                "date": exp["date"],
                "amount": exp["amount"],
                "comment": exp.get("comment") or "-",
                "category": exp.get("category") or "-",
            })
    large_expense_list.sort(key=lambda x: x["date"], reverse=True)

    # --- Summary counts ---
    total_alerts = (
        len(void_list) + len(zero_payment_list) + len(underpayment_list)
        + len(discount_list) + len(cash_discrepancy_list) + len(large_expense_list)
    )

    # --- Daily chart (all alerts combined) ---
    daily_alerts = defaultdict(lambda: {"count": 0, "amount": 0})
    for v in void_list:
        day = v["date"].split(' ')[0] if ' ' in v["date"] else v["date"]
        daily_alerts[day]["count"] += 1
        daily_alerts[day]["amount"] += v["amount"]
    for z in zero_payment_list:
        day = z["date"].split(' ')[0] if ' ' in z["date"] else z["date"]
        daily_alerts[day]["count"] += 1
        daily_alerts[day]["amount"] += z["amount"]
    for u in underpayment_list:
        day = u["date"].split(' ')[0] if ' ' in u["date"] else u["date"]
        daily_alerts[day]["count"] += 1
        daily_alerts[day]["amount"] += u["shortage"]
    for d in discount_list:
        day = d["date"].split(' ')[0] if ' ' in d["date"] else d["date"]
        daily_alerts[day]["count"] += 1
        daily_alerts[day]["amount"] += d["discount"]
    for c in cash_discrepancy_list:
        day = c["shift_end"].split(' ')[0] if ' ' in c["shift_end"] else c["shift_end"]
        daily_alerts[day]["count"] += 1
        daily_alerts[day]["amount"] += abs(c["discrepancy"])
    for e in large_expense_list:
        day = e["date"].split(' ')[0] if ' ' in e["date"] else e["date"]
        daily_alerts[day]["count"] += 1
        daily_alerts[day]["amount"] += e["amount"]

    sorted_days = sorted(daily_alerts.items())
    daily_chart = {
        "labels": [d[0] for d in sorted_days],
        "counts": [d[1]["count"] for d in sorted_days],
        "amounts": [d[1]["amount"] for d in sorted_days],
    } if sorted_days else None

    return templates.TemplateResponse("alerts.html", {
        "request": request,
        "active_page": "alerts",
        "period": period,
        "display": display,
        "total_alerts": total_alerts,
        "void_list": void_list,
        "total_void_loss": total_void_loss,
        "zero_payment_list": zero_payment_list,
        "underpayment_list": underpayment_list,
        "discount_list": discount_list,
        "discount_threshold": LARGE_DISCOUNT_THRESHOLD,
        "cash_discrepancy_list": cash_discrepancy_list,
        "large_expense_list": large_expense_list,
        "expense_threshold": LARGE_EXPENSE_THRESHOLD // 100,
        "daily_chart": json.dumps(daily_chart) if daily_chart else "null",
        "date_from_iso": date_from_iso,
        "date_to_iso": date_to_iso,
        "format_currency": format_currency,
        "username": session["username"],
        "is_admin": session.get("is_admin", False),
    })


@dashboard_app.get("/expenses", response_class=HTMLResponse)
async def page_expenses(
    request: Request,
    period: str = "month",
    date_from: str = Query(None),
    date_to: str = Query(None),
):
    """Expenses breakdown page."""
    session = check_basic_auth(request)
    if session is None:
        return _unauthorized_response()

    from app import fetch_finance_transactions, calculate_expenses, format_currency, adjust_poster_time
    from collections import defaultdict

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

    finance_txns = await _run_sync(fetch_finance_transactions, date_from_api, date_to_api)
    expenses = calculate_expenses(finance_txns)

    # Group by category
    by_category = defaultdict(int)
    for exp in expenses["expense_list"]:
        label = exp.get("category") or "Uncategorized"
        by_category[label] += exp["amount"]
    sorted_categories = sorted(by_category.items(), key=lambda x: x[1], reverse=True)

    # Group by comment (more granular)
    by_comment = defaultdict(int)
    for exp in expenses["expense_list"]:
        label = exp.get("comment") or exp.get("category") or "Uncategorized"
        by_comment[label] += exp["amount"]
    sorted_comments = sorted(by_comment.items(), key=lambda x: x[1], reverse=True)

    # Pie chart by category
    category_pie = {
        "labels": [c[0] for c in sorted_categories],
        "values": [c[1] for c in sorted_categories],
    } if sorted_categories else None

    # Pie chart by comment
    comment_pie = {
        "labels": [c[0] for c in sorted_comments],
        "values": [c[1] for c in sorted_comments],
    } if sorted_comments else None

    # Daily breakdown for bar chart
    daily_expenses = defaultdict(int)
    for exp in expenses["expense_list"]:
        day = exp["date"].split(' ')[0] if ' ' in exp["date"] else exp["date"]
        daily_expenses[day] += exp["amount"]
    sorted_days = sorted(daily_expenses.items())
    daily_chart = {
        "labels": [d[0] for d in sorted_days],
        "values": [d[1] for d in sorted_days],
    } if sorted_days else None

    # Expense list sorted by date (most recent first)
    expense_list = sorted(expenses["expense_list"], key=lambda x: x["date"], reverse=True)

    return templates.TemplateResponse("expenses.html", {
        "request": request,
        "active_page": "expenses",
        "period": period,
        "display": display,
        "total_expenses": expenses["total_expenses"],
        "expense_count": len(expenses["expense_list"]),
        "expense_list": expense_list,
        "category_pie": json.dumps(category_pie) if category_pie else "null",
        "comment_pie": json.dumps(comment_pie) if comment_pie else "null",
        "daily_chart": json.dumps(daily_chart) if daily_chart else "null",
        "date_from_iso": date_from_iso,
        "date_to_iso": date_to_iso,
        "format_currency": format_currency,
        "username": session["username"],
        "is_admin": session.get("is_admin", False),
    })


@dashboard_app.get("/inventory", response_class=HTMLResponse)
async def page_inventory(request: Request):
    """Inventory / stock levels page."""
    session = check_basic_auth(request)
    if session is None:
        return _unauthorized_response()

    from app import fetch_stock_levels, fetch_ingredient_usage, get_business_date, format_currency

    stock_data = await _run_sync(fetch_stock_levels)

    # Categorize stock items
    negative_stock = []
    low_stock = []
    normal_stock = []

    for item in stock_data:
        name = item.get('ingredient_name', 'Unknown')
        left = float(item.get('ingredient_left', 0) or 0)
        unit = item.get('ingredient_unit', '')
        limit_val = float(item.get('limit_value', 0) or 0)
        hidden = item.get('hidden', '0') == '1'

        if hidden:
            continue

        entry = {"name": name, "left": left, "unit": unit, "limit": limit_val}

        if left < 0:
            negative_stock.append(entry)
        elif limit_val > 0 and left <= limit_val:
            low_stock.append(entry)
        elif left > 0:
            normal_stock.append(entry)

    negative_stock.sort(key=lambda x: x["left"])
    low_stock.sort(key=lambda x: x["left"])
    normal_stock.sort(key=lambda x: x["name"])

    total_items = len(negative_stock) + len(low_stock) + len(normal_stock)

    # Fetch ingredient usage for last 30 days
    today = get_business_date()
    date_from = (today - timedelta(days=29)).strftime('%Y%m%d')
    date_to = today.strftime('%Y%m%d')
    usage_data = await _run_sync(fetch_ingredient_usage, date_from, date_to)

    top_used = []
    for item in usage_data:
        usage = float(item.get('write_offs', 0) or 0)
        if usage > 0:
            top_used.append({
                "name": item.get('ingredient_name', 'Unknown'),
                "usage": usage,
            })
    top_used.sort(key=lambda x: x["usage"], reverse=True)
    top_used = top_used[:20]

    # Chart data for top used ingredients
    usage_chart = {
        "labels": [i["name"] for i in top_used],
        "values": [i["usage"] for i in top_used],
    } if top_used else None

    return templates.TemplateResponse("inventory.html", {
        "request": request,
        "active_page": "inventory",
        "negative_stock": negative_stock,
        "low_stock": low_stock,
        "normal_stock": normal_stock,
        "total_items": total_items,
        "alert_count": len(negative_stock) + len(low_stock),
        "usage_chart": json.dumps(usage_chart) if usage_chart else "null",
        "username": session["username"],
        "is_admin": session.get("is_admin", False),
    })


@dashboard_app.get("/chat", response_class=HTMLResponse)
async def page_chat(request: Request):
    """AI Chat page."""
    session = check_basic_auth(request)
    if session is None:
        return _unauthorized_response()

    user_id = str(session["user_id"])
    used, limit = config.get_agent_usage(user_id)

    return templates.TemplateResponse("chat.html", {
        "request": request,
        "active_page": "chat",
        "usage_used": used,
        "usage_limit": limit,
        "username": session["username"],
        "is_admin": session.get("is_admin", False),
    })


class ChatRequest(BaseModel):
    message: str


@dashboard_app.post("/api/chat")
async def api_chat(body: ChatRequest, session: dict = Depends(require_auth)):
    """Send a message to the AI agent and get a response."""
    from agent import run_agent

    user_id = str(session["user_id"])
    message = body.message.strip()

    if not message:
        raise HTTPException(status_code=400, detail="Message cannot be empty")

    # Rate limiting
    allowed, remaining = config.check_agent_rate_limit(user_id)
    if not allowed:
        limits = config.get_agent_limits(user_id)
        raise HTTPException(
            status_code=429,
            detail=f"Daily limit reached ({limits['daily_limit']} requests/day). Try again tomorrow.",
        )

    config.record_agent_usage(user_id)
    used, limit = config.get_agent_usage(user_id)

    user_limits = config.get_agent_limits(user_id)
    history = config.agent_conversations.get(user_id, [])

    try:
        response_text, updated_history, charts = await run_agent(
            message,
            config.ANTHROPIC_API_KEY,
            config.POSTER_ACCESS_TOKEN,
            history=history,
            max_iterations=user_limits['max_iterations'],
            source="dashboard",
        )

        config.agent_conversations[user_id] = updated_history

        # Encode charts as base64 data URIs
        chart_images = []
        for chart_buf in charts:
            chart_buf.seek(0)
            b64 = base64.b64encode(chart_buf.read()).decode('utf-8')
            chart_images.append(f"data:image/png;base64,{b64}")

        return {
            "response": response_text,
            "charts": chart_images,
            "usage": {"used": used, "limit": limit},
        }

    except Exception as e:
        logger.error(f"Dashboard agent error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@dashboard_app.post("/api/chat/clear")
async def api_chat_clear(session: dict = Depends(require_auth)):
    """Clear conversation history for the current user."""
    user_id = str(session["user_id"])
    if user_id in config.agent_conversations:
        del config.agent_conversations[user_id]
    return {"status": "ok"}


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
