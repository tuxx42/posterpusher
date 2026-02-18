"""
Anthropic AI Agent for querying Poster POS API.
"""
import json
import requests
from datetime import datetime, date, timedelta

from charts import generate_generic_chart

POSTER_API_URL = "https://joinposter.com/api"

FORMATTING_TELEGRAM = """IMPORTANT - Use Telegram HTML formatting only:
- <b>bold</b> for emphasis and headers
- <i>italic</i> for secondary emphasis
- <code>monospace</code> for numbers and IDs
- Do NOT use Markdown (no ##, **, -, ```, etc.)
- Use plain line breaks for lists, not bullet characters
- Example list format:
  <b>Items:</b>
  Beer: 24 bottles
  Wine: 12 bottles"""

FORMATTING_MARKDOWN = """IMPORTANT - Use Markdown formatting:
- Use **bold** for emphasis and headers
- Use *italic* for secondary emphasis
- Use `monospace` for numbers and IDs
- Use ## for section headers
- Use - for bullet lists
- Use ```language blocks for code/data tables
- Do NOT use HTML tags"""

POSTER_API_REFERENCE = """
## POSTER POS API REFERENCE

All endpoints use HTTP GET. Dates: Ymd format (e.g. 20240115). Money values in cents (divide by 100).

### DASHBOARD / REPORTS (dash.*)

**dash.getTransactions** — List transactions/orders
  Params: dateFrom (Ymd), dateTo (Ymd), status (0=all,1=open,2=closed,3=removed), type (waiters|spots|clients), id (entity ID when using type filter), include_products (bool), include_history (bool), service_mode (1=dine-in,2=takeout,3=delivery), next_tr (int, pagination cursor), table_id (int)
  Response: transaction_id, date_close_date, status, sum (total cents), total_profit (cents), payed_cash, payed_card, payed_sum, pay_type (0=none,1=cash,2=card,3=mixed), guests_count, discount, spot_id, table_id, table_name, name (waiter), user_id (waiter ID), client_id, client_firstname, client_lastname, transaction_comment, service_mode, tip_sum, round_sum, products[] (if include_products), history[] (if include_history)

**dash.getTransactionProducts** — Products in a specific transaction
  Params: transaction_id (required)
  Response: product_id, product_name, modification_id, modificator_name, num (qty), payed_sum, product_sum, product_cost, product_profit, discount, category_id, tax_value

**dash.getTransactionHistory** — Operation history for a transaction
  Params: transaction_id (required)
  Response: transaction_id, type_history (open|close|delete|additem|deleteitem|changeitemcount|print|settable|setclient|comment|...), time (ms), value, value2, value3, value_text

**dash.getProductsSales** — Product sales report
  Params: date_from (Ymd), date_to (Ymd), spot_id
  Response: product_id, product_name, modification_id, modificator_name, category_id, count (qty sold), payed_sum (revenue cents), product_sum (price cents), product_profit (cents), unit, weight_flag, discount

**dash.getCategoriesSales** — Category sales report
  Params: dateFrom (Ymd), dateTo (Ymd), spot_id
  Response: category_id, category_name, revenue (cents), profit (cents), count

**dash.getWaitersSales** — Sales by waiter
  Params: dateFrom (Ymd), dateTo (Ymd)
  Response: user_id, name, revenue (cents), profit (cents), clients (order count), middle_invoice (avg bill), middle_time (avg service mins), worked_time (total mins)

**dash.getClientsSales** — Sales by customer
  Params: dateFrom (Ymd), dateTo (Ymd)
  Response: client_id, firstname, lastname, sum (cents), revenue (cents), profit (cents), clients (order count), middle_invoice

### MENU / PRODUCTS (menu.*)

**menu.getProducts** — All products and dishes
  Params: category_id (int), type (products|batchtickets)
  Response: product_id, product_name, menu_category_id, category_name, type (1=semi-finished,2=dish,3=product), cost (food cost cents), barcode, product_code, unit, weight_flag, photo, out (stock status), spots[] (spot_id, price, profit, visible), group_modifications[], ingredients[]

**menu.getProduct** — Single product details
  Params: product_id (required)
  Response: same as menu.getProducts single object

**menu.getCategories** — All product categories
  Params: fiscal (0|1)
  Response: category_id, category_name, category_photo, parent_category, category_hidden, sort_order, fiscal, nodiscount, tax_id, level

**menu.getCategory** — Single category details
  Params: category_id (required)
  Response: same as menu.getCategories single object plus visible[] per-spot settings

### CLIENTS / CUSTOMERS (clients.*)

**clients.getClients** — Customer list
  Params: num (limit), offset, group_id, phone, birthday (md format), client_id_only (bool), order_by (default: client_id), sort (asc|desc), loyalty_type (1=bonus,2=discount)
  Response: client_id, firstname, lastname, phone, email, birthday, bonus (loyalty points cents), total_payed_sum (cents), discount_per, card_number, client_sex (0=unspec,1=male,2=female), country, city, address, client_groups_id, client_groups_name, loyalty_type, client_groups_discount, ewallet (cents), delete

**clients.getClient** — Single customer details
  Params: client_id (required)
  Response: same as clients.getClients single object plus accumulation_products, prize_products[]

**clients.getGroups** — Customer groups
  Params: none
  Response: client_groups_id, client_groups_name, client_groups_discount, loyalty_type (1=points,2=discount), birthday_bonus, count_groups_clients, use_ewallet, delete

### FINANCE (finance.*)

**finance.getCashShifts** — Register/cash shifts
  Params: spot_id, dateFrom (Ymd), dateTo (Ymd)
  Response: cash_shift_id, spot_id, date_start (Y-m-d H:i:s), date_end, amount_start (cents), amount_end (cents), amount_debit (income cents), amount_sell_cash, amount_sell_card, amount_credit (expense cents), amount_collection (safe drop cents), user_id_start, user_id_end, comment, spot_name

**finance.getTransactions** — Financial transactions (income/expenses)
  Params: account_id, category_id, type (0=expense,1=income), account_type (1=bank,2=card,3=cash), dateFrom (Ymd), dateTo (Ymd)
  Response: transaction_id, account_id, category_id, type (0=expense,1=income), amount (cents), balance, date, comment, account_name, category_name, supplier_name

**finance.getAccounts** — Financial accounts
  Params: type (1=non-cash,2=bank card,3=cash)
  Response: account_id, name, type, balance, currency_symbol, currency_code_iso

**finance.getCategories** — Financial categories
  Params: none
  Response: category_id, name, parent_id, operations (1=income,2=expense,3=both), action, delete

### STORAGE / INVENTORY (storage.*)

**storage.getStorageLeftovers** — Current stock levels
  Params: storage_id, type (1=product,2=dish,3=semi-finished,4=ingredient,5=product modifier), category_id, zero_leftovers (bool)
  Response: ingredient_id, ingredient_name, ingredient_left (total qty), limit_value (low stock threshold), ingredient_unit (kg|p|l), ingredients_type, prime_cost (cents)

**storage.getReportMovement** — Ingredient movement report
  Params: dateFrom (Ymd), dateTo (Ymd), storage_id, type (1=ingredient,2=product,3=mod,4=dish,5=prep)
  Response: ingredient_id, ingredient_name, start (opening balance), income (received), write_offs (used), end (closing balance), cost_start, cost_end

**storage.getSupplies** — Supply records
  Params: dateFrom (Ymd), dateTo (Ymd), limit, offset
  Response: supply_id, storage_id, supplier_id, date, supply_sum (cents), supply_comment, storage_name, supplier_name, delete

**storage.getSuppliers** — Supplier list
  Params: none
  Response: supplier_id, supplier_name, supplier_phone, supplier_adress, supplier_comment, supplier_code, supplier_tin, delete

**storage.getManufactures** — Manufacturing records
  Params: num (limit), offset
  Response: manufacture_id, storage_name, storage_id, user_id, date, sum, products[] (ingredient_id, product_id, product_name, product_num, type)

**storage.getWastes** — Waste records
  Params: dateFrom (Ymd), dateTo (Ymd)
  Response: waste_id, total_sum, user_id, storage_id, date, reason_id, reason_name, delete

### LOCATIONS (spots.*)

**spots.getSpots** — All locations/venues
  Params: none
  Response: spot_id, name, address

**spots.getSpotTablesHalls** — Floor sections (halls/rooms)
  Params: none
  Response: hall_id, hall_name, hall_order, spot_id, delete

**spots.getTableHallTables** — Tables
  Params: spot_id, hall_id, without_deleted (0|1)
  Response: table_id, table_num, table_title, spot_id, table_shape, hall_id, is_deleted

### EMPLOYEES (access.*)

**access.getEmployees** — Employee list
  Params: none
  Response: user_id, name, user_type (0=waiter,1=admin,2=marketer,3=storekeeper,4=floor admin,50=manager,90=owner), role_id, role_name, phone, last_in

**access.getTablets** — POS registers/tablets
  Params: none
  Response: tablet_id, tablet_name, spot_id, type (mobile|default)

### SETTINGS (settings.*)

**settings.getAllSettings** — Account settings
  Params: none
  Response: COMPANY_ID, company_name, company_type (1=cafe/restaurant,2=store), FIZ_ADRESS_CITY, FIZ_ADRESS_PHONE, uses_tables, uses_cash_shifts, uses_taxes, tip_amount, timezones, lang, currency (currency_id, currency_name, currency_symbol, currency_code_iso)
"""

SYSTEM_PROMPT_TEMPLATE = """You are a helpful assistant for a bar/restaurant business using Poster POS system.
You have READ-ONLY access to query the Poster API for sales, products, inventory, expenses, and cash register data.
You cannot modify any data - only retrieve and analyze it.

Today's date is: {today}

IMPORTANT: You have a maximum of {max_iterations} tool calls for this request. Plan accordingly:
- Prioritize the most important data first
- Combine related queries if possible
- If a request requires more data than you can fetch, answer with what you have and note what's missing

When the user asks questions about the business, use the poster_api tool with the appropriate method name and parameters from the API reference below.

Guidelines:
- Use appropriate date ranges when querying data (YYYYMMDD format)
- For "today", use {today_yyyymmdd}
- For "yesterday", use {yesterday_yyyymmdd}
- For "this week", use the last 7 days
- For "this month", use the first day of the current month to today
- Summarize data clearly with key metrics and insights
- Currency values from the API are in satang (1/100 of baht), divide by 100 for display
- Format currency as Thai Baht (฿)
- Timestamps from the API are already in local time - do NOT convert or assume UTC
- Keep responses concise but informative

{formatting_instructions}

When presenting numerical data, ALWAYS use the plot_graph tool to create visualizations:
- Use pie charts for showing proportions or market share
- Use bar charts for comparing categories or showing rankings
- Use line charts for showing trends over time
- Use horizontal bar charts for ranked lists with long labels
Always provide a text summary alongside any chart.

IMPORTANT - Optimize data requests to avoid running out of context:
- ALWAYS use the 'fields' parameter to request only the fields you need for your analysis
- This is especially critical for large date ranges or queries that may return many records
- Example: poster_api(method="dash.getTransactions", params={{dateFrom: "..."}}, fields=["sum", "total_profit", "date_close_date"])
- For simple totals, you may only need 1-2 fields
- For detailed breakdowns, request only the fields relevant to the breakdown
""" + POSTER_API_REFERENCE

TOOLS = [
    {
        "name": "poster_api",
        "description": "Call any Poster POS API method. Use the API reference in the system prompt to find the right method and parameters.",
        "input_schema": {
            "type": "object",
            "properties": {
                "method": {
                    "type": "string",
                    "description": "API method name, e.g. 'dash.getTransactions', 'menu.getProducts', 'clients.getClients'"
                },
                "params": {
                    "type": "object",
                    "description": "Query parameters as key-value pairs, e.g. {\"dateFrom\": \"20240101\", \"dateTo\": \"20240131\"}. Do NOT include the token."
                },
                "fields": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional: filter response to only these field names to reduce data size"
                }
            },
            "required": ["method"]
        }
    },
    {
        "name": "plot_graph",
        "description": "Generate a chart/graph visualization. Use this after fetching data to visualize it. Always provide a text summary alongside the chart.",
        "input_schema": {
            "type": "object",
            "properties": {
                "chart_type": {
                    "type": "string",
                    "enum": ["bar", "horizontal_bar", "pie", "line"],
                    "description": "Type of chart: bar (vertical), horizontal_bar, pie, or line"
                },
                "labels": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Labels for data points (x-axis for bar/line, slice names for pie)"
                },
                "data": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "Single series of numeric data values"
                },
                "series": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {"type": "string"},
                            "data": {"type": "array", "items": {"type": "number"}}
                        }
                    },
                    "description": "Multiple data series for grouped charts (alternative to 'data')"
                },
                "title": {
                    "type": "string",
                    "description": "Chart title"
                },
                "x_label": {
                    "type": "string",
                    "description": "X-axis label"
                },
                "y_label": {
                    "type": "string",
                    "description": "Y-axis label"
                }
            },
            "required": ["chart_type", "labels"]
        }
    }
]


def _clean_orphaned_messages(messages: list) -> list:
    """Remove orphaned tool_use/tool_result messages from the start of history.

    The API requires that every tool_result has a corresponding tool_use in the
    previous assistant message. This function removes any orphaned messages.
    """
    cleaned = list(messages)

    # Keep removing orphaned messages from the start until we have a valid sequence
    while cleaned:
        first_msg = cleaned[0]
        should_remove = False

        # Check if first message is a user message with tool_results (orphaned)
        if first_msg.get("role") == "user":
            content = first_msg.get("content", [])
            if isinstance(content, list) and any(
                isinstance(block, dict) and block.get("type") == "tool_result"
                for block in content
            ):
                should_remove = True

        # Check if first message is assistant with tool_use (orphaned without user prompt)
        elif first_msg.get("role") == "assistant":
            content = first_msg.get("content", [])
            if isinstance(content, list) and any(
                isinstance(block, dict) and block.get("type") == "tool_use"
                for block in content
            ):
                should_remove = True

        if should_remove:
            cleaned = cleaned[1:]
        else:
            break

    return cleaned


def _compress_history_results(messages: list) -> list:
    """Compress tool results in message history to reduce token usage."""
    compressed = []
    for msg in messages:
        content = msg.get("content", [])
        if isinstance(content, list):
            new_content = []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "tool_result":
                    block = dict(block)
                    block["content"] = _compress_tool_result(block.get("content", ""))
                new_content.append(block)
            compressed.append({**msg, "content": new_content})
        else:
            compressed.append(msg)
    return compressed


def _estimate_chars(messages: list) -> int:
    """Estimate total character count of messages."""
    total = 0
    for msg in messages:
        content = msg.get("content", "")
        if isinstance(content, str):
            total += len(content)
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, dict):
                    total += len(str(block.get("content", "")))
                    total += len(str(block.get("text", "")))
                    total += len(json.dumps(block.get("input", {}))) if block.get("input") else 0
    return total


def _trim_history(messages: list, max_messages: int = 10, max_chars: int = 30000) -> list:
    """Trim message history by count and character budget.

    The API requires that every tool_result has a corresponding tool_use in the
    previous assistant message. Naive trimming can break this pairing.
    """
    # First compress tool results
    compressed = _compress_history_results(messages)

    # Trim by message count
    if len(compressed) > max_messages:
        compressed = compressed[-max_messages:]
        compressed = _clean_orphaned_messages(compressed)

    # Trim by character budget — drop oldest messages until under budget
    while len(compressed) > 2 and _estimate_chars(compressed) > max_chars:
        compressed = compressed[1:]
        compressed = _clean_orphaned_messages(compressed)

    return compressed


# Timestamp fields that need timezone correction
TIMESTAMP_FIELDS = {'date_close_date', 'date', 'date_start', 'date_end'}
TIMESTAMP_OFFSET_HOURS = 4  # Poster API returns timestamps 4 hours behind local time


def _adjust_timestamp(value: str) -> str:
    """Add timezone offset to a timestamp string.

    Args:
        value: Timestamp in 'YYYY-MM-DD HH:MM:SS' format

    Returns:
        Adjusted timestamp string
    """
    try:
        dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        dt = dt + timedelta(hours=TIMESTAMP_OFFSET_HOURS)
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError):
        return value


def _adjust_timestamps(data):
    """Recursively adjust timestamps in API response data.

    Args:
        data: API response data (list or dict)

    Returns:
        Data with adjusted timestamps
    """
    if isinstance(data, list):
        return [_adjust_timestamps(item) for item in data]

    if isinstance(data, dict):
        return {
            k: _adjust_timestamp(v) if k in TIMESTAMP_FIELDS and isinstance(v, str)
            else _adjust_timestamps(v) if isinstance(v, (dict, list))
            else v
            for k, v in data.items()
        }

    return data


def _filter_fields(data, fields: list[str] | None):
    """Filter API response to only include specified fields, recursively.

    Filters at every dict level — top-level and nested dicts within lists/values
    all get the same field filter applied.

    Args:
        data: API response data (list or dict)
        fields: List of field names to keep, or None for all fields

    Returns:
        Filtered data with only requested fields
    """
    if fields is None:
        return data

    fields_set = set(fields)

    def _filter(obj):
        if isinstance(obj, list):
            return [_filter(item) for item in obj]
        if isinstance(obj, dict):
            return {k: _filter(v) for k, v in obj.items() if k in fields_set}
        return obj

    return _filter(data)


MAX_HISTORY_RESULT_CHARS = 2000


def _compress_tool_result(result: str) -> str:
    """Compress a tool result for storage in conversation history.

    Large API responses are summarized to avoid bloating the context window
    on subsequent requests.
    """
    if len(result) <= MAX_HISTORY_RESULT_CHARS:
        return result

    # Try to parse as JSON and summarize
    try:
        data = json.loads(result)
    except (json.JSONDecodeError, ValueError):
        return result[:MAX_HISTORY_RESULT_CHARS] + "... (compressed for history)"

    if isinstance(data, list):
        # Keep first 3 records as sample + record count
        sample = data[:3]
        summary = json.dumps(sample, ensure_ascii=False)
        if len(summary) > MAX_HISTORY_RESULT_CHARS:
            summary = summary[:MAX_HISTORY_RESULT_CHARS]
        return f"{summary}\n({len(data)} records total, showing first 3)"

    if isinstance(data, dict):
        # Keep just the keys and a truncated version
        summary = json.dumps(data, ensure_ascii=False)
        if len(summary) > MAX_HISTORY_RESULT_CHARS:
            summary = summary[:MAX_HISTORY_RESULT_CHARS] + "..."
        return summary

    return result[:MAX_HISTORY_RESULT_CHARS] + "... (compressed for history)"


# Whitelist of allowed read-only tools
ALLOWED_TOOLS = {
    "poster_api",
    "plot_graph",
}


def execute_tool(tool_name: str, tool_input: dict, poster_token: str) -> str | tuple[str, object]:
    """Execute a tool call.

    For API tools: strictly read-only (HTTP GET only).
    For plot_graph: generates a chart image.

    Returns:
        str: Tool result text (for API tools)
        tuple[str, BytesIO]: (result text, chart buffer) for plot_graph
    """
    # Safety check: only allow whitelisted tools
    if tool_name not in ALLOWED_TOOLS:
        return json.dumps({"error": f"Tool not allowed: {tool_name}"})

    try:
        # Handle plot_graph tool separately
        if tool_name == "plot_graph":
            chart_type = tool_input.get("chart_type", "bar")
            labels = tool_input.get("labels", [])
            data = tool_input.get("data")
            series = tool_input.get("series")
            title = tool_input.get("title")
            x_label = tool_input.get("x_label")
            y_label = tool_input.get("y_label")

            chart_buf = generate_generic_chart(
                chart_type=chart_type,
                labels=labels,
                data=data,
                series=series,
                title=title,
                x_label=x_label,
                y_label=y_label
            )

            if chart_buf:
                return ("Chart generated successfully.", chart_buf)
            else:
                return json.dumps({"error": "Failed to generate chart. Charts may not be available."})

        method = tool_input.get("method")
        if not method:
            return json.dumps({"error": "Missing required 'method' parameter"})
        url = f"{POSTER_API_URL}/{method}"
        params = dict(tool_input.get("params", {}))
        params["token"] = poster_token

        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        # Return the response data
        result = data.get("response", data)

        # Adjust timestamps to correct for Poster API timezone offset
        result = _adjust_timestamps(result)

        # Apply field filtering if specified
        fields = tool_input.get("fields")
        if fields:
            result = _filter_fields(result, fields)

        # Limit response size to avoid token bloat
        MAX_RESULT_CHARS = 15000
        if isinstance(result, list) and len(json.dumps(result, ensure_ascii=False)) > MAX_RESULT_CHARS:
            # Truncate list at record boundaries instead of mid-JSON
            truncated = []
            total_len = 2  # for []
            for item in result:
                item_str = json.dumps(item, ensure_ascii=False)
                if total_len + len(item_str) + 2 > MAX_RESULT_CHARS:
                    break
                truncated.append(item)
                total_len += len(item_str) + 2
            result_str = json.dumps(truncated, ensure_ascii=False)
            result_str += f"\n(showing {len(truncated)} of {len(result)} records, use 'fields' param to reduce size)"
        else:
            result_str = json.dumps(result, ensure_ascii=False)
            if len(result_str) > MAX_RESULT_CHARS:
                result_str = result_str[:MAX_RESULT_CHARS] + "... (truncated)"
        return result_str

    except requests.RequestException as e:
        return json.dumps({"error": f"API request failed: {str(e)}"})
    except Exception as e:
        return json.dumps({"error": f"Tool execution failed: {str(e)}"})


async def run_agent(prompt: str, anthropic_api_key: str, poster_token: str, model: str = "claude-sonnet-4-20250514", history: list = None, max_iterations: int = 5, source: str = "telegram") -> tuple[str, list, list]:
    """Run the Anthropic agent with tool calling.

    Args:
        prompt: The user's question
        anthropic_api_key: Anthropic API key
        poster_token: Poster POS API token
        model: Model to use
        history: Previous conversation messages for context
        max_iterations: Maximum tool use iterations (default 5)

    Returns:
        Tuple of (response_text, trimmed_history, charts)
        where charts is a list of BytesIO buffers containing generated chart images
    """
    import anthropic

    client = anthropic.Anthropic(api_key=anthropic_api_key)

    # Build system prompt with current date and iteration limit
    from app import get_business_date
    today = get_business_date()
    yesterday = today - timedelta(days=1)
    formatting = FORMATTING_MARKDOWN if source == "dashboard" else FORMATTING_TELEGRAM
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        today=today.strftime('%B %d, %Y'),
        today_yyyymmdd=today.strftime('%Y%m%d'),
        yesterday_yyyymmdd=yesterday.strftime('%Y%m%d'),
        max_iterations=max_iterations,
        formatting_instructions=formatting
    )

    # Start with history (if provided) + new user message
    # Validate incoming history to remove any orphaned tool_use/tool_result pairs
    messages = _clean_orphaned_messages(list(history)) if history else []
    messages.append({"role": "user", "content": prompt})

    iteration = 0
    charts = []  # Track generated chart images

    while iteration < max_iterations:
        iteration += 1

        response = client.messages.create(
            model=model,
            max_tokens=2048,  # Reduced to limit costs
            system=system_prompt,
            tools=TOOLS,
            messages=messages
        )

        # Check if we need to handle tool calls
        if response.stop_reason == "tool_use":
            # Process all tool calls
            tool_results = []
            assistant_content = response.content

            for block in response.content:
                if block.type == "tool_use":
                    tool_result = execute_tool(
                        block.name,
                        block.input,
                        poster_token
                    )

                    # Handle chart generation (returns tuple)
                    if isinstance(tool_result, tuple):
                        result_text, chart_buf = tool_result
                        charts.append(chart_buf)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result_text
                        })
                    else:
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": tool_result
                        })

            # Add assistant response and tool results to messages
            messages.append({"role": "assistant", "content": assistant_content})
            messages.append({"role": "user", "content": tool_results})

        else:
            # No more tool calls, extract final text response
            final_text = ""
            for block in response.content:
                if hasattr(block, "text"):
                    final_text += block.text

            # Add assistant response to messages for history
            messages.append({"role": "assistant", "content": final_text})

            response_text = final_text if final_text else "No response generated."
            return response_text, _trim_history(messages), charts

    # Reached max iterations - ask the model to summarize what it found
    messages.append({
        "role": "user",
        "content": "You've reached the maximum number of tool calls. Please summarize what you've found so far based on the data you've already retrieved. If you couldn't complete the request, explain what information is missing."
    })

    try:
        summary_response = client.messages.create(
            model=model,
            max_tokens=1024,
            system=system_prompt,
            messages=messages
        )

        summary_text = ""
        for block in summary_response.content:
            if hasattr(block, "text"):
                summary_text += block.text

        if summary_text:
            messages.append({"role": "assistant", "content": summary_text})
            return summary_text, _trim_history(messages), charts
    except Exception:
        pass

    return "Agent reached maximum iterations without completing.", _trim_history(messages), charts
