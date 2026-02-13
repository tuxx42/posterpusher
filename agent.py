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

SYSTEM_PROMPT_TEMPLATE = """You are a helpful assistant for a bar/restaurant business using Poster POS system.
You have READ-ONLY access to query the Poster API for sales, products, inventory, expenses, and cash register data.
You cannot modify any data - only retrieve and analyze it.

Today's date is: {today}

IMPORTANT: You have a maximum of {max_iterations} tool calls for this request. Plan accordingly:
- Prioritize the most important data first
- Combine related queries if possible
- If a request requires more data than you can fetch, answer with what you have and note what's missing

When the user asks questions about the business, use the available tools to fetch the relevant data and provide a helpful summary.

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
- Example: get_transactions with fields: ["sum", "total_profit", "date_close_date"]
- For simple totals, you may only need 1-2 fields
- For detailed breakdowns, request only the fields relevant to the breakdown
"""

TOOLS = [
    {
        "name": "get_transactions",
        "description": "Get sales transactions for a date range. Returns list of transactions with totals, payment types, and timestamps.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {
                    "type": "string",
                    "description": "Start date in YYYYMMDD format"
                },
                "date_to": {
                    "type": "string",
                    "description": "End date in YYYYMMDD format (optional, defaults to date_from)"
                },
                "fields": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Fields to include. Available: transaction_id (unique ID), sum (total amount in satang), total_profit (profit in satang), payed_cash (cash paid), payed_card (card paid), date_close_date (closing timestamp YYYY-MM-DD HH:MM:SS), status (2=closed)"
                }
            },
            "required": ["date_from"]
        }
    },
    {
        "name": "get_product_sales",
        "description": "Get product-level sales data for a date range. Returns which products were sold and quantities.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {
                    "type": "string",
                    "description": "Start date in YYYYMMDD format"
                },
                "date_to": {
                    "type": "string",
                    "description": "End date in YYYYMMDD format (optional, defaults to date_from)"
                },
                "fields": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Fields to include. Available: product_id (unique ID), product_name (name), payed_sum (revenue in satang), product_profit (profit in satang), num (quantity sold)"
                }
            },
            "required": ["date_from"]
        }
    },
    {
        "name": "get_stock_levels",
        "description": "Get current inventory/stock levels for all ingredients and products.",
        "input_schema": {
            "type": "object",
            "properties": {
                "fields": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Fields to include. Available: ingredient_id (unique ID), ingredient_name (name), storage_ingredient_left (current stock quantity), storage_ingredient_unit (unit of measurement)"
                }
            },
            "required": []
        }
    },
    {
        "name": "get_ingredient_usage",
        "description": "Get ingredient usage/movement report for a date range. Shows how ingredients were consumed.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {
                    "type": "string",
                    "description": "Start date in YYYYMMDD format"
                },
                "date_to": {
                    "type": "string",
                    "description": "End date in YYYYMMDD format (optional, defaults to date_from)"
                },
                "fields": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Fields to include. Available: ingredient_id (unique ID), ingredient_name (name), write_offs (quantity used), start (opening balance), income (received), end (closing balance)"
                }
            },
            "required": ["date_from"]
        }
    },
    {
        "name": "get_finance_transactions",
        "description": "Get finance transactions including expenses and income for a date range.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {
                    "type": "string",
                    "description": "Start date in YYYYMMDD format"
                },
                "date_to": {
                    "type": "string",
                    "description": "End date in YYYYMMDD format (optional, defaults to date_from)"
                },
                "fields": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Fields to include. Available: transaction_id (unique ID), amount (in satang, negative=expense), comment (description), category_name (expense category), date (YYYY-MM-DD HH:MM:SS)"
                }
            },
            "required": ["date_from"]
        }
    },
    {
        "name": "get_cash_shifts",
        "description": "Get cash register shift data including opening/closing amounts.",
        "input_schema": {
            "type": "object",
            "properties": {
                "fields": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Fields to include. Available: shift_id (unique ID), date_start (shift open time), date_end (shift close time), cash_start (opening cash), cash_end (closing cash)"
                }
            },
            "required": []
        }
    },
    {
        "name": "get_transaction_products",
        "description": "Get products/items included in a specific transaction.",
        "input_schema": {
            "type": "object",
            "properties": {
                "transaction_id": {
                    "type": "string",
                    "description": "The transaction ID to get products for"
                },
                "fields": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Fields to include. Available: product_id (unique ID), product_name (name), num (quantity), payed_sum (line total in satang)"
                }
            },
            "required": ["transaction_id"]
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


def _trim_history(messages: list, max_messages: int = 10) -> list:
    """Trim message history while keeping tool_use/tool_result pairs intact.

    The API requires that every tool_result has a corresponding tool_use in the
    previous assistant message. Naive trimming can break this pairing.
    """
    if len(messages) <= max_messages:
        return messages

    # Take the last N messages, then clean any orphaned messages
    trimmed = messages[-max_messages:]
    return _clean_orphaned_messages(trimmed)


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
    """Filter API response to only include specified fields.

    Args:
        data: API response data (list or dict)
        fields: List of field names to keep, or None for all fields

    Returns:
        Filtered data with only requested fields
    """
    if fields is None:
        return data

    if isinstance(data, list):
        return [_filter_fields(item, fields) for item in data]

    if isinstance(data, dict):
        return {k: v for k, v in data.items() if k in fields}

    return data


# Whitelist of allowed read-only tools
ALLOWED_TOOLS = {
    "get_transactions",
    "get_product_sales",
    "get_stock_levels",
    "get_ingredient_usage",
    "get_finance_transactions",
    "get_cash_shifts",
    "get_transaction_products",
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

        if tool_name == "get_transactions":
            url = f"{POSTER_API_URL}/dash.getTransactions"
            params = {
                "token": poster_token,
                "dateFrom": tool_input.get("date_from"),
                "dateTo": tool_input.get("date_to", tool_input.get("date_from"))
            }
        elif tool_name == "get_product_sales":
            url = f"{POSTER_API_URL}/dash.getProductsSales"
            params = {
                "token": poster_token,
                "dateFrom": tool_input.get("date_from"),
                "dateTo": tool_input.get("date_to", tool_input.get("date_from"))
            }
        elif tool_name == "get_stock_levels":
            url = f"{POSTER_API_URL}/storage.getStorageLeftovers"
            params = {"token": poster_token}
        elif tool_name == "get_ingredient_usage":
            url = f"{POSTER_API_URL}/storage.getReportMovement"
            params = {
                "token": poster_token,
                "dateFrom": tool_input.get("date_from"),
                "dateTo": tool_input.get("date_to", tool_input.get("date_from"))
            }
        elif tool_name == "get_finance_transactions":
            url = f"{POSTER_API_URL}/finance.getTransactions"
            params = {
                "token": poster_token,
                "dateFrom": tool_input.get("date_from"),
                "dateTo": tool_input.get("date_to", tool_input.get("date_from"))
            }
        elif tool_name == "get_cash_shifts":
            url = f"{POSTER_API_URL}/finance.getCashShifts"
            params = {"token": poster_token}
        elif tool_name == "get_transaction_products":
            url = f"{POSTER_API_URL}/dash.getTransactionProducts"
            params = {
                "token": poster_token,
                "transaction_id": tool_input.get("transaction_id")
            }
        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

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

        # Limit response size to avoid token limits
        result_str = json.dumps(result, ensure_ascii=False)
        if len(result_str) > 50000:
            result_str = result_str[:50000] + "... (truncated)"
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
