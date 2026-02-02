"""
Anthropic AI Agent for querying Poster POS API.
"""
import json
import requests
from datetime import datetime, date, timedelta

POSTER_API_URL = "https://joinposter.com/api"

SYSTEM_PROMPT_TEMPLATE = """You are a helpful assistant for a bar/restaurant business using Poster POS system.
You can query the Poster API to get information about sales, products, inventory, expenses, and cash register data.

Today's date is: {today}

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
- Keep responses concise but informative

IMPORTANT - Use Telegram HTML formatting only:
- <b>bold</b> for emphasis and headers
- <i>italic</i> for secondary emphasis
- <code>monospace</code> for numbers and IDs
- Do NOT use Markdown (no ##, **, -, ```, etc.)
- Use plain line breaks for lists, not bullet characters
- Example list format:
  <b>Items:</b>
  Beer: 24 bottles
  Wine: 12 bottles
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
            "properties": {},
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
            "properties": {},
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
                }
            },
            "required": ["transaction_id"]
        }
    }
]


def execute_tool(tool_name: str, tool_input: dict, poster_token: str) -> str:
    """Execute a tool call against the Poster API."""
    try:
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
        # Limit response size to avoid token limits
        result_str = json.dumps(result, ensure_ascii=False)
        if len(result_str) > 50000:
            result_str = result_str[:50000] + "... (truncated)"
        return result_str

    except requests.RequestException as e:
        return json.dumps({"error": f"API request failed: {str(e)}"})
    except Exception as e:
        return json.dumps({"error": f"Tool execution failed: {str(e)}"})


async def run_agent(prompt: str, anthropic_api_key: str, poster_token: str) -> str:
    """Run the Anthropic agent with tool calling."""
    import anthropic

    client = anthropic.Anthropic(api_key=anthropic_api_key)

    # Build system prompt with current date
    today = date.today()
    yesterday = today - timedelta(days=1)
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        today=today.strftime('%B %d, %Y'),
        today_yyyymmdd=today.strftime('%Y%m%d'),
        yesterday_yyyymmdd=yesterday.strftime('%Y%m%d')
    )

    messages = [{"role": "user", "content": prompt}]

    max_iterations = 10
    iteration = 0

    while iteration < max_iterations:
        iteration += 1

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4096,
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

            return final_text if final_text else "No response generated."

    return "Agent reached maximum iterations without completing."
