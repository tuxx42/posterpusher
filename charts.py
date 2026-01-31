"""
Chart generation functions for Ban Sabai POS Bot.
"""
import io
from datetime import datetime, timedelta

try:
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
    CHARTS_AVAILABLE = True
except ImportError:
    plt = None
    FuncFormatter = None
    CHARTS_AVAILABLE = False


def generate_sales_chart(transactions, date_from, date_to, title, finance_transactions=None):
    """Generate a bar chart showing daily gross profit, net profit, and expenses."""
    if not CHARTS_AVAILABLE:
        return None

    # Group transactions by date
    daily_data = {}
    current = date_from
    while current <= date_to:
        daily_data[current] = {'sales': 0, 'gross_profit': 0, 'expenses': 0}
        current += timedelta(days=1)

    for txn in transactions:
        txn_date = txn.get('date_close_date', '')[:10]  # Get YYYY-MM-DD
        if txn_date:
            try:
                d = datetime.strptime(txn_date, '%Y-%m-%d').date()
                if d in daily_data:
                    daily_data[d]['sales'] += int(txn.get('sum', 0) or 0)
                    daily_data[d]['gross_profit'] += int(txn.get('total_profit', 0) or 0)
            except ValueError:
                continue

    # Process expenses by date
    if finance_transactions:
        for txn in finance_transactions:
            amount = int(txn.get('amount', 0) or 0)
            comment = txn.get('comment', '')

            # Skip cash payments (sales income)
            if 'Cash payments' in comment:
                continue

            # Only count expenses (negative amounts)
            if amount < 0:
                txn_date = txn.get('date', '')[:10]
                if txn_date:
                    try:
                        d = datetime.strptime(txn_date, '%Y-%m-%d').date()
                        if d in daily_data:
                            daily_data[d]['expenses'] += abs(amount)
                    except ValueError:
                        continue

    # Prepare data for plotting
    dates = sorted(daily_data.keys())
    gross_profits = [daily_data[d]['gross_profit'] / 100 for d in dates]  # Convert to THB
    expenses = [-(daily_data[d]['expenses'] / 100) for d in dates]  # Negative for display
    net_profits = [(daily_data[d]['gross_profit'] - daily_data[d]['expenses']) / 100 for d in dates]

    # Create chart
    fig, ax = plt.subplots(figsize=(10, 5))
    x = range(len(dates))
    width = 0.27

    # Three bars: Gross Profit, Net Profit, Expenses (negative)
    ax.bar([i - width for i in x], gross_profits, width, label='Gross Profit', color='#4CAF50')
    ax.bar([i for i in x], net_profits, width, label='Net Profit', color='#2196F3')
    ax.bar([i + width for i in x], expenses, width, label='Expenses', color='#F44336')

    # Add horizontal line at y=0
    ax.axhline(y=0, color='black', linewidth=0.5)

    ax.set_xlabel('Date')
    ax.set_ylabel('Amount (฿)')
    ax.set_title(title)
    ax.set_xticks(list(x))
    ax.set_xticklabels([d.strftime('%d %b') for d in dates], rotation=45, ha='right')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)

    # Format y-axis with thousands separator
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:,.0f}'))

    plt.tight_layout()

    # Save to BytesIO
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)

    return buf


def generate_products_chart(product_sales, title, top_n=10):
    """Generate a horizontal bar chart showing top products by revenue."""
    if not product_sales or not CHARTS_AVAILABLE:
        return None

    # Sort by revenue and take top N
    sorted_products = sorted(product_sales, key=lambda x: int(x.get('payed_sum', 0) or 0), reverse=True)[:top_n]
    sorted_products.reverse()  # Reverse for horizontal bar (top at top)

    names = [p.get('product_name', 'Unknown')[:20] for p in sorted_products]
    revenues = [int(p.get('payed_sum', 0) or 0) / 100 for p in sorted_products]
    profits = [int(p.get('product_profit', 0) or 0) / 100 for p in sorted_products]

    fig, ax = plt.subplots(figsize=(10, 6))
    y = range(len(names))
    height = 0.35

    ax.barh([i - height/2 for i in y], revenues, height, label='Revenue', color='#2196F3')
    ax.barh([i + height/2 for i in y], profits, height, label='Profit', color='#4CAF50')

    ax.set_xlabel('Amount (฿)')
    ax.set_title(title)
    ax.set_yticks(list(y))
    ax.set_yticklabels(names)
    ax.legend()
    ax.grid(axis='x', alpha=0.3)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:,.0f}'))

    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)

    return buf


def generate_ingredients_chart(usage_data, title, top_n=15):
    """Generate a horizontal bar chart showing top ingredients by usage."""
    if not usage_data or not CHARTS_AVAILABLE:
        return None

    # Filter and sort by usage
    used_items = [item for item in usage_data if float(item.get('write_offs', 0)) > 0]
    sorted_items = sorted(used_items, key=lambda x: float(x.get('write_offs', 0)), reverse=True)[:top_n]
    sorted_items.reverse()  # Reverse for horizontal bar

    names = [item.get('ingredient_name', 'Unknown')[:25] for item in sorted_items]
    usage = [float(item.get('write_offs', 0)) for item in sorted_items]

    fig, ax = plt.subplots(figsize=(10, 6))
    y = range(len(names))

    ax.barh(y, usage, color='#FF9800')

    ax.set_xlabel('Usage')
    ax.set_title(title)
    ax.set_yticks(list(y))
    ax.set_yticklabels(names)
    ax.grid(axis='x', alpha=0.3)

    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)

    return buf


def generate_stats_chart(current_sales, prev_sales, title, current_label, prev_label):
    """Generate a comparison bar chart for stats."""
    if not current_sales or not CHARTS_AVAILABLE:
        return None

    # Get top products by revenue from current period
    sorted_current = sorted(current_sales, key=lambda x: int(x.get('payed_sum', 0) or 0), reverse=True)[:8]

    # Create lookup for previous period
    prev_lookup = {p.get('product_name'): p for p in prev_sales} if prev_sales else {}

    names = []
    current_values = []
    prev_values = []

    for p in sorted_current:
        name = p.get('product_name', 'Unknown')
        names.append(name[:15])
        current_values.append(int(p.get('payed_sum', 0) or 0) / 100)
        prev_p = prev_lookup.get(name, {})
        prev_values.append(int(prev_p.get('payed_sum', 0) or 0) / 100)

    fig, ax = plt.subplots(figsize=(10, 5))
    x = range(len(names))
    width = 0.35

    ax.bar([i - width/2 for i in x], current_values, width, label=current_label, color='#2196F3')
    ax.bar([i + width/2 for i in x], prev_values, width, label=prev_label, color='#9E9E9E')

    ax.set_ylabel('Revenue (฿)')
    ax.set_title(title)
    ax.set_xticks(list(x))
    ax.set_xticklabels(names, rotation=45, ha='right')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f'{x:,.0f}'))

    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    buf.seek(0)
    plt.close(fig)

    return buf
