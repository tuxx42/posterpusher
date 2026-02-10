/**
 * POS Dashboard — client-side helpers
 * Chart.js rendering, currency formatting, WebSocket client
 */

// ============================================================
// Currency formatting
// ============================================================

function formatCurrency(amountInCents) {
    const amount = amountInCents / 100;
    return '\u0E3F' + amount.toLocaleString('en-US', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });
}

// ============================================================
// Chart.js helpers
// ============================================================

const CHART_COLORS = [
    'rgba(33, 150, 243, 0.7)',   // blue
    'rgba(76, 175, 80, 0.7)',    // green
    'rgba(255, 152, 0, 0.7)',    // orange
    'rgba(244, 67, 54, 0.7)',    // red
    'rgba(156, 39, 176, 0.7)',   // purple
    'rgba(0, 188, 212, 0.7)',    // cyan
    'rgba(255, 193, 7, 0.7)',    // amber
    'rgba(96, 125, 139, 0.7)',   // blue-grey
    'rgba(233, 30, 99, 0.7)',    // pink
    'rgba(139, 195, 74, 0.7)',   // light-green
];

function renderBarChart(canvasId, labels, datasets) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;
    new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: { labels: { color: '#ccc' } },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.dataset.label + ': ' + formatCurrency(context.raw * 100);
                        }
                    }
                }
            },
            scales: {
                x: { ticks: { color: '#999' }, grid: { color: 'rgba(255,255,255,0.05)' } },
                y: {
                    ticks: {
                        color: '#999',
                        callback: function(value) { return formatCurrency(value * 100); }
                    },
                    grid: { color: 'rgba(255,255,255,0.05)' }
                }
            }
        }
    });
}

function renderHorizontalBarChart(canvasId, labels, datasets) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;
    new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: { labels: { color: '#ccc' } },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.dataset.label + ': ' + formatCurrency(context.raw * 100);
                        }
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        color: '#999',
                        callback: function(value) { return formatCurrency(value * 100); }
                    },
                    grid: { color: 'rgba(255,255,255,0.05)' }
                },
                y: { ticks: { color: '#999' }, grid: { color: 'rgba(255,255,255,0.05)' } }
            }
        }
    });
}

function renderPieChart(canvasId, labels, data) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;
    new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels,
            datasets: [{
                data,
                backgroundColor: CHART_COLORS.slice(0, labels.length),
                borderWidth: 1,
                borderColor: 'rgba(0,0,0,0.3)'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    position: 'right',
                    labels: { color: '#ccc', font: { size: 11 } }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const pct = ((context.raw / total) * 100).toFixed(1);
                            return context.label + ': ' + formatCurrency(context.raw * 100) + ' (' + pct + '%)';
                        }
                    }
                }
            }
        }
    });
}

// ============================================================
// WebSocket client with auto-reconnect
// ============================================================

function initSalesWebSocket(url, onSale) {
    const indicator = document.getElementById('ws-indicator');
    const text = document.getElementById('ws-text');
    let retryDelay = 1000;
    let ws = null;

    function setStatus(connected) {
        if (indicator) {
            indicator.className = 'ws-status ' + (connected ? 'ws-connected' : 'ws-disconnected');
        }
        if (text) {
            text.textContent = connected ? 'live' : 'reconnecting...';
        }
    }

    function connect() {
        ws = new WebSocket(url);

        ws.onopen = function() {
            setStatus(true);
            retryDelay = 1000; // Reset backoff on successful connection
        };

        ws.onmessage = function(event) {
            try {
                const sale = JSON.parse(event.data);
                onSale(sale);
            } catch (e) {
                console.error('Failed to parse WebSocket message:', e);
            }
        };

        ws.onclose = function() {
            setStatus(false);
            // Reconnect with exponential backoff
            setTimeout(function() {
                retryDelay = Math.min(retryDelay * 2, 30000);
                connect();
            }, retryDelay);
        };

        ws.onerror = function() {
            ws.close();
        };
    }

    connect();
}
