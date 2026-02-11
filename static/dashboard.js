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
    'rgba(33, 150, 243, 0.8)',   // blue
    'rgba(76, 175, 80, 0.8)',    // green
    'rgba(255, 152, 0, 0.8)',    // orange
    'rgba(244, 67, 54, 0.8)',    // red
    'rgba(156, 39, 176, 0.8)',   // purple
    'rgba(0, 188, 212, 0.8)',    // cyan
    'rgba(255, 193, 7, 0.8)',    // amber
    'rgba(96, 125, 139, 0.8)',   // blue-grey
    'rgba(233, 30, 99, 0.8)',    // pink
    'rgba(139, 195, 74, 0.8)',   // light-green
];

// Shared zoom/pan plugin config
const ZOOM_OPTIONS = {
    zoom: {
        wheel: { enabled: true },
        pinch: { enabled: true },
        mode: 'x',
    },
    pan: {
        enabled: true,
        mode: 'x',
    },
    limits: {
        x: { minRange: 1 },
    },
};

// Shared animation config
const ANIMATION_OPTIONS = {
    duration: 800,
    easing: 'easeOutQuart',
};

// Shared interaction config for better hover feel
const INTERACTION_OPTIONS = {
    mode: 'index',
    intersect: false,
};

function renderBarChart(canvasId, labels, datasets) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;

    // Add hover color brightening to datasets
    datasets.forEach(function(ds) {
        ds.hoverBackgroundColor = ds.backgroundColor.replace(/[\d.]+\)$/, '1)');
        ds.borderRadius = 4;
        ds.borderSkipped = false;
    });

    new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            animation: ANIMATION_OPTIONS,
            interaction: INTERACTION_OPTIONS,
            plugins: {
                legend: { labels: { color: '#ccc', usePointStyle: true, padding: 16 } },
                tooltip: {
                    backgroundColor: 'rgba(0,0,0,0.85)',
                    titleFont: { size: 13 },
                    bodyFont: { size: 12 },
                    padding: 12,
                    cornerRadius: 8,
                    callbacks: {
                        label: function(context) {
                            return ' ' + context.dataset.label + ': ' + formatCurrency(context.raw * 100);
                        }
                    }
                },
                zoom: ZOOM_OPTIONS,
            },
            scales: {
                x: {
                    ticks: { color: '#999' },
                    grid: { color: 'rgba(255,255,255,0.05)' }
                },
                y: {
                    ticks: {
                        color: '#999',
                        callback: function(value) { return formatCurrency(value * 100); }
                    },
                    grid: { color: 'rgba(255,255,255,0.08)' }
                }
            },
            onHover: function(event, elements) {
                event.native.target.style.cursor = elements.length ? 'pointer' : 'default';
            }
        }
    });
}

function renderHorizontalBarChart(canvasId, labels, datasets) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;

    datasets.forEach(function(ds) {
        ds.hoverBackgroundColor = ds.backgroundColor.replace(/[\d.]+\)$/, '1)');
        ds.borderRadius = 4;
        ds.borderSkipped = false;
    });

    new Chart(ctx, {
        type: 'bar',
        data: { labels, datasets },
        options: {
            indexAxis: 'y',
            responsive: true,
            maintainAspectRatio: true,
            animation: ANIMATION_OPTIONS,
            interaction: INTERACTION_OPTIONS,
            plugins: {
                legend: { labels: { color: '#ccc', usePointStyle: true, padding: 16 } },
                tooltip: {
                    backgroundColor: 'rgba(0,0,0,0.85)',
                    titleFont: { size: 13 },
                    bodyFont: { size: 12 },
                    padding: 12,
                    cornerRadius: 8,
                    callbacks: {
                        label: function(context) {
                            return ' ' + context.dataset.label + ': ' + formatCurrency(context.raw * 100);
                        }
                    }
                },
                zoom: {
                    zoom: { wheel: { enabled: true }, pinch: { enabled: true }, mode: 'y' },
                    pan: { enabled: true, mode: 'y' },
                },
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
            },
            onHover: function(event, elements) {
                event.native.target.style.cursor = elements.length ? 'pointer' : 'default';
            }
        }
    });
}

function renderPieChart(canvasId, labels, data) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;

    const bgColors = CHART_COLORS.slice(0, labels.length);
    const hoverColors = bgColors.map(function(c) { return c.replace(/[\d.]+\)$/, '1)'); });

    new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels,
            datasets: [{
                data,
                backgroundColor: bgColors,
                hoverBackgroundColor: hoverColors,
                borderWidth: 2,
                borderColor: 'rgba(0,0,0,0.3)',
                hoverBorderColor: '#fff',
                hoverOffset: 8,
                spacing: 2,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            animation: {
                animateRotate: true,
                animateScale: true,
                duration: 1000,
                easing: 'easeOutQuart',
            },
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        color: '#ccc',
                        font: { size: 11 },
                        usePointStyle: true,
                        padding: 12,
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(0,0,0,0.85)',
                    titleFont: { size: 13 },
                    bodyFont: { size: 12 },
                    padding: 12,
                    cornerRadius: 8,
                    callbacks: {
                        label: function(context) {
                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                            const pct = ((context.raw / total) * 100).toFixed(1);
                            return ' ' + context.label + ': ' + formatCurrency(context.raw * 100) + ' (' + pct + '%)';
                        }
                    }
                }
            },
            onHover: function(event, elements) {
                event.native.target.style.cursor = elements.length ? 'pointer' : 'default';
            }
        }
    });
}

function renderLineChart(canvasId, labels, datasets) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return;

    datasets.forEach(function(ds) {
        ds.tension = 0.3;
        ds.pointRadius = 3;
        ds.pointHoverRadius = 6;
        ds.borderWidth = 2;
        ds.fill = ds.fill !== undefined ? ds.fill : false;
    });

    return new Chart(ctx, {
        type: 'line',
        data: { labels, datasets },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            animation: ANIMATION_OPTIONS,
            interaction: INTERACTION_OPTIONS,
            plugins: {
                legend: { labels: { color: '#ccc', usePointStyle: true, padding: 16 } },
                tooltip: {
                    backgroundColor: 'rgba(0,0,0,0.85)',
                    titleFont: { size: 13 },
                    bodyFont: { size: 12 },
                    padding: 12,
                    cornerRadius: 8,
                    callbacks: {
                        label: function(context) {
                            return ' ' + context.dataset.label + ': ' + formatCurrency(context.raw * 100);
                        }
                    }
                },
                zoom: ZOOM_OPTIONS,
            },
            scales: {
                x: {
                    ticks: { color: '#999' },
                    grid: { color: 'rgba(255,255,255,0.05)' }
                },
                y: {
                    ticks: {
                        color: '#999',
                        callback: function(value) { return formatCurrency(value * 100); }
                    },
                    grid: { color: 'rgba(255,255,255,0.08)' }
                }
            },
            onHover: function(event, elements) {
                event.native.target.style.cursor = elements.length ? 'pointer' : 'default';
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
            retryDelay = 1000;
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

// ============================================================
// Sortable tables
// ============================================================

(function() {
    document.querySelectorAll('.sortable-table').forEach(function(table) {
        const headers = table.querySelectorAll('th[data-sort]');
        let currentSort = { col: null, asc: true };

        headers.forEach(function(th, colIndex) {
            th.style.cursor = 'pointer';
            th.addEventListener('click', function() {
                const key = th.dataset.sort;
                const asc = currentSort.col === key ? !currentSort.asc : true;
                currentSort = { col: key, asc: asc };

                headers.forEach(function(h) { h.textContent = h.textContent.replace(/ [▲▼]$/, ''); });
                th.textContent += asc ? ' ▲' : ' ▼';

                const tbody = table.querySelector('tbody');
                const rows = Array.from(tbody.querySelectorAll('tr'));

                rows.sort(function(a, b) {
                    const cellA = a.children[colIndex];
                    const cellB = b.children[colIndex];
                    let valA = cellA.dataset.value || cellA.textContent.trim();
                    let valB = cellB.dataset.value || cellB.textContent.trim();

                    const numA = parseFloat(valA);
                    const numB = parseFloat(valB);
                    if (!isNaN(numA) && !isNaN(numB)) {
                        return asc ? numA - numB : numB - numA;
                    }
                    return asc ? valA.localeCompare(valB) : valB.localeCompare(valA);
                });

                rows.forEach(function(row) { tbody.appendChild(row); });
            });
        });
    });
})();
