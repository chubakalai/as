import hashlib
import hmac
import http.server
import json
import logging
import os
import threading
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

DEBUG = False

MEXC_BASE = "https://contract.mexc.co"
PORT = int(os.getenv("PORT", "8080"))

# 15 minutes
INTERVAL_SECONDS = 900

# Adapted for Fly.io: persistent volume is mounted at /data (see fly.toml).
# Falls back to the local directory if /data does not exist, so the script
# remains runnable unmodified outside of the Fly.io environment.
HISTORY_DIR = "/data" if os.path.isdir("/data") else "."
HISTORY_FILE = os.path.join(HISTORY_DIR, "mexc_history.jsonl")

# Chart configuration
CHART_VIEWBOX_SIZE = 600
CHART_MARGIN_LEFT = 70
CHART_MARGIN_RIGHT = 30
CHART_MARGIN_TOP = 30
CHART_MARGIN_BOTTOM = 60

# Drawdown chart bar-geometry configuration
DRAWDOWN_MIN_SCOPE_DAYS = 30
DRAWDOWN_BARS_PER_SCOPE = 30

# Rolling drawdown window
ROLLING_WINDOW_SECONDS = 86400

# VAR alert configuration
VAR_THRESHOLD_PCT = 62.0
VAR_ALERT_COUNT = 4
VAR_ALERT_WINDOW_DAYS = 30


# ─────────────────────────────────────────────────────────────────────────────
# .ENV LOADER
# ─────────────────────────────────────────────────────────────────────────────

def load_dotenv(dotenv_path: str = ".env") -> None:
    if not os.path.exists(dotenv_path):
        return

    try:
        with open(dotenv_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()

                if not line or line.startswith("#") or "=" not in line:
                    continue

                key, value = line.split("=", 1)

                key = key.strip()
                value = value.strip().strip("'\"")

                if key and key not in os.environ:
                    os.environ[key] = value

    except Exception as exc:
        logging.warning(
            "Failed to read %s: %s",
            dotenv_path,
            exc,
        )


load_dotenv()


# ─────────────────────────────────────────────────────────────────────────────
# API CREDENTIALS
# ─────────────────────────────────────────────────────────────────────────────

MEXC_KEY = os.getenv("MEXC") or os.getenv("MEXC_KEY") or ""
MEXC_SECRET = os.getenv("MEXCSECRET") or os.getenv("MEXC_SECRET") or ""


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING & STATE
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

latest_text = "Initializing...\n"
text_lock = threading.Lock()

# Historical data storage
history_data: List[Dict[str, Any]] = []
history_lock = threading.Lock()

# Boolean VAR alert flag.
var_alert = False

# Debug state storage
debug_state: Dict[str, Any] = {
    "credentials_present": False,
    "api_ok": False,
    "endpoint": "",
    "raw_response": None,
    "selected_account": None,
    "last_error": "",
}

debug_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# UTC HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def utc_dt(timeslot: float) -> datetime:
    """Convert a Unix timestamp to a timezone-aware UTC datetime."""
    return datetime.fromtimestamp(timeslot, tz=timezone.utc)


def utc_midnight(timeslot: float) -> int:
    """Return the Unix timestamp of the UTC midnight on or before timeslot."""
    dt = utc_dt(timeslot)
    midnight = datetime(
        dt.year,
        dt.month,
        dt.day,
        tzinfo=timezone.utc,
    )
    return int(midnight.timestamp())


# ─────────────────────────────────────────────────────────────────────────────
# HISTORY MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

def load_history() -> None:
    """Load existing history from the JSONL file into memory."""

    if not os.path.exists(HISTORY_FILE):
        return

    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()

                if not line:
                    continue

                record = json.loads(line)
                history_data.append(record)

        logging.info(
            "Loaded %d historical records.",
            len(history_data),
        )

    except Exception as exc:
        logging.error(
            "Failed to load history: %s",
            exc,
        )


def save_to_history(
    timeslot: int,
    raw_response: Dict[str, Any],
) -> None:
    """
    Append a raw API response to the history file.

    This function is only called for completed 15-minute time slots.
    """

    record = {
        "timeslot": timeslot,
        "datetime": utc_dt(timeslot).isoformat(),
        "raw_response": raw_response,
    }

    with history_lock:
        try:
            with open(
                HISTORY_FILE,
                "a",
                encoding="utf-8",
            ) as f:
                f.write(
                    json.dumps(record) + "\n"
                )

            history_data.append(record)

            logging.info(
                "Saved API response for 15-minute boundary %s",
                record["datetime"],
            )

        except Exception as exc:
            logging.error(
                "Failed to save history: %s",
                exc,
            )


# ─────────────────────────────────────────────────────────────────────────────
# MEXC API REQUEST
# ─────────────────────────────────────────────────────────────────────────────

def mexc_request(
    method: str,
    endpoint: str,
) -> Dict[str, Any]:

    if not MEXC_KEY or not MEXC_SECRET:
        logging.error(
            "Missing MEXC credentials."
        )

        with debug_lock:
            debug_state["credentials_present"] = False
            debug_state["last_error"] = (
                "Missing MEXC_KEY or MEXC_SECRET."
            )

        return {}

    with debug_lock:
        debug_state["credentials_present"] = True

    timestamp = str(
        int(time.time() * 1000)
    )

    signature_payload = (
        MEXC_KEY + timestamp
    )

    signature = hmac.new(
        MEXC_SECRET.encode("utf-8"),
        signature_payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    headers = {
        "ApiKey": MEXC_KEY,
        "Request-Time": timestamp,
        "Signature": signature,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    url = MEXC_BASE + endpoint

    request = urllib.request.Request(
        url,
        headers=headers,
        method=method,
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=12,
        ) as response:

            raw = response.read().decode(
                "utf-8"
            )

            if not raw.strip():
                return {}

            return json.loads(raw)

    except Exception as exc:
        logging.error(
            "API request failed [%s %s]: %s",
            method,
            endpoint,
            exc,
        )

        with debug_lock:
            debug_state["api_ok"] = False
            debug_state["last_error"] = (
                f"[{method} {endpoint}] {exc}"
            )

        return {}


# ─────────────────────────────────────────────────────────────────────────────
# FETCH ACCOUNT ASSETS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_account_assets() -> Tuple[
    Optional[Dict[str, Any]],
    Optional[Dict[str, Any]],
]:
    """
    Returns:
        (raw_response, usdt_account)
    """

    endpoint = (
        "/api/v1/private/account/assets"
    )

    response = mexc_request(
        "GET",
        endpoint,
    )

    with debug_lock:
        debug_state["endpoint"] = endpoint
        debug_state["raw_response"] = response

    if not response:
        return None, None

    with debug_lock:
        debug_state["api_ok"] = True
        debug_state["last_error"] = ""

    data = response.get("data")

    if not isinstance(data, list):
        with debug_lock:
            debug_state["last_error"] = (
                "Expected account-assets data "
                "to be a list."
            )

        return response, None

    usdt_account = None

    for account in data:
        if (
            isinstance(account, dict)
            and str(
                account.get("currency", "")
            ).upper() == "USDT"
        ):
            usdt_account = account
            break

    if usdt_account is None:
        with debug_lock:
            debug_state["last_error"] = (
                "No USDT account record found."
            )

        return response, None

    with debug_lock:
        debug_state["selected_account"] = (
            usdt_account
        )

    return response, usdt_account


# ─────────────────────────────────────────────────────────────────────────────
# DATA FORMATTING
# ─────────────────────────────────────────────────────────────────────────────

def get_number(
    account: Dict[str, Any],
    field: str,
) -> Optional[float]:

    value = account.get(field)

    if value is None:
        return None

    try:
        return float(value)

    except (TypeError, ValueError):
        return None


def format_number(
    value: Optional[float],
) -> str:

    if value is None:
        return "N/A"

    return f"{value:.2f}"


# ─────────────────────────────────────────────────────────────────────────────
# HEADLINE / REPORTED VALUES
# ─────────────────────────────────────────────────────────────────────────────

def build_headline(
    account: Optional[Dict[str, Any]],
    current_time: float,
) -> str:

    if account is None:
        return "Status: UNAVAILABLE\n"

    # ─────────────────────────────────────────────────────────────────────────
    # Five reported variables
    # ─────────────────────────────────────────────────────────────────────────

    equity = get_number(
        account,
        "equity",
    )

    unrealized = get_number(
        account,
        "unrealized",
    )

    wallet_balance = None

    if (
        equity is not None
        and unrealized is not None
    ):
        wallet_balance = (
            equity - unrealized
        )

    available_margin = get_number(
        account,
        "availableOpen",
    )

    position_margin = get_number(
        account,
        "positionMargin",
    )

    last_updated = utc_dt(current_time).strftime(
        "%Y-%m-%d %H:%M:%S"
    ) + " UTC"

    return (
        f"Last Updated: {last_updated}\n"
        "-----------------------------------\n"
        f"Equity:           "
        f"{format_number(equity)}\n"
        f"Unrealized P&L:   "
        f"{format_number(unrealized)}\n"
        f"Wallet Balance:   "
        f"{format_number(wallet_balance)}\n"
        f"Available Margin: "
        f"{format_number(available_margin)}\n"
        f"Position Margin:  "
        f"{format_number(position_margin)}\n"
    )


# ─────────────────────────────────────────────────────────────────────────────
# SERIES EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_equity_series() -> List[Tuple[int, float]]:
    """
    Extract (timeslot, equity) pairs from historical data.
    """
    series: List[Tuple[int, float]] = []

    with history_lock:
        records = list(history_data)

    for record in records:
        timeslot = record.get("timeslot")
        raw_response = record.get("raw_response")

        if not isinstance(timeslot, int):
            continue

        if not isinstance(raw_response, dict):
            continue

        data = raw_response.get("data")

        if not isinstance(data, list):
            continue

        usdt_account = None

        for account in data:
            if (
                isinstance(account, dict)
                and str(
                    account.get("currency", "")
                ).upper() == "USDT"
            ):
                usdt_account = account
                break

        if usdt_account is None:
            continue

        equity = get_number(
            usdt_account,
            "equity",
        )

        if equity is None:
            continue

        series.append((timeslot, equity))

    series.sort(key=lambda pair: pair[0])

    return series


def extract_margin_series() -> List[Tuple[int, float, float]]:
    """
    Extract (timeslot, unrealized_pnl, position_margin) triples from
    historical data. These are used to calculate a dollar-denominated
    drawdown expressed as a percentage of the capital placed at risk
    (position margin) at that same point in time.
    """
    series: List[Tuple[int, float, float]] = []

    with history_lock:
        records = list(history_data)

    for record in records:
        timeslot = record.get("timeslot")
        raw_response = record.get("raw_response")

        if not isinstance(timeslot, int):
            continue

        if not isinstance(raw_response, dict):
            continue

        data = raw_response.get("data")

        if not isinstance(data, list):
            continue

        usdt_account = None

        for account in data:
            if (
                isinstance(account, dict)
                and str(
                    account.get("currency", "")
                ).upper() == "USDT"
            ):
                usdt_account = account
                break

        if usdt_account is None:
            continue

        unrealized = get_number(
            usdt_account,
            "unrealized",
        )

        margin = get_number(
            usdt_account,
            "positionMargin",
        )

        if unrealized is None or margin is None:
            continue

        series.append((timeslot, unrealized, margin))

    series.sort(key=lambda triple: triple[0])

    return series


# ─────────────────────────────────────────────────────────────────────────────
# SVG EQUITY CHART
# ─────────────────────────────────────────────────────────────────────────────

def build_equity_chart_svg(
    series: List[Tuple[int, float]],
) -> str:
    """
    Render a square (1:1) SVG line chart of equity over time.

    The x-axis marks every UTC hour boundary with a bare hour-of-day
    number (00-23). At the specific hour mark where a new UTC calendar
    day begins, the "00" label is followed by a second line beneath it
    showing the day and month of that new day in dd.mm format
    (e.g., "10.09"). All other hour marks show only their single-line
    hh label, unchanged.

    Dashed reference/gridlines (both the horizontal min/max lines and
    the vertical per-hour gridlines) have been removed; only the solid
    axis lines, solid tick marks, and text labels remain.
    """

    if len(series) < 2:
        return (
            '<div class="chart-placeholder">'
            "Insufficient historical data to render a chart. "
            "At least two recorded data points are required."
            "</div>"
        )

    size = CHART_VIEWBOX_SIZE

    plot_left = CHART_MARGIN_LEFT
    plot_right = size - CHART_MARGIN_RIGHT
    plot_top = CHART_MARGIN_TOP
    plot_bottom = size - CHART_MARGIN_BOTTOM

    plot_width = plot_right - plot_left
    plot_height = plot_bottom - plot_top

    timeslots = [pair[0] for pair in series]
    equities = [pair[1] for pair in series]

    min_time = min(timeslots)
    max_time = max(timeslots)

    min_equity = min(equities)
    max_equity = max(equities)

    time_span = max_time - min_time
    equity_span = max_equity - min_equity

    if time_span == 0:
        time_span = 1

    if equity_span == 0:
        equity_span = 1

    def x_for(timeslot: int) -> float:
        fraction = (timeslot - min_time) / time_span
        return plot_left + fraction * plot_width

    def y_for(equity: float) -> float:
        fraction = (equity - min_equity) / equity_span
        return plot_bottom - fraction * plot_height

    points_attr = " ".join(
        f"{x_for(t):.2f},{y_for(e):.2f}"
        for t, e in series
    )

    y_axis_elements = [
        f'<line x1="{plot_left}" y1="{plot_top}" '
        f'x2="{plot_left}" y2="{plot_bottom}" '
        'stroke="#333333" stroke-width="1.5" />',

        f'<text x="{plot_left - 8}" y="{plot_top + 4}" '
        'text-anchor="end" font-size="12" font-family="monospace" '
        f'fill="#333333">{format_number(max_equity)}</text>',

        f'<text x="{plot_left - 8}" y="{plot_bottom + 4}" '
        'text-anchor="end" font-size="12" font-family="monospace" '
        f'fill="#333333">{format_number(min_equity)}</text>',
    ]

    x_axis_elements = [
        f'<line x1="{plot_left}" y1="{plot_bottom}" '
        f'x2="{plot_right}" y2="{plot_bottom}" '
        'stroke="#333333" stroke-width="1.5" />',
    ]

    seconds_per_hour = 3600

    first_hour_mark = (
        (min_time // seconds_per_hour) + 1
    ) * seconds_per_hour

    if min_time % seconds_per_hour == 0:
        first_hour_mark = min_time

    hour_mark = first_hour_mark

    while hour_mark <= max_time:
        x = x_for(hour_mark)

        mark_dt = utc_dt(hour_mark)

        x_axis_elements.append(
            f'<line x1="{x:.2f}" y1="{plot_bottom}" '
            f'x2="{x:.2f}" y2="{plot_bottom + 6}" '
            'stroke="#333333" stroke-width="1.5" />'
        )

        if mark_dt.hour == 0:
            # New UTC calendar day: show "00" with the date on a
            # second line beneath it.
            date_label = mark_dt.strftime("%d.%m")

            x_axis_elements.append(
                f'<text x="{x:.2f}" y="{plot_bottom + 20}" '
                'text-anchor="middle" font-size="11" '
                'font-family="monospace" fill="#333333">00</text>'
            )

            x_axis_elements.append(
                f'<text x="{x:.2f}" y="{plot_bottom + 33}" '
                'text-anchor="middle" font-size="11" '
                f'font-family="monospace" fill="#333333">{date_label}</text>'
            )

        else:
            label = f"{mark_dt.hour:02d}"

            x_axis_elements.append(
                f'<text x="{x:.2f}" y="{plot_bottom + 20}" '
                'text-anchor="middle" font-size="11" '
                f'font-family="monospace" fill="#333333">{label}</text>'
            )

        hour_mark += seconds_per_hour

    point_elements = [
        f'<circle cx="{x_for(t):.2f}" cy="{y_for(e):.2f}" r="2.5" '
        'fill="#1a73e8" />'
        for t, e in series
    ]

    svg_parts = [
        f'<svg viewBox="0 0 {size} {size}" width="{size}" height="{size}" '
        'xmlns="http://www.w3.org/2000/svg" role="img" '
        'aria-label="Equity over time">',

        f'<rect x="0" y="0" width="{size}" height="{size}" fill="#ffffff" />',

        f'<text x="{size / 2:.2f}" y="18" text-anchor="middle" '
        'font-size="14" font-family="monospace" font-weight="bold" '
        'fill="#111111">Equity Over Time (UTC)</text>',

        *y_axis_elements,
        *x_axis_elements,

        f'<polyline points="{points_attr}" fill="none" '
        'stroke="#1a73e8" stroke-width="2" />',

        *point_elements,

        "</svg>",
    ]

    return "".join(svg_parts)


# ─────────────────────────────────────────────────────────────────────────────
# ROLLING DRAWDOWN EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_rolling_drawdowns(
    series: List[Tuple[int, float, float]],
) -> List[Tuple[int, float]]:
    """
    Compute, for every observed timeslot, the maximum drawdown within
    the trailing 24-hour window ending at that timeslot. Drawdown is
    measured in absolute dollar terms as the decline in unrealized PnL
    from its running peak *within that trailing window only* (the peak
    is not carried over from outside the window). That dollar figure
    is then divided by the position margin recorded at the same
    timeslot, per the relationship:

        drawdown_pct = abs_dollar_drawdown / position_margin_at_trough

    This differs from an approach that measures the percentage decline
    of position margin itself. Position margin can change for reasons
    unrelated to trading losses (manual top-ups, leverage changes,
    position resizing), so using it as the numerator produces drawdown
    percentages with no reliable relationship to actual dollar losses.

    Unlike a fixed-calendar-day partition, this window is anchored to
    each individual timeslot and looks back exactly
    ROLLING_WINDOW_SECONDS, so it does not reset at a fixed UTC
    boundary.

    Returns a list of (timeslot, rolling_drawdown_pct) pairs, one per
    input observation, sorted by timeslot.
    """

    if not series:
        return []

    # series is already sorted by timeslot (guaranteed by
    # extract_margin_series), which allows a two-pointer sliding
    # window rather than an O(n^2) rescan per timeslot.

    rolling: List[Tuple[int, float]] = []

    window_start_idx = 0
    n = len(series)

    for i in range(n):
        current_time = series[i][0]
        window_floor = current_time - ROLLING_WINDOW_SECONDS

        # Advance the window's left edge past any observation now
        # older than the trailing 24-hour cutoff for this timeslot.
        while (
            window_start_idx < i
            and series[window_start_idx][0] < window_floor
        ):
            window_start_idx += 1

        window_slice = series[window_start_idx: i + 1]

        running_peak_pnl = window_slice[0][1]
        max_dd_pct = 0.0

        for _, unrealized, margin in window_slice:
            if unrealized > running_peak_pnl:
                running_peak_pnl = unrealized

            abs_dollar_drawdown = running_peak_pnl - unrealized

            if margin > 0:
                dd_pct = (abs_dollar_drawdown / margin) * 100.0
                if dd_pct > max_dd_pct:
                    max_dd_pct = dd_pct

        rolling.append((current_time, max_dd_pct))

    return rolling


def bucket_rolling_drawdowns_by_day(
    rolling: List[Tuple[int, float]],
) -> List[Tuple[int, float]]:
    """
    Group the per-observation rolling 24-hour drawdown series by UTC
    calendar day, taking the maximum rolling drawdown value observed
    at any point during that day as the day's bar height.

    Returns (utc_midnight_timeslot, max_rolling_dd_pct) pairs, one per
    UTC calendar day present in the data, sorted by day.
    """

    if not rolling:
        return []

    days: Dict[int, float] = {}

    for t, dd in rolling:
        midnight = utc_midnight(t)

        if midnight not in days or dd > days[midnight]:
            days[midnight] = dd

    return sorted(days.items(), key=lambda pair: pair[0])


# ─────────────────────────────────────────────────────────────────────────────
# SVG DRAWDOWN CHART
# ─────────────────────────────────────────────────────────────────────────────

def check_var_alert(
    drawdowns: List[Tuple[int, float]],
) -> bool:
    """Return True when 4 daily bars exceed VAR within the latest 30 days."""

    if not drawdowns:
        return False

    latest_day = max(t for t, _ in drawdowns)
    window_start = latest_day - (VAR_ALERT_WINDOW_DAYS * 86400)

    exceedances = sum(
        1
        for t, dd in drawdowns
        if window_start <= t <= latest_day
        and dd > VAR_THRESHOLD_PCT
    )

    return exceedances >= VAR_ALERT_COUNT


def build_drawdown_chart_svg(
    drawdowns: List[Tuple[int, float]],
) -> str:
    """
    Render a square (1:1) SVG bar chart of daily bars, where each bar's
    height is the maximum trailing-24-hour rolling drawdown observed
    during that UTC calendar day, expressed as absolute-dollar PnL
    decline relative to position margin.
    The y-axis marks the 99% VAR at 62.0%. Bars above 62.0% are colored red.

    Bar geometry:
      - The x-axis scope spans a minimum of DRAWDOWN_MIN_SCOPE_DAYS days
        (one month). If the data spans more than this, the scope expands
        to match the full span of the data exactly.
      - Each bar's width is fixed at (scope / DRAWDOWN_BARS_PER_SCOPE).
        At the minimum one-month scope, this yields one bar per day.
        As the scope grows beyond one month (because more than
        DRAWDOWN_BARS_PER_SCOPE daily observations exist), bar width
        shrinks proportionally, since it remains pegged at one-thirtieth
        of the now-larger scope.
      - Bars are drawn with no inter-bar margin: each bar's rendered
        width equals its full allotted slot width along the time axis.
        Corners are square and fill opacity is full, so each bar is a
        precise rectangle.

    X-axis labeling:
      - Every day receives a tick mark and a label.
      - An ordinary day is labeled with the bare day-of-month number
        (1, 2, 3, and so on).
      - The first day encountered within a new calendar month
        (including the very first day of the dataset) is labeled with
        the first three letters of that month's name instead of its
        numeral, marking the month threshold.

    All day boundaries and labels are computed in UTC.
    """

    if not drawdowns:
        return (
            '<div class="chart-placeholder">'
            "Insufficient historical data to render drawdown chart."
            "</div>"
        )

    size = CHART_VIEWBOX_SIZE

    plot_left = CHART_MARGIN_LEFT
    plot_right = size - CHART_MARGIN_RIGHT
    plot_top = CHART_MARGIN_TOP
    plot_bottom = size - CHART_MARGIN_BOTTOM

    plot_width = plot_right - plot_left
    plot_height = plot_bottom - plot_top

    timeslots = [pair[0] for pair in drawdowns]
    dd_values = [pair[1] for pair in drawdowns]

    seconds_per_day = 86400
    min_scope_seconds = (
        DRAWDOWN_MIN_SCOPE_DAYS * seconds_per_day
    )

    earliest_time = min(timeslots)
    latest_time = max(timeslots)

    data_span = latest_time - earliest_time

    # The axis scope is the greater of the one-month minimum and the
    # actual span of the data. The scope always begins at the earliest
    # observed day.
    scope_seconds = max(
        min_scope_seconds,
        data_span,
    )

    min_time = earliest_time
    max_time = min_time + scope_seconds

    # 99% VAR threshold
    max_dd = max(max(dd_values), VAR_THRESHOLD_PCT + 3.0)
    min_dd = 0.0

    dd_span = max_dd - min_dd
    if dd_span == 0:
        dd_span = 1.0

    # Bar width is fixed at one-thirtieth of the current axis scope.
    # This yields a one-day-wide bar at the one-month minimum scope,
    # and shrinks proportionally as the scope grows past one month.
    bar_width_seconds = (
        scope_seconds / DRAWDOWN_BARS_PER_SCOPE
    )

    pixels_per_second = (
        plot_width / scope_seconds
    )

    drawn_bar_width = (
        bar_width_seconds * pixels_per_second
    )

    def x_for_bar(timeslot: int) -> float:
        return (
            plot_left
            + (timeslot - min_time) * pixels_per_second
        )

    def y_for(dd: float) -> float:
        fraction = (dd - min_dd) / dd_span
        return plot_bottom - fraction * plot_height

    # ─────────────────────────────────────────────────────────────────────────
    # Bars
    # ─────────────────────────────────────────────────────────────────────────

    bar_elements = []

    for t, dd in drawdowns:
        x = x_for_bar(t)
        y = y_for(dd)
        h = plot_bottom - y

        if dd > VAR_THRESHOLD_PCT:
            color = "#ff4444"
        else:
            color = "#1a73e8"

        bar_elements.append(
            f'<rect x="{x:.2f}" y="{y:.2f}" '
            f'width="{drawn_bar_width:.2f}" height="{h:.2f}" '
            f'fill="{color}" />'
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Y-axis
    # ─────────────────────────────────────────────────────────────────────────

    y_axis_elements = [
        f'<line x1="{plot_left}" y1="{plot_top}" '
        f'x2="{plot_left}" y2="{plot_bottom}" '
        'stroke="#333333" stroke-width="1.5" />',

        f'<line x1="{plot_left}" y1="{y_for(VAR_THRESHOLD_PCT):.2f}" '
        f'x2="{plot_right}" y2="{y_for(VAR_THRESHOLD_PCT):.2f}" '
        'stroke="#ff4444" stroke-width="1.5" stroke-dasharray="4,4" />',

        f'<text x="{plot_left - 8}" y="{y_for(VAR_THRESHOLD_PCT) + 4:.2f}" '
        'text-anchor="end" font-size="12" font-family="monospace" '
        'fill="#ff4444">62.0%</text>',
    ]

    # ─────────────────────────────────────────────────────────────────────────
    # X-axis
    # ─────────────────────────────────────────────────────────────────────────

    x_axis_elements = [
        f'<line x1="{plot_left}" y1="{plot_bottom}" '
        f'x2="{plot_right}" y2="{plot_bottom}" '
        'stroke="#333333" stroke-width="1.5" />',
    ]

    seen_months = set()

    for t, _ in drawdowns:
        dt = utc_dt(t)
        month_key = (dt.year, dt.month)

        x = x_for_bar(t) + (drawn_bar_width / 2.0)

        if month_key not in seen_months:
            seen_months.add(month_key)
            label = dt.strftime("%b")
        else:
            label = str(dt.day)

        x_axis_elements.append(
            f'<line x1="{x:.2f}" y1="{plot_bottom}" '
            f'x2="{x:.2f}" y2="{plot_bottom + 6}" '
            'stroke="#333333" stroke-width="1.5" />'
        )

        x_axis_elements.append(
            f'<text x="{x:.2f}" y="{plot_bottom + 18}" '
            'text-anchor="middle" font-size="10" font-family="monospace" '
            f'fill="#333333">{label}</text>'
        )

    svg_parts = [
        f'<svg viewBox="0 0 {size} {size}" width="{size}" height="{size}" '
        'xmlns="http://www.w3.org/2000/svg" role="img" '
        'aria-label="Daily Margin Drawdown Chart">',

        f'<rect x="0" y="0" width="{size}" height="{size}" fill="#ffffff" />',

        f'<text x="{size / 2:.2f}" y="18" text-anchor="middle" '
        'font-size="14" font-family="monospace" font-weight="bold" '
        'fill="#111111">Rolling 24h Drawdown &amp; 99% VAR (UTC)</text>',

        *bar_elements,
        *y_axis_elements,
        *x_axis_elements,

        "</svg>",
    ]

    return "".join(svg_parts)


# ─────────────────────────────────────────────────────────────────────────────
# DEBUG OUTPUT
# ─────────────────────────────────────────────────────────────────────────────

def build_debug_block() -> str:

    with debug_lock:
        state = dict(debug_state)

    lines = []

    lines.append("")
    lines.append("---- DEBUG ----")

    lines.append(
        "Credentials present: "
        f"{state['credentials_present']}"
    )

    lines.append(
        "API request OK: "
        f"{state['api_ok']}"
    )

    lines.append(
        "Endpoint: "
        f"{state['endpoint'] or 'N/A'}"
    )

    lines.append("")

    # ─────────────────────────────────────────────────────────────────────────
    # SELECTED ACCOUNT
    # ─────────────────────────────────────────────────────────────────────────

    lines.append(
        "SELECTED USDT ACCOUNT:"
    )

    selected = state[
        "selected_account"
    ]

    if selected is None:
        lines.append("N/A")

    else:
        lines.append(
            json.dumps(
                selected,
                indent=2,
                sort_keys=True,
            )
        )

    lines.append("")

    # ─────────────────────────────────────────────────────────────────────────
    # ACCOUNT FIELDS
    # ─────────────────────────────────────────────────────────────────────────

    if selected is not None:

        lines.append(
            "ACCOUNT FIELDS:"
        )

        fields = (
            "equity",
            "unrealized",
            "availableBalance",
            "availableCash",
            "availableOpen",
            "cashBalance",
            "positionMargin",
            "frozenBalance",
            "bonus",
            "contributeMarginAmount",
            "debtAmount",
        )

        for field in fields:
            lines.append(
                f"{field}: "
                f"{selected.get(field, 'N/A')}"
            )

        lines.append("")

    # ─────────────────────────────────────────────────────────────────────────
    # RAW API RESPONSE
    # ─────────────────────────────────────────────────────────────────────────

    lines.append(
        "COMPLETE RAW API RESPONSE:"
    )

    raw_response = state[
        "raw_response"
    ]

    if raw_response is None:
        lines.append("N/A")

    else:
        lines.append(
            json.dumps(
                raw_response,
                indent=2,
                sort_keys=True,
            )
        )

    if state["last_error"]:
        lines.append("")
        lines.append(
            f"Last error: "
            f"{state['last_error']}"
        )

    lines.append(
        "---- END DEBUG ----"
    )

    return (
        "\n".join(lines)
        + "\n"
    )


# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT
# ─────────────────────────────────────────────────────────────────────────────

def build_output_html(
    account: Optional[Dict[str, Any]],
    current_time: float,
) -> str:
    """
    Build the complete HTML document served to the browser.
    """

    headline = build_headline(
        account,
        current_time,
    )

    text_block = headline

    if DEBUG:
        text_block += build_debug_block()

    equity_series = extract_equity_series()
    chart_svg = build_equity_chart_svg(equity_series)

    # Use unrealized PnL relative to position margin for the drawdown chart,
    # computed as a rolling trailing-24-hour window rather than a fixed
    # calendar-day reset, then bucketed to one bar per UTC calendar day.
    margin_series = extract_margin_series()
    rolling_drawdowns = extract_rolling_drawdowns(margin_series)
    daily_drawdowns = bucket_rolling_drawdowns_by_day(rolling_drawdowns)

    global var_alert
    var_alert = check_var_alert(daily_drawdowns)

    drawdown_chart_svg = build_drawdown_chart_svg(daily_drawdowns)

    alert_html = (
        '<div class="var-alert" role="alert" aria-live="assertive">ALERT</div>'
        if var_alert
        else ""
    )

    escaped_text_block = (
        text_block
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

    style_block = (
        "body {{ font-family: monospace; margin: 2rem; "
        "background-color: #fafafa; color: #111111; }}\n"
        "pre {{ background-color: #ffffff; padding: 1rem; "
        "border: 1px solid #dddddd; }}\n"
        ".chart-container {{ margin-top: 1.5rem; max-width: {chart_size}px; }}\n"
        ".chart-placeholder {{ padding: 1rem; border: 1px dashed "
        "#bbbbbb; color: #666666; }}\n"
        ".var-alert {{ margin-top: 2rem; margin-bottom: 2rem; "
        "font-family: monospace; font-size: 4rem; font-weight: 900; "
        "line-height: 1; text-align: center; color: #ff0000; }}\n"
    ).format(chart_size=CHART_VIEWBOX_SIZE)

    return (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n'
        "<head>\n"
        '<meta charset="utf-8" />\n'
        '<meta http-equiv="refresh" content="60" />\n'
        "<title>MEXC Assets Monitor</title>\n"
        "<style>\n"
        f"{style_block}"
        "</style>\n"
        "</head>\n"
        "<body>\n"
        f"<pre>{escaped_text_block}</pre>\n"
        '<div class="chart-container">\n'
        f"{chart_svg}\n"
        "</div>\n"
        '<div class="chart-container">\n'
        f"{drawdown_chart_svg}\n"
        "</div>\n"
        f"{alert_html}\n"
        "</body>\n"
        "</html>\n"
    )


# ─────────────────────────────────────────────────────────────────────────────
# API REFRESH
# ─────────────────────────────────────────────────────────────────────────────

def refresh_account(
    save_to_history_file: bool = False,
    timeslot: Optional[int] = None,
) -> None:
    """
    Fetch the current account state and update the web output.
    """

    global latest_text

    logging.info(
        "Running MEXC account sync..."
    )

    raw_response, account = (
        fetch_account_assets()
    )

    if (
        save_to_history_file
        and raw_response is not None
        and timeslot is not None
    ):
        save_to_history(
            timeslot,
            raw_response,
        )

    output = build_output_html(
        account,
        time.time(),
    )

    with text_lock:
        latest_text = output


# ─────────────────────────────────────────────────────────────────────────────
# 15-MINUTE MONITORING LOOP
# ─────────────────────────────────────────────────────────────────────────────

def monitoring_loop() -> None:
    """
    Waits for each 15-minute boundary and performs one logged API request.
    """

    while True:
        try:
            now = time.time()

            current_boundary = (
                int(now) // INTERVAL_SECONDS
            ) * INTERVAL_SECONDS

            next_boundary = (
                current_boundary
                + INTERVAL_SECONDS
            )

            sleep_seconds = (
                next_boundary - now
            )

            if sleep_seconds > 0:
                time.sleep(
                    sleep_seconds
                )

            boundary_time = (
                int(time.time())
                // INTERVAL_SECONDS
            ) * INTERVAL_SECONDS

            refresh_account(
                save_to_history_file=True,
                timeslot=boundary_time,
            )

        except Exception as exc:
            logging.error(
                "Monitor loop error: %s",
                exc,
                exc_info=True,
            )

            time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP SERVER
# ─────────────────────────────────────────────────────────────────────────────

class RequestHandler(
    http.server.BaseHTTPRequestHandler
):

    def do_GET(self) -> None:

        with text_lock:
            content = latest_text.encode(
                "utf-8"
            )

        self.send_response(200)

        self.send_header(
            "Content-Type",
            "text/html; charset=utf-8",
        )

        self.send_header(
            "Content-Length",
            str(len(content)),
        )

        self.end_headers()

        self.wfile.write(content)

    def log_message(
        self,
        format: str,
        *args: Any,
    ) -> None:
        return


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    load_history()

    refresh_account(
        save_to_history_file=False,
    )

    threading.Thread(
        target=monitoring_loop,
        daemon=True,
    ).start()

    server = http.server.HTTPServer(
        ("0.0.0.0", PORT),
        RequestHandler,
    )

    logging.info(
        "MEXC Assets Monitor online at "
        "http://localhost:%s",
        PORT,
    )

    server.serve_forever()