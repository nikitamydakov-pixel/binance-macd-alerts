
#!/usr/bin/env python3
"""BingX MACD 15m alerts with ENTRY / TAKE / STOP PNG charts."""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import io
import json
import logging
import math
import os
from pathlib import Path
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass

from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from matplotlib.patches import Rectangle
from matplotlib.ticker import FuncFormatter


# ==========================================
# НАСТРОЙКИ
# ==========================================

API_URL = "https://open-api.bingx.com"

INTERVAL = "15m"
PERIOD_MS = 15 * 60_000

MIN_TURNOVER_24H_USDT = 1_000_000.0

MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

EXTREMA_MIN_RATIO = 1.5

KLINE_LIMIT = 1000
MIN_CANDLES = 100
WARMUP_INDEX = 75

CLOSE_GRACE_MS = 2500

# Количество свечей на картинке
CHART_BARS = 90

# Количество свечей для стоп-лосса
STOP_LOOKBACK_BARS = 12

# Соотношение риск/прибыль 1:1
REWARD_RISK_RATIO = 1.0

RPS = float(
    os.getenv("BINGX_REQUESTS_PER_SECOND", "1")
)

WORKERS = int(
    os.getenv("MAX_WORKERS", "6")
)

STATE_PATH = Path(
    os.getenv(
        "STATE_PATH",
        str(
            Path.home()
            / "bingx-macd-state"
            / "state.json"
        )
    )
)

HTTP_TIMEOUT = 25

LOG = logging.getLogger("bingx-macd")


# ==========================================
# СТРУКТУРЫ ДАННЫХ
# ==========================================

@dataclass(frozen=True)
class Candle:
    start: int
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class Extremes:
    macd_abs: float
    hist_abs: float
    ratio: float
    macd_index: int
    hist_index: int
    previous_cross_index: int


@dataclass(frozen=True)
class Levels:
    entry: float
    take: float
    stop: float


@dataclass(frozen=True)
class Alert:

    symbol: str
    direction: str
    close_ms: int

    previous_macd: float
    current_macd: float

    extremes: Extremes
    previous_cross_ms: int

    levels: Levels
    chart_candles: tuple[Candle, ...]

    @property
    def key(self) -> str:
        return (
            f"{self.symbol}|15m|{self.close_ms}"
        )

    def caption(self) -> str:

        mark = (
            "🟢" if self.direction == "LONG"
            else "🔴"
        )

        chart_symbol = (
            "BINGX:"
            + self.symbol.replace("-", "")
            + ".P"
        )

        chart_link = (
            "https://www.tradingview.com/chart/?"
            + urllib.parse.urlencode({
                "symbol": chart_symbol,
                "interval": 15
            })
        )

        e = self.extremes

        return (
            f"{mark} {self.direction} | "
            f"{self.symbol} | 15m\n"
            f"MACD(12,26,9): "
            f"{self.previous_macd:.8g} "
            f"→ {self.current_macd:.8g}\n"
            f"Пересечение: {utc_time(self.close_ms)}\n"
            f"Экстремум |MACD|: {e.macd_abs:.8g}\n"
            f"Экстремум |Histogram|: {e.hist_abs:.8g}\n"
            f"Соотношение: {e.ratio:.2f}×\n"
            f"ВХОД: {price_text(self.levels.entry)}\n"
            f"ТЕЙК: {price_text(self.levels.take)}\n"
            f"СТОП: {price_text(self.levels.stop)}\n"
            f"R:R = 1:{REWARD_RISK_RATIO:g}\n"
            f"Уровни расчётные, ордеров нет.\n"
            f"График: {chart_link}"
        )


# ==========================================
# ОГРАНИЧЕНИЕ ЗАПРОСОВ
# ==========================================

class RateLimiter:

    def __init__(self, rps: float):

        if not 0 < rps <= 10:
            raise ValueError(
                "Invalid requests per second"
            )

        self.lock = threading.Lock()
        self.step = 1 / rps

        self.next_at = 0.0
        self.blocked_until = 0.0

    def wait(self):

        with self.lock:

            now = time.monotonic()

            start = max(
                now,
                self.next_at,
                self.blocked_until
            )

            self.next_at = start + self.step

        if start > now:
            time.sleep(start - now)

    def cooldown(self, seconds: float):

        with self.lock:

            self.blocked_until = max(
                self.blocked_until,
                time.monotonic() + seconds
            )

            self.next_at = max(
                self.next_at,
                self.blocked_until
            )


LIMITER = RateLimiter(RPS)


# ==========================================
# HTTP И BINGX API
# ==========================================

def request_json(
    url: str,
    *,
    bingx: bool = False,
    data: bytes | None = None,
    headers: dict | None = None
) -> dict:

    hdrs = {
        "Accept": "application/json",
        "User-Agent": "bingx-macd-chart/2.0"
    }

    hdrs.update(headers or {})

    safe_url = (
        "Telegram API"
        if "api.telegram.org/bot" in url
        else url.split("?", 1)[0]
    )

    for attempt in range(5):

        if bingx:
            LIMITER.wait()

        req = urllib.request.Request(
            url,
            data=data,
            headers=hdrs,
            method=(
                "POST"
                if data is not None
                else "GET"
            )
        )

        try:

            with urllib.request.urlopen(
                req,
                timeout=HTTP_TIMEOUT
            ) as response:

                result = json.load(response)

            if (
                bingx
                and str(
                    result.get("code", "missing")
                ) != "0"
            ):

                code = str(
                    result.get("code")
                )

                if (
                    code in {
                        "100410",
                        "100500",
                        "109500"
                    }
                    and attempt < 4
                ):

                    delay = min(
                        60,
                        5 * 2 ** attempt
                    )

                    LIMITER.cooldown(delay)

                    LOG.warning(
                        "BingX code %s; cooldown %ss",
                        code,
                        delay
                    )

                    continue

                raise RuntimeError(
                    f"BingX code {code}: "
                    f"{result.get('msg', '')}"
                )

            return result

        except urllib.error.HTTPError as exc:

            if (
                exc.code in {
                    429, 500, 502, 503, 504
                }
                and attempt < 4
            ):

                delay = min(
                    60,
                    5 * 2 ** attempt
                )

                if bingx:
                    LIMITER.cooldown(delay)
                else:
                    time.sleep(delay)

                continue

            detail = exc.read(200).decode(
                "utf-8",
                "replace"
            )

            raise RuntimeError(
                f"HTTP {exc.code} "
                f"at {safe_url}: {detail}"
            ) from exc

        except (
            urllib.error.URLError,
            TimeoutError,
            json.JSONDecodeError
        ) as exc:

            if attempt == 4:
                raise RuntimeError(
                    f"Network error: {exc}"
                ) from exc

            time.sleep(
                min(12, 2 ** attempt)
            )

    raise RuntimeError(
        f"Too many retries: {safe_url}"
    )


def bingx_get(
    path: str,
    params: dict | None = None
):

    url = API_URL + path

    if params:
        url += "?" + urllib.parse.urlencode(params)

    return request_json(
        url,
        bingx=True
    ).get("data")


def get_server_ms() -> int:

    data = bingx_get(
        "/openApi/swap/v2/server/time"
    )

    if (
        isinstance(data, dict)
        and data.get("serverTime") is not None
    ):
        return int(data["serverTime"])

    if isinstance(data, (int, str)):
        return int(data)

    raise RuntimeError(
        "Unexpected BingX server time"
    )


# ==========================================
# ФИЛЬТР ОБОРОТА ОТ 1 МЛН USDT
# ==========================================

def get_liquid_symbols() -> tuple[list[str], int]:

    contracts = bingx_get(
        "/openApi/swap/v2/quote/contracts"
    )

    if (
        not isinstance(contracts, list)
        or not contracts
    ):
        raise RuntimeError(
            "Invalid BingX contracts response"
        )

    active = {
        c["symbol"]
        for c in contracts
        if (
            isinstance(c, dict)
            and str(c.get("status")) == "1"
            and c.get("currency") == "USDT"
            and str(
                c.get("symbol", "")
            ).endswith("-USDT")
            and not str(
                c.get("symbol", "")
            ).startswith("NCS")
        )
    }

    if not active:
        raise RuntimeError(
            "No active USDT perpetuals"
        )

    tickers = bingx_get(
        "/openApi/swap/v2/quote/ticker"
    )

    if isinstance(tickers, dict):
        tickers = [tickers]

    if (
        not isinstance(tickers, list)
        or not tickers
    ):
        raise RuntimeError(
            "Invalid BingX tickers response"
        )

    eligible = set()

    for ticker in tickers:

        if not isinstance(ticker, dict):
            continue

        if ticker.get("symbol") not in active:
            continue

        try:

            turnover = float(
                ticker["quoteVolume"]
            )

        except (
            KeyError,
            ValueError,
            TypeError
        ):
            continue

        if (
            math.isfinite(turnover)
            and turnover >= MIN_TURNOVER_24H_USDT
        ):

            eligible.add(
                ticker["symbol"]
            )

    if not eligible:
        raise RuntimeError(
            "No symbols passed turnover filter"
        )

    LOG.info(
        "BingX eligible >= %.0f USDT/24h: %d/%d",
        MIN_TURNOVER_24H_USDT,
        len(eligible),
        len(active)
    )

    return sorted(eligible), len(active)


# ==========================================
# РАСЧЁТ MACD (12, 26, 9)
# ==========================================

def ema(
    values: list[float],
    period: int
) -> list[float]:

    if not values:
        return []

    alpha = 2 / (period + 1)

    output = [values[0]]

    for value in values[1:]:

        output.append(
            alpha * value
            + (1 - alpha) * output[-1]
        )

    return output


def calculate_macd(
    closes: list[float]
) -> tuple[list[float], list[float]]:

    fast = ema(
        closes,
        MACD_FAST
    )

    slow = ema(
        closes,
        MACD_SLOW
    )

    line = [
        a - b
        for a, b in zip(fast, slow)
    ]

    signal = ema(
        line,
        MACD_SIGNAL
    )

    return line, signal


def cross_direction(
    before: float,
    after: float
) -> str | None:

    if before <= 0 < after:
        return "LONG"

    if before >= 0 > after:
        return "SHORT"

    return None


# ==========================================
# ФИЛЬТР ЭКСТРЕМУМОВ MACD
# ==========================================

def find_extrema(
    line: list[float],
    signal: list[float],
    index: int,
    candles: list[Candle]
) -> Extremes | None:

    previous = None

    # Находим предыдущее пересечение нуля
    for j in range(
        index - 1,
        WARMUP_INDEX,
        -1
    ):

        direction = cross_direction(
            line[j - 1],
            line[j]
        )

        if direction:
            previous = j
            break

    if previous is None:
        return None

    # Не допускаем пропущенных свечей
    for j in range(
        previous,
        index + 1
    ):

        if (
            candles[j].start
            - candles[j - 1].start
            != PERIOD_MS
        ):
            return None

    # Предыдущее пересечение включаем,
    # текущее исключаем
    segment = range(
        previous,
        index
    )

    macd_index = max(
        segment,
        key=lambda j: abs(line[j])
    )

    hist_index = max(
        segment,
        key=lambda j: abs(
            line[j] - signal[j]
        )
    )

    macd_abs = abs(
        line[macd_index]
    )

    hist_abs = abs(
        line[hist_index]
        - signal[hist_index]
    )

    if not all(
        map(
            math.isfinite,
            (macd_abs, hist_abs)
        )
    ):
        return None

    if hist_abs <= 0:
        return None

    ratio = (
        macd_abs / hist_abs
    )

    if ratio < EXTREMA_MIN_RATIO:
        return None

    return Extremes(
        macd_abs,
        hist_abs,
        ratio,
        macd_index,
        hist_index,
        previous
    )


# ==========================================
# СВЕЧИ BINGX
# ==========================================

def parse_candles(
    raw: list,
    server_ms: int
) -> list[Candle]:

    if not isinstance(raw, list):
        raise ValueError(
            "Invalid BingX candle list"
        )

    parsed = {}

    for row in raw:

        try:

            if isinstance(row, dict):

                stamp = next(
                    (
                        row.get(k)
                        for k in (
                            "time",
                            "openTime",
                            "startTime"
                        )
                        if row.get(k) is not None
                    ),
                    None
                )

                c = Candle(
                    int(stamp),
                    float(row["open"]),
                    float(row["high"]),
                    float(row["low"]),
                    float(row["close"])
                )

            elif (
                isinstance(row, (list, tuple))
                and len(row) >= 5
            ):

                c = Candle(
                    int(row[0]),
                    float(row[1]),
                    float(row[2]),
                    float(row[3]),
                    float(row[4])
                )

            else:
                raise ValueError(
                    "Unsupported OHLC format"
                )

        except (
            ValueError,
            TypeError,
            KeyError
        ) as exc:

            shape = (
                sorted(row)
                if isinstance(row, dict)
                else type(row).__name__
            )

            raise ValueError(
                f"Malformed BingX kline: {shape}"
            ) from exc

        if c.start % PERIOD_MS:
            raise ValueError(
                f"Unaligned 15m candle: {c.start}"
            )

        if not all(
            math.isfinite(v) and v > 0
            for v in (
                c.open,
                c.high,
                c.low,
                c.close
            )
        ):
            raise ValueError(
                "Invalid OHLC price"
            )

        if (
            c.low > min(c.open, c.close)
            or c.high < max(c.open, c.close)
        ):
            raise ValueError(
                "Invalid candle high/low"
            )

        # Только закрытые свечи
        if (
            c.start + PERIOD_MS
            <= server_ms - CLOSE_GRACE_MS
        ):
            parsed[c.start] = c

    return sorted(
        parsed.values(),
        key=lambda c: c.start
    )


def utc_time(ms: int) -> str:

    return dt.datetime.fromtimestamp(
        ms / 1000,
        tz=dt.timezone.utc
    ).strftime("%d.%m %H:%M UTC")


def closed_boundary(
    server_ms: int
) -> int:

    return (
        (server_ms - CLOSE_GRACE_MS)
        // PERIOD_MS
        * PERIOD_MS
    )


def price_text(
    value: float
) -> str:

    if value >= 100:
        return f"{value:,.2f}"

    if value >= 1:
        return f"{value:.4f}"

    if value >= 0.01:
        return f"{value:.5f}"

    if value >= 0.001:
        return f"{value:.6f}"

    return f"{value:.8g}"


# ==========================================
# РАСЧЁТ ENTRY, TAKE PROFIT, STOP LOSS
# ==========================================

def make_levels(
    candles: tuple[Candle, ...],
    direction: str
) -> Levels:

    if not candles:
        raise ValueError(
            "No candles for chart"
        )

    # Вход по закрытию сигнальной свечи
    entry = candles[-1].close

    recent = candles[
        -min(
            len(candles),
            STOP_LOOKBACK_BARS
        ):
    ]

    if direction == "LONG":

        # Стоп ниже локального минимума
        stop = min(
            c.low for c in recent
        )

        risk = entry - stop

        if risk <= 0:

            risk = max(
                entry * 0.005,
                max(
                    c.high - c.low
                    for c in recent
                )
            )

            stop = entry - risk

        take = (
            entry
            + risk * REWARD_RISK_RATIO
        )

    elif direction == "SHORT":

        # Стоп выше локального максимума
        stop = max(
            c.high for c in recent
        )

        risk = stop - entry

        if risk <= 0:

            risk = max(
                entry * 0.005,
                max(
                    c.high - c.low
                    for c in recent
                )
            )

            stop = entry + risk

        # Защита от отрицательного TP
        if risk * REWARD_RISK_RATIO >= entry:

            risk = (
                entry * 0.90
                / REWARD_RISK_RATIO
            )

            stop = entry + risk

        take = (
            entry
            - risk * REWARD_RISK_RATIO
        )

    else:
        raise ValueError(
            "Invalid signal direction"
        )

    if not all(
        math.isfinite(v) and v > 0
        for v in (entry, stop, take)
    ):
        raise ValueError(
            "Invalid price levels"
        )

    return Levels(
        entry,
        take,
        stop
    )


# ==========================================
# СКАНИРОВАНИЕ МОНЕТЫ
# ==========================================

def scan_symbol(
    symbol: str,
    server_ms: int,
    last_ms: int | None
):

    raw = bingx_get(
        "/openApi/swap/v3/quote/klines",
        {
            "symbol": symbol,
            "interval": INTERVAL,
            "limit": KLINE_LIMIT
        }
    )

    candles = parse_candles(
        raw,
        server_ms
    )

    if len(candles) < MIN_CANDLES:
        return [], None, "young"

    closes = [
        c.close for c in candles
    ]

    line, signal = calculate_macd(closes)

    newest = (
        candles[-1].start + PERIOD_MS
    )

    # Первая синхронизация
    if last_ms is None:
        return [], newest, "bootstrapped"

    alerts = []

    for i in range(
        WARMUP_INDEX + 1,
        len(candles)
    ):

        bar_close = (
            candles[i].start + PERIOD_MS
        )

        if bar_close <= last_ms:
            continue

        if (
            candles[i].start
            - candles[i - 1].start
            != PERIOD_MS
        ):
            continue

        direction = cross_direction(
            line[i - 1],
            line[i]
        )

        if direction is None:
            continue

        # Сохраняем прежнее условие экстремумов
        extremes = find_extrema(
            line,
            signal,
            i,
            candles
        )

        if extremes is None:
            continue

        # Свечи для изображения
        chart_candles = tuple(
            candles[
                max(0, i + 1 - CHART_BARS):
                i + 1
            ]
        )

        # Рассчитываем уровни
        levels = make_levels(
            chart_candles,
            direction
        )

        alerts.append(
            Alert(
                symbol=symbol,
                direction=direction,
                close_ms=bar_close,
                previous_macd=line[i - 1],
                current_macd=line[i],
                extremes=extremes,
                previous_cross_ms=(
                    candles[
                        extremes.previous_cross_index
                    ].start + PERIOD_MS
                ),
                levels=levels,
                chart_candles=chart_candles
            )
        )

    return alerts, newest, "ok"


# ==========================================
# РИСОВАНИЕ ГРАФИКА
# ==========================================

def render_chart(
    alert: Alert
) -> bytes:

    bars = alert.chart_candles

    if not bars:
        raise ValueError(
            "No candles to plot"
        )

    entry = alert.levels.entry
    take = alert.levels.take
    stop = alert.levels.stop

    # Цвета свечей
    green = "#009f25"
    red = "#fa2020"

    # Цвета уровней
    colors = {
        "entry": "#19b5fe",
        "take": "#00cc55",
        "stop": "#ff3333"
    }

    fig = Figure(
        figsize=(14.5, 8.5),
        dpi=110,
        facecolor="#000000"
    )

    FigureCanvasAgg(fig)

    ax = fig.add_axes(
        [0.055, 0.13, 0.82, 0.75],
        facecolor="#000000"
    )

    last_price = bars[-1].close

    # Рисуем свечи
    for i, c in enumerate(bars):

        color = (
            green
            if c.close >= c.open
            else red
        )

        # Тень свечи
        ax.vlines(
            i,
            c.low,
            c.high,
            colors=color,
            linewidth=1.1,
            zorder=2
        )

        # Тело свечи
        low_body = min(
            c.open,
            c.close
        )

        body_height = max(
            abs(c.close - c.open),
            max(
                last_price * 0.00003,
                1e-12
            )
        )

        ax.add_patch(
            Rectangle(
                (i - 0.34, low_body),
                0.68,
                body_height,
                facecolor=color,
                edgecolor=color,
                linewidth=0.3,
                zorder=3
            )
        )

    # =====================================
    # ТРИ УРОВНЯ: ENTRY, TAKE, STOP
    # =====================================

    x_end = len(bars) + 16

    level_data = (
        (
            take,
            "ТЕЙК",
            colors["take"]
        ),
        (
            entry,
            "ВХОД",
            colors["entry"]
        ),
        (
            stop,
            "СТОП",
            colors["stop"]
        )
    )

    for y, label, color in level_data:

        # Горизонтальная линия
        ax.axhline(
            y,
            color=color,
            linewidth=2.8,
            alpha=0.98,
            zorder=1
        )

        # Цена уровня справа
        ax.text(
            len(bars) + 1.3,
            y,
            f"{label}  {price_text(y)}",
            color=color,
            fontsize=11,
            fontweight="bold",
            va="center",
            ha="left",
            bbox={
                "facecolor": "#000000",
                "edgecolor": "none",
                "alpha": 0.82,
                "pad": 1.8
            },
            zorder=5
        )

    # Маркер цены входа
    ax.scatter(
        len(bars) - 1,
        last_price,
        s=30,
        color="white",
        zorder=6
    )

    # =====================================
    # МАСШТАБ ГРАФИКА
    # =====================================

    y_min = min(
        [c.low for c in bars]
        + [take, entry, stop]
    )

    y_max = max(
        [c.high for c in bars]
        + [take, entry, stop]
    )

    padding = max(
        (y_max - y_min) * 0.10,
        last_price * 0.002
    )

    ax.set_ylim(
        y_min - padding,
        y_max + padding
    )

    ax.set_xlim(
        -1.5,
        x_end
    )

    # Цены справа
    ax.yaxis.tick_right()

    ax.yaxis.set_major_formatter(
        FuncFormatter(
            lambda value, _:
            price_text(value)
        )
    )

    ax.tick_params(
        axis="y",
        colors="#d9d9d9",
        labelsize=10,
        length=0,
        pad=10
    )

    # Убираем нижнюю ось
    ax.tick_params(
        axis="x",
        bottom=False,
        labelbottom=False
    )

    for spine in ax.spines.values():
        spine.set_visible(False)

    # Заголовок
    ax.set_title(
        f"{alert.symbol.replace('-', '')} "
        f"15m · {alert.direction} "
        f"(цена сигнала "
        f"${price_text(entry)})",
        color="white",
        fontsize=20,
        pad=24
    )

    # Подпись под графиком
    fig.text(
        0.49,
        0.055,
        f"{utc_time(alert.close_ms)} "
        f"• Entry = close сигнальной свечи "
        f"• Стоп: {STOP_LOOKBACK_BARS} свечей "
        f"• R:R {REWARD_RISK_RATIO}:1 "
        f"• Без EMA",
        color="#c0c0c0",
        ha="center",
        fontsize=10
    )

    # Сохраняем PNG в память
    out = io.BytesIO()

    fig.savefig(
        out,
        format="png",
        facecolor="#000000",
        dpi=110
    )

    return out.getvalue()


# ==========================================
# TELEGRAM: ФОТО + ТЕКСТ
# ==========================================

def telegram_send_photo(
    token: str,
    chat_id: str,
    alert: Alert
):

    png = render_chart(alert)

    if len(png) > 9_000_000:
        raise RuntimeError(
            "Generated PNG too large"
        )

    caption = alert.caption()

    if (
        len(
            caption.encode("utf-16-le")
        ) // 2 > 1024
    ):
        raise RuntimeError(
            "Telegram caption too long"
        )

    boundary = (
        "----BingXChart"
        + uuid.uuid4().hex
    )

    pieces = []

    # Поля Telegram
    for name, value in (
        ("chat_id", chat_id),
        ("caption", caption)
    ):

        pieces.append(
            (
                f"--{boundary}\r\n"
                f"Content-Disposition: "
                f'form-data; name="{name}"'
                f"\r\n\r\n"
                f"{value}\r\n"
            ).encode("utf-8")
        )

    # Поле PNG
    pieces.append(
        (
            f"--{boundary}\r\n"
            "Content-Disposition: form-data; "
            'name="photo"; '
            'filename="macd_entry_tp_sl.png"\r\n'
            "Content-Type: image/png\r\n\r\n"
        ).encode("utf-8")
    )

    pieces.extend([
        png,
        (
            f"\r\n--{boundary}--\r\n"
        ).encode("ascii")
    ])

    response = request_json(
        f"https://api.telegram.org/bot"
        f"{token}/sendPhoto",
        data=b"".join(pieces),
        headers={
            "Content-Type":
            f"multipart/form-data; "
            f"boundary={boundary}"
        }
    )

    if not response.get("ok"):
        raise RuntimeError(
            "Telegram sendPhoto failed: "
            + response.get(
                "description", ""
            )
        )


def telegram_send_text(
    token: str,
    chat_id: str,
    message: str
):

    payload = json.dumps({
        "chat_id": chat_id,
        "text": message
    }).encode("utf-8")

    result = request_json(
        f"https://api.telegram.org/bot"
        f"{token}/sendMessage",
        data=payload,
        headers={
            "Content-Type":
            "application/json"
        }
    )

    if not result.get("ok"):
        raise RuntimeError(
            "Telegram sendMessage failed"
        )


# ==========================================
# СОХРАНЕНИЕ СОСТОЯНИЯ
# ==========================================

def load_state() -> dict:

    if not STATE_PATH.exists():

        return {
            "version": 3,
            "exchange": "bingx",
            "last": {},
            "sent": {}
        }

    state = json.loads(
        STATE_PATH.read_text(
            encoding="utf-8-sig"
        )
    )

    if (
        state.get("version") != 3
        or state.get("exchange") != "bingx"
        or not isinstance(
            state.get("last"),
            dict
        )
    ):
        raise RuntimeError(
            "Invalid BingX state file"
        )

    if "sent" not in state:
        state["sent"] = {}

    if not isinstance(
        state["sent"],
        dict
    ):
        raise RuntimeError(
            "Invalid sent dictionary"
        )

    return state


def save_state(state: dict):

    STATE_PATH.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    tmp = STATE_PATH.with_suffix(".tmp")

    tmp.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            sort_keys=True
        ),
        encoding="utf-8"
    )

    tmp.replace(STATE_PATH)


# ==========================================
# ОСНОВНОЙ ПРОЦЕСС
# ==========================================

def main():

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s "
            "%(levelname)s "
            "%(message)s"
        )
    )

    token = os.getenv(
        "TELEGRAM_BOT_TOKEN", ""
    ).strip()

    chat_id = os.getenv(
        "TELEGRAM_CHAT_ID", ""
    ).strip()

    if not token or not chat_id:
        raise RuntimeError(
            "Missing Telegram secrets"
        )

    if not 1 <= WORKERS <= 12:
        raise ValueError(
            "MAX_WORKERS must be 1..12"
        )

    state = load_state()
    old_last = state["last"]

    server_ms = get_server_ms()

    symbols, total = get_liquid_symbols()

    eligible = set(symbols)

    # Сохраняем контрольные точки 15m
    new_last = {
        key: ms
        for key, ms in old_last.items()
        if (
            key.endswith("|15m")
            and key.partition("|")[0]
            in eligible
        )
    }

    alerts = []
    errors = []
    jobs = {}

    skipped = 0
    bootstrapped = 0
    young = 0

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=WORKERS
    ) as pool:

        for symbol in symbols:

            key = f"{symbol}|15m"

            previous = old_last.get(key)

            # Пропускаем обработанные свечи
            if (
                previous is not None
                and previous >= closed_boundary(
                    server_ms
                )
            ):

                skipped += 1
                continue

            future = pool.submit(
                scan_symbol,
                symbol,
                server_ms,
                previous
            )

            jobs[future] = key

        for count, future in enumerate(
            concurrent.futures.as_completed(
                jobs
            ),
            1
        ):

            key = jobs[future]

            try:

                batch, newest, status = (
                    future.result()
                )

            except Exception as exc:

                errors.append(
                    (key, str(exc))
                )

                LOG.error(
                    "BingX failed %s: %s",
                    key,
                    exc
                )

                continue

            if newest is not None:
                new_last[key] = newest

            alerts.extend(batch)

            bootstrapped += (
                status == "bootstrapped"
            )

            young += (
                status == "young"
            )

            if count % 100 == 0:

                LOG.info(
                    "Processed %d/%d symbols",
                    count,
                    len(jobs)
                )

    LOG.info(
        "BingX 15m: eligible=%d/%d "
        "requested=%d skipped=%d "
        "bootstrapped=%d young=%d "
        "errors=%d alerts=%d",
        len(symbols),
        total,
        len(jobs),
        skipped,
        bootstrapped,
        young,
        len(errors),
        len(alerts)
    )

    if (
        jobs
        and len(errors) > max(
            10,
            len(jobs) // 5
        )
    ):
        raise RuntimeError(
            f"Too many API errors: "
            f"{len(errors)}/{len(jobs)}"
        )

    # По времени и монете
    alerts.sort(
        key=lambda a: (
            a.close_ms,
            a.symbol
        )
    )

    # Сообщение первого запуска
    if not any(
        key.endswith("|15m")
        for key in old_last
    ):

        telegram_send_text(
            token,
            chat_id,
            (
                "✅ BingX MACD готов.\n"
                f"Монет с оборотом "
                f"от 1 млн USDT: "
                f"{len(symbols)}/{total}\n"
                "Таймфрейм: 15m\n"
                "Новые сигналы будут "
                "приходить с PNG-графиками, "
                "входом, тейком и стопом."
            )
        )

    # Отправляем отдельную картинку
    # для каждого сигнала
    sent = state["sent"]

    delivered = 0

    for alert in alerts:

        if alert.key in sent:
            continue

        telegram_send_photo(
            token,
            chat_id,
            alert
        )

        # Запоминаем успешно
        # отправленный сигнал
        sent[alert.key] = alert.close_ms

        save_state(state)

        delivered += 1

        time.sleep(1.1)

    # Обновляем контрольные точки
    state["last"] = new_last

    cutoff_ms = (
        server_ms
        - 30 * 24 * 3600 * 1000
    )

    state["sent"] = {
        key: value
        for key, value in sent.items()
        if value >= cutoff_ms
    }

    state["updated_utc"] = (
        dt.datetime.now(
            dt.timezone.utc
        ).isoformat()
    )

    save_state(state)

    LOG.info(
        "Saved %d checkpoints, "
        "delivered=%d images",
        len(new_last),
        delivered
    )

    if errors:
        LOG.warning(
            "Some symbols failed; "
            "previous checkpoints preserved"
        )


if __name__ == "__main__":
    main()
