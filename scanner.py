
#!/usr/bin/env python3
"""Bybit MACD zero-line Telegram scanner with ratio filter."""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import json
import logging
import math
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from dataclasses import dataclass
from pathlib import Path


# ==========================================
# НАСТРОЙКИ
# ==========================================

API_URL = "https://api.bybit.com"

# Только 15-минутный таймфрейм
INTERVAL_MINUTES = {"15m": 15}

# Минимальный оборот торгов за последние 24 часа
MIN_TURNOVER_24H_USDT = 1_000_000.0

# Настройки MACD (12, 26, 9)
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# Новый фильтр: MACD / гистограмма >= 1.5
MACD_HIST_MIN_RATIO = 1.5

# Проверять предыдущие 12 закрытых свечей
PRE_CROSS_LOOKBACK_BARS = 12

KLINE_LIMIT = 240
MIN_CANDLES = 100
WARMUP_INDEX = 75
CLOSE_GRACE_MS = 2500

RPS = float(os.environ.get(
    "BYBIT_REQUESTS_PER_SECOND", "7"
))

WORKERS = int(os.environ.get(
    "MAX_WORKERS", "8"
))

STATE_PATH = Path(os.environ.get(
    "STATE_PATH", ".state/state.json"
))

HTTP_TIMEOUT = 18

LOG = logging.getLogger("bybit-macd")


# ==========================================
# ОГРАНИЧИТЕЛЬ ЗАПРОСОВ
# ==========================================

class RequestLimiter:

    def __init__(self, rps: float):

        if not 0 < rps <= 10:
            raise ValueError("Invalid requests per second")

        self.delay = 1.0 / rps
        self.lock = threading.Lock()
        self.next_slot = 0.0
        self.blocked_until = 0.0

    def wait(self):

        with self.lock:
            now = time.monotonic()

            deadline = max(
                now,
                self.next_slot,
                self.blocked_until
            )

            self.next_slot = deadline + self.delay

        pause = deadline - now

        if pause > 0:
            time.sleep(pause)

    def pause_all(self, seconds: float):

        with self.lock:
            self.blocked_until = max(
                self.blocked_until,
                time.monotonic() + seconds
            )
            self.next_slot = max(
                self.next_slot,
                self.blocked_until
            )


LIMITER = RequestLimiter(RPS)


# ==========================================
# HTTP API
# ==========================================

def _request_json(
    url: str,
    *,
    method: str = "GET",
    payload=None,
    bybit=False
) -> dict:

    data = (
        None if payload is None
        else json.dumps(payload).encode("utf-8")
    )

    headers = {
        "Accept": "application/json",
        "User-Agent": "bybit-macd-zero-telegram/1.0"
    }

    if data is not None:
        headers["Content-Type"] = "application/json"

    safe_url = (
        "Telegram sendMessage"
        if "api.telegram.org/bot" in url
        else url.split("?", 1)[0]
    )

    for attempt in range(5):

        if bybit:
            LIMITER.wait()

        req = urllib.request.Request(
            url,
            data=data,
            headers=headers,
            method=method
        )

        try:

            with urllib.request.urlopen(
                req,
                timeout=HTTP_TIMEOUT
            ) as response:
                result = json.load(response)

            if bybit:

                code = result.get("retCode")

                if code != 0:

                    if (
                        code in (10006, 10000, 10016, 429)
                        and attempt < 4
                    ):

                        delay = min(60, 5 * (2 ** attempt))
                        LIMITER.pause_all(delay)

                        LOG.warning(
                            "Bybit retCode %s; retry in %ss",
                            code,
                            delay
                        )

                        continue

                    if code == 10009:
                        raise RuntimeError(
                            "Bybit API restricted in this region"
                        )

                    raise RuntimeError(
                        f"Bybit retCode {code}: "
                        f"{result.get('retMsg', '')}"
                    )

            return result

        except urllib.error.HTTPError as exc:

            status = exc.code

            body = exc.read(250).decode(
                "utf-8",
                errors="replace"
            )

            if bybit and status == 403:
                raise RuntimeError(
                    "Bybit API HTTP 403: "
                    "IP or region may be restricted"
                ) from exc

            if (
                status in (429, 500, 502, 503, 504)
                and attempt < 4
            ):

                try:
                    delay = float(
                        exc.headers.get("Retry-After", "")
                    )
                except (ValueError, TypeError):
                    delay = min(60, 5 * (2 ** attempt))

                delay = max(5.0, min(90.0, delay))

                if bybit:
                    LIMITER.pause_all(delay)
                else:
                    time.sleep(delay)

                continue

            raise RuntimeError(
                f"HTTP {status}: {safe_url}: {body}"
            ) from exc

        except (
            urllib.error.URLError,
            TimeoutError,
            json.JSONDecodeError
        ) as exc:

            if attempt == 4:
                raise RuntimeError(
                    f"Network error: {safe_url}: {exc}"
                ) from exc

            time.sleep(min(12, 2 ** attempt))

    raise RuntimeError("Too many request retries")


def bybit_get(path: str, params=None) -> dict:

    url = API_URL + path

    if params:
        url += "?" + urllib.parse.urlencode(params)

    return _request_json(url, bybit=True)


# ==========================================
# ВРЕМЯ BYBIT
# ==========================================

def get_server_ms() -> int:

    payload = bybit_get("/v5/market/time")

    if payload.get("time") is not None:
        return int(payload["time"])

    return int(
        payload["result"]["timeSecond"]
    ) * 1000


# ==========================================
# АКТИВНЫЕ USDT-ФЬЮЧЕРСЫ
# ==========================================

def get_symbols() -> list[str]:

    symbols = set()
    cursor = ""
    seen_cursors = set()

    while True:

        params = {
            "category": "linear",
            "status": "Trading",
            "limit": 1000
        }

        if cursor:
            params["cursor"] = cursor

        result = bybit_get(
            "/v5/market/instruments-info",
            params
        )["result"]

        instruments = result["list"]

        if not isinstance(instruments, list):
            raise RuntimeError(
                "Invalid instrument list"
            )

        for item in instruments:

            if (
                item.get("status") == "Trading"
                and item.get("contractType") == "LinearPerpetual"
                and item.get("quoteCoin") == "USDT"
                and item.get("settleCoin") == "USDT"
                and not item.get("isPreListing", False)
            ):
                symbols.add(item["symbol"])

        next_cursor = result.get("nextPageCursor") or ""

        if not next_cursor:
            break

        if next_cursor in seen_cursors:
            raise RuntimeError(
                "Repeated pagination cursor"
            )

        seen_cursors.add(next_cursor)
        cursor = next_cursor

    if not symbols:
        raise RuntimeError(
            "No active USDT perpetual contracts"
        )

    return sorted(symbols)


# ==========================================
# ФИЛЬТР ОБОРОТА 1 МЛН USDT
# ==========================================

def get_liquid_symbols() -> tuple[list[str], int]:

    active = set(get_symbols())

    result = bybit_get(
        "/v5/market/tickers",
        {"category": "linear"}
    )["result"]

    tickers = result.get("list")

    if not isinstance(tickers, list) or not tickers:
        raise RuntimeError("Invalid Bybit tickers")

    liquid = set()
    tickers_seen = set()

    for ticker in tickers:

        symbol = ticker.get("symbol")

        if symbol not in active:
            continue

        tickers_seen.add(symbol)

        try:
            turnover = float(ticker["turnover24h"])
        except (KeyError, TypeError, ValueError):
            continue

        if (
            math.isfinite(turnover)
            and turnover >= MIN_TURNOVER_24H_USDT
        ):
            liquid.add(symbol)

    missing = active - tickers_seen

    if missing:
        LOG.warning(
            "Missing tickers: %d",
            len(missing)
        )

    LOG.info(
        "Turnover >= %.0f USDT: %d/%d symbols",
        MIN_TURNOVER_24H_USDT,
        len(liquid),
        len(active)
    )

    return sorted(liquid), len(active)


# ==========================================
# MACD (12,26,9)
# ==========================================

def ema(values: list[float], length: int):

    if not values:
        return []

    alpha = 2 / (length + 1)
    output = [values[0]]

    for value in values[1:]:

        output.append(
            alpha * value
            + (1 - alpha) * output[-1]
        )

    return output


def macd(values: list[float]):

    fast = ema(values, MACD_FAST)
    slow = ema(values, MACD_SLOW)

    line = [
        fast_value - slow_value
        for fast_value, slow_value in zip(fast, slow)
    ]

    signal = ema(line, MACD_SIGNAL)

    return line, signal


# ==========================================
# ПЕРЕСЕЧЕНИЕ НУЛЕВОЙ ЛИНИИ
# ==========================================

def crossing(before: float, after: float):

    if before <= 0 < after:
        return "LONG"

    if before >= 0 > after:
        return "SHORT"

    return None


# ==========================================
# НОВЫЙ ФИЛЬТР MACD / ГИСТОГРАММА
# ==========================================

def prior_macd_histogram_ratio(
    line: list[float],
    signal: list[float],
    cross_index: int,
    direction: str
):

    # LONG: до пересечения MACD ниже нуля
    # SHORT: до пересечения MACD выше нуля

    old_side = -1 if direction == "LONG" else 1

    first_index = max(
        0,
        cross_index - PRE_CROSS_LOOKBACK_BARS
    )

    # Проверяем только предыдущие свечи,
    # не включая свечу пересечения

    for j in range(
        cross_index - 1,
        first_index - 1,
        -1
    ):

        # Останавливаемся, если достигли
        # предыдущего пересечения нуля

        if old_side * line[j] <= 0:
            break

        # Гистограмма = MACD - Signal
        histogram = line[j] - signal[j]

        # Не используем практически нулевую
        # гистограмму из-за нестабильного деления

        if abs(histogram) <= max(
            1e-16,
            abs(line[j]) * 1e-10
        ):
            continue

        ratio = abs(line[j]) / abs(histogram)

        if ratio >= MACD_HIST_MIN_RATIO:
            return ratio, j

    return None


# ==========================================
# ВРЕМЯ СВЕЧЕЙ
# ==========================================

def format_utc(milliseconds: int):

    return dt.datetime.fromtimestamp(
        milliseconds / 1000,
        tz=dt.timezone.utc
    ).strftime("%d.%m %H:%M UTC")


def latest_closed_boundary(
    server_ms: int,
    interval: str
):

    period = INTERVAL_MINUTES[interval] * 60_000

    return (
        (server_ms - CLOSE_GRACE_MS) // period
    ) * period


# ==========================================
# СОСТОЯНИЕ ДЛЯ ЗАЩИТЫ ОТ ДУБЛЕЙ
# ==========================================

def load_state():

    if not STATE_PATH.exists():

        LOG.info("Initializing Bybit checkpoints")

        return {
            "version": 2,
            "exchange": "bybit",
            "last": {}
        }

    state = json.loads(
        STATE_PATH.read_text(encoding="utf-8")
    )

    if (
        state.get("version") != 2
        or state.get("exchange") != "bybit"
        or not isinstance(state.get("last"), dict)
    ):
        raise RuntimeError("Invalid Bybit state schema")

    return state


def save_state(state):

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
# TELEGRAM-СИГНАЛ
# ==========================================

@dataclass(frozen=True)
class Alert:

    symbol: str
    interval: str
    direction: str
    candle_close_ms: int
    old_macd: float
    new_macd: float
    qualifying_ratio: float
    qualifying_bar_close_ms: int

    def display(self):

        icon = (
            "🟢" if self.direction == "LONG"
            else "🔴"
        )

        chart_symbol = urllib.parse.quote(
            "BYBIT:" + self.symbol + ".P",
            safe=""
        )

        timeframe = INTERVAL_MINUTES[self.interval]

        chart = (
            "https://www.tradingview.com/chart/"
            f"?symbol={chart_symbol}"
            f"&interval={timeframe}"
        )

        return (
            f"{icon} {self.direction} | "
            f"{self.symbol} | {self.interval}\n"
            f"Закрытие: {format_utc(self.candle_close_ms)}\n"
            f"MACD: {self.old_macd:.9g} "
            f"→ {self.new_macd:.9g}\n"
            f"До пересечения: "
            f"|MACD| / |гистограмма| = "
            f"{self.qualifying_ratio:.2f}x\n"
            f"Свеча условия: "
            f"{format_utc(self.qualifying_bar_close_ms)}\n"
            f"{chart}"
        )


# ==========================================
# СКАНИРОВАНИЕ МОНЕТЫ
# ==========================================

def scan_symbol(
    symbol: str,
    interval: str,
    server_ms: int,
    last_ms: int | None
):

    period = INTERVAL_MINUTES[interval] * 60_000

    result = bybit_get(
        "/v5/market/kline",
        {
            "category": "linear",
            "symbol": symbol,
            "interval": str(INTERVAL_MINUTES[interval]),
            "limit": KLINE_LIMIT
        }
    )["result"]

    raw = result["list"]

    if not isinstance(raw, list):
        raise RuntimeError("Invalid kline response")

    # Используем только закрытые свечи
    candles = sorted(
        (
            (int(row[0]), float(row[4]))
            for row in raw
            if int(row[0]) + period
            <= server_ms - CLOSE_GRACE_MS
        ),
        key=lambda candle: candle[0]
    )

    if len(candles) < MIN_CANDLES:
        return [], None, "young"

    if any(
        not math.isfinite(close) or close <= 0
        for _, close in candles
    ):
        raise ValueError("Invalid candle close")

    if any(
        candles[i][0] <= candles[i - 1][0]
        for i in range(1, len(candles))
    ):
        raise ValueError("Repeated candle timestamp")

    closes = [c for _, c in candles]

    line, signal = macd(closes)

    newest_close = candles[-1][0] + period

    if last_ms is None:
        return [], newest_close, "bootstrapped"

    alerts = []

    for index in range(
        WARMUP_INDEX,
        len(candles)
    ):

        close_ms = candles[index][0] + period

        if close_ms <= last_ms:
            continue

        direction = crossing(
            line[index - 1],
            line[index]
        )

        if not direction:
            continue

        # Проверка условия 1.5x
        qualification = prior_macd_histogram_ratio(
            line,
            signal,
            index,
            direction
        )

        if qualification is None:
            continue

        ratio, ratio_index = qualification

        alerts.append(
            Alert(
                symbol=symbol,
                interval=interval,
                direction=direction,
                candle_close_ms=close_ms,
                old_macd=line[index - 1],
                new_macd=line[index],
                qualifying_ratio=ratio,
                qualifying_bar_close_ms=(
                    candles[ratio_index][0] + period
                )
            )
        )

    return alerts, newest_close, "ok"


# ==========================================
# ГРУППИРОВКА СИГНАЛОВ
# ==========================================

def group_messages(alerts):

    header = (
        "Bybit MACD (12,26,9)\n"
        "Пересечение нулевого уровня\n"
        f"Условие: MACD / гистограмма >= "
        f"{MACD_HIST_MIN_RATIO}x\n"
        f"Проверка: {PRE_CROSS_LOOKBACK_BARS} "
        "предыдущих свечей\n"
    )

    messages = []
    current = header

    for alert in alerts:

        entry = "\n" + alert.display() + "\n"

        if (
            len(current) + len(entry) > 3500
            and current != header
        ):
            messages.append(current.rstrip())
            current = header

        current += entry

    if current != header:
        messages.append(current.rstrip())

    return messages


# ==========================================
# TELEGRAM API
# ==========================================

def telegram_send(token, chat_id, message):

    endpoint = (
        f"https://api.telegram.org/bot"
        f"{token}/sendMessage"
    )

    response = _request_json(
        endpoint,
        method="POST",
        payload={
            "chat_id": chat_id,
            "text": message,
            "disable_web_page_preview": True
        }
    )

    if not response.get("ok"):
        raise RuntimeError(
            f"Telegram error: "
            f"{response.get('description', 'unknown')}"
        )


# ==========================================
# ОСНОВНОЙ ПРОЦЕСС
# ==========================================

def main():

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    token = os.environ.get(
        "TELEGRAM_BOT_TOKEN", ""
    ).strip()

    chat_id = os.environ.get(
        "TELEGRAM_CHAT_ID", ""
    ).strip()

    if not token or not chat_id:
        raise RuntimeError(
            "Configure Telegram secrets in GitHub"
        )

    if not 1 <= WORKERS <= 12:
        raise ValueError(
            "MAX_WORKERS must be in [1,12]"
        )

    state = load_state()
    old_last = state["last"]

    server_ms = get_server_ms()

    # Только монеты с оборотом >= 1 млн USDT
    symbols, active_count = get_liquid_symbols()

    LOG.info(
        "Bybit: %d/%d eligible USDT perpetuals",
        len(symbols),
        active_count
    )

    eligible = set(symbols)

    # Исключаем из состояния монеты,
    # больше не проходящие фильтр ликвидности

    new_last = {
        key: value
        for key, value in old_last.items()
        if key.partition("|")[0] in eligible
    }

    alerts = []
    errors = []

    young = 0
    bootstrapped = 0
    skipped = 0

    tasks = {}

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=WORKERS
    ) as executor:

        for symbol in symbols:

            for interval in INTERVAL_MINUTES:

                key = f"{symbol}|{interval}"

                last_ms = old_last.get(key)

                if (
                    last_ms is not None
                    and last_ms >= latest_closed_boundary(
                        server_ms, interval
                    )
                ):
                    skipped += 1
                    continue

                future = executor.submit(
                    scan_symbol,
                    symbol,
                    interval,
                    server_ms,
                    last_ms
                )

                tasks[future] = key

        for index, future in enumerate(
            concurrent.futures.as_completed(tasks),
            start=1
        ):

            key = tasks[future]

            try:
                batch, newest_ms, status = future.result()

            except Exception as exc:

                errors.append((key, str(exc)))

                LOG.error(
                    "Failed %s: %s",
                    key,
                    exc
                )

                continue

            if newest_ms is not None:
                new_last[key] = newest_ms

            alerts.extend(batch)

            young += status == "young"
            bootstrapped += status == "bootstrapped"

            if index % 200 == 0:
                LOG.info(
                    "Processed %d/%d",
                    index,
                    len(tasks)
                )

    count = len(tasks)

    LOG.info(
        "Requested: %d | Skipped: %d | "
        "Errors: %d | Young: %d | "
        "Baseline: %d | Alerts: %d",
        count,
        skipped,
        len(errors),
        young,
        bootstrapped,
        len(alerts)
    )

    if (
        count
        and len(errors) > max(10, count // 5)
    ):
        raise RuntimeError(
            f"Too many failed requests: "
            f"{len(errors)}/{count}"
        )

    alerts.sort(
        key=lambda x: (
            x.candle_close_ms,
            x.interval,
            x.symbol
        )
    )

    messages = group_messages(alerts)

    if not old_last:

        messages.insert(
            0,
            (
                "✅ Bybit MACD-сканер запущен.\n"
                f"Оборот >= 1 млн USDT: "
                f"{len(symbols)}/{active_count}\n"
                "Таймфрейм: 15m\n"
                "MACD: 12,26,9\n"
                f"MACD/гистограмма >= "
                f"{MACD_HIST_MIN_RATIO}x\n"
                "Первый запуск: синхронизация."
            )
        )

    # Отправляем сигналы в Telegram
    for i, message in enumerate(messages):

        if i:
            time.sleep(1.2)

        telegram_send(
            token,
            chat_id,
            message
        )

    # Сохраняем обработанные свечи
    state["last"] = new_last

    state["updated_utc"] = (
        dt.datetime.now(
            dt.timezone.utc
        ).isoformat()
    )

    save_state(state)

    LOG.info(
        "Saved %d checkpoints. Telegram messages: %d",
        len(new_last),
        len(messages)
    )

    if errors:
        LOG.warning(
            "Failed pairs retained previous checkpoints"
        )


if __name__ == "__main__":
    main()
