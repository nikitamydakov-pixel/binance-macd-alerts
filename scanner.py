
#!/usr/bin/env python3
"""Bybit V5 USDT perpetual MACD zero-line Telegram scanner."""

from __future__ import annotations

import concurrent.futures
import datetime as dt
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
from dataclasses import dataclass


# ========================================
# НАСТРОЙКИ СКАНЕРА
# ========================================

API_URL = "https://api.bybit.com"

# Только 15-минутный таймфрейм
INTERVAL_MINUTES = {"15m": 15}

# Минимальный торговый оборот за 24 часа
MIN_TURNOVER_24H_USDT = 1_000_000.0

# Параметры MACD: 12, 26, 9
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

KLINE_LIMIT = 240
MIN_CANDLES = 100
WARMUP_INDEX = 75
CLOSE_GRACE_MS = 2500

RPS = float(
    os.environ.get("BYBIT_REQUESTS_PER_SECOND", "7")
)

WORKERS = int(
    os.environ.get("MAX_WORKERS", "8")
)

STATE_PATH = Path(
    os.environ.get("STATE_PATH", ".state/state.json")
)

HTTP_TIMEOUT = 18

LOG = logging.getLogger("bybit-macd")


# ========================================
# ОГРАНИЧЕНИЕ ЗАПРОСОВ К BYBIT
# ========================================

class RequestLimiter:

    def __init__(self, rps: float):
        if not 0 < rps <= 10:
            raise ValueError(
                "BYBIT_REQUESTS_PER_SECOND must be in (0, 10]"
            )

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


# ========================================
# HTTP-ЗАПРОСЫ
# ========================================

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
                        delay = min(
                            60,
                            5 * (2 ** attempt)
                        )

                        LIMITER.pause_all(delay)

                        LOG.warning(
                            "Bybit retCode %s; retry in %ss",
                            code,
                            delay
                        )

                        continue

                    if code == 10009:
                        raise RuntimeError(
                            "Bybit API restricted in runner region"
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
                    "Bybit API returned HTTP 403. "
                    "IP/region may be restricted."
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
                    delay = min(
                        60,
                        5 * (2 ** attempt)
                    )

                delay = max(
                    5.0,
                    min(90.0, delay)
                )

                if bybit:
                    LIMITER.pause_all(delay)
                else:
                    time.sleep(delay)

                LOG.warning(
                    "HTTP %d; retry in %.1fs",
                    status,
                    delay
                )

                continue

            raise RuntimeError(
                f"HTTP {status} for {safe_url}: {body}"
            ) from exc

        except (
            urllib.error.URLError,
            TimeoutError,
            json.JSONDecodeError
        ) as exc:

            if attempt == 4:
                raise RuntimeError(
                    f"Network error for {safe_url}: {exc}"
                ) from exc

            time.sleep(
                min(12, 2 ** attempt)
            )

    raise RuntimeError(
        f"Too many retries: {safe_url}"
    )


def bybit_get(
    path: str,
    params: dict | None = None
) -> dict:

    url = API_URL + path

    if params:
        url += "?" + urllib.parse.urlencode(params)

    return _request_json(
        url,
        bybit=True
    )


# ========================================
# ВРЕМЯ BYBIT
# ========================================

def get_server_ms() -> int:

    payload = bybit_get(
        "/v5/market/time"
    )

    if payload.get("time") is not None:
        return int(payload["time"])

    return (
        int(payload["result"]["timeSecond"])
        * 1000
    )


# ========================================
# СПИСОК АКТИВНЫХ USDT-ФЬЮЧЕРСОВ
# ========================================

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
                "Invalid Bybit instrument list"
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

        next_cursor = (
            result.get("nextPageCursor") or ""
        )

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
            "No active USDT perpetual contracts found"
        )

    return sorted(symbols)


# ========================================
# ФИЛЬТР ОБОРОТА ОТ 1 МЛН USDT
# ========================================

def get_liquid_symbols() -> tuple[list[str], int]:

    active = set(get_symbols())

    data = bybit_get(
        "/v5/market/tickers",
        {"category": "linear"}
    )["result"]

    tickers = data.get("list")

    if not isinstance(tickers, list) or not tickers:
        raise RuntimeError(
            "Invalid ticker list. "
            "Cannot apply turnover filter."
        )

    liquid = set()
    tickers_seen = set()

    for ticker in tickers:

        symbol = ticker.get("symbol")

        if symbol not in active:
            continue

        tickers_seen.add(symbol)

        try:
            turnover = float(
                ticker["turnover24h"]
            )

        except (KeyError, ValueError, TypeError):
            continue

        if (
            math.isfinite(turnover)
            and turnover >= MIN_TURNOVER_24H_USDT
        ):
            liquid.add(symbol)

    missing = active - tickers_seen

    if missing:
        LOG.warning(
            "No 24h ticker for %d active contracts",
            len(missing)
        )

    LOG.info(
        "24h turnover >= %.0f USDT: %d/%d contracts",
        MIN_TURNOVER_24H_USDT,
        len(liquid),
        len(active)
    )

    return sorted(liquid), len(active)


# ========================================
# РАСЧЁТ MACD (12, 26, 9)
# ========================================

def ema(
    values: list[float],
    length: int
) -> list[float]:

    if not values:
        return []

    alpha = 2 / (length + 1)

    out = [values[0]]

    for value in values[1:]:
        out.append(
            alpha * value
            + (1 - alpha) * out[-1]
        )

    return out


def macd(
    values: list[float]
) -> tuple[list[float], list[float]]:

    fast = ema(values, MACD_FAST)
    slow = ema(values, MACD_SLOW)

    line = [
        a - b
        for a, b in zip(fast, slow)
    ]

    signal = ema(
        line,
        MACD_SIGNAL
    )

    return line, signal


def crossing(
    before: float,
    after: float
) -> str | None:

    if before <= 0 < after:
        return "LONG"

    if before >= 0 > after:
        return "SHORT"

    return None


# ========================================
# ВРЕМЯ ЗАКРЫТИЯ СВЕЧЕЙ
# ========================================

def format_utc(
    milliseconds: int
) -> str:

    return dt.datetime.fromtimestamp(
        milliseconds / 1000,
        tz=dt.timezone.utc
    ).strftime("%d.%m %H:%M UTC")


def latest_closed_boundary(
    server_ms: int,
    interval: str
) -> int:

    period = (
        INTERVAL_MINUTES[interval] * 60_000
    )

    return (
        (server_ms - CLOSE_GRACE_MS)
        // period
    ) * period


# ========================================
# СОХРАНЕНИЕ СОСТОЯНИЯ
# ========================================

def load_state() -> dict:

    if not STATE_PATH.exists():

        LOG.info(
            "First run: initializing checkpoints"
        )

        return {
            "version": 2,
            "exchange": "bybit",
            "last": {}
        }

    state = json.loads(
        STATE_PATH.read_text(
            encoding="utf-8"
        )
    )

    if (
        state.get("version") != 2
        or state.get("exchange") != "bybit"
        or not isinstance(state.get("last"), dict)
    ):
        raise RuntimeError(
            "Invalid Bybit state schema"
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


# ========================================
# ОФОРМЛЕНИЕ TELEGRAM-СИГНАЛА
# ========================================

@dataclass(frozen=True)
class Alert:

    symbol: str
    interval: str
    direction: str
    candle_close_ms: int
    old_macd: float
    new_macd: float

    def display(self) -> str:

        icon = (
            "🟢"
            if self.direction == "LONG"
            else "🔴"
        )

        chart_symbol = urllib.parse.quote(
            "BYBIT:" + self.symbol + ".P",
            safe=""
        )

        timeframe = INTERVAL_MINUTES[
            self.interval
        ]

        chart = (
            "https://www.tradingview.com/chart/"
            f"?symbol={chart_symbol}"
            f"&interval={timeframe}"
        )

        return (
            f"{icon} {self.direction} | "
            f"{self.symbol} | {self.interval}\n"
            f"Закрытие: "
            f"{format_utc(self.candle_close_ms)}\n"
            f"MACD: {self.old_macd:.9g} "
            f"→ {self.new_macd:.9g}\n"
            f"{chart}"
        )


# ========================================
# СКАНИРОВАНИЕ ОДНОЙ МОНЕТЫ
# ========================================

def scan_symbol(
    symbol: str,
    interval: str,
    server_ms: int,
    last_ms: int | None
):

    period = (
        INTERVAL_MINUTES[interval] * 60_000
    )

    result = bybit_get(
        "/v5/market/kline",
        {
            "category": "linear",
            "symbol": symbol,
            "interval": str(
                INTERVAL_MINUTES[interval]
            ),
            "limit": KLINE_LIMIT
        }
    )["result"]

    raw = result["list"]

    if not isinstance(raw, list):
        raise RuntimeError(
            f"Invalid candles: {symbol}"
        )

    candles = sorted(
        (
            (int(row[0]), float(row[4]))
            for row in raw
            if (
                int(row[0]) + period
                <= server_ms - CLOSE_GRACE_MS
            )
        ),
        key=lambda candle: candle[0]
    )

    if len(candles) < MIN_CANDLES:
        return [], None, "young"

    if any(
        not math.isfinite(close) or close <= 0
        for _, close in candles
    ):
        raise ValueError(
            f"Invalid candle close: {symbol}"
        )

    if any(
        candles[i][0] <= candles[i - 1][0]
        for i in range(1, len(candles))
    ):
        raise ValueError(
            f"Repeated candle: {symbol}"
        )

    closes = [
        close
        for _, close in candles
    ]

    line, _ = macd(closes)

    newest_close = (
        candles[-1][0] + period
    )

    if last_ms is None:
        return (
            [],
            newest_close,
            "bootstrapped"
        )

    earliest = (
        candles[WARMUP_INDEX][0] + period
    )

    if last_ms < earliest:
        LOG.warning(
            "%s %s: old checkpoint",
            symbol,
            interval
        )

    alerts = []

    for index in range(
        WARMUP_INDEX,
        len(candles)
    ):

        close_ms = (
            candles[index][0] + period
        )

        if close_ms <= last_ms:
            continue

        direction = crossing(
            line[index - 1],
            line[index]
        )

        if direction:
            alerts.append(
                Alert(
                    symbol,
                    interval,
                    direction,
                    close_ms,
                    line[index - 1],
                    line[index]
                )
            )

    return alerts, newest_close, "ok"


# ========================================
# ГРУППИРОВКА УВЕДОМЛЕНИЙ
# ========================================

def group_messages(
    alerts: list[Alert]
) -> list[str]:

    header = (
        "Bybit MACD (12,26,9)\n"
        "Пересечение нуля на закрытой свече\n"
        "Фильтр: оборот от 1 млн USDT/24ч\n"
    )

    messages = []
    current = header

    for alert in alerts:

        entry = (
            "\n" + alert.display() + "\n"
        )

        if (
            len(current) + len(entry) > 3500
            and current != header
        ):
            messages.append(
                current.rstrip()
            )

            current = header

        current += entry

    if current != header:
        messages.append(
            current.rstrip()
        )

    return messages


# ========================================
# TELEGRAM API
# ========================================

def telegram_send(
    token: str,
    chat_id: str,
    message: str
):

    endpoint = (
        f"https://api.telegram.org/bot"
        f"{token}/sendMessage"
    )

    data = {
        "chat_id": chat_id,
        "text": message,
        "disable_web_page_preview": True
    }

    response = _request_json(
        endpoint,
        method="POST",
        payload=data
    )

    if not response.get("ok"):
        raise RuntimeError(
            "Telegram error: "
            + response.get(
                "description",
                "unknown"
            )
        )


# ========================================
# ОСНОВНОЙ ПРОЦЕСС
# ========================================

def main():

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s "
            "%(levelname)s "
            "%(message)s"
        )
    )

    token = os.environ.get(
        "TELEGRAM_BOT_TOKEN",
        ""
    ).strip()

    chat_id = os.environ.get(
        "TELEGRAM_CHAT_ID",
        ""
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

    # Получаем только ликвидные монеты
    symbols, active_count = get_liquid_symbols()

    LOG.info(
        "Bybit: %d/%d eligible USDT perpetuals",
        len(symbols),
        active_count
    )

    # Удаляем состояние монет,
    # не прошедших фильтр оборота
    eligible = set(symbols)

    new_last = {
        key: value
        for key, value in old_last.items()
        if key.partition("|")[0] in eligible
    }

    alerts: list[Alert] = []

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

                key = (
                    f"{symbol}|{interval}"
                )

                last_ms = old_last.get(key)

                # Пропускаем уже проверенные свечи
                if (
                    last_ms is not None
                    and last_ms >= latest_closed_boundary(
                        server_ms,
                        interval
                    )
                ):
                    skipped += 1
                    continue

                fut = executor.submit(
                    scan_symbol,
                    symbol,
                    interval,
                    server_ms,
                    last_ms
                )

                tasks[fut] = key

        for index, fut in enumerate(
            concurrent.futures.as_completed(tasks),
            start=1
        ):

            key = tasks[fut]

            try:
                batch, newest_ms, status = (
                    fut.result()
                )

            except Exception as exc:

                errors.append(
                    (key, str(exc))
                )

                LOG.error(
                    "Failed %s: %s",
                    key,
                    exc
                )

                continue

            if newest_ms is not None:
                new_last[key] = newest_ms

            alerts.extend(batch)

            young += (
                status == "young"
            )

            bootstrapped += (
                status == "bootstrapped"
            )

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
        and len(errors) > max(
            10,
            count // 5
        )
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
                f"Оборот от 1 млн USDT/24ч: "
                f"{len(symbols)} из {active_count}\n"
                "Таймфрейм: 15m\n"
                "MACD: 12,26,9\n"
                "Первый запуск: синхронизация."
                " Сигналы начнутся со следующих"
                " закрытых свечей."
            )
        )

    # Отправляем Telegram-сигналы
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
        "Saved %d checkpoints. "
        "Telegram messages: %d",
        len(new_last),
        len(messages)
    )

    if errors:
        LOG.warning(
            "Failed pairs kept previous checkpoints"
        )


if __name__ == "__main__":
    main()
