
#!/usr/bin/env python3
"""BingX MACD 15m zero-line Telegram scanner."""

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


# ========================================
# НАСТРОЙКИ
# ========================================

API_URL = "https://open-api.bingx.com"

# Только 15 минут
INTERVAL = "15m"
PERIOD_MS = 15 * 60_000

# Минимальный оборот за 24 часа
MIN_TURNOVER_24H_USDT = 1_000_000.0

# MACD (12, 26, 9)
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# Минимальное соотношение MACD и гистограммы
MACD_HIST_MIN_RATIO = 1.5

# Глубина истории
KLINE_LIMIT = 1000
MIN_CANDLES = 100
WARMUP_INDEX = 75
CLOSE_GRACE_MS = 2500

# Ограничения BingX API
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

HTTP_TIMEOUT = 20

LOG = logging.getLogger("bingx-macd")


# ========================================
# ОГРАНИЧИТЕЛЬ ЗАПРОСОВ
# ========================================

class RequestLimiter:

    def __init__(self, rps: float):

        if not 0 < rps <= 10:
            raise ValueError(
                "Invalid requests per second"
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

        if deadline > now:
            time.sleep(deadline - now)

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

def request_json(
    url: str,
    *,
    bingx=False,
    post=None
):

    safe_url = (
        "Telegram sendMessage"
        if "api.telegram.org/bot" in url
        else url.split("?", 1)[0]
    )

    payload = (
        None
        if post is None
        else json.dumps(post).encode("utf-8")
    )

    headers = {
        "User-Agent": "bingx-macd-15m-telegram/1.0",
        "Accept": "application/json"
    }

    if post is not None:
        headers["Content-Type"] = "application/json"

    for attempt in range(5):

        if bingx:
            LIMITER.wait()

        req = urllib.request.Request(
            url,
            data=payload,
            headers=headers,
            method="POST" if post is not None else "GET"
        )

        try:

            with urllib.request.urlopen(
                req,
                timeout=HTTP_TIMEOUT
            ) as response:
                result = json.load(response)

            if bingx:

                code = str(
                    result.get("code", "missing")
                )

                if code != "0":

                    if (
                        code in (
                            "100410",
                            "100500",
                            "109500"
                        )
                        and attempt < 4
                    ):

                        delay = min(
                            60,
                            5 * 2 ** attempt
                        )

                        LIMITER.pause_all(delay)

                        LOG.warning(
                            "BingX code %s; retry after %ss",
                            code,
                            delay
                        )

                        continue

                    raise RuntimeError(
                        f"BingX {code}: "
                        f"{result.get('msg', '')}"
                    )

            return result

        except urllib.error.HTTPError as exc:

            if (
                exc.code in (
                    429, 500, 502, 503, 504
                )
                and attempt < 4
            ):

                delay = min(
                    60,
                    5 * 2 ** attempt
                )

                if bingx:
                    LIMITER.pause_all(delay)
                else:
                    time.sleep(delay)

                continue

            body = exc.read(150).decode(
                "utf-8",
                errors="replace"
            )

            raise RuntimeError(
                f"HTTP {exc.code} at {safe_url}: {body}"
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

            time.sleep(min(12, 2 ** attempt))

    raise RuntimeError(
        "Too many request retries"
    )


def bingx_get(
    path: str,
    params=None
):

    url = API_URL + path

    if params:
        url += "?" + urllib.parse.urlencode(params)

    return request_json(
        url,
        bingx=True
    ).get("data")


# ========================================
# ВРЕМЯ СЕРВЕРА
# ========================================

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


# ========================================
# МОНЕТЫ С ОБОРОТОМ ОТ 1 МЛН USDT
# ========================================

def get_liquid_symbols() -> tuple[list[str], int]:

    contracts = bingx_get(
        "/openApi/swap/v2/quote/contracts"
    )

    if not isinstance(contracts, list) or not contracts:
        raise RuntimeError(
            "BingX contract list missing"
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
            "No active USDT perpetual contracts"
        )

    tickers = bingx_get(
        "/openApi/swap/v2/quote/ticker"
    )

    if isinstance(tickers, dict):
        tickers = [tickers]

    if not isinstance(tickers, list) or not tickers:
        raise RuntimeError(
            "BingX ticker list missing"
        )

    liquid = set()

    for ticker in tickers:

        if not isinstance(ticker, dict):
            continue

        symbol = ticker.get("symbol")

        if symbol not in active:
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
            liquid.add(symbol)

    if not liquid:
        raise RuntimeError(
            "No contracts passed turnover filter"
        )

    LOG.info(
        "BingX turnover >= %.0f USDT: %d/%d",
        MIN_TURNOVER_24H_USDT,
        len(liquid),
        len(active)
    )

    return sorted(liquid), len(active)


# ========================================
# РАСЧЁТ MACD
# ========================================

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
):

    fast = ema(closes, MACD_FAST)
    slow = ema(closes, MACD_SLOW)

    line = [
        a - b
        for a, b in zip(fast, slow)
    ]

    signal = ema(
        line,
        MACD_SIGNAL
    )

    return line, signal


# ========================================
# ПЕРЕСЕЧЕНИЕ НУЛЕВОЙ ЛИНИИ
# ========================================

def direction_of_cross(
    before: float,
    after: float
):

    if before <= 0 < after:
        return "LONG"

    if before >= 0 > after:
        return "SHORT"

    return None


# ========================================
# НОВЫЙ ФИЛЬТР MACD / ГИСТОГРАММА
# ВЕСЬ УЧАСТОК МЕЖДУ ПЕРЕСЕЧЕНИЯМИ НУЛЯ
# ========================================

def qualifying_ratio(
    line: list[float],
    signal: list[float],
    index: int,
    direction: str
):

    """
    Находит предыдущее пересечение
    нулевой линии MACD.

    Затем проверяет ВСЕ закрытые свечи
    от предыдущего пересечения нуля
    до текущего пересечения.

    Условие:

    |MACD| >= 1.5 * |MACD - Signal|

    Достаточно одной подходящей свечи.

    Если предыдущее пересечение
    не найдено, сигнал отклоняется.
    """

    if index <= WARMUP_INDEX:
        return None

    # Ищем предыдущее пересечение нуля
    previous_cross = None

    for j in range(
        index - 1,
        WARMUP_INDEX,
        -1
    ):

        previous_direction = direction_of_cross(
            line[j - 1],
            line[j]
        )

        if previous_direction is not None:
            previous_cross = j
            break

    # Предыдущее пересечение не найдено
    if previous_cross is None:
        return None

    # До LONG линия MACD была ниже нуля
    # До SHORT линия MACD была выше нуля
    old_side = (
        -1 if direction == "LONG"
        else 1
    )

    strongest = None

    # Проверяем весь участок
    # начиная с предыдущего пересечения
    # и заканчивая свечой перед текущим

    for j in range(
        previous_cross,
        index
    ):

        # Проверяем нужную сторону нуля
        if old_side * line[j] <= 0:
            continue

        # Гистограмма MACD
        histogram = (
            line[j] - signal[j]
        )

        # Исключаем почти нулевую
        # гистограмму
        if abs(histogram) <= max(
            1e-16,
            abs(line[j]) * 1e-10
        ):
            continue

        ratio = (
            abs(line[j]) / abs(histogram)
        )

        # Условие выполнено
        if ratio >= MACD_HIST_MIN_RATIO:

            # Сохраняем максимальное
            # значение на всём участке
            if (
                strongest is None
                or ratio > strongest[0]
            ):
                strongest = (ratio, j)

    return strongest


# ========================================
# ВРЕМЯ
# ========================================

def format_utc(ms: int):

    return dt.datetime.fromtimestamp(
        ms / 1000,
        tz=dt.timezone.utc
    ).strftime("%d.%m %H:%M UTC")


def latest_closed_boundary(
    server_ms: int
):

    return (
        (server_ms - CLOSE_GRACE_MS)
        // PERIOD_MS
        * PERIOD_MS
    )


# ========================================
# СВЕЧИ BINGX 15m
# ========================================

def parse_candles(
    raw,
    server_ms: int
):

    if not isinstance(raw, list):
        raise ValueError(
            "Malformed BingX kline list"
        )

    parsed = {}

    for row in raw:

        try:

            if isinstance(row, dict):

                stamp = next(
                    (
                        row[key]
                        for key in (
                            "time",
                            "openTime",
                            "startTime"
                        )
                        if row.get(key) is not None
                    ),
                    None
                )

                start = int(stamp)
                close = float(row["close"])

            elif (
                isinstance(row, (list, tuple))
                and len(row) >= 5
            ):

                start = int(row[0])
                close = float(row[4])

            else:
                raise ValueError(
                    "Invalid candle format"
                )

        except (
            KeyError,
            TypeError,
            ValueError
        ) as exc:

            desc = (
                sorted(row.keys())
                if isinstance(row, dict)
                else type(row).__name__
            )

            raise ValueError(
                f"Malformed BingX candle: {desc}"
            ) from exc

        if start % PERIOD_MS != 0:
            raise ValueError(
                f"Misaligned 15m candle: {start}"
            )

        if (
            not math.isfinite(close)
            or close <= 0
        ):
            raise ValueError(
                "Invalid candle close"
            )

        # Только закрытые свечи
        if (
            start + PERIOD_MS
            <= server_ms - CLOSE_GRACE_MS
        ):
            parsed[start] = close

    return sorted(parsed.items())


# ========================================
# ФОРМАТ TELEGRAM-СИГНАЛА
# ========================================

@dataclass(frozen=True)
class Alert:

    symbol: str
    direction: str
    candle_close_ms: int
    old_macd: float
    new_macd: float
    ratio: float
    condition_close_ms: int

    def display(self) -> str:

        icon = (
            "🟢" if self.direction == "LONG"
            else "🔴"
        )

        tv_symbol = urllib.parse.quote(
            "BINGX:"
            + self.symbol.replace("-", "")
            + ".P",
            safe=""
        )

        tv = (
            "https://www.tradingview.com/chart/"
            f"?symbol={tv_symbol}&interval=15"
        )

        return (
            f"{icon} {self.direction} | "
            f"{self.symbol} | 15m\n"
            f"Закрытие: "
            f"{format_utc(self.candle_close_ms)}\n"
            f"MACD: {self.old_macd:.8g} "
            f"→ {self.new_macd:.8g}\n"
            f"С прошлого пересечения "
            f"|MACD|/|гистограмма|: "
            f"{self.ratio:.2f}x\n"
            f"Свеча условия: "
            f"{format_utc(self.condition_close_ms)}\n"
            f"График: {tv}"
        )


# ========================================
# СКАНИРОВАНИЕ МОНЕТЫ
# ========================================

def scan_symbol(
    symbol: str,
    server_ms: int,
    last_ms: int | None
):

    # Получаем свечи 15m
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
        close
        for _, close in candles
    ]

    line, signal = calculate_macd(closes)

    newest = (
        candles[-1][0] + PERIOD_MS
    )

    # При первом запуске только синхронизация
    if last_ms is None:
        return [], newest, "bootstrapped"

    if (
        last_ms
        < candles[WARMUP_INDEX][0] + PERIOD_MS
    ):
        LOG.warning(
            "%s: checkpoint older than available history",
            symbol
        )

    alerts = []

    for i in range(
        WARMUP_INDEX,
        len(candles)
    ):

        close_ms = (
            candles[i][0] + PERIOD_MS
        )

        # Уже обработанная свеча
        if close_ms <= last_ms:
            continue

        # Не пересекаем пропущенные свечи
        if (
            candles[i][0] - candles[i - 1][0]
            != PERIOD_MS
        ):
            continue

        direction = direction_of_cross(
            line[i - 1],
            line[i]
        )

        if not direction:
            continue

        # Проверяем весь предыдущий участок
        condition = qualifying_ratio(
            line,
            signal,
            i,
            direction
        )

        if condition is None:
            continue

        ratio, qualifying_index = condition

        alerts.append(
            Alert(
                symbol=symbol,
                direction=direction,
                candle_close_ms=close_ms,
                old_macd=line[i - 1],
                new_macd=line[i],
                ratio=ratio,
                condition_close_ms=(
                    candles[qualifying_index][0]
                    + PERIOD_MS
                )
            )
        )

    return alerts, newest, "ok"


# ========================================
# СОХРАНЕНИЕ СОСТОЯНИЯ
# ========================================

def load_state() -> dict:

    if not STATE_PATH.exists():

        LOG.info(
            "First BingX run: initialization"
        )

        return {
            "version": 3,
            "exchange": "bingx",
            "last": {}
        }

    state = json.loads(
        STATE_PATH.read_text(
            encoding="utf-8-sig"
        )
    )

    if (
        state.get("version") != 3
        or state.get("exchange") != "bingx"
        or not isinstance(state.get("last"), dict)
    ):
        raise RuntimeError(
            "Invalid BingX state"
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

    response = request_json(
        endpoint,
        post={
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


# ========================================
# ГРУППИРОВКА УВЕДОМЛЕНИЙ
# ========================================

def group_alerts(
    alerts: list[Alert]
):

    header = (
        "BingX MACD (12,26,9)\n"
        "Пересечение нулевой линии\n"
        "Таймфрейм: 15m\n"
        "Оборот: от 1 млн USDT/24ч\n"
        f"С предыдущего пересечения: "
        f"|MACD|/|гистограмма| "
        f"≥ {MACD_HIST_MIN_RATIO}x\n"
    )

    messages = []
    current = header

    for alert in alerts:

        entry = "\n" + alert.display() + "\n"

        if (
            len(current) + len(entry) > 3400
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
# ОСНОВНОЙ ПРОЦЕСС
# ========================================

def main():

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    token = os.getenv(
        "TELEGRAM_BOT_TOKEN", ""
    ).strip()

    chat_id = os.getenv(
        "TELEGRAM_CHAT_ID", ""
    ).strip()

    if not token or not chat_id:
        raise RuntimeError(
            "Configure Telegram secrets in GitHub"
        )

    if not 1 <= WORKERS <= 12:
        raise ValueError(
            "MAX_WORKERS must be between 1 and 12"
        )

    state = load_state()
    old_last = state["last"]

    server_ms = get_server_ms()

    symbols, total = get_liquid_symbols()

    eligible = set(symbols)

    # Оставляем только состояние 15m
    # для монет, прошедших фильтр оборота
    new_last = {
        key: value
        for key, value in old_last.items()
        if (
            key.endswith("|15m")
            and key.partition("|")[0] in eligible
        )
    }

    alerts = []
    errors = []

    skipped = 0
    bootstrapped = 0
    young = 0

    jobs = {}

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=WORKERS
    ) as executor:

        for symbol in symbols:

            key = f"{symbol}|15m"

            previous = old_last.get(key)

            # Пропускаем уже обработанные свечи
            if (
                previous is not None
                and previous >= latest_closed_boundary(
                    server_ms
                )
            ):
                skipped += 1
                continue

            future = executor.submit(
                scan_symbol,
                symbol,
                server_ms,
                previous
            )

            jobs[future] = key

        for i, future in enumerate(
            concurrent.futures.as_completed(jobs),
            1
        ):

            key = jobs[future]

            try:
                found, newest, status = (
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

            alerts.extend(found)

            bootstrapped += (
                status == "bootstrapped"
            )

            young += (
                status == "young"
            )

            if i % 100 == 0:
                LOG.info(
                    "Processed %d/%d",
                    i,
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

    # Не сохраняем состояние при
    # массовых ошибках запросов
    if (
        jobs
        and len(errors) > max(
            10,
            len(jobs) // 5
        )
    ):
        raise RuntimeError(
            f"Too many BingX errors: "
            f"{len(errors)}/{len(jobs)}"
        )

    alerts.sort(
        key=lambda a: (
            a.candle_close_ms,
            a.symbol
        )
    )

    messages = group_alerts(alerts)

    # Первичная синхронизация
    if not any(
        key.endswith("|15m")
        for key in old_last
    ):

        messages.insert(
            0,
            (
                "✅ BingX MACD-сканер активен.\n"
                f"Активных USDT-фьючерсов: {total}\n"
                f"Оборот от 1 млн USDT: "
                f"{len(symbols)}\n"
                "Таймфрейм: 15m\n"
                f"Условие: ≥ "
                f"{MACD_HIST_MIN_RATIO}x\n"
                "Проверка всего участка "
                "между пересечениями нуля."
            )
        )

    # Отправка Telegram-сигналов
    for i, message in enumerate(messages):

        if i:
            time.sleep(1.1)

        telegram_send(
            token,
            chat_id,
            message
        )

    # Сохранение контрольных точек
    state["last"] = new_last

    state["updated_utc"] = (
        dt.datetime.now(
            dt.timezone.utc
        ).isoformat()
    )

    save_state(state)

    LOG.info(
        "Saved %d checkpoints. Telegram messages=%d",
        len(new_last),
        len(messages)
    )

    if errors:
        LOG.warning(
            "Some symbols failed; previous checkpoints retained"
        )


if __name__ == "__main__":
    main()
