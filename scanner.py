
#!/usr/bin/env python3
"""BingX perpetual MACD zero-cross alerts, 15m.

Signal: MACD(12,26,9) crosses zero, provided that
max(abs(MACD)) >= 1.5 * max(abs(MACD - signal))
on closed candles between the previous and current
zero crossings.
"""

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

INTERVAL = "15m"
PERIOD_MS = 15 * 60_000

MIN_TURNOVER_24H_USDT = 1_000_000.0

MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# Минимальное отношение экстремумов
EXTREMA_MIN_RATIO = 1.5

KLINE_LIMIT = 1000
MIN_CANDLES = 100
WARMUP_INDEX = 75
CLOSE_GRACE_MS = 2500

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
# ОГРАНИЧЕНИЕ ЗАПРОСОВ
# ========================================

class RateLimiter:

    def __init__(self, rps: float):

        if not 0 < rps <= 10:
            raise ValueError(
                "BINGX_REQUESTS_PER_SECOND "
                "must be between 0 and 10"
            )

        self.delay = 1 / rps
        self.lock = threading.Lock()
        self.next_at = 0.0
        self.blocked_until = 0.0

    def wait(self):

        with self.lock:
            now = time.monotonic()
            deadline = max(
                now,
                self.next_at,
                self.blocked_until
            )
            self.next_at = deadline + self.delay

        if deadline > now:
            time.sleep(deadline - now)

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


# ========================================
# HTTP И BINGX API
# ========================================

def request_json(
    url: str,
    *,
    bingx=False,
    payload=None
) -> dict:

    body = (
        None
        if payload is None
        else json.dumps(payload).encode("utf-8")
    )

    headers = {
        "Accept": "application/json",
        "User-Agent": "bingx-macd-extrema/1.0"
    }

    if body is not None:
        headers["Content-Type"] = "application/json"

    safe_url = (
        "Telegram sendMessage"
        if "api.telegram.org/bot" in url
        else url.split("?", 1)[0]
    )

    for attempt in range(5):

        if bingx:
            LIMITER.wait()

        req = urllib.request.Request(
            url,
            data=body,
            headers=headers,
            method=(
                "POST"
                if body is not None
                else "GET"
            )
        )

        try:
            with urllib.request.urlopen(
                req,
                timeout=HTTP_TIMEOUT
            ) as response:
                data = json.load(response)

            if (
                bingx
                and str(data.get("code", "missing")) != "0"
            ):

                code = str(data.get("code"))

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
                        5 * 2**attempt
                    )
                    LIMITER.cooldown(delay)

                    LOG.warning(
                        "BingX code %s; cooldown %ss",
                        code,
                        delay
                    )
                    continue

                raise RuntimeError(
                    f"BingX API code {code}: "
                    f"{data.get('msg', '')}"
                )

            return data

        except urllib.error.HTTPError as exc:

            if (
                exc.code in {
                    429, 500, 502, 503, 504
                }
                and attempt < 4
            ):
                delay = min(
                    60,
                    5 * 2**attempt
                )

                if bingx:
                    LIMITER.cooldown(delay)
                else:
                    time.sleep(delay)

                LOG.warning(
                    "HTTP %d at %s; cooldown %ss",
                    exc.code,
                    safe_url,
                    delay
                )
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
                    f"Network error at "
                    f"{safe_url}: {exc}"
                ) from exc

            time.sleep(
                min(12, 2**attempt)
            )

    raise RuntimeError(
        f"Too many retries for {safe_url}"
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


# ========================================
# ВРЕМЯ СЕРВЕРА
# ========================================

def get_server_ms() -> int:

    value = bingx_get(
        "/openApi/swap/v2/server/time"
    )

    if (
        isinstance(value, dict)
        and value.get("serverTime") is not None
    ):
        return int(value["serverTime"])

    if isinstance(value, (int, str)):
        return int(value)

    raise RuntimeError(
        f"Unexpected BingX time format: "
        f"{str(value)[:100]}"
    )


# ========================================
# ФИЛЬТР ОБОРОТА ОТ 1 МЛН USDT
# ========================================

def get_liquid_symbols() -> tuple[list[str], int]:

    contracts = bingx_get(
        "/openApi/swap/v2/quote/contracts"
    )

    if (
        not isinstance(contracts, list)
        or not contracts
    ):
        raise RuntimeError(
            "BingX returned no contracts"
        )

    active = {
        item["symbol"]
        for item in contracts
        if (
            isinstance(item, dict)
            and str(item.get("status")) == "1"
            and item.get("currency") == "USDT"
            and str(
                item.get("symbol", "")
            ).endswith("-USDT")
            and not str(
                item.get("symbol", "")
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
            "Invalid BingX ticker list"
        )

    liquid = set()

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
            TypeError,
            ValueError,
            KeyError
        ):
            continue

        if (
            math.isfinite(turnover)
            and turnover >= MIN_TURNOVER_24H_USDT
        ):
            liquid.add(ticker["symbol"])

    if not liquid:
        raise RuntimeError(
            "No contracts passed 24-hour "
            "USDT turnover filter"
        )

    LOG.info(
        "Turnover >= %.0f USDT: "
        "%d / %d active contracts",
        MIN_TURNOVER_24H_USDT,
        len(liquid),
        len(active)
    )

    return sorted(liquid), len(active)


# ========================================
# MACD (12, 26, 9)
# ========================================

def ema(
    values: list[float],
    length: int
) -> list[float]:

    if not values:
        return []

    alpha = 2.0 / (length + 1)
    result = [values[0]]

    for value in values[1:]:
        result.append(
            alpha * value
            + (1 - alpha) * result[-1]
        )

    return result


def macd(
    closes: list[float]
) -> tuple[list[float], list[float]]:

    fast = ema(closes, MACD_FAST)
    slow = ema(closes, MACD_SLOW)

    line = [
        f - s
        for f, s in zip(fast, slow)
    ]

    return line, ema(line, MACD_SIGNAL)


def cross_direction(
    before: float,
    after: float
) -> str | None:

    if before <= 0 < after:
        return "LONG"

    if before >= 0 > after:
        return "SHORT"

    return None


# ========================================
# НОВОЕ УСЛОВИЕ:
# СРАВНЕНИЕ ЭКСТРЕМУМОВ
# ========================================

@dataclass(frozen=True)
class Extremes:

    macd_abs: float
    histogram_abs: float
    ratio: float

    macd_bar: int
    hist_bar: int
    previous_cross_bar: int


def find_extrema(
    line: list[float],
    signal: list[float],
    current_cross: int,
    candles: list[tuple[int, float]]
) -> Extremes | None:

    """
    Находим предыдущее пересечение MACD нуля.

    Рассматриваем все свечи между предыдущим
    и текущим пересечениями.

    Экстремумы определяем независимо:

    macd_extreme = max(abs(MACD))
    hist_extreme = max(abs(MACD - Signal))

    Условие:
    macd_extreme >= 1.5 * hist_extreme

    Предыдущее пересечение включается,
    текущее пересечение исключается.
    """

    previous_cross = None

    # Ищем предыдущее пересечение нуля
    for j in range(
        current_cross - 1,
        WARMUP_INDEX,
        -1
    ):

        direction = cross_direction(
            line[j - 1],
            line[j]
        )

        if direction is not None:
            previous_cross = j
            break

    if previous_cross is None:
        return None

    # Не допускаем пропусков свечей
    # между пересечениями
    for k in range(
        previous_cross + 1,
        current_cross + 1
    ):

        if (
            candles[k][0] - candles[k - 1][0]
            != PERIOD_MS
        ):
            return None

    # Весь участок между пересечениями
    period = range(
        previous_cross,
        current_cross
    )

    # Индекс максимального модуля MACD
    macd_bar = max(
        period,
        key=lambda j: abs(line[j])
    )

    # Индекс максимального модуля гистограммы
    hist_bar = max(
        period,
        key=lambda j: abs(
            line[j] - signal[j]
        )
    )

    # Модули двух экстремумов
    macd_extreme = abs(
        line[macd_bar]
    )

    histogram_extreme = abs(
        line[hist_bar] - signal[hist_bar]
    )

    if (
        not math.isfinite(macd_extreme)
        or not math.isfinite(histogram_extreme)
    ):
        return None

    # Защита от деления на ноль
    if histogram_extreme <= 0:
        return None

    ratio = (
        macd_extreme / histogram_extreme
    )

    # Сравниваем именно экстремумы
    if ratio < EXTREMA_MIN_RATIO:
        return None

    return Extremes(
        macd_abs=macd_extreme,
        histogram_abs=histogram_extreme,
        ratio=ratio,
        macd_bar=macd_bar,
        hist_bar=hist_bar,
        previous_cross_bar=previous_cross
    )


# ========================================
# ВРЕМЯ И ЗАКРЫТЫЕ СВЕЧИ
# ========================================

def time_utc(ms: int) -> str:

    return dt.datetime.fromtimestamp(
        ms / 1000,
        tz=dt.timezone.utc
    ).strftime("%d.%m %H:%M UTC")


def latest_closed_boundary(
    server_ms: int
) -> int:

    return (
        (server_ms - CLOSE_GRACE_MS)
        // PERIOD_MS
        * PERIOD_MS
    )


# ========================================
# ПОЛУЧЕНИЕ СВЕЧЕЙ BINGX
# ========================================

def parse_candles(
    raw,
    server_ms: int
) -> list[tuple[int, float]]:

    if not isinstance(raw, list):
        raise ValueError(
            "Invalid BingX kline response"
        )

    parsed = {}

    for row in raw:

        try:

            if isinstance(row, dict):

                stamp = next(
                    (
                        row[k]
                        for k in (
                            "time",
                            "openTime",
                            "startTime"
                        )
                        if row.get(k) is not None
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
                    "Unrecognized candle structure"
                )

        except (
            ValueError,
            KeyError,
            TypeError
        ) as exc:

            desc = (
                sorted(row)
                if isinstance(row, dict)
                else type(row).__name__
            )

            raise ValueError(
                f"Malformed BingX candle: {desc}"
            ) from exc

        if start % PERIOD_MS:
            raise ValueError(
                f"Unaligned 15m candle {start}"
            )

        if (
            not math.isfinite(close)
            or close <= 0
        ):
            raise ValueError(
                f"Invalid close at {start}"
            )

        # Исключаем текущую незакрытую свечу
        if (
            start + PERIOD_MS
            <= server_ms - CLOSE_GRACE_MS
        ):
            parsed[start] = close

    return sorted(parsed.items())


# ========================================
# TELEGRAM-СООБЩЕНИЕ
# ========================================

@dataclass(frozen=True)
class Alert:

    symbol: str
    direction: str
    candle_close_ms: int

    old_macd: float
    new_macd: float

    extremes: Extremes
    candles: list[tuple[int, float]]

    def display(self) -> str:

        icon = (
            "🟢"
            if self.direction == "LONG"
            else "🔴"
        )

        chart_symbol = urllib.parse.quote(
            "BINGX:"
            + self.symbol.replace("-", "")
            + ".P",
            safe=""
        )

        chart = (
            "https://www.tradingview.com/chart/"
            f"?symbol={chart_symbol}"
            f"&interval=15"
        )

        e = self.extremes

        macd_time = time_utc(
            self.candles[e.macd_bar][0]
            + PERIOD_MS
        )

        hist_time = time_utc(
            self.candles[e.hist_bar][0]
            + PERIOD_MS
        )

        previous_time = time_utc(
            self.candles[e.previous_cross_bar][0]
            + PERIOD_MS
        )

        return (
            f"{icon} {self.direction} | "
            f"{self.symbol} | 15m\n"
            f"Пересечение: "
            f"{time_utc(self.candle_close_ms)}\n"
            f"MACD: {self.old_macd:.8g} "
            f"→ {self.new_macd:.8g}\n"
            f"Экстремум |MACD|: "
            f"{e.macd_abs:.8g} ({macd_time})\n"
            f"Экстремум |Histogram|: "
            f"{e.histogram_abs:.8g} ({hist_time})\n"
            f"Соотношение экстремумов: "
            f"{e.ratio:.2f}x\n"
            f"Предыдущее пересечение: "
            f"{previous_time}\n"
            f"График: {chart}"
        )


# ========================================
# СКАНИРОВАНИЕ МОНЕТЫ
# ========================================

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

    line, signal = macd(
        [close for _, close in candles]
    )

    newest = (
        candles[-1][0] + PERIOD_MS
    )

    # Первая проверка монеты
    if last_ms is None:
        return [], newest, "bootstrapped"

    alerts = []

    for i in range(
        WARMUP_INDEX + 1,
        len(candles)
    ):

        close_ms = (
            candles[i][0] + PERIOD_MS
        )

        # Уже обработанная свеча
        if close_ms <= last_ms:
            continue

        # Пропуск свечи не считается
        # подтверждённым пересечением
        if (
            candles[i][0] - candles[i - 1][0]
            != PERIOD_MS
        ):
            continue

        direction = cross_direction(
            line[i - 1],
            line[i]
        )

        if direction is None:
            continue

        # НОВОЕ УСЛОВИЕ: сравнение
        # экстремумов двух индикаторов
        extremes = find_extrema(
            line,
            signal,
            i,
            candles
        )

        if extremes is not None:

            alerts.append(
                Alert(
                    symbol=symbol,
                    direction=direction,
                    candle_close_ms=close_ms,
                    old_macd=line[i - 1],
                    new_macd=line[i],
                    extremes=extremes,
                    candles=candles
                )
            )

    return alerts, newest, "ok"


# ========================================
# СОХРАНЕНИЕ СОСТОЯНИЯ
# ========================================

def load_state() -> dict:

    if not STATE_PATH.exists():

        LOG.info(
            "No BingX state; first scan "
            "creates checkpoints"
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
            "Invalid BingX checkpoint file"
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
# ОТПРАВКА В TELEGRAM
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


def group_alerts(
    alerts: list[Alert]
) -> list[str]:

    heading = (
        "BingX MACD(12,26,9) — "
        "пересечение нуля\n"
        "ТФ: 15m | "
        "Оборот ≥ 1 млн USDT за 24 ч\n"
        f"Экстремум |MACD| / "
        f"экстремум |Histogram| "
        f"≥ {EXTREMA_MIN_RATIO}x\n"
    )

    messages = []
    current = heading

    for alert in alerts:

        entry = (
            "\n" + alert.display() + "\n"
        )

        if (
            len(current) + len(entry) > 3400
            and current != heading
        ):
            messages.append(current.rstrip())
            current = heading

        current += entry

    if current != heading:
        messages.append(current.rstrip())

    return messages


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

    token = os.getenv(
        "TELEGRAM_BOT_TOKEN", ""
    ).strip()

    chat_id = os.getenv(
        "TELEGRAM_CHAT_ID", ""
    ).strip()

    if not token or not chat_id:
        raise RuntimeError(
            "Set GitHub secrets "
            "TELEGRAM_BOT_TOKEN and "
            "TELEGRAM_CHAT_ID"
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

    # Сохраняем только состояние 15m
    # для монет с подходящим оборотом
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

    tasks = {}

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=WORKERS
    ) as pool:

        for symbol in symbols:

            key = f"{symbol}|15m"
            last_ms = old_last.get(key)

            if (
                last_ms is not None
                and last_ms >= latest_closed_boundary(
                    server_ms
                )
            ):
                skipped += 1
                continue

            future = pool.submit(
                scan_symbol,
                symbol,
                server_ms,
                last_ms
            )

            tasks[future] = key

        for index, future in enumerate(
            concurrent.futures.as_completed(tasks),
            1
        ):

            key = tasks[future]

            try:
                batch, newest, status = (
                    future.result()
                )

            except Exception as exc:

                LOG.error(
                    "BingX failed %s: %s",
                    key,
                    exc
                )

                errors.append(
                    (key, str(exc))
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

            if index % 100 == 0:
                LOG.info(
                    "Processed %d/%d",
                    index,
                    len(tasks)
                )

    LOG.info(
        "BingX 15m: eligible=%d/%d "
        "requested=%d skipped=%d "
        "bootstrapped=%d young=%d "
        "errors=%d alerts=%d",
        len(symbols),
        total,
        len(tasks),
        skipped,
        bootstrapped,
        young,
        len(errors),
        len(alerts)
    )

    # При массовых ошибках
    # не сохраняем контрольные точки
    if (
        tasks
        and len(errors) > max(
            10,
            len(tasks) // 5
        )
    ):
        raise RuntimeError(
            f"Too many BingX errors: "
            f"{len(errors)}/{len(tasks)}"
        )

    alerts.sort(
        key=lambda a: (
            a.candle_close_ms,
            a.symbol
        )
    )

    messages = group_alerts(alerts)

    # Служебное сообщение при
    # первой синхронизации
    if not any(
        key.endswith("|15m")
        for key in old_last
    ):

        messages.insert(
            0,
            (
                "✅ BingX MACD scanner ready\n"
                f"USDT contracts: {total}\n"
                f"Passed turnover: {len(symbols)}\n"
                "Timeframe: 15m\n"
                "Filter: extreme |MACD| >= "
                "1.5 * extreme |Histogram|\n"
                "Initial scan: synchronization."
            )
        )

    # Telegram
    for index, message in enumerate(messages):

        if index:
            time.sleep(1.1)

        telegram_send(
            token,
            chat_id,
            message
        )

    # Сохраняем состояние
    state["last"] = new_last

    state["updated_utc"] = (
        dt.datetime.now(
            dt.timezone.utc
        ).isoformat()
    )

    save_state(state)

    LOG.info(
        "Saved %d BingX checkpoints; "
        "Telegram messages=%d",
        len(new_last),
        len(messages)
    )

    if errors:
        LOG.warning(
            "Failed symbols keep their checkpoints"
        )


if __name__ == "__main__":
    main()
