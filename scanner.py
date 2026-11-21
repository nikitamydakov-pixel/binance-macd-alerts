
#!/usr/bin/env python3
"""BingX USDT perpetual MACD(12,26,9) alerts: 5m and 15m.

For each zero crossing on a closed candle, check the preceding MACD
zero-to-zero segment (previous crossing included; current excluded):
    max(abs(MACD)) >= 1.5 * max(abs(MACD - signal))
Extrema can occur on DIFFERENT candles. Standard library only.
"""

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


# ==========================================
# НАСТРОЙКИ
# ==========================================

API_URL = "https://open-api.bingx.com"

TIMEFRAMES = {
    "5m": 5,
    "15m": 15
}

MIN_TURNOVER_24H_USDT = 1_000_000.0

MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# Минимальное отношение экстремумов
EXTREMA_MIN_RATIO = 1.5

# До 5 суток 5-минутных свечей
KLINE_LIMIT = 1440

MIN_CANDLES = 100
WARMUP_INDEX = 75
CLOSE_GRACE_MS = 2500

REQUESTS_PER_SECOND = float(
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


# ==========================================
# ОГРАНИЧЕНИЕ ЗАПРОСОВ
# ==========================================

class RateLimiter:

    def __init__(
        self,
        requests_per_second: float
    ):

        if not 0 < requests_per_second <= 10:
            raise ValueError(
                "BINGX_REQUESTS_PER_SECOND "
                "must be between 0 and 10"
            )

        self.delay = 1.0 / requests_per_second
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


LIMITER = RateLimiter(REQUESTS_PER_SECOND)


# ==========================================
# HTTP-ЗАПРОСЫ
# ==========================================

def request_json(
    url: str,
    *,
    bingx: bool = False,
    payload: dict | None = None
) -> dict:

    body = (
        None
        if payload is None
        else json.dumps(payload).encode("utf-8")
    )

    headers = {
        "Accept": "application/json",
        "User-Agent": "bingx-macd-extrema/2.0"
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

            if bingx:

                code = str(
                    data.get("code", "missing")
                )

                if code != "0":

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
                            "BingX %s: retry after %ds",
                            code,
                            delay
                        )

                        continue

                    raise RuntimeError(
                        f"BingX code {code}: "
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
                    "HTTP %d at %s; retry in %ds",
                    exc.code,
                    safe_url,
                    delay
                )

                continue

            details = exc.read(200).decode(
                "utf-8",
                "replace"
            )

            raise RuntimeError(
                f"HTTP {exc.code} at "
                f"{safe_url}: {details}"
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
        f"Request retry limit reached: {safe_url}"
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


# ==========================================
# ВРЕМЯ BINGX
# ==========================================

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
        f"Unexpected BingX server time: "
        f"{str(data)[:100]}"
    )


# ==========================================
# ФИЛЬТР ОБОРОТА
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
            "Empty or invalid BingX contracts list"
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
            "No active USDT perpetual contracts"
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
            "Invalid BingX 24-hour ticker list"
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
            ValueError,
            TypeError,
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
            "No symbols passed "
            "24-hour turnover filter"
        )

    LOG.info(
        "24h turnover >= %.0f USDT: "
        "%d/%d symbols",
        MIN_TURNOVER_24H_USDT,
        len(liquid),
        len(active)
    )

    return sorted(liquid), len(active)


# ==========================================
# РАСЧЁТ MACD (12, 26, 9)
# ==========================================

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


def calculate_macd(
    prices: list[float]
) -> tuple[list[float], list[float]]:

    fast = ema(
        prices,
        MACD_FAST
    )

    slow = ema(
        prices,
        MACD_SLOW
    )

    line = [
        x - y
        for x, y in zip(fast, slow)
    ]

    signal = ema(
        line,
        MACD_SIGNAL
    )

    return line, signal


# ==========================================
# ПЕРЕСЕЧЕНИЕ НУЛЕВОЙ ЛИНИИ
# ==========================================

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
# СРАВНЕНИЕ ЭКСТРЕМУМОВ
# ==========================================

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
    candles: list[tuple[int, float]],
    period_ms: int
) -> Extremes | None:

    """
    Сравнивает независимые экстремумы
    MACD и гистограммы между двумя
    последовательными пересечениями нуля.

    Предыдущее пересечение включается.
    Текущее пересечение исключается.

    |MACD|max >= 1.5 * |Histogram|max
    """

    previous_cross = None

    # Ищем предыдущее пересечение
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

    # Проверяем непрерывность свечей
    for j in range(
        previous_cross,
        current_cross + 1
    ):

        if (
            candles[j][0] - candles[j - 1][0]
            != period_ms
        ):
            return None

    # Полный участок между пересечениями
    indexes = range(
        previous_cross,
        current_cross
    )

    # Максимальный модуль MACD
    macd_bar = max(
        indexes,
        key=lambda j: abs(line[j])
    )

    # Максимальный модуль гистограммы
    hist_bar = max(
        indexes,
        key=lambda j: abs(
            line[j] - signal[j]
        )
    )

    macd_abs = abs(
        line[macd_bar]
    )

    hist_abs = abs(
        line[hist_bar] - signal[hist_bar]
    )

    if (
        not math.isfinite(macd_abs)
        or not math.isfinite(hist_abs)
        or hist_abs <= 0
    ):
        return None

    ratio = macd_abs / hist_abs

    if ratio < EXTREMA_MIN_RATIO:
        return None

    return Extremes(
        macd_abs=macd_abs,
        histogram_abs=hist_abs,
        ratio=ratio,
        macd_bar=macd_bar,
        hist_bar=hist_bar,
        previous_cross_bar=previous_cross
    )


# ==========================================
# ПОЛУЧЕНИЕ СВЕЧЕЙ 5m
# ==========================================

def parse_five_minute_candles(
    raw,
    server_ms: int
) -> list[tuple[int, float]]:

    """
    Поддерживает форматы BingX:

    {"time": ..., "close": ...}

    [time, open, high, low, close, ...]
    """

    if not isinstance(raw, list):
        raise ValueError(
            "Malformed BingX candle response: "
            f"{type(raw).__name__}"
        )

    period_ms = 5 * 60_000
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
                    "Unexpected candle structure"
                )

        except (
            ValueError,
            TypeError,
            KeyError
        ) as exc:

            shape = (
                list(row.keys())
                if isinstance(row, dict)
                else type(row).__name__
            )

            raise ValueError(
                f"Malformed BingX candle: {shape}"
            ) from exc

        if start % period_ms:
            raise ValueError(
                f"5m candle not time-aligned: {start}"
            )

        if (
            not math.isfinite(close)
            or close <= 0
        ):
            raise ValueError(
                f"Invalid close at {start}"
            )

        # Исключаем незакрытые свечи
        if (
            start + period_ms
            <= server_ms - CLOSE_GRACE_MS
        ):
            parsed[start] = close

    return sorted(parsed.items())


# ==========================================
# ПОСТРОЕНИЕ СВЕЧЕЙ 15m ИЗ 5m
# ==========================================

def build_fifteen_minute_candles(
    five: list[tuple[int, float]]
) -> list[tuple[int, float]]:

    """
    Одна 15m свеча состоит из
    трёх последовательных 5m свечей.

    Цена закрытия 15m = закрытие
    третьей 5m свечи.
    """

    lookup = dict(five)

    p5 = 5 * 60_000
    p15 = 15 * 60_000

    starts = sorted({
        t // p15 * p15
        for t, _ in five
    })

    result = []

    for start in starts:

        if all(
            start + offset * p5 in lookup
            for offset in range(3)
        ):

            result.append((
                start,
                lookup[start + 2 * p5]
            ))

    return result


# ==========================================
# ВРЕМЯ
# ==========================================

def time_utc(ms: int) -> str:

    return dt.datetime.fromtimestamp(
        ms / 1000,
        tz=dt.timezone.utc
    ).strftime("%d.%m %H:%M UTC")


def last_closed_boundary(
    server_ms: int,
    timeframe: str
) -> int:

    period_ms = (
        TIMEFRAMES[timeframe] * 60_000
    )

    return (
        (server_ms - CLOSE_GRACE_MS)
        // period_ms
        * period_ms
    )


# ==========================================
# УВЕДОМЛЕНИЕ TELEGRAM
# ==========================================

@dataclass(frozen=True)
class Alert:

    symbol: str
    timeframe: str
    direction: str
    candle_close_ms: int

    previous_macd: float
    current_macd: float

    extremes: Extremes

    macd_extreme_time_ms: int
    hist_extreme_time_ms: int
    prior_cross_time_ms: int

    def display(self) -> str:

        marker = (
            "🟢" if self.direction == "LONG"
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
            f"&interval={TIMEFRAMES[self.timeframe]}"
        )

        return (
            f"{marker} {self.direction} | "
            f"{self.symbol} | {self.timeframe}\n"
            f"Пересечение: "
            f"{time_utc(self.candle_close_ms)}\n"
            f"MACD: {self.previous_macd:.8g} "
            f"→ {self.current_macd:.8g}\n"
            f"Экстремум |MACD|: "
            f"{self.extremes.macd_abs:.8g} "
            f"({time_utc(self.macd_extreme_time_ms)})\n"
            f"Экстремум |Histogram|: "
            f"{self.extremes.histogram_abs:.8g} "
            f"({time_utc(self.hist_extreme_time_ms)})\n"
            f"Соотношение экстремумов: "
            f"{self.extremes.ratio:.2f}x\n"
            f"Предыдущее пересечение: "
            f"{time_utc(self.prior_cross_time_ms)}\n"
            f"График: {chart}"
        )


# ==========================================
# АНАЛИЗ ОДНОГО ТАЙМФРЕЙМА
# ==========================================

def scan_timeframe(
    symbol: str,
    timeframe: str,
    candles: list[tuple[int, float]],
    last_ms: int | None
):

    if len(candles) < MIN_CANDLES:
        return [], None, "young"

    period_ms = (
        TIMEFRAMES[timeframe] * 60_000
    )

    closes = [
        close
        for _, close in candles
    ]

    line, signal = calculate_macd(closes)

    newest = (
        candles[-1][0] + period_ms
    )

    # Первый запуск: без исторических сигналов
    if last_ms is None:
        return [], newest, "bootstrapped"

    alerts = []

    for i in range(
        WARMUP_INDEX + 1,
        len(candles)
    ):

        close_ms = (
            candles[i][0] + period_ms
        )

        if close_ms <= last_ms:
            continue

        # Не допускаем пересечения
        # через отсутствующую свечу
        if (
            candles[i][0] - candles[i - 1][0]
            != period_ms
        ):
            continue

        direction = cross_direction(
            line[i - 1],
            line[i]
        )

        if direction is None:
            continue

        # Отдельно рассчитываем
        # экстремумы для каждого таймфрейма
        extremes = find_extrema(
            line,
            signal,
            i,
            candles,
            period_ms
        )

        if extremes is None:
            continue

        alerts.append(
            Alert(
                symbol=symbol,
                timeframe=timeframe,
                direction=direction,
                candle_close_ms=close_ms,
                previous_macd=line[i - 1],
                current_macd=line[i],
                extremes=extremes,
                macd_extreme_time_ms=(
                    candles[extremes.macd_bar][0]
                    + period_ms
                ),
                hist_extreme_time_ms=(
                    candles[extremes.hist_bar][0]
                    + period_ms
                ),
                prior_cross_time_ms=(
                    candles[extremes.previous_cross_bar][0]
                    + period_ms
                )
            )
        )

    return alerts, newest, "ok"


# ==========================================
# АНАЛИЗ ОДНОЙ МОНЕТЫ
# ==========================================

def scan_symbol(
    symbol: str,
    server_ms: int,
    scan_start_monotonic: float,
    checkpoints: dict
) -> dict:

    # Один запрос 5m вместо двух запросов
    raw = bingx_get(
        "/openApi/swap/v3/quote/klines",
        {
            "symbol": symbol,
            "interval": "5m",
            "limit": KLINE_LIMIT
        }
    )

    # Учитываем время ожидания очереди API
    effective_ms = (
        server_ms
        + int(
            (
                time.monotonic()
                - scan_start_monotonic
            ) * 1000
        )
    )

    five = parse_five_minute_candles(
        raw,
        effective_ms
    )

    fifteen = build_fifteen_minute_candles(
        five
    )

    # 5m и 15m анализируются независимо
    return {
        "5m": scan_timeframe(
            symbol,
            "5m",
            five,
            checkpoints.get(f"{symbol}|5m")
        ),
        "15m": scan_timeframe(
            symbol,
            "15m",
            fifteen,
            checkpoints.get(f"{symbol}|15m")
        )
    }


# ==========================================
# СОХРАНЕНИЕ КОНТРОЛЬНЫХ ТОЧЕК
# ==========================================

def load_state() -> dict:

    if not STATE_PATH.exists():

        LOG.info(
            "First BingX scan: "
            "creating checkpoints"
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
            "Invalid BingX checkpoint state"
        )

    return state


def save_state(state: dict):

    STATE_PATH.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    temp = STATE_PATH.with_suffix(".tmp")

    temp.write_text(
        json.dumps(
            state,
            sort_keys=True,
            ensure_ascii=False
        ),
        encoding="utf-8"
    )

    temp.replace(STATE_PATH)


# ==========================================
# ОТПРАВКА В TELEGRAM
# ==========================================

def telegram_send(
    token: str,
    chat_id: str,
    text: str
):

    url = (
        f"https://api.telegram.org/bot"
        f"{token}/sendMessage"
    )

    response = request_json(
        url,
        payload={
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True
        }
    )

    if not response.get("ok"):
        raise RuntimeError(
            "Telegram error: "
            f"{response.get('description', 'unknown')}"
        )


def group_alerts(
    alerts: list[Alert]
) -> list[str]:

    header = (
        "BingX MACD(12,26,9) — пересечение нуля\n"
        "Таймфреймы: 5m, 15m\n"
        "Оборот ≥ 1 млн USDT/24ч\n"
        f"Экстремум |MACD| / "
        f"экстремум |Histogram| "
        f"≥ {EXTREMA_MIN_RATIO}x\n"
    )

    messages = []
    message = header

    for alert in alerts:

        piece = (
            "\n" + alert.display() + "\n"
        )

        if (
            len(message) + len(piece) > 3400
            and message != header
        ):
            messages.append(
                message.rstrip()
            )
            message = header

        message += piece

    if message != header:
        messages.append(
            message.rstrip()
        )

    return messages


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
        "TELEGRAM_BOT_TOKEN",
        ""
    ).strip()

    chat_id = os.getenv(
        "TELEGRAM_CHAT_ID",
        ""
    ).strip()

    if not token or not chat_id:
        raise RuntimeError(
            "Set GitHub secrets "
            "TELEGRAM_BOT_TOKEN "
            "and TELEGRAM_CHAT_ID"
        )

    if not 1 <= WORKERS <= 12:
        raise ValueError(
            "MAX_WORKERS must be between 1 and 12"
        )

    state = load_state()
    old_last = state["last"]

    server_ms = get_server_ms()
    scan_start_monotonic = time.monotonic()

    symbols, total = get_liquid_symbols()

    eligible = set(symbols)

    # Сохраняем контрольные точки
    # обоих таймфреймов
    new_last = {
        key: value
        for key, value in old_last.items()
        if (
            key.partition("|")[0] in eligible
            and key.partition("|")[2] in TIMEFRAMES
        )
    }

    jobs = {}
    alerts = []
    errors = []

    skipped = 0
    bootstrapped = 0
    young = 0

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=WORKERS
    ) as executor:

        for symbol in symbols:

            # Пропускаем монету, если на обоих
            # таймфреймах свечи уже обработаны
            if all(
                (
                    old_last.get(f"{symbol}|{tf}")
                    is not None
                    and old_last[f"{symbol}|{tf}"]
                    >= last_closed_boundary(
                        server_ms,
                        tf
                    )
                )
                for tf in TIMEFRAMES
            ):
                skipped += 1
                continue

            future = executor.submit(
                scan_symbol,
                symbol,
                server_ms,
                scan_start_monotonic,
                old_last
            )

            jobs[future] = symbol

        for count, future in enumerate(
            concurrent.futures.as_completed(jobs),
            1
        ):

            symbol = jobs[future]

            try:
                results = future.result()

            except Exception as exc:

                LOG.error(
                    "BingX failed %s: %s",
                    symbol,
                    exc
                )

                errors.append(
                    (symbol, str(exc))
                )
                continue

            for timeframe, result in results.items():

                batch, newest, status = result

                if newest is not None:
                    new_last[
                        f"{symbol}|{timeframe}"
                    ] = newest

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
        "BingX 5m+15m: "
        "eligible=%d/%d "
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

    # При массовых ошибках
    # контрольные точки не сохраняем
    if (
        jobs
        and len(errors) > max(
            10,
            len(jobs) // 5
        )
    ):
        raise RuntimeError(
            "Too many BingX errors: "
            f"{len(errors)}/{len(jobs)}"
        )

    alerts.sort(
        key=lambda alert: (
            alert.candle_close_ms,
            TIMEFRAMES[alert.timeframe],
            alert.symbol
        )
    )

    messages = group_alerts(alerts)

    # Служебное сообщение при первом запуске
    if not any(
        (
            key.endswith("|5m")
            or key.endswith("|15m")
        )
        for key in old_last
    ):

        messages.insert(
            0,
            (
                "✅ BingX MACD scanner active\n"
                f"USDT perpetuals: {total}\n"
                f"Passed turnover: {len(symbols)}\n"
                "Timeframes: 5m and 15m\n"
                f"Extrema ratio threshold: "
                f"{EXTREMA_MIN_RATIO}x\n"
                "Initial synchronization completed."
            )
        )

    # Отправляем сигналы
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
        "Saved %d checkpoints; "
        "Telegram messages=%d",
        len(new_last),
        len(messages)
    )

    if errors:
        LOG.warning(
            "Failed contracts keep previous "
            "checkpoints for retry"
        )


if __name__ == "__main__":
    main()
