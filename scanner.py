
#!/usr/bin/env python3
"""BingX USDT perpetual MACD zero-cross Telegram scanner.

Timeframes: 5m and 15m
MACD: 12, 26, 9
Minimum 24h turnover: 1,000,000 USDT
Pre-cross MACD / histogram ratio: >= 1.5
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
# SETTINGS
# ==========================================

API_URL = "https://open-api.bingx.com"

INTERVAL_MINUTES = {
    "5m": 5,
    "15m": 15,
}

MIN_TURNOVER_24H_USDT = 1_000_000.0

MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

MACD_HIST_MIN_RATIO = 1.5
PRE_CROSS_LOOKBACK_BARS = 12

KLINE_LIMIT = 630
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
        str(Path.home() / "bingx-macd-state" / "state.json")
    )
)

HTTP_TIMEOUT = 20

LOG = logging.getLogger("bingx-macd")


# ==========================================
# REQUEST LIMITER
# ==========================================

class RequestLimiter:

    def __init__(self, rps: float):

        if not 0 < rps <= 10:
            raise ValueError(
                "BINGX_REQUESTS_PER_SECOND must be > 0 and <= 10"
            )

        self.lock = threading.Lock()
        self.delay = 1 / rps
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


# ==========================================
# HTTP REQUESTS
# ==========================================

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
        "User-Agent": "bingx-macd-telegram/2.0",
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

                        cooldown = min(
                            60,
                            5 * 2 ** attempt
                        )

                        LIMITER.pause_all(cooldown)

                        LOG.warning(
                            "BingX code %s; retry after %ss",
                            code,
                            cooldown
                        )

                        continue

                    raise RuntimeError(
                        f"BingX {code}: "
                        f"{result.get('msg', '')} "
                        f"for {safe_url}"
                    )

            return result

        except urllib.error.HTTPError as exc:

            if (
                exc.code in (
                    429,
                    500,
                    502,
                    503,
                    504
                )
                and attempt < 4
            ):

                cooldown = min(
                    60,
                    5 * 2 ** attempt
                )

                if bingx:
                    LIMITER.pause_all(cooldown)
                else:
                    time.sleep(cooldown)

                LOG.warning(
                    "HTTP %s for %s; retry after %ss",
                    exc.code,
                    safe_url,
                    cooldown
                )

                continue

            detail = exc.read(150).decode(
                "utf-8",
                errors="replace"
            )

            raise RuntimeError(
                f"HTTP {exc.code} "
                f"for {safe_url}: {detail}"
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
# SERVER TIME
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
        f"Unexpected BingX server time response: "
        f"{str(data)[:100]}"
    )


# ==========================================
# SYMBOL FILTER
# ==========================================

def get_liquid_symbols() -> tuple[list[str], int]:

    contracts = bingx_get(
        "/openApi/swap/v2/quote/contracts"
    )

    if not isinstance(contracts, list) or not contracts:
        raise RuntimeError(
            "No BingX contracts: unexpected API response"
        )

    active = {
        c["symbol"]
        for c in contracts
        if isinstance(c, dict)
        and str(c.get("status")) == "1"
        and c.get("currency") == "USDT"
        and str(c.get("symbol", "")).endswith("-USDT")
        and not str(c.get("symbol", "")).startswith("NCS")
    }

    if not active:
        raise RuntimeError(
            "BingX returned no active USDT perpetual contracts"
        )

    tickers = bingx_get(
        "/openApi/swap/v2/quote/ticker"
    )

    if isinstance(tickers, dict):
        tickers = [tickers]

    if not isinstance(tickers, list) or not tickers:
        raise RuntimeError(
            "BingX 24h ticker response is missing"
        )

    liquid = set()

    for ticker in tickers:

        if not isinstance(ticker, dict):
            continue

        symbol = ticker.get("symbol")

        if symbol not in active:
            continue

        try:
            volume = float(
                ticker["quoteVolume"]
            )
        except (
            KeyError,
            ValueError,
            TypeError
        ):
            continue

        if (
            math.isfinite(volume)
            and volume >= MIN_TURNOVER_24H_USDT
        ):
            liquid.add(symbol)

    if not liquid:
        raise RuntimeError(
            "No active contracts pass turnover filter. "
            "Check BingX ticker API."
        )

    LOG.info(
        "BingX eligible USDT perpetuals >= %.0f USDT/24h: %d / %d",
        MIN_TURNOVER_24H_USDT,
        len(liquid),
        len(active)
    )

    return sorted(liquid), len(active)


# ==========================================
# MACD CALCULATION
# ==========================================

def ema(
    values: list[float],
    length: int
) -> list[float]:

    if not values:
        return []

    alpha = 2.0 / (length + 1)

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


# ==========================================
# ZERO CROSSING
# ==========================================

def direction_of_cross(
    before: float,
    after: float
) -> str | None:

    if before <= 0 < after:
        return "LONG"

    if before >= 0 > after:
        return "SHORT"

    return None


# ==========================================
# MACD / HISTOGRAM FILTER
# ==========================================

def qualifying_ratio(
    line: list[float],
    signal: list[float],
    index: int,
    direction: str
) -> tuple[float, int] | None:

    old_side = (
        -1 if direction == "LONG"
        else 1
    )

    first_index = max(
        -1,
        index - 1 - PRE_CROSS_LOOKBACK_BARS
    )

    for j in range(
        index - 1,
        first_index,
        -1
    ):

        if old_side * line[j] <= 0:
            break

        histogram = (
            line[j] - signal[j]
        )

        if abs(histogram) <= max(
            1e-16,
            abs(line[j]) * 1e-10
        ):
            continue

        ratio = (
            abs(line[j]) / abs(histogram)
        )

        if ratio >= MACD_HIST_MIN_RATIO:
            return ratio, j

    return None


# ==========================================
# TIME HELPERS
# ==========================================

def fmt_time(ms: int) -> str:

    return dt.datetime.fromtimestamp(
        ms / 1000,
        tz=dt.timezone.utc
    ).strftime("%d.%m %H:%M UTC")


def latest_closed_boundary(
    server_ms: int,
    interval: str
) -> int:

    duration = (
        INTERVAL_MINUTES[interval] * 60_000
    )

    return (
        (server_ms - CLOSE_GRACE_MS)
        // duration
        * duration
    )


# ==========================================
# FIXED BINGX CANDLE PARSER
# ==========================================

def parse_five_minute_candles(
    raw: list,
    server_ms: int
) -> list[tuple[int, float]]:

    """
    Supports BingX candles in two formats:

    Dictionary:
    {
        "time": 1234567890000,
        "open": "...",
        "high": "...",
        "low": "...",
        "close": "..."
    }

    Array:
    [timestamp, open, high, low, close, ...]
    """

    duration = 5 * 60_000

    if not isinstance(raw, list):
        raise ValueError(
            f"Malformed BingX candle list: "
            f"{type(raw).__name__}"
        )

    parsed = {}

    for row in raw:

        try:

            if isinstance(row, dict):

                start_value = row.get("openTime")

                if start_value is None:
                    start_value = row.get("time")

                if start_value is None:
                    start_value = row.get("startTime")

                start = int(start_value)
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
            TypeError,
            KeyError,
            ValueError
        ) as exc:

            if isinstance(row, dict):
                shape = (
                    f"object keys="
                    f"{sorted(map(str, row.keys()))}"
                )
            else:
                shape = (
                    f"type={type(row).__name__}"
                )

            raise ValueError(
                f"Malformed BingX kline item ({shape})"
            ) from exc

        if start % duration != 0:
            raise ValueError(
                f"Misaligned 5m candle: {start}"
            )

        if (
            not math.isfinite(close)
            or close <= 0
        ):
            raise ValueError(
                f"Invalid BingX candle price at {start}"
            )

        # Exclude incomplete candles
        if (
            start + duration
            <= server_ms - CLOSE_GRACE_MS
        ):
            parsed[start] = close

    return sorted(parsed.items())


# ==========================================
# BUILD 15M FROM 5M CANDLES
# ==========================================

def make_fifteen_minute_candles(
    five: list[tuple[int, float]]
) -> list[tuple[int, float]]:

    lookup = dict(five)

    five_ms = 5 * 60_000
    fifteen_ms = 15 * 60_000

    buckets = sorted({
        t // fifteen_ms * fifteen_ms
        for t, _ in five
    })

    result = []

    for start in buckets:

        if all(
            start + step * five_ms in lookup
            for step in range(3)
        ):

            result.append((
                start,
                lookup[start + 2 * five_ms]
            ))

    return result


# ==========================================
# TELEGRAM ALERT FORMAT
# ==========================================

@dataclass(frozen=True)
class Alert:

    symbol: str
    interval: str
    direction: str
    candle_close_ms: int
    old_macd: float
    new_macd: float
    ratio: float
    condition_close_ms: int

    def display(self) -> str:

        marker = (
            "🟢" if self.direction == "LONG"
            else "🔴"
        )

        name = (
            self.symbol.replace("-", "") + ".P"
        )

        tv_symbol = urllib.parse.quote(
            "BINGX:" + name,
            safe=""
        )

        timeframe = INTERVAL_MINUTES[
            self.interval
        ]

        tv = (
            "https://www.tradingview.com/chart/"
            f"?symbol={tv_symbol}"
            f"&interval={timeframe}"
        )

        bingx = (
            "https://bingx.com/en/perpetual/"
            f"{urllib.parse.quote(self.symbol)}"
        )

        return (
            f"{marker} {self.direction} | "
            f"{self.symbol} | {self.interval}\n"
            f"Закрытие: {fmt_time(self.candle_close_ms)}\n"
            f"MACD: {self.old_macd:.8g} "
            f"→ {self.new_macd:.8g}\n"
            f"До пересечения |MACD|/|Histogram| "
            f"= {self.ratio:.2f}x\n"
            f"Свеча условия: "
            f"{fmt_time(self.condition_close_ms)}\n"
            f"График: {tv}\n"
            f"Биржа: {bingx}"
        )


# ==========================================
# SCAN TIMEFRAME
# ==========================================

def scan_interval(
    symbol: str,
    interval: str,
    candles: list[tuple[int, float]],
    last_ms: int | None
) -> tuple[list[Alert], int | None, str]:

    if len(candles) < MIN_CANDLES:
        return [], None, "young"

    duration = (
        INTERVAL_MINUTES[interval] * 60_000
    )

    line, signal = calculate_macd(
        [price for _, price in candles]
    )

    newest = (
        candles[-1][0] + duration
    )

    if last_ms is None:
        return [], newest, "bootstrapped"

    if (
        last_ms
        < candles[WARMUP_INDEX][0] + duration
    ):

        LOG.warning(
            "%s %s: checkpoint gap exceeds "
            "available warmup",
            symbol,
            interval
        )

    alerts = []

    for i in range(
        WARMUP_INDEX,
        len(candles)
    ):

        close_ms = (
            candles[i][0] + duration
        )

        if close_ms <= last_ms:
            continue

        # Do not calculate crossing
        # across missing candles
        if (
            candles[i][0] - candles[i - 1][0]
            != duration
        ):
            continue

        direction = direction_of_cross(
            line[i - 1],
            line[i]
        )

        if not direction:
            continue

        condition = qualifying_ratio(
            line,
            signal,
            i,
            direction
        )

        if condition is None:
            continue

        ratio, j = condition

        alerts.append(
            Alert(
                symbol=symbol,
                interval=interval,
                direction=direction,
                candle_close_ms=close_ms,
                old_macd=line[i - 1],
                new_macd=line[i],
                ratio=ratio,
                condition_close_ms=(
                    candles[j][0] + duration
                )
            )
        )

    return alerts, newest, "ok"


# ==========================================
# SCAN SYMBOL
# ==========================================

def scan_symbol(
    symbol: str,
    server_ms: int,
    last: dict
):

    # One BingX request for 5m candles.
    # 15m candles are constructed locally.

    raw = bingx_get(
        "/openApi/swap/v3/quote/klines",
        {
            "symbol": symbol,
            "interval": "5m",
            "limit": KLINE_LIMIT
        }
    )

    five = parse_five_minute_candles(
        raw,
        server_ms
    )

    fifteen = make_fifteen_minute_candles(
        five
    )

    results = {}

    for interval, candles in (
        ("5m", five),
        ("15m", fifteen)
    ):

        key = f"{symbol}|{interval}"

        results[key] = scan_interval(
            symbol,
            interval,
            candles,
            last.get(key)
        )

    return results


# ==========================================
# PERSISTENT STATE
# ==========================================

def load_state() -> dict:

    if not STATE_PATH.exists():

        LOG.info(
            "No BingX state: "
            "bootstrapping without historical alerts"
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
            "State is not BingX version 3. "
            "Do not reuse Bybit checkpoints."
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
# TELEGRAM API
# ==========================================

def telegram_send(
    token: str,
    chat_id: str,
    message: str
):

    endpoint = (
        f"https://api.telegram.org/bot"
        f"{token}/sendMessage"
    )

    result = request_json(
        endpoint,
        post={
            "chat_id": chat_id,
            "text": message,
            "disable_web_page_preview": True
        }
    )

    if not result.get("ok"):

        raise RuntimeError(
            "Telegram rejected alert: "
            f"{result.get('description', 'unknown')}"
        )


# ==========================================
# GROUP TELEGRAM ALERTS
# ==========================================

def group_alerts(
    alerts: list[Alert]
) -> list[str]:

    heading = (
        "BingX MACD(12,26,9) — пересечение нуля\n"
        "ТФ: 5m и 15m | Оборот ≥ 1 млн USDT/24ч\n"
        f"До пересечения: |MACD|/|гистограмма| "
        f"≥ {MACD_HIST_MIN_RATIO}x\n"
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

            messages.append(
                current.rstrip()
            )

            current = heading

        current += entry

    if current != heading:
        messages.append(
            current.rstrip()
        )

    return messages


# ==========================================
# MAIN
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
            "GitHub Secrets TELEGRAM_BOT_TOKEN "
            "and TELEGRAM_CHAT_ID are required"
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

    # Remove checkpoints for symbols
    # below liquidity threshold
    new_last = {
        key: value
        for key, value in old_last.items()
        if key.partition("|")[0] in eligible
    }

    alerts = []
    errors = []

    bootstrapped = 0
    young = 0
    skipped = 0

    tasks = {}

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=WORKERS
    ) as pool:

        for symbol in symbols:

            prev5 = old_last.get(
                f"{symbol}|5m"
            )

            prev15 = old_last.get(
                f"{symbol}|15m"
            )

            if (
                prev5 is not None
                and prev15 is not None
                and prev5 >= latest_closed_boundary(
                    server_ms, "5m"
                )
                and prev15 >= latest_closed_boundary(
                    server_ms, "15m"
                )
            ):

                skipped += 1
                continue

            future = pool.submit(
                scan_symbol,
                symbol,
                server_ms,
                old_last
            )

            tasks[future] = symbol

        for i, future in enumerate(
            concurrent.futures.as_completed(tasks),
            1
        ):

            symbol = tasks[future]

            try:

                results = future.result()

            except Exception as exc:

                LOG.error(
                    "BingX failed for %s: %s",
                    symbol,
                    exc
                )

                errors.append((
                    symbol,
                    str(exc)
                ))

                continue

            for key, result in results.items():

                batch, newest, status = result

                if newest is not None:
                    new_last[key] = newest

                alerts.extend(batch)

                bootstrapped += (
                    status == "bootstrapped"
                )

                young += (
                    status == "young"
                )

            if i % 100 == 0:

                LOG.info(
                    "Processed %d/%d symbols",
                    i,
                    len(tasks)
                )

    LOG.info(
        "BingX: %d eligible/%d active, "
        "requested=%d skipped=%d, "
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

    if (
        tasks
        and len(errors) > max(
            10,
            len(tasks) // 5
        )
    ):

        raise RuntimeError(
            f"Too many BingX failures: "
            f"{len(errors)}/{len(tasks)}. "
            "Check rate limits/API."
        )

    alerts.sort(
        key=lambda a: (
            a.candle_close_ms,
            a.interval,
            a.symbol
        )
    )

    messages = group_alerts(alerts)

    if not old_last:

        messages.insert(
            0,
            (
                "✅ BingX MACD-сканер запущен.\n"
                f"Активных USDT-фьючерсов: {total}\n"
                f"Прошли оборот ≥ 1 млн USDT/24ч: "
                f"{len(symbols)}\n"
                "Таймфреймы: 5m и 15m\n"
                f"Условие до пересечения: "
                f"≥ {MACD_HIST_MIN_RATIO}x\n"
                "Первый запуск: синхронизация "
                "без исторических сигналов."
            )
        )

    # Send Telegram messages
    for index, message in enumerate(messages):

        if index:
            time.sleep(1.1)

        telegram_send(
            token,
            chat_id,
            message
        )

    # Save checkpoints
    state["last"] = new_last

    state["updated_utc"] = (
        dt.datetime.now(
            dt.timezone.utc
        ).isoformat()
    )

    save_state(state)

    LOG.info(
        "Saved %d BingX checkpoints into %s; "
        "Telegram messages=%d",
        len(new_last),
        STATE_PATH,
        len(messages)
    )

    if errors:

        LOG.warning(
            "Failed symbols retain prior checkpoints "
            "and will be retried"
        )


if __name__ == "__main__":
    main()
