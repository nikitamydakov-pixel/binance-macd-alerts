#!/usr/bin/env python3
"""Bybit V5 USDT perpetual MACD(12,26,9) zero-line Telegram alerts.

Runs on GitHub Actions every 5 minutes. Python 3.12+, standard library only.
Only confirmed/closed exchange candles are considered.
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

API_URL = "https://api.bybit.com"
INTERVAL_MINUTES = {"15m": 15}
KLINE_LIMIT = 240
MIN_CANDLES = 100
WARMUP_INDEX = 75
CLOSE_GRACE_MS = 2500
RPS = float(os.environ.get("BYBIT_REQUESTS_PER_SECOND", "7"))
WORKERS = int(os.environ.get("MAX_WORKERS", "8"))
STATE_PATH = Path(os.environ.get("STATE_PATH", ".state/state.json"))
HTTP_TIMEOUT = 18
LOG = logging.getLogger("bybit-macd")


class RequestLimiter:
    """Share request pacing and cooldown between all scanner threads."""

    def __init__(self, rps: float):
        if not 0 < rps <= 10:
            raise ValueError("BYBIT_REQUESTS_PER_SECOND must be in (0, 10]")
        self.delay = 1.0 / rps
        self.lock = threading.Lock()
        self.next_slot = 0.0
        self.blocked_until = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            deadline = max(now, self.next_slot, self.blocked_until)
            self.next_slot = deadline + self.delay
        pause = deadline - now
        if pause > 0:
            time.sleep(pause)

    def pause_all(self, seconds: float):
        with self.lock:
            self.blocked_until = max(self.blocked_until, time.monotonic() + seconds)
            self.next_slot = max(self.next_slot, self.blocked_until)


LIMITER = RequestLimiter(RPS)


def _request_json(url: str, *, method: str = "GET", payload=None, bybit=False) -> dict:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept": "application/json", "User-Agent": "bybit-macd-zero-telegram/1.0"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    safe_url = ("Telegram sendMessage" if "api.telegram.org/bot" in url
                else url.split("?", 1)[0])
    for attempt in range(5):
        if bybit:
            LIMITER.wait()
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as response:
                result = json.load(response)
            if bybit:
                code = result.get("retCode")
                if code != 0:
                    if code in (10006, 10000, 10016, 429) and attempt < 4:
                        delay = min(60, 5 * (2 ** attempt))
                        LIMITER.pause_all(delay)
                        LOG.warning("Bybit retCode %s for %s; retry in %ss", code, safe_url, delay)
                        continue
                    if code == 10009:
                        raise RuntimeError("Bybit API restricted in this runner's region (retCode 10009)")
                    raise RuntimeError(f"Bybit retCode {code} for {safe_url}: {result.get('retMsg', '')}")
            return result
        except urllib.error.HTTPError as exc:
            status = exc.code
            body = exc.read(250).decode("utf-8", errors="replace")
            if bybit and status == 403:
                raise RuntimeError(
                    "Bybit API returned HTTP 403: access blocked for this GitHub runner's "
                    "IP/region or IP rate limit. Bybit restricts some regions. "
                    "Do not repeatedly retry a blocked IP."
                ) from exc
            if status in (429, 500, 502, 503, 504) and attempt < 4:
                try:
                    delay = float(exc.headers.get("Retry-After", ""))
                except (ValueError, TypeError):
                    delay = min(60, 5 * (2 ** attempt))
                delay = max(5.0, min(90.0, delay))
                if bybit:
                    LIMITER.pause_all(delay)
                else:
                    time.sleep(delay)
                LOG.warning("HTTP %d for %s; retry in %.1fs", status, safe_url, delay)
                continue
            raise RuntimeError(f"HTTP {status} for {safe_url}: {body}") from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            if attempt == 4:
                raise RuntimeError(f"Network error for {safe_url}: {exc}") from exc
            time.sleep(min(12, 2 ** attempt))
    raise RuntimeError(f"Too many retries: {safe_url}")


def bybit_get(path: str, params: dict | None = None) -> dict:
    url = API_URL + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    return _request_json(url, bybit=True)


def get_server_ms() -> int:
    payload = bybit_get("/v5/market/time")
    if payload.get("time") is not None:
        return int(payload["time"])
    return int(payload["result"]["timeSecond"]) * 1000


def get_symbols() -> list[str]:
    """Get ALL Trading USDT-settled linear perpetuals, handling pagination."""
    symbols = set()
    cursor = ""
    seen_cursors = set()
    while True:
        params = {"category": "linear", "status": "Trading", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        result = bybit_get("/v5/market/instruments-info", params)["result"]
        instruments = result["list"]
        if not isinstance(instruments, list):
            raise RuntimeError("Bybit returned an invalid instrument list")
        for item in instruments:
            if (item.get("status") == "Trading"
                    and item.get("contractType") == "LinearPerpetual"
                    and item.get("quoteCoin") == "USDT"
                    and item.get("settleCoin") == "USDT"
                    and not item.get("isPreListing", False)):
                symbols.add(item["symbol"])
        next_cursor = result.get("nextPageCursor") or ""
        if not next_cursor:
            break
        if next_cursor in seen_cursors:
            raise RuntimeError("Bybit instrument pagination cursor repeated")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    if not symbols:
        raise RuntimeError("Bybit returned no Trading USDT perpetual contracts")
    return sorted(symbols)


def ema(values: list[float], length: int) -> list[float]:
    if not values:
        return []
    alpha = 2 / (length + 1)
    out = [values[0]]
    for value in values[1:]:
        out.append(alpha * value + (1 - alpha) * out[-1])
    return out


def macd(values: list[float]) -> tuple[list[float], list[float]]:
    fast = ema(values, 12)
    slow = ema(values, 26)
    line = [a - b for a, b in zip(fast, slow)]
    signal = ema(line, 9)
    return line, signal


def crossing(before: float, after: float) -> str | None:
    if before <= 0 < after:
        return "LONG"
    if before >= 0 > after:
        return "SHORT"
    return None


def format_utc(milliseconds: int) -> str:
    return dt.datetime.fromtimestamp(milliseconds / 1000, tz=dt.timezone.utc).strftime("%d.%m %H:%M UTC")


def latest_closed_boundary(server_ms: int, interval: str) -> int:
    period = INTERVAL_MINUTES[interval] * 60_000
    return ((server_ms - CLOSE_GRACE_MS) // period) * period


def load_state() -> dict:
    if not STATE_PATH.exists():
        LOG.info("No previous Bybit state: first run initializes checkpoints without historical alerts")
        return {"version": 2, "exchange": "bybit", "last": {}}
    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    if (state.get("version") != 2 or state.get("exchange") != "bybit"
            or not isinstance(state.get("last"), dict)):
        raise RuntimeError("Invalid state schema: need Bybit state version 2, not Binance state")
    return state


def save_state(state: dict):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    tmp.replace(STATE_PATH)


@dataclass(frozen=True)
class Alert:
    symbol: str
    interval: str
    direction: str
    candle_close_ms: int
    old_macd: float
    new_macd: float

    def display(self) -> str:
        icon = "🟢" if self.direction == "LONG" else "🔴"
        chart_symbol = urllib.parse.quote("BYBIT:" + self.symbol + ".P", safe="")
        timeframe = INTERVAL_MINUTES[self.interval]
        chart = f"https://www.tradingview.com/chart/?symbol={chart_symbol}&interval={timeframe}"
        return (
            f"{icon} {self.direction} | {self.symbol} | {self.interval}\n"
            f"Закрытие: {format_utc(self.candle_close_ms)}\n"
            f"MACD: {self.old_macd:.9g} → {self.new_macd:.9g}\n"
            f"{chart}"
        )


def scan_symbol(symbol: str, interval: str, server_ms: int, last_ms: int | None):
    """Fetch Bybit newest-first candles, reverse, exclude forming candles, scan after checkpoint."""
    period = INTERVAL_MINUTES[interval] * 60_000
    result = bybit_get("/v5/market/kline", {
        "category": "linear", "symbol": symbol,
        "interval": str(INTERVAL_MINUTES[interval]), "limit": KLINE_LIMIT,
    })["result"]
    raw = result["list"]
    if not isinstance(raw, list):
        raise RuntimeError(f"Invalid klines payload for {symbol} {interval}")
    # Bybit V5 returns newest first; each candle timestamp is its START, not close.
    candles = sorted(
        ((int(row[0]), float(row[4])) for row in raw
         if int(row[0]) + period <= server_ms - CLOSE_GRACE_MS),
        key=lambda candle: candle[0],
    )
    if len(candles) < MIN_CANDLES:
        return [], None, "young"
    if any(not math.isfinite(close) or close <= 0 for _, close in candles):
        raise ValueError(f"Invalid candle close for {symbol} {interval}")
    if any(candles[i][0] <= candles[i - 1][0] for i in range(1, len(candles))):
        raise ValueError(f"Repeated candle time for {symbol} {interval}")
    closes = [c for _, c in candles]
    line, _ = macd(closes)
    newest_close = candles[-1][0] + period
    if last_ms is None:
        return [], newest_close, "bootstrapped"
    earliest = candles[WARMUP_INDEX][0] + period
    if last_ms < earliest:
        LOG.warning("%s %s: checkpoint too old, scanning from %s", symbol, interval, format_utc(earliest))
    alerts = []
    for index in range(WARMUP_INDEX, len(candles)):
        close_ms = candles[index][0] + period
        if close_ms <= last_ms:
            continue
        direction = crossing(line[index - 1], line[index])
        if direction:
            alerts.append(Alert(symbol, interval, direction, close_ms, line[index - 1], line[index]))
    return alerts, newest_close, "ok"


def group_messages(alerts: list[Alert]) -> list[str]:
    header = "Bybit MACD (12,26,9): пересечение нуля на закрытой свече\n"
    messages = []
    current = header
    for alert in alerts:
        entry = "\n" + alert.display() + "\n"
        if len(current) + len(entry) > 3500 and current != header:
            messages.append(current.rstrip())
            current = header
        current += entry
    if current != header:
        messages.append(current.rstrip())
    return messages


def telegram_send(token: str, chat_id: str, message: str):
    endpoint = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": message, "disable_web_page_preview": True}
    response = _request_json(endpoint, method="POST", payload=data)
    if not response.get("ok"):
        raise RuntimeError(f"Telegram error: {response.get('description', 'unknown')}")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise RuntimeError("Configure TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in GitHub repository secrets")
    if not 1 <= WORKERS <= 12:
        raise ValueError("MAX_WORKERS must be in [1,12]")

    state = load_state()
    old_last = state["last"]
    server_ms = get_server_ms()
    symbols = get_symbols()
    LOG.info("Bybit: %d active USDT perpetual symbols", len(symbols))

    new_last = dict(old_last)
    alerts: list[Alert] = []
    errors = []
    young = 0
    bootstrapped = 0
    skipped = 0
    tasks = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        for symbol in symbols:
            for interval in INTERVAL_MINUTES:
                key = f"{symbol}|{interval}"
                last_ms = old_last.get(key)
                # A 15m candle changes only every 15 minutes. Skip unnecessary calls.
                if last_ms is not None and last_ms >= latest_closed_boundary(server_ms, interval):
                    skipped += 1
                    continue
                fut = executor.submit(scan_symbol, symbol, interval, server_ms, last_ms)
                tasks[fut] = key
        for index, fut in enumerate(concurrent.futures.as_completed(tasks), start=1):
            key = tasks[fut]
            try:
                batch, newest_ms, status = fut.result()
            except Exception as exc:
                errors.append((key, str(exc)))
                LOG.error("Failed %s: %s", key, exc)
                continue
            if newest_ms is not None:
                new_last[key] = newest_ms
            alerts.extend(batch)
            young += status == "young"
            bootstrapped += status == "bootstrapped"
            if index % 200 == 0:
                LOG.info("Processed %d/%d requested pairs", index, len(tasks))

    count = len(tasks)
    LOG.info("Processed: %d requested / %d skipped; %d errors, %d young, %d baseline, %d alerts",
             count, skipped, len(errors), young, bootstrapped, len(alerts))
    if count and len(errors) > max(10, count // 5):
        raise RuntimeError(f"Too many failed Bybit requests: {len(errors)}/{count}. State NOT saved")

    alerts.sort(key=lambda x: (x.candle_close_ms, x.interval, x.symbol))
    messages = group_messages(alerts)
    if not old_last:
        messages.insert(0,
            f"✅ Bybit MACD-сканер запущен.\n"
            f"Активных USDT Perpetual: {len(symbols)}\n"
            "Таймфреймы: 15m\n"
            "Первый запуск: только синхронизация. Сигналы начнутся со следующих закрытых свечей."
            + (f"\nОшибок проверки: {len(errors)}" if errors else ""))
    # A network/persistence failure AFTER Telegram delivery can lead to a duplicate
    # on retry. Exactly-once delivery across external APIs is not mathematically guaranteed.
    for i, message in enumerate(messages):
        if i:
            time.sleep(1.2)
        telegram_send(token, chat_id, message)

    state["last"] = new_last
    state["updated_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    save_state(state)
    LOG.info("Saved %d Bybit checkpoints to %s; Telegram messages: %d",
             len(new_last), STATE_PATH, len(messages))
    if errors:
        LOG.warning("Failed pairs kept their previous checkpoints and will be retried")


if __name__ == "__main__":
    main()
