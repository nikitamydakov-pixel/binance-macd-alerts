#!/usr/bin/env python3
"""MACD(12,26,9) zero-line alerts for Binance USDⓈ-M USDT futures.

Python 3.12+, standard library only. Designed for GitHub Actions every 5 minutes.
State is restored/saved by the workflow as an Actions artifact.
"""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import json
import logging
import os
from pathlib import Path
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass

BINANCE_API = "https://fapi.binance.com"
INTERVALS = ("5m", "15m")
KLINE_LIMIT = 210   # 2 request-weight units per symbol/interval (100 <= limit < 500)
MIN_CANDLES = 100  # enough warm-up to keep MACD near TradingView's long-history EMA
WARMUP_INDEX = 75  # ignore initial bars where EMA seeding can influence zero crosses
CLOSE_GRACE_MS = 2500
RPS = float(os.environ.get("BINANCE_REQUESTS_PER_SECOND", "7"))
WORKERS = int(os.environ.get("MAX_WORKERS", "8"))
STATE_PATH = Path(os.environ.get("STATE_PATH", ".state/state.json"))
HTTP_TIMEOUT = 18
LOG = logging.getLogger("macd-scanner")


class RequestLimiter:
    """Process-wide request pacing; Binance rate limits are by *shared public IP*."""

    def __init__(self, rps: float):
        if rps <= 0 or rps > 10:
            raise ValueError("BINANCE_REQUESTS_PER_SECOND must be in (0, 10]")
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


def _request_json(url: str, method="GET", payload=None, *, binance=False):
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"User-Agent": "github-macd-zero-scanner/1.0", "Accept": "application/json"}
    if body is not None:
        headers["Content-Type"] = "application/json"
    safe_url = ("https://api.telegram.org/bot<redacted>/sendMessage"
                if url.startswith("https://api.telegram.org/bot") else url.split("?")[0])
    retries = 5
    for attempt in range(retries):
        if binance:
            LIMITER.wait()
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            status = exc.code
            error_body = exc.read(350).decode("utf-8", errors="replace")
            if binance and status == 451:
                raise RuntimeError(
                    "Binance Futures returned HTTP 451: market data blocked for this "
                    "GitHub runner's IP/region. GitHub-hosted runners may be in a "
                    "restricted location. A compliant runner with access to Binance "
                    "is required."
                ) from exc
            if binance and status == 418:
                raise RuntimeError("Binance temporary IP ban (HTTP 418); stopping requests") from exc
            if status in (429, 500, 502, 503, 504) and attempt < retries - 1:
                retry_after = exc.headers.get("Retry-After", "")
                try:
                    delay = float(retry_after)
                except ValueError:
                    delay = min(65, 2 ** (attempt + 2))
                delay = max(5.0, min(delay, 180.0))
                if binance:
                    LIMITER.pause_all(delay if status != 429 else max(30.0, delay))
                else:
                    time.sleep(delay)
                LOG.warning("HTTP %s; retrying %s in %.1fs", status, safe_url, delay)
                continue
            raise RuntimeError(f"HTTP {status} for {safe_url}: {error_body}") from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            if attempt == retries - 1:
                raise RuntimeError(f"Network failure: {safe_url}: {exc}") from exc
            time.sleep(min(16, 2 ** attempt))
    raise RuntimeError("Unexpected retry loop exit")


def binance_get(path: str, params: dict | None = None):
    url = BINANCE_API + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    return _request_json(url, binance=True)


def ema(values: list[float], length: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (length + 1)
    out = [values[0]]  # matches ta.ema-style first-sample initialization
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


def format_utc(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).strftime("%d.%m %H:%M UTC")


def load_state() -> dict:
    if not STATE_PATH.is_file():
        LOG.info("Previous state not found: initial synchronization (no historical alerts).")
        return {"version": 1, "last": {}}
    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    if state.get("version") != 1 or not isinstance(state.get("last"), dict):
        raise ValueError("Invalid state schema")
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
    candle_close_ms: int  # exclusive close boundary: closeTime + 1
    old_macd: float
    new_macd: float

    def display(self) -> str:
        icon = "🟢" if self.direction == "LONG" else "🔴"
        chart_symbol = urllib.parse.quote("BINANCE:" + self.symbol + ".P", safe="")
        tv_min = "5" if self.interval == "5m" else "15"
        chart = f"https://www.tradingview.com/chart/?symbol={chart_symbol}&interval={tv_min}"
        return (
            f"{icon} {self.direction} | {self.symbol} | {self.interval}\n"
            f"Закрытие: {format_utc(self.candle_close_ms)}\n"
            f"MACD: {self.old_macd:.9g} → {self.new_macd:.9g}\n"
            f"{chart}"
        )


def scan_symbol(symbol: str, interval: str, server_ms: int, last_ms: int | None):
    raw = binance_get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": KLINE_LIMIT})
    if not isinstance(raw, list):
        raise RuntimeError(f"Unexpected klines payload for {symbol} {interval}")
    candles = [row for row in raw if int(row[6]) + 1 <= server_ms - CLOSE_GRACE_MS]
    if len(candles) < MIN_CANDLES:
        # Newly listed instruments are checked again next run; no fabricated MACD.
        return [], None, "young"
    closes = [float(row[4]) for row in candles]
    if not all(x > 0 for x in closes):
        raise ValueError(f"Unexpected nonpositive close: {symbol} {interval}")
    macd_line, _ = macd(closes)
    newest_close = int(candles[-1][6]) + 1
    if last_ms is None:
        return [], newest_close, "bootstrapped"

    earliest_usable_close = int(candles[WARMUP_INDEX][6]) + 1
    if last_ms < earliest_usable_close:
        LOG.warning(
            "%s %s: previous state too old for full backfill; oldest scan date %s",
            symbol, interval, format_utc(earliest_usable_close),
        )
    alerts = []
    for i in range(WARMUP_INDEX, len(candles)):
        t = int(candles[i][6]) + 1
        if t <= last_ms:
            continue
        direction = crossing(macd_line[i - 1], macd_line[i])
        if direction:
            alerts.append(Alert(symbol, interval, direction, t, macd_line[i - 1], macd_line[i]))
    return alerts, newest_close, "ok"


def group_messages(alerts: list[Alert]) -> list[str]:
    """Send all alerts, grouping within Telegram's 4096-character limit."""
    messages = []
    header = "MACD (12,26,9): пересечение нуля на закрытой свече\n"
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
        raise RuntimeError(f"Telegram rejected message: {response.get('description', 'unknown error')}")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise RuntimeError("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in GitHub Actions secrets")

    state = load_state()
    old_last = state["last"]
    # Fetch server time once to freeze the universe of *closed* candles for this run.
    server_ms = int(binance_get("/fapi/v1/time")["serverTime"])
    info = binance_get("/fapi/v1/exchangeInfo")
    symbols = sorted({
        item["symbol"] for item in info["symbols"]
        if item.get("status") == "TRADING" and item.get("quoteAsset") == "USDT"
    })
    if not symbols:
        raise RuntimeError("No TRADING USDT futures returned by Binance")
    LOG.info("Found %d active USDT futures; scan = %d pairs", len(symbols), len(symbols) * len(INTERVALS))

    # Carry prior checkpoints for all symbols. Only successfully checked pairs advance.
    new_last = dict(old_last)
    alerts: list[Alert] = []
    errors = []
    young = 0
    bootstrapped = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {}
        for symbol in symbols:
            for interval in INTERVALS:
                key = f"{symbol}|{interval}"
                fut = executor.submit(scan_symbol, symbol, interval, server_ms, old_last.get(key))
                futures[fut] = key
        for idx, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            key = futures[fut]
            try:
                batch, newest_ms, status = fut.result()
            except Exception as exc:
                errors.append((key, str(exc)))
                LOG.error("Scan failed %s: %s", key, exc)
                continue
            if newest_ms is not None:
                new_last[key] = newest_ms
            alerts.extend(batch)
            young += status == "young"
            bootstrapped += status == "bootstrapped"
            if idx % 200 == 0:
                LOG.info("Scanned %s/%s pairs", idx, len(futures))

    total = len(futures)
    LOG.info("Done: %d scanned, %d errors, %d young, %d baseline, %d alerts",
             total, len(errors), young, bootstrapped, len(alerts))
    if len(errors) > max(10, total // 5):
        raise RuntimeError(f"Too many failed requests: {len(errors)}/{total}; state was NOT advanced")

    alerts.sort(key=lambda x: (x.candle_close_ms, x.interval, x.symbol))
    messages = group_messages(alerts)
    if not old_last:
        # A successful initial bootstrap is otherwise silent, which looks broken.
        messages.insert(0,
            f"✅ Сканер MACD запущен.\n"
            f"Активных USDT-фьючерсов: {len(symbols)}.\n"
            f"Таймфреймы: 5m, 15m.\n"
            "Первый запуск синхронизирует историю; сигналы придут по следующим закрытым свечам."
            + (f"\nНе удалось проверить пар: {len(errors)}; сканер повторит попытку." if errors else "")
        )
    for index, message in enumerate(messages):
        if index:
            time.sleep(1.2)  # Telegram: roughly <=1 message/s per private chat
        telegram_send(token, chat_id, message)
    # Write AFTER delivery. If Telegram fails, state artifact won't upload; retry later.
    state["last"] = new_last
    state["updated_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    save_state(state)
    LOG.info("Saved %d checkpoints to %s; sent %d Telegram messages", len(new_last), STATE_PATH, len(messages))
    if errors:
        LOG.warning("%d pairs failed; their checkpoint was left unchanged for recovery", len(errors))


if __name__ == "__main__":
    main()
