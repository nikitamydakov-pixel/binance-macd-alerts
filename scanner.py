#!/usr/bin/env python3
"""BingX MACD zero-cross scanner: each Telegram signal includes a candle-chart PNG.

Uses GitHub Actions on a Windows self-hosted runner. Python 3.12 and matplotlib.
Both 5m and 15m are enabled to match the current scanner in GitHub.
"""
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

# matplotlib renders PNGs without opening a window on your Windows computer.
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle
from matplotlib.ticker import FuncFormatter

API_URL = "https://open-api.bingx.com"
TIMEFRAMES = {"5m": 5, "15m": 15}
MIN_TURNOVER_24H_USDT = 1_000_000.0
MACD_FAST, MACD_SLOW, MACD_SIGNAL = 12, 26, 9
EXTREMA_MIN_RATIO = 1.5  # independent max(abs(MACD)) / max(abs(histogram))
KLINE_LIMIT = 1440  # 5m bars for both timeframes
MIN_CANDLES = 100
WARMUP_INDEX = 75
CLOSE_GRACE_MS = 2500
CHART_BARS = 100
CHART_EMA_PERIOD = 50
SWING_WINDOW = 3
RPS = float(os.getenv("BINGX_REQUESTS_PER_SECOND", "1"))
WORKERS = int(os.getenv("MAX_WORKERS", "6"))
STATE_PATH = Path(os.getenv("STATE_PATH", str(Path.home() / "bingx-macd-state" / "state.json")))
HTTP_TIMEOUT = 25
LOG = logging.getLogger("bingx-macd")


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
    macd_bar: int
    hist_bar: int
    previous_cross_bar: int


@dataclass(frozen=True)
class Alert:
    symbol: str
    timeframe: str
    direction: str
    close_ms: int
    previous_macd: float
    current_macd: float
    extremes: Extremes
    macd_extreme_ms: int
    hist_extreme_ms: int
    prior_cross_ms: int
    chart_candles: tuple[Candle, ...]
    chart_ema: tuple[float, ...]

    @property
    def key(self) -> str:
        return f"{self.symbol}|{self.timeframe}|{self.close_ms}"

    def caption(self) -> str:
        sign = "🟢" if self.direction == "LONG" else "🔴"
        tv = "BINGX:" + self.symbol.replace("-", "") + ".P"
        link = "https://www.tradingview.com/chart/?" + urllib.parse.urlencode(
            {"symbol": tv, "interval": TIMEFRAMES[self.timeframe]}
        )
        return (
            f"{sign} {self.direction} | {self.symbol} | {self.timeframe}\n"
            f"Закрытие: {fmt_time(self.close_ms)}\n"
            f"MACD: {self.previous_macd:.8g} → {self.current_macd:.8g}\n"
            f"Экстремум |MACD|: {self.extremes.macd_abs:.8g}\n"
            f"Экстремум |гистограммы|: {self.extremes.hist_abs:.8g}\n"
            f"Соотношение: {self.extremes.ratio:.2f}× (мин. {EXTREMA_MIN_RATIO})\n"
            f"Предыдущее пересечение: {fmt_time(self.prior_cross_ms)}\n"
            f"Максимумы: {fmt_time(self.macd_extreme_ms)} / "
            f"{fmt_time(self.hist_extreme_ms)}\n"
            f"График: {link}"
        )


class RateLimiter:
    def __init__(self, rps: float):
        if not 0 < rps <= 10:
            raise ValueError("BINGX_REQUESTS_PER_SECOND must be in (0, 10]")
        self.lock = threading.Lock()
        self.step = 1 / rps
        self.next_at = 0.0
        self.blocked_until = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            start = max(now, self.next_at, self.blocked_until)
            self.next_at = start + self.step
        if start > now:
            time.sleep(start - now)

    def cooldown(self, seconds: float):
        with self.lock:
            self.blocked_until = max(self.blocked_until, time.monotonic() + seconds)
            self.next_at = max(self.next_at, self.blocked_until)


LIMITER = RateLimiter(RPS)


def request_json(url: str, *, bingx: bool = False, data: bytes | None = None,
                 headers: dict | None = None) -> dict:
    """Rate-limited market calls and Telegram POSTs with bounded retries."""
    hdrs = {"Accept": "application/json", "User-Agent": "bingx-macd-chart/1.0"}
    hdrs.update(headers or {})
    safe = "Telegram API" if "api.telegram.org/bot" in url else url.split("?", 1)[0]
    for attempt in range(5):
        if bingx:
            LIMITER.wait()
        req = urllib.request.Request(url, data=data, headers=hdrs,
                                     method="POST" if data is not None else "GET")
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as response:
                result = json.load(response)
            if bingx and str(result.get("code", "missing")) != "0":
                code = str(result.get("code"))
                if code in {"100410", "100500", "109500"} and attempt < 4:
                    delay = min(60, 5 * 2 ** attempt)
                    LIMITER.cooldown(delay)
                    LOG.warning("BingX code %s, retry after %ss", code, delay)
                    continue
                raise RuntimeError(f"BingX code {code}: {result.get('msg', '')}")
            return result
        except urllib.error.HTTPError as exc:
            if exc.code in {429, 500, 502, 503, 504} and attempt < 4:
                delay = min(60, 5 * 2 ** attempt)
                if bingx:
                    LIMITER.cooldown(delay)
                else:
                    time.sleep(delay)
                LOG.warning("HTTP %d at %s; retry in %ss", exc.code, safe, delay)
                continue
            detail = exc.read(200).decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} at {safe}: {detail}") from exc
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            if attempt == 4:
                raise RuntimeError(f"Network error at {safe}: {exc}") from exc
            time.sleep(min(12, 2 ** attempt))
    raise RuntimeError(f"Too many retries at {safe}")


def bingx_get(path: str, params: dict | None = None):
    url = API_URL + path + ("?" + urllib.parse.urlencode(params) if params else "")
    return request_json(url, bingx=True).get("data")


def get_server_ms() -> int:
    data = bingx_get("/openApi/swap/v2/server/time")
    if isinstance(data, dict) and data.get("serverTime") is not None:
        return int(data["serverTime"])
    if isinstance(data, (str, int)):
        return int(data)
    raise RuntimeError(f"Unrecognized BingX server time: {str(data)[:80]}")


def get_liquid_symbols() -> tuple[list[str], int]:
    contracts = bingx_get("/openApi/swap/v2/quote/contracts")
    if not isinstance(contracts, list) or not contracts:
        raise RuntimeError("BingX returned invalid contract list")
    active = {
        item["symbol"] for item in contracts
        if isinstance(item, dict) and str(item.get("status")) == "1"
        and item.get("currency") == "USDT"
        and str(item.get("symbol", "")).endswith("-USDT")
        and not str(item.get("symbol", "")).startswith("NCS")
    }
    if not active:
        raise RuntimeError("No active BingX USDT perpetuals")
    tickers = bingx_get("/openApi/swap/v2/quote/ticker")
    if isinstance(tickers, dict):
        tickers = [tickers]
    if not isinstance(tickers, list) or not tickers:
        raise RuntimeError("No BingX 24h ticker data")
    eligible = set()
    for ticker in tickers:
        if not isinstance(ticker, dict) or ticker.get("symbol") not in active:
            continue
        try:
            turnover = float(ticker["quoteVolume"])
        except (ValueError, TypeError, KeyError):
            continue
        if math.isfinite(turnover) and turnover >= MIN_TURNOVER_24H_USDT:
            eligible.add(ticker["symbol"])
    if not eligible:
        raise RuntimeError("No contracts passed 24h quoteVolume filter")
    LOG.info("24h turnover >= %.0f USDT: %d/%d contracts",
             MIN_TURNOVER_24H_USDT, len(eligible), len(active))
    return sorted(eligible), len(active)


def ema(values: list[float], length: int) -> list[float]:
    if not values:
        return []
    alpha = 2 / (length + 1)
    result = [values[0]]
    for price in values[1:]:
        result.append(alpha * price + (1 - alpha) * result[-1])
    return result


def calculate_macd(closes: list[float]) -> tuple[list[float], list[float]]:
    fast, slow = ema(closes, MACD_FAST), ema(closes, MACD_SLOW)
    line = [f - s for f, s in zip(fast, slow)]
    return line, ema(line, MACD_SIGNAL)


def cross_direction(before: float, after: float) -> str | None:
    if before <= 0 < after:
        return "LONG"
    if before >= 0 > after:
        return "SHORT"
    return None


def find_extrema(line: list[float], signal: list[float], index: int,
                 candles: list[Candle], period_ms: int) -> Extremes | None:
    previous = None
    for j in range(index - 1, WARMUP_INDEX, -1):
        if cross_direction(line[j - 1], line[j]):
            previous = j
            break
    if previous is None:
        return None
    if any(candles[j].start - candles[j - 1].start != period_ms
           for j in range(previous, index + 1)):
        return None
    indexes = range(previous, index)  # exclude current crossing bar
    macd_i = max(indexes, key=lambda j: abs(line[j]))
    hist_i = max(indexes, key=lambda j: abs(line[j] - signal[j]))
    macd_max = abs(line[macd_i])
    hist_max = abs(line[hist_i] - signal[hist_i])
    if not all(map(math.isfinite, (macd_max, hist_max))) or hist_max <= 0:
        return None
    ratio = macd_max / hist_max
    if ratio < EXTREMA_MIN_RATIO:
        return None
    return Extremes(macd_max, hist_max, ratio, macd_i, hist_i, previous)


def parse_five_minute_candles(raw: list, server_ms: int) -> list[Candle]:
    """BingX object {time,open,high,low,close} or positional OHLC array."""
    period = 5 * 60_000
    if not isinstance(raw, list):
        raise ValueError("Invalid BingX kline list")
    parsed: dict[int, Candle] = {}
    for item in raw:
        try:
            if isinstance(item, dict):
                timestamp = next((item.get(k) for k in ("time", "openTime", "startTime")
                                  if item.get(k) is not None), None)
                c = Candle(int(timestamp), float(item["open"]), float(item["high"]),
                           float(item["low"]), float(item["close"]))
            elif isinstance(item, (list, tuple)) and len(item) >= 5:
                c = Candle(int(item[0]), float(item[1]), float(item[2]),
                           float(item[3]), float(item[4]))
            else:
                raise ValueError("Unsupported kline format")
        except (KeyError, TypeError, ValueError) as exc:
            shape = sorted(item) if isinstance(item, dict) else type(item).__name__
            raise ValueError(f"Malformed BingX OHLC candle: {shape}") from exc
        if c.start % period:
            raise ValueError(f"Unaligned 5m candle: {c.start}")
        if not all(math.isfinite(v) and v > 0 for v in (c.open, c.high, c.low, c.close)):
            raise ValueError(f"Invalid OHLC price at {c.start}")
        if c.high < max(c.open, c.close, c.low) or c.low > min(c.open, c.close):
            raise ValueError(f"Invalid high/low at {c.start}")
        if c.start + period <= server_ms - CLOSE_GRACE_MS:
            parsed[c.start] = c
    return [parsed[t] for t in sorted(parsed)]


def build_fifteen_minute_candles(five: list[Candle]) -> list[Candle]:
    p5, p15 = 5 * 60_000, 15 * 60_000
    candles = {c.start: c for c in five}
    buckets = sorted({c.start // p15 * p15 for c in five})
    result = []
    for t in buckets:
        if not all(t + i * p5 in candles for i in range(3)):
            continue
        a, b, c = (candles[t + i * p5] for i in range(3))
        result.append(Candle(t, a.open, max(a.high, b.high, c.high),
                             min(a.low, b.low, c.low), c.close))
    return result


def fmt_time(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).strftime("%d.%m %H:%M UTC")


def closed_boundary(server_ms: int, timeframe: str) -> int:
    period = TIMEFRAMES[timeframe] * 60_000
    return (server_ms - CLOSE_GRACE_MS) // period * period


def scan_timeframe(symbol: str, timeframe: str, candles: list[Candle],
                   last_ms: int | None) -> tuple[list[Alert], int | None, str]:
    if len(candles) < MIN_CANDLES:
        return [], None, "young"
    duration = TIMEFRAMES[timeframe] * 60_000
    closes = [c.close for c in candles]
    line, signal = calculate_macd(closes)
    ema_chart = ema(closes, CHART_EMA_PERIOD)
    newest = candles[-1].start + duration
    if last_ms is None:
        return [], newest, "bootstrapped"
    alerts = []
    for i in range(WARMUP_INDEX + 1, len(candles)):
        bar_close = candles[i].start + duration
        if bar_close <= last_ms:
            continue
        if candles[i].start - candles[i - 1].start != duration:
            continue
        direction = cross_direction(line[i - 1], line[i])
        if not direction:
            continue
        extreme = find_extrema(line, signal, i, candles, duration)
        if extreme is None:
            continue
        first = max(0, i + 1 - CHART_BARS)
        alerts.append(Alert(
            symbol, timeframe, direction, bar_close,
            line[i - 1], line[i], extreme,
            candles[extreme.macd_bar].start + duration,
            candles[extreme.hist_bar].start + duration,
            candles[extreme.previous_cross_bar].start + duration,
            tuple(candles[first:i + 1]), tuple(ema_chart[first:i + 1]),
        ))
    return alerts, newest, "ok"


def scan_symbol(symbol: str, server_ms: int, start_mono: float, state: dict):
    # Only one 5m API request; 15m OHLC assembled from complete sets of 3.
    raw = bingx_get("/openApi/swap/v3/quote/klines", {
        "symbol": symbol, "interval": "5m", "limit": KLINE_LIMIT,
    })
    effective_time = server_ms + int((time.monotonic() - start_mono) * 1000)
    five = parse_five_minute_candles(raw, effective_time)
    fifteen = build_fifteen_minute_candles(five)
    sources = {"5m": five, "15m": fifteen}
    return {
        tf: scan_timeframe(symbol, tf, sources[tf], state.get(f"{symbol}|{tf}"))
        for tf in TIMEFRAMES
    }


def fmt_price(value: float) -> str:
    if value >= 100:
        return f"{value:,.2f}"
    if value >= 1:
        return f"{value:.4f}"
    if value >= 0.01:
        return f"{value:.5f}"
    if value >= 0.001:
        return f"{value:.6f}"
    return f"{value:.8g}"


def auto_levels(candles: tuple[Candle, ...]) -> tuple[float | None, list[float]]:
    """Automatic chart-only support/resistance via OHLC swing lows/highs.

    These levels are NOT the manual lines from the example screenshot.
    """
    if len(candles) < 2 * SWING_WINDOW + 1:
        return None, []
    price = candles[-1].close
    lows, highs = [], []
    for i in range(SWING_WINDOW, len(candles) - SWING_WINDOW):
        section = candles[i - SWING_WINDOW:i + SWING_WINDOW + 1]
        if candles[i].low == min(c.low for c in section):
            lows.append(candles[i].low)
        if candles[i].high == max(c.high for c in section):
            highs.append(candles[i].high)
    all_lows = [c.low for c in candles]
    all_highs = [c.high for c in candles]
    supports = [x for x in lows if x < price] or [x for x in all_lows if x < price]
    support = max(supports) if supports else None
    candidates = sorted(x for x in highs if x > price)
    if len(candidates) < 2:
        candidates.extend(x for x in all_highs if x > price)
        candidates.sort()
    resistances = []
    for x in candidates:
        if all(abs(x - chosen) >= max(price * 0.004, 1e-12) for chosen in resistances):
            resistances.append(x)
        if len(resistances) == 2:
            break
    return support, resistances


def render_chart(alert: Alert) -> bytes:
    """Return a PNG resembling the submitted dark candle/EMA/level chart."""
    candles = alert.chart_candles
    if not candles or len(candles) != len(alert.chart_ema):
        raise ValueError("Missing OHLC or EMA data for chart")
    support, resistance = auto_levels(candles)
    color_up, color_down, color_ema = "#00a126", "#ff1919", "#ffff00"
    last = candles[-1].close

    fig = Figure(figsize=(14, 9), dpi=110, facecolor="black")
    FigureCanvasAgg(fig)
    ax = fig.add_axes([0.08, 0.10, 0.82, 0.79], facecolor="black")
    for i, c in enumerate(candles):
        color = color_up if c.close >= c.open else color_down
        ax.vlines(i, c.low, c.high, colors=color, linewidth=0.9, zorder=2)
        lo = min(c.open, c.close)
        height = max(abs(c.close - c.open), max(last * 0.00008, 1e-12))
        ax.add_patch(Rectangle((i - 0.31, lo), 0.62, height,
                               facecolor=color, edgecolor=color, linewidth=0.5, zorder=3))

    # EMA is plotted from candle-close history. Last bar = signal bar, not a live quote.
    ax.plot(range(len(candles)), alert.chart_ema, color=color_ema,
            linewidth=1.6, zorder=4, label=f"EMA {CHART_EMA_PERIOD}")
    handles = []
    if support is not None:
        ax.axhline(support, color="#008000", linewidth=4.2, alpha=0.95, zorder=1)
        handles.append(Line2D([0], [0], color="#008000", linewidth=4,
                              label=f"Уровень {fmt_price(support)}"))
    for level in resistance:
        ax.axhline(level, color="#ff1414", linewidth=4.2, alpha=0.96, zorder=1)
        handles.append(Line2D([0], [0], color="#ff1414", linewidth=4,
                              label=f"Уровень {fmt_price(level)}"))
    handles.append(Line2D([0], [0], color=color_ema, lw=1.7,
                          label=f"EMA {CHART_EMA_PERIOD}"))
    ax.hlines(last, len(candles) - 1, len(candles) + 4,
              colors=color_ema, linestyles=":", linewidth=1.2, zorder=3)

    minimum = min([c.low for c in candles] + list(alert.chart_ema) +
                  ([support] if support is not None else []) + resistance)
    maximum = max([c.high for c in candles] + list(alert.chart_ema) +
                  ([support] if support is not None else []) + resistance)
    padding = max((maximum - minimum) * 0.10, last * 0.01)
    ax.set_ylim(minimum - padding, maximum + padding)
    ax.set_xlim(-1, len(candles) + 4)
    ax.yaxis.tick_right()
    ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: fmt_price(y)))
    ax.tick_params(axis="y", colors="#e3e3e3", labelsize=10, length=0, pad=10)
    ax.tick_params(axis="x", bottom=False, labelbottom=False)
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.set_title(
        f"{alert.symbol.replace('-', '')} {alert.timeframe} "
        f"(Цена закрытия ${fmt_price(last)})",
        color="white", fontsize=19, pad=23,
    )
    legend = ax.legend(handles=handles, loc="upper left", fontsize=11,
                       facecolor="#f1f1f1", edgecolor="#dddddd", framealpha=0.88)
    for text in legend.get_texts():
        text.set_color("black")
    fig.text(0.49, 0.055,
             f"{alert.direction} • {fmt_time(alert.close_ms)} • Уровни рассчитаны автоматически",
             ha="center", va="center", fontsize=9, color="#c6c6c6")
    buf = io.BytesIO()
    fig.savefig(buf, format="png", facecolor="black", dpi=110)
    return buf.getvalue()


def telegram_send_photo(token: str, chat_id: str, alert: Alert):
    """Telegram sendPhoto with multipart PNG; exactly one photo+caption per alert."""
    png = render_chart(alert)
    if len(png) > 9_000_000:
        raise RuntimeError("PNG too large for Telegram sendPhoto")
    caption = alert.caption()
    if len(caption) > 1024:
        raise ValueError("Telegram photo caption too long")
    boundary = "----BingXAlert" + uuid.uuid4().hex
    chunks = []
    for name, value in (("chat_id", chat_id), ("caption", caption)):
        chunks.append((f"--{boundary}\r\nContent-Disposition: form-data; "
                       f'name="{name}"\r\n\r\n{value}\r\n').encode("utf-8"))
    chunks.append((f"--{boundary}\r\nContent-Disposition: form-data; "
                   'name="photo"; filename="bingx_chart.png"\r\n'
                   "Content-Type: image/png\r\n\r\n").encode("utf-8"))
    chunks.extend([png, f"\r\n--{boundary}--\r\n".encode("ascii")])
    body = b"".join(chunks)
    result = request_json(f"https://api.telegram.org/bot{token}/sendPhoto", data=body,
                          headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})
    if not result.get("ok"):
        raise RuntimeError(f"Telegram sendPhoto failed: {result.get('description', '')}")


def telegram_send_text(token: str, chat_id: str, text: str):
    payload = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
    result = request_json(f"https://api.telegram.org/bot{token}/sendMessage",
                          data=payload, headers={"Content-Type": "application/json"})
    if not result.get("ok"):
        raise RuntimeError(f"Telegram message rejected: {result.get('description', '')}")


def load_state() -> dict:
    if not STATE_PATH.exists():
        return {"version": 3, "exchange": "bingx", "last": {}, "sent": {}}
    result = json.loads(STATE_PATH.read_text(encoding="utf-8-sig"))
    if (result.get("version") != 3 or result.get("exchange") != "bingx"
            or not isinstance(result.get("last"), dict)):
        raise RuntimeError("Existing state is not BingX v3")
    if "sent" not in result:
        result["sent"] = {}
    if not isinstance(result["sent"], dict):
        raise RuntimeError("Invalid sent-message checkpoint format")
    return result


def save_state(state: dict):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp = STATE_PATH.with_suffix(".tmp")
    temp.write_text(json.dumps(state, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    temp.replace(STATE_PATH)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        raise RuntimeError("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in GitHub Secrets")
    if not 1 <= WORKERS <= 12:
        raise ValueError("MAX_WORKERS must be between 1 and 12")

    state = load_state()
    old_last = state["last"]
    server_ms = get_server_ms()
    scan_start_mono = time.monotonic()
    symbols, total = get_liquid_symbols()
    eligible = set(symbols)
    new_last = {key: value for key, value in old_last.items()
                if key.partition("|")[0] in eligible and key.partition("|")[2] in TIMEFRAMES}

    alerts, errors, jobs = [], [], {}
    skipped = bootstrapped = young = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for symbol in symbols:
            if all(old_last.get(f"{symbol}|{tf}") is not None
                   and old_last[f"{symbol}|{tf}"] >= closed_boundary(server_ms, tf)
                   for tf in TIMEFRAMES):
                skipped += 1
                continue
            future = pool.submit(scan_symbol, symbol, server_ms, scan_start_mono, old_last)
            jobs[future] = symbol
        for count, future in enumerate(concurrent.futures.as_completed(jobs), 1):
            symbol = jobs[future]
            try:
                results = future.result()
            except Exception as exc:
                errors.append((symbol, str(exc)))
                LOG.error("BingX scan failed %s: %s", symbol, exc)
                continue
            for tf, (found, newest, status) in results.items():
                if newest is not None:
                    new_last[f"{symbol}|{tf}"] = newest
                alerts.extend(found)
                bootstrapped += status == "bootstrapped"
                young += status == "young"
            if count % 100 == 0:
                LOG.info("Processed %d/%d symbols", count, len(jobs))

    LOG.info("BingX %s: eligible=%d/%d requested=%d skipped=%d "
             "bootstrapped=%d young=%d errors=%d alerts=%d",
             ",".join(TIMEFRAMES), len(symbols), total, len(jobs), skipped,
             bootstrapped, young, len(errors), len(alerts))
    if jobs and len(errors) > max(10, len(jobs) // 5):
        raise RuntimeError(f"Too many BingX errors: {len(errors)}/{len(jobs)}; state unchanged")

    alerts.sort(key=lambda a: (a.close_ms, TIMEFRAMES[a.timeframe], a.symbol))
    if not any("|" in key for key in old_last):
        telegram_send_text(token, chat_id,
                           f"✅ BingX MACD scanner: {len(symbols)} / {total} USDT perpetuals, "
                           f"timeframes: {', '.join(TIMEFRAMES)}. "
                           "Charts are attached to each new signal.")

    # A persisted sent key guards against repeating photos after a later job failure.
    # Network response ambiguity means mathematically exact-once delivery is impossible.
    sent = state["sent"]
    delivered = 0
    for alert in alerts:
        if alert.key in sent:
            continue
        telegram_send_photo(token, chat_id, alert)
        sent[alert.key] = alert.close_ms
        save_state(state)  # checkpoint each accepted Telegram photo before the next one
        delivered += 1
        time.sleep(1.1)

    state["last"] = new_last
    # Retain recent photo IDs for 14 days; older signals cannot normally reappear.
    now_ms = int(time.time() * 1000)
    state["sent"] = {key: value for key, value in sent.items()
                     if isinstance(value, (int, float)) and value >= now_ms - 14 * 86400_000}
    state["updated_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    save_state(state)
    LOG.info("Saved %d checkpoints; signal photos=%d; scan failures=%d",
             len(new_last), delivered, len(errors))
    if errors:
        LOG.warning("Failed symbols preserve previous checkpoints for retry")


if __name__ == "__main__":
    main()
