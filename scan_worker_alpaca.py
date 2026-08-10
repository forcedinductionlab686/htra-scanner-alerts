"""
HTR Scan Worker (Alpaca edition) -- v4, streaming
-----------------------------------------------------
This is a structural rewrite, not a patch. Two problems with the prior
REST-polling version (v3) motivated it:

1. Alpaca's free-tier REST endpoints (the ones v3 polled -- the movers
   screener and the snapshot lookup) are 15-minute delayed. Only the
   WebSocket feed is genuinely real-time on the free tier. v3 was
   unknowingly running on delayed data the whole time.

2. Even with real-time data, a "top gainers" list is structurally
   reactive: a symbol can only appear on it *after* it has already
   moved enough to qualify. That's why MB and YJ both showed up already
   at +200%+ the first time v3 ever saw them -- there's no way for a
   pre-filtered gainers list to include a stock before it starts moving.

This version fixes both by not using the movers/snapshot endpoints at
all during the trading day. Instead it opens a single WebSocket
connection, subscribes to *every* trade on the IEX feed (the free
tier's real-time source), and computes %change / relative volume
itself, tick by tick, in memory. A symbol fires the moment *our own*
threshold trips -- not whenever Alpaca's internal gainers algorithm
decides to surface it.

What this can't fix (structural limits of the free tier, not this
script): IEX real-time only reflects trades executed on the IEX
exchange specifically -- roughly a few percent of total US market
volume, not the full consolidated tape. A stock that trades almost
entirely on NYSE/Nasdaq with little IEX activity may still be caught
late, or missed, under this design. Full-tape coverage requires
Alpaca's paid SIP tier.

Architecture:
    - Once per trading day (pre-market): fetch the active US equity
      symbol universe, then batch-fetch each one's previous close and
      previous-day volume (a single REST call per batch -- reference
      data that's fine to be static/slightly stale, unlike live prices).
    - During the session: an open WebSocket stream delivers every IEX
      trade tick. Each tick updates that symbol's running volume and
      last price in memory, and Tier 1/Tier 2 logic runs on every tick.
    - A separate lightweight loop sweeps already-alerted symbols every
      60s during regular hours only, looking for ones that have gone
      quiet for several minutes -- that's the halt/reopen signal in a
      streaming world (no new ticks, instead of no new poll results).

Env vars (required):
    ALPACA_API_KEY_ID, ALPACA_API_SECRET_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

Env vars (optional):
    PRICE_MIN (1)                PRICE_MAX (50)
    TIER1_PCT_CHANGE_MIN (0.10)  TIER1_REL_VOLUME_MIN (1.5)
    TIER2_PCT_CHANGE_MIN (0.05)  TIER2_VOLUME_MIN (700000)
    HALT_QUIET_MINUTES (3)       RELVOL_DISPLAY_CAP (100)
    REFERENCE_CHUNK_SIZE (200)

Dependencies: alpaca-py (official SDK), requests
"""

import os
import time
import html
import logging
import threading
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import requests
from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("scan_worker_alpaca_stream")

ALPACA_KEY_ID = os.environ["ALPACA_API_KEY_ID"]
ALPACA_SECRET_KEY = os.environ["ALPACA_API_SECRET_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

PRICE_MIN = float(os.environ.get("PRICE_MIN", "1"))
PRICE_MAX = float(os.environ.get("PRICE_MAX", "50"))

TIER1_PCT_CHANGE_MIN = float(os.environ.get("TIER1_PCT_CHANGE_MIN", "0.10"))
TIER1_REL_VOLUME_MIN = float(os.environ.get("TIER1_REL_VOLUME_MIN", "1.5"))

TIER2_PCT_CHANGE_MIN = float(os.environ.get("TIER2_PCT_CHANGE_MIN", "0.05"))
TIER2_VOLUME_MIN = float(os.environ.get("TIER2_VOLUME_MIN", "700000"))

HALT_QUIET_MINUTES = float(os.environ.get("HALT_QUIET_MINUTES", "3"))
RELVOL_DISPLAY_CAP = float(os.environ.get("RELVOL_DISPLAY_CAP", "100"))
REFERENCE_CHUNK_SIZE = int(os.environ.get("REFERENCE_CHUNK_SIZE", "200"))

TRADING_BASE = "https://api.alpaca.markets"
DATA_BASE = "https://data.alpaca.markets"
HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY_ID,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
}

ET = ZoneInfo("America/New_York")
HALT_DETECTION_START = dtime(9, 30)
HALT_DETECTION_END = dtime(16, 0)

# --- Shared in-memory state, guarded by a lock since the WebSocket
# handler and the halt-sweep loop run on different threads ---
_lock = threading.Lock()
_reference = {}       # symbol -> {"prev_close": float, "prev_vol": float}
_live = {}            # symbol -> {"day_vol": float, "last_price": float, "last_trade_time": datetime}
_tier1_alerted = set()
_tier2_alerted = set()
_halted_symbols = set()
_current_day = None


def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        log.error("Telegram send failed: %s", e)


def format_relvol(rel_volume):
    if rel_volume is None:
        return "n/a"
    if rel_volume >= RELVOL_DISPLAY_CAP:
        return f"{RELVOL_DISPLAY_CAP:.0f}x+ (extreme -- baseline volume was negligible)"
    return f"{rel_volume:.1f}x"


def fetch_active_symbols():
    """Once-daily call: the tradable US equity universe. This is
    reference/setup data, not a live-price call, so staleness doesn't
    matter the way it did for the old REST movers polling."""
    url = f"{TRADING_BASE}/v2/assets"
    params = {"status": "active", "asset_class": "us_equity"}
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    assets = r.json()
    return [a["symbol"] for a in assets if a.get("tradable")]


def fetch_reference_data(symbols):
    """Batch-fetch previous close + previous volume for the whole
    universe, chunked to stay under Alpaca's per-request symbol limit.
    Also once-daily; this is exactly the kind of call that's fine to be
    a few minutes stale, unlike the live price stream."""
    reference = {}
    for i in range(0, len(symbols), REFERENCE_CHUNK_SIZE):
        chunk = symbols[i:i + REFERENCE_CHUNK_SIZE]
        url = f"{DATA_BASE}/v2/stocks/snapshots"
        params = {"symbols": ",".join(chunk), "feed": "iex"}
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning("Reference chunk %d failed: %s", i // REFERENCE_CHUNK_SIZE, e)
            continue
        for symbol, snap in (data or {}).items():
            if snap is None:
                continue
            prev_bar = snap.get("prevDailyBar") or {}
            prev_close = prev_bar.get("c")
            prev_vol = prev_bar.get("v")
            if prev_close is None:
                continue
            if not (PRICE_MIN <= prev_close <= PRICE_MAX * 3):
                # Loose upper bound here (3x PRICE_MAX) -- a stock near
                # the ceiling could still drop into range intraday, but
                # anything wildly outside is safe to skip watching.
                continue
            reference[symbol] = {"prev_close": prev_close, "prev_vol": prev_vol or 0}
    return reference


def reset_daily_state():
    global _current_day
    now = datetime.now(ET)
    key = now.strftime("%Y-%m-%d")
    if _current_day == key:
        return
    log.info("Refreshing reference data for new trading day: %s", key)
    symbols = fetch_active_symbols()
    log.info("Active US equity universe: %d symbols", len(symbols))
    reference = fetch_reference_data(symbols)
    log.info("Reference data loaded for %d symbols in price range", len(reference))
    with _lock:
        _reference.clear()
        _reference.update(reference)
        _live.clear()
        _tier1_alerted.clear()
        _tier2_alerted.clear()
        _halted_symbols.clear()
        _current_day = key


def evaluate_symbol(symbol, price, day_vol):
    """Runs Tier 1 / Tier 2 logic for one symbol given its current
    running state. Called on every trade tick."""
    ref = _reference.get(symbol)
    if ref is None or ref["prev_close"] <= 0:
        return
    if not (PRICE_MIN <= price <= PRICE_MAX):
        return

    pct_change = (price - ref["prev_close"]) / ref["prev_close"]
    prev_vol = ref["prev_vol"]
    rel_volume = (day_vol / prev_vol) if prev_vol > 0 else None

    if symbol not in _tier1_alerted:
        if pct_change >= TIER1_PCT_CHANGE_MIN and rel_volume is not None and rel_volume >= TIER1_REL_VOLUME_MIN:
            _tier1_alerted.add(symbol)
            msg = (
                f"\U0001F440 <b>EARLY WARNING: {html.escape(symbol)}</b> ${price:.2f} "
                f"({pct_change*100:+.1f}%)\n"
                f"RelVol: {format_relvol(rel_volume)} | Vol so far: {day_vol:,.0f}\n"
                f"Source: Alpaca streaming real-time (IEX) | Not yet confirmed."
            )
            log.info("TIER1: %s", msg.replace("\n", " "))
            send_telegram(msg)

    if symbol not in _tier2_alerted:
        if pct_change >= TIER2_PCT_CHANGE_MIN and day_vol >= TIER2_VOLUME_MIN:
            _tier2_alerted.add(symbol)
            msg = (
                f"\U0001F6A8 <b>CONFIRMED: {html.escape(symbol)}</b> ${price:.2f} "
                f"({pct_change*100:+.1f}%)\n"
                f"Vol: {day_vol:,.0f} | RelVol: {format_relvol(rel_volume)}\n"
                f"Source: Alpaca streaming real-time (IEX)"
            )
            log.info("TIER2: %s", msg.replace("\n", " "))
            send_telegram(msg)


async def handle_trade(trade):
    """Fires on every single IEX trade tick, for every symbol. This
    replaces the old polling loop entirely -- there's no sleep() here,
    the stream just delivers ticks as they happen."""
    symbol = trade.symbol
    price = float(trade.price)
    size = float(trade.size)
    now = datetime.now(ET)

    with _lock:
        if symbol not in _reference:
            return  # outside our watched price-range universe
        state = _live.setdefault(symbol, {"day_vol": 0.0, "last_price": price, "last_trade_time": now})
        state["day_vol"] += size
        state["last_price"] = price
        was_halted = symbol in _halted_symbols
        state["last_trade_time"] = now
        day_vol = state["day_vol"]
        if was_halted:
            _halted_symbols.discard(symbol)

    if was_halted:
        ref = _reference.get(symbol, {})
        pct_change = ((price - ref["prev_close"]) / ref["prev_close"]) if ref.get("prev_close") else 0
        msg = (
            f"\u25B6\uFE0F <b>REOPENED: {html.escape(symbol)}</b> ${price:.2f} "
            f"({pct_change*100:+.1f}%)\n"
            f"Trading resumed after going quiet -- worth checking direction immediately."
        )
        log.info("REOPEN: %s", msg.replace("\n", " "))
        send_telegram(msg)

    evaluate_symbol(symbol, price, day_vol)


def halt_sweep_loop():
    """Runs on its own thread. Every 60s during regular hours, checks
    whether any already-alerted symbol has gone quiet for
    HALT_QUIET_MINUTES -- the streaming equivalent of the old
    'frozen for N polls' check, but based on tick gaps instead."""
    while True:
        time.sleep(60)
        now = datetime.now(ET)
        if not (HALT_DETECTION_START <= now.time() < HALT_DETECTION_END):
            continue
        with _lock:
            watched = list(_tier1_alerted | _tier2_alerted)
            for symbol in watched:
                state = _live.get(symbol)
                if state is None or symbol in _halted_symbols:
                    continue
                quiet_minutes = (now - state["last_trade_time"]).total_seconds() / 60.0
                if quiet_minutes >= HALT_QUIET_MINUTES:
                    _halted_symbols.add(symbol)
                    price = state["last_price"]
                    ref = _reference.get(symbol, {})
                    pct_change = ((price - ref["prev_close"]) / ref["prev_close"]) if ref.get("prev_close") else 0
                    msg = (
                        f"\u23F8 <b>POSSIBLE HALT: {html.escape(symbol)}</b> ${price:.2f} "
                        f"({pct_change*100:+.1f}%)\n"
                        f"No trades for {quiet_minutes:.1f} minutes."
                    )
                    log.info("HALT: %s", msg.replace("\n", " "))
                    send_telegram(msg)


def daily_refresh_loop():
    """Runs on its own thread, checks once a minute whether the ET date
    has rolled over and refreshes reference data if so. Cheap to poll
    since it's just a local time comparison, not a network call, except
    on the actual rollover."""
    while True:
        reset_daily_state()
        time.sleep(60)


def main():
    log.info(
        "Scan worker (Alpaca streaming) v4 starting. Tier1: %%chg>%s relvol>%sx | "
        "Tier2: %%chg>%s vol>%s | Halt quiet=%s min",
        TIER1_PCT_CHANGE_MIN, TIER1_REL_VOLUME_MIN, TIER2_PCT_CHANGE_MIN, TIER2_VOLUME_MIN,
        HALT_QUIET_MINUTES,
    )

    reset_daily_state()

    threading.Thread(target=daily_refresh_loop, daemon=True).start()
    threading.Thread(target=halt_sweep_loop, daemon=True).start()

    stream = StockDataStream(ALPACA_KEY_ID, ALPACA_SECRET_KEY, feed=DataFeed.IEX)
    stream.subscribe_trades(handle_trade, "*")

    # stream.run() blocks and manages its own reconnect logic internally.
    # Wrap in a retry loop in case the connection drops in a way that
    # surfaces as an exception rather than an internal reconnect.
    while True:
        try:
            stream.run()
        except Exception as e:
            log.error("Stream crashed, reconnecting in 10s: %s", e)
            time.sleep(10)


if __name__ == "__main__":
    main()