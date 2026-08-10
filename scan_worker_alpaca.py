"""
HTR Scan Worker (Alpaca edition) -- v3
------------------------------------------
Same three additions as scan_worker.py (Polygon edition) -- halt
detection, a no-volume-floor micro-float tier, and a relative-volume
display cap. See that file's docstring for the full rationale (the
DSY/MB/YJ case study from today's session).

Note: Alpaca's market data API doesn't expose float/shares-outstanding,
so the micro-float tier can't be applied here the same way -- flagged,
not faked. If float matters, layer in a separate reference-data call.

Env vars (required):
    ALPACA_API_KEY_ID, ALPACA_API_SECRET_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

Env vars (optional):
    PRICE_MIN (1)               PRICE_MAX (50)
    TIER1_PCT_CHANGE_MIN (0.10) TIER1_REL_VOLUME_MIN (1.5)
    TIER2_PCT_CHANGE_MIN (0.05) TIER2_VOLUME_MIN (700000)
    HALT_FREEZE_CYCLES (3)      RELVOL_DISPLAY_CAP (100)
    MOVERS_TOP (50)
"""

import os
import time
import html
import logging
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("scan_worker_alpaca")

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

HALT_FREEZE_CYCLES = int(os.environ.get("HALT_FREEZE_CYCLES", "3"))
RELVOL_DISPLAY_CAP = float(os.environ.get("RELVOL_DISPLAY_CAP", "100"))

MOVERS_TOP = int(os.environ.get("MOVERS_TOP", "50"))

DATA_BASE = "https://data.alpaca.markets"
HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY_ID,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
}

ET = ZoneInfo("America/New_York")
_tier1_alerted = set()
_tier2_alerted = set()
_halt_state = {}
_halted_symbols = set()


def current_interval_seconds():
    now = datetime.now(ET).time()
    if dtime(4, 0) <= now < dtime(7, 0):
        return 75
    if dtime(7, 0) <= now < dtime(9, 30):
        return 90
    if dtime(9, 30) <= now < dtime(15, 30):
        return 420
    if dtime(15, 30) <= now < dtime(16, 0):
        return 180
    if dtime(16, 0) <= now < dtime(20, 0):
        return 1100
    return 2400


def get_movers():
    url = f"{DATA_BASE}/v1beta1/screener/stocks/movers"
    params = {"top": MOVERS_TOP}
    r = requests.get(url, headers=HEADERS, params=params, timeout=10)
    r.raise_for_status()
    return r.json().get("gainers", [])


def get_snapshots(symbols):
    if not symbols:
        return {}
    url = f"{DATA_BASE}/v2/stocks/snapshots"
    params = {"symbols": ",".join(symbols), "feed": "iex"}
    r = requests.get(url, headers=HEADERS, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        log.error("Telegram send failed: %s", e)


def maybe_reset_daily_state():
    now = datetime.now(ET)
    key = now.strftime("%Y-%m-%d")
    if getattr(maybe_reset_daily_state, "_day", None) != key:
        maybe_reset_daily_state._day = key
        _tier1_alerted.clear()
        _tier2_alerted.clear()
        _halt_state.clear()
        _halted_symbols.clear()
        log.info("New trading day: %s", key)


def format_relvol(rel_volume):
    if rel_volume >= RELVOL_DISPLAY_CAP:
        return f"{RELVOL_DISPLAY_CAP:.0f}x+ (extreme -- baseline volume was negligible)"
    return f"{rel_volume:.1f}x"


def check_halt_state(symbol, price, day_vol):
    prev = _halt_state.get(symbol)
    if prev is not None and prev["price"] == price and prev["vol"] == day_vol:
        prev["frozen_count"] += 1
    else:
        was_halted = symbol in _halted_symbols
        _halt_state[symbol] = {"price": price, "vol": day_vol, "frozen_count": 0}
        if was_halted:
            _halted_symbols.discard(symbol)
            return "reopened"
        return None

    if _halt_state[symbol]["frozen_count"] == HALT_FREEZE_CYCLES and symbol not in _halted_symbols:
        _halted_symbols.add(symbol)
        return "halted"
    return None


def run_once():
    maybe_reset_daily_state()
    try:
        movers = get_movers()
    except Exception as e:
        log.error("Movers fetch failed: %s", e)
        return

    candidates = []
    for m in movers:
        symbol = m.get("symbol")
        price = m.get("price")
        pct_change = m.get("percent_change")
        if symbol is None or price is None or pct_change is None:
            continue
        if not (PRICE_MIN <= price <= PRICE_MAX):
            continue
        if pct_change < TIER1_PCT_CHANGE_MIN * 100 and pct_change < TIER2_PCT_CHANGE_MIN * 100:
            continue
        candidates.append((symbol, price, pct_change))

    if not candidates:
        return

    try:
        snapshots = get_snapshots([c[0] for c in candidates])
    except Exception as e:
        log.error("Snapshot fetch failed: %s", e)
        return

    for symbol, price, pct_change in candidates:
        snap = snapshots.get(symbol) or {}
        day_bar = snap.get("dailyBar") or {}
        prev_bar = snap.get("prevDailyBar") or {}
        day_vol = day_bar.get("v") or 0
        prev_vol = prev_bar.get("v") or 0
        rel_volume = (day_vol / prev_vol) if prev_vol > 0 else None
        pct_change_ratio = pct_change / 100.0

        if symbol in _tier1_alerted or symbol in _tier2_alerted:
            halt_status = check_halt_state(symbol, price, day_vol)
            if halt_status == "halted":
                msg = (
                    f"\u23F8 <b>POSSIBLE HALT: {html.escape(symbol)}</b> ${price:.2f} "
                    f"({pct_change:+.1f}%)\n"
                    f"Price/volume unchanged for {HALT_FREEZE_CYCLES} consecutive checks."
                )
                log.info("HALT: %s", msg.replace("\n", " "))
                send_telegram(msg)
            elif halt_status == "reopened":
                msg = (
                    f"\u25B6\uFE0F <b>REOPENED: {html.escape(symbol)}</b> ${price:.2f} "
                    f"({pct_change:+.1f}%)\n"
                    f"Trading resumed after a freeze -- worth checking direction immediately."
                )
                log.info("REOPEN: %s", msg.replace("\n", " "))
                send_telegram(msg)

        # --- Tier 1 ---
        if symbol not in _tier1_alerted:
            if (
                pct_change_ratio >= TIER1_PCT_CHANGE_MIN
                and rel_volume is not None
                and rel_volume >= TIER1_REL_VOLUME_MIN
            ):
                _tier1_alerted.add(symbol)
                msg = (
                    f"\U0001F440 <b>EARLY WARNING: {html.escape(symbol)}</b> ${price:.2f} "
                    f"({pct_change:+.1f}%)\n"
                    f"RelVol: {format_relvol(rel_volume)} | Vol so far: {day_vol:,.0f}\n"
                    f"Source: Alpaca real-time (IEX) | Not yet confirmed."
                )
                log.info("TIER1: %s", msg.replace("\n", " "))
                send_telegram(msg)

        # --- Tier 2 ---
        if symbol not in _tier2_alerted:
            if pct_change_ratio >= TIER2_PCT_CHANGE_MIN and day_vol >= TIER2_VOLUME_MIN:
                _tier2_alerted.add(symbol)
                relvol_str = format_relvol(rel_volume) if rel_volume is not None else "n/a"
                msg = (
                    f"\U0001F6A8 <b>CONFIRMED: {html.escape(symbol)}</b> ${price:.2f} "
                    f"({pct_change:+.1f}%)\n"
                    f"Vol: {day_vol:,.0f} | RelVol: {relvol_str}\n"
                    f"Source: Alpaca real-time (IEX)"
                )
                log.info("TIER2: %s", msg.replace("\n", " "))
                send_telegram(msg)


def main():
    log.info(
        "Scan worker (Alpaca) v3 starting. Tier1: %%chg>%s relvol>%sx | Tier2: %%chg>%s vol>%s | "
        "Halt freeze=%s cycles",
        TIER1_PCT_CHANGE_MIN, TIER1_REL_VOLUME_MIN, TIER2_PCT_CHANGE_MIN, TIER2_VOLUME_MIN,
        HALT_FREEZE_CYCLES,
    )
    while True:
        run_once()
        interval = current_interval_seconds()
        log.info("Sleeping %ss", interval)
        time.sleep(interval)


if __name__ == "__main__":
    main()
