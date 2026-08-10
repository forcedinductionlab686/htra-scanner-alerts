"""
HTR Filing Watcher
------------------
Polls SEC EDGAR's real-time filing feed for new 8-K / 6-K filings, filters
them down to a target sector (via SIC code), and sends a Telegram alert
for anything that matches. Built to catch after-hours catalyst filings
(mergers, offerings, ATM terminations, etc.) that price/volume-based
scanners are structurally blind to, since those filings post before any
price reaction shows up in a candle.

Env vars (required):
    TELEGRAM_BOT_TOKEN   - from @BotFather
    TELEGRAM_CHAT_ID     - your chat/user id (see README)
    SEC_USER_AGENT        - "Your Name your-email@example.com" (SEC requires
                             a real contact string on every request)

Env vars (optional):
    TARGET_SIC_CODES     - comma-separated SIC codes to watch
                            (default: AI/semiconductor complex, see README)
    FILING_TYPES          - comma-separated form types (default: "8-K,6-K")
    POLL_INTERVAL_SECONDS - how often to poll (default: 300 = 5 min)
"""

import os
import re
import time
import html
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("filing_watcher")

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
SEC_USER_AGENT = os.environ.get("SEC_USER_AGENT", "")

if not SEC_USER_AGENT:
    raise SystemExit(
        "SEC_USER_AGENT is required — SEC blocks requests without a real "
        "contact string, e.g. 'F. Brunner htr.fund@example.com'"
    )

# Default target sectors: AI / semiconductor complex, matching HTR Fund's
# current concentration (NVDA, AVGO, CRDO, MU, ALAB, AMAT, KTOS, IONQ, etc.)
# 3674 Semiconductors & Related Devices
# 3559 Special Industry Machinery (semi-cap equipment, e.g. AMAT-adjacent)
# 3576 Computer Communications Equipment
# 3577 Computer Peripheral Equipment
# 3812 Search/Detection/Navigation/Guidance (defense/aero, e.g. KTOS-adjacent)
# 7372 Prepackaged Software
# 3827 Laboratory/Analytical Instruments
DEFAULT_SIC_CODES = "3674,3559,3576,3577,3812,7372,3827"
TARGET_SIC_CODES = set(
    c.strip() for c in os.environ.get("TARGET_SIC_CODES", DEFAULT_SIC_CODES).split(",") if c.strip()
)

FILING_TYPES = [t.strip() for t in os.environ.get("FILING_TYPES", "8-K,6-K").split(",") if t.strip()]
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "300"))
SEEN_FILE = os.environ.get("SEEN_FILE", "seen_filings.txt")

HEADERS = {"User-Agent": SEC_USER_AGENT}

_sic_cache = {}


def load_seen():
    if not os.path.exists(SEEN_FILE):
        return set()
    with open(SEEN_FILE) as f:
        return set(line.strip() for line in f if line.strip())


def mark_seen(key):
    with open(SEEN_FILE, "a") as f:
        f.write(key + "\n")


def get_sic_name_ticker(cik):
    """Look up a filer's SIC code, company name, and ticker from EDGAR's
    submissions API. Cached in-memory for the life of the process."""
    if cik in _sic_cache:
        return _sic_cache[cik]
    url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        r.raise_for_status()
        data = r.json()
        sic = str(data.get("sic", ""))
        name = data.get("name", "")
        tickers = data.get("tickers", [])
        ticker = tickers[0] if tickers else ""
        result = (sic, name, ticker)
    except Exception as e:
        log.warning("SIC lookup failed for CIK %s: %s", cik, e)
        result = ("", "", "")
    _sic_cache[cik] = result
    return result


def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        log.error("Telegram send failed: %s", e)


def fetch_recent_filings(form_type):
    """Pull SEC EDGAR's 'current filings' atom feed for a given form type.
    This lists the most recent filings across ALL filers, refreshed
    continuously during the trading day/evening."""
    url = (
        "https://www.sec.gov/cgi-bin/browse-edgar"
        f"?action=getcurrent&type={form_type}&company=&dateb=&owner=include"
        "&count=100&output=atom"
    )
    r = requests.get(url, headers=HEADERS, timeout=10)
    r.raise_for_status()
    return r.text


_ENTRY_RE = re.compile(r"<entry>(.*?)</entry>", re.S)
_TITLE_RE = re.compile(r"<title>(.*?)</title>", re.S)
_LINK_RE = re.compile(r'<link[^>]*href="([^"]+)"')
_CIK_RE = re.compile(r"CIK=(\d+)")


def parse_entries(atom_xml):
    entries = []
    for block in _ENTRY_RE.findall(atom_xml):
        title_m = _TITLE_RE.search(block)
        link_m = _LINK_RE.search(block)
        if not (title_m and link_m):
            continue
        entries.append({
            "title": html.unescape(title_m.group(1)),
            "link": link_m.group(1),
        })
    return entries


def run_once(seen):
    for form_type in FILING_TYPES:
        try:
            xml = fetch_recent_filings(form_type)
        except Exception as e:
            log.error("Fetch failed for %s: %s", form_type, e)
            continue

        for entry in parse_entries(xml):
            key = entry["link"]
            if key in seen:
                continue
            seen.add(key)
            mark_seen(key)

            cik_m = _CIK_RE.search(entry["link"])
            if not cik_m:
                continue
            sic, name, ticker = get_sic_name_ticker(cik_m.group(1))

            if sic in TARGET_SIC_CODES:
                msg = (
                    f"\U0001F6A8 <b>{form_type} Filing</b>\n"
                    f"<b>{html.escape(name)}</b>"
                    f"{' (' + html.escape(ticker) + ')' if ticker else ''}\n"
                    f"SIC: {sic}\n"
                    f"{html.escape(entry['title'])}\n"
                    f"{entry['link']}"
                )
                log.info("MATCH: %s (%s) SIC %s", name, ticker, sic)
                send_telegram(msg)

            time.sleep(0.15)  # stay well under SEC's 10 req/sec limit


def main():
    log.info("Filing watcher starting. Watching SIC codes: %s", sorted(TARGET_SIC_CODES))
    log.info("Form types: %s | Poll interval: %ss", FILING_TYPES, POLL_INTERVAL_SECONDS)
    seen = load_seen()
    while True:
        run_once(seen)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
