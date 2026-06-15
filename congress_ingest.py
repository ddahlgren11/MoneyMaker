#!/usr/bin/env python3
"""
Congressional trade ingester.

Pulls the latest House & Senate STOCK Act disclosures from Financial Modeling
Prep (free tier) and writes structured rows into the `congress_trades` table.
The watcher (watch.py) then trades newly disclosed trades via poll_congress_trades().

This replaces the old, fragile path of scraping aggregator tweets
(capitoltrades / unusual_whales) and regex-parsing them — the data here is
already structured (ticker, buy/sell, dates, amount) and needs no Twitter auth.

Free-tier constraints (handled here):
  - only page 0 of each *-latest feed is accessible (pagination is premium → 402)
  - passing a large `limit` also 402s; the default page 0 returns the ~100 most
    recent disclosures, which spans several days
So each run is exactly 2 API calls (house + senate); a stable dedup key avoids
re-inserting trades already stored.

Usage:
    python3 congress_ingest.py          # ingest latest House + Senate disclosures

Requires FMP_API_KEY and DATABASE_URL in the environment.
"""
import os
import sys
import json
import logging
import hashlib
import urllib.request

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("congress_ingest")

FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE    = "https://financialmodelingprep.com/stable"

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if DATABASE_URL.startswith("postgres://") and not DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True) if DATABASE_URL else None

CONGRESS_TRADES_DDL = """
CREATE TABLE IF NOT EXISTS congress_trades (
    id              SERIAL PRIMARY KEY,
    dedup_key       TEXT UNIQUE,
    chamber         TEXT,
    member          TEXT,
    ticker          TEXT,
    asset_type      TEXT,
    txn_type        TEXT,
    direction       TEXT,            -- 'Up' (purchase) | 'Down' (sale)
    txn_date        TEXT,            -- transaction date  (YYYY-MM-DD)
    disclosure_date TEXT,            -- when it was filed  (YYYY-MM-DD)
    amount          TEXT,
    owner           TEXT,
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    processed       BOOLEAN DEFAULT FALSE
);
"""


def _fetch(feed: str) -> list[dict]:
    """Fetch page 0 of an FMP *-latest feed. Returns a list of disclosure dicts."""
    url = f"{FMP_BASE}/{feed}?page=0&apikey={FMP_API_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def _direction(txn_type: str) -> str | None:
    """Map an FMP transaction type to a trade direction. Skips exchanges/other."""
    t = (txn_type or "").lower()
    if "purchase" in t:
        return "Up"
    if "sale" in t:
        return "Down"
    return None


def _ticker_ok(sym: str) -> bool:
    return bool(sym) and sym.isalpha() and 1 <= len(sym) <= 5


def ingest() -> int:
    if not FMP_API_KEY:
        log.error("FMP_API_KEY not set — cannot ingest. Add it to .env / the FMP_API_KEY secret.")
        sys.exit(1)
    if engine is None:
        log.error("DATABASE_URL not set.")
        sys.exit(1)

    with engine.begin() as conn:
        conn.execute(text(CONGRESS_TRADES_DDL))

    total_new = 0
    with engine.begin() as conn:
        for chamber, feed in (("house", "house-latest"), ("senate", "senate-latest")):
            try:
                records = _fetch(feed)
            except Exception as e:
                log.error("%s fetch failed: %s", feed, e)
                continue
            log.info("%s: fetched %d disclosures", chamber, len(records))

            new = 0
            for rec in records:
                sym        = (rec.get("symbol") or "").strip().upper()
                asset_type = rec.get("assetType") or ""
                txn_type   = rec.get("type") or ""
                direction  = _direction(txn_type)

                # Quality filter: a real equity ticker, an equity asset, and a
                # clear buy/sell direction. Drops bonds, funds, and exchanges.
                if not _ticker_ok(sym) or direction is None or "stock" not in asset_type.lower():
                    continue

                member = (rec.get("office")
                          or f"{rec.get('firstName', '')} {rec.get('lastName', '')}").strip()
                disc   = rec.get("disclosureDate") or ""
                txn    = rec.get("transactionDate") or ""
                amount = rec.get("amount") or ""
                owner  = rec.get("owner") or ""

                key = hashlib.sha1(
                    "|".join([chamber, member, sym, txn_type, txn, disc, amount]).encode()
                ).hexdigest()

                res = conn.execute(
                    text("""
                        INSERT INTO congress_trades
                            (dedup_key, chamber, member, ticker, asset_type, txn_type,
                             direction, txn_date, disclosure_date, amount, owner)
                        VALUES
                            (:k, :ch, :m, :tk, :at, :tt, :d, :td, :dd, :am, :ow)
                        ON CONFLICT (dedup_key) DO NOTHING
                    """),
                    {"k": key, "ch": chamber, "m": member, "tk": sym, "at": asset_type,
                     "tt": txn_type, "d": direction, "td": txn, "dd": disc,
                     "am": amount, "ow": owner},
                )
                if res.rowcount:
                    new += 1

            log.info("%s: %d new equity trade(s) inserted", chamber, new)
            total_new += new

    log.info("Done — %d new congressional trades ingested", total_new)
    return total_new


if __name__ == "__main__":
    ingest()
