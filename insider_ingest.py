#!/usr/bin/env python3
"""
SEC Form 4 (corporate insider trade) ingester.

Pulls the latest Form 4 filings from SEC EDGAR and writes structured rows into
the `insider_trades` table. The watcher (watch.py) then trades freshly disclosed
insider transactions via poll_insider_trades() — exactly the structured fast-path
the congressional pipeline already uses (ticker + direction are explicit, so it
bypasses the registry/ML entirely).

This is the corporate-insider analogue of congress_ingest.py:
  - When an officer / director / 10%-owner buys or sells their own company's
    stock, they must file Form 4 with the SEC within 2 business days.
  - Open-market purchases (transaction code `P`) → Up; sales (`S`) → Down.
  - Data is fully free and structured — no API key, just a descriptive
    User-Agent header (SEC's fair-access requirement).

Cost / rate limits (handled here):
  - One request for the "latest Form 4 filings" Atom feed, then one request per
    filing to fetch its form4.xml. Capped by INSIDER_MAX_FILINGS (default 100).
  - SEC asks for <= 10 requests/sec; we sleep between fetches to stay well under.
  - A stable dedup key avoids re-inserting transactions already stored.

Usage:
    python3 insider_ingest.py              # ingest latest Form 4 filings
    python3 insider_ingest.py --dry-run    # parse + print, no DB writes
    python3 insider_ingest.py --limit 40   # cap number of filings scanned

Requires DATABASE_URL in the environment. SEC_USER_AGENT is recommended
(falls back to a generic descriptive string).
"""
import os
import re
import sys
import time
import json
import hashlib
import logging
import argparse
import urllib.request
import xml.etree.ElementTree as ET

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("insider_ingest")

# SEC requires a descriptive User-Agent identifying the requester.
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "MoneyMaker research (contact: set SEC_USER_AGENT)")
SEC_BASE       = "https://www.sec.gov"
GETCURRENT     = (SEC_BASE + "/cgi-bin/browse-edgar?action=getcurrent"
                  "&type=4&owner=include&output=atom&count={count}")

# Open-market transaction codes worth acting on. Everything else (option
# exercises `M`, grants `A`, gifts `G`, tax withholding `F`, other `J`, ...) is
# noise for a directional signal, so we drop it.
_DIRECTION_BY_CODE = {"P": "Up", "S": "Down"}

# Minimum dollar value of a transaction to bother storing (drops trivial trades).
INSIDER_MIN_VALUE   = float(os.getenv("INSIDER_MIN_VALUE", "10000"))
INSIDER_MAX_FILINGS = int(os.getenv("INSIDER_MAX_FILINGS", "100"))
_REQUEST_PAUSE_S    = 0.15  # ~6.6 req/s, comfortably under SEC's 10/s ceiling

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if DATABASE_URL.startswith("postgres://") and not DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True) if DATABASE_URL else None

INSIDER_TRADES_DDL = """
CREATE TABLE IF NOT EXISTS insider_trades (
    id              SERIAL PRIMARY KEY,
    dedup_key       TEXT UNIQUE,
    accession       TEXT,
    cik             TEXT,
    insider         TEXT,
    role            TEXT,            -- Director / Officer (title) / 10% Owner
    ticker          TEXT,
    txn_code        TEXT,            -- 'P' (open-market buy) | 'S' (sale)
    direction       TEXT,            -- 'Up' (P) | 'Down' (S)
    shares          DOUBLE PRECISION,
    price           DOUBLE PRECISION,
    value           DOUBLE PRECISION,-- shares * price (USD)
    txn_date        TEXT,            -- transaction date   (YYYY-MM-DD)
    disclosure_date TEXT,            -- filing date         (YYYY-MM-DD)
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    processed       BOOLEAN DEFAULT FALSE
);
"""


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": SEC_USER_AGENT,
                                               "Accept-Encoding": "gzip, deflate"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            import gzip
            data = gzip.decompress(data)
    return data


def _strip_ns(tag: str) -> str:
    """Drop any XML namespace prefix so lookups are namespace-agnostic."""
    return tag.rsplit("}", 1)[-1]


def _find(elem, *path):
    """Namespace-agnostic nested findall-first: _find(root, 'a', 'b') -> a/b text holder."""
    cur = elem
    for name in path:
        nxt = None
        for child in list(cur):
            if _strip_ns(child.tag) == name:
                nxt = child
                break
        if nxt is None:
            return None
        cur = nxt
    return cur


def _text(elem, *path) -> str:
    node = _find(elem, *path)
    return (node.text or "").strip() if node is not None and node.text else ""


def _value(elem, *path) -> str:
    """Form 4 wraps most leaf data in a <value> child; read it transparently."""
    node = _find(elem, *path)
    if node is None:
        return ""
    val = _find(node, "value")
    if val is not None and val.text:
        return val.text.strip()
    return (node.text or "").strip()


# ---------------------------------------------------------------------------
# Feed + filing parsing
# ---------------------------------------------------------------------------

def _latest_filings(count: int) -> list[dict]:
    """Return [{cik, accession, index_dir, filed}] for the most recent Form 4s."""
    raw = _get(GETCURRENT.format(count=count))
    root = ET.fromstring(raw)
    out = []
    seen: set[str] = set()  # EDGAR lists each filing twice (issuer + owner views)
    for entry in root.iter():
        if _strip_ns(entry.tag) != "entry":
            continue
        href = filed = ""
        for child in entry:
            tag = _strip_ns(child.tag)
            if tag == "link":
                href = child.attrib.get("href", "") or href
            elif tag == "summary":
                # "<b>Filed:</b> 2026-06-18 <b>AccNo:</b> 0001140361-26-025796 ..."
                # ET unescapes the HTML, so just grab the first YYYY-MM-DD.
                m = re.search(r"\d{4}-\d{2}-\d{2}", child.text or "")
                if m:
                    filed = m.group(0)
        # href: .../Archives/edgar/data/<cik>/<accession_nodash>/<acc>-index.htm
        if "/Archives/edgar/data/" in href:
            parts = href.split("/Archives/edgar/data/", 1)[1].split("/")
            if len(parts) >= 2:
                cik, acc_nodash = parts[0], parts[1]
                if acc_nodash in seen:
                    continue
                seen.add(acc_nodash)
                out.append({
                    "cik": cik,
                    "accession": acc_nodash,
                    "index_dir": f"{SEC_BASE}/Archives/edgar/data/{cik}/{acc_nodash}",
                    "filed": filed,
                })
    return out


def _form4_xml_url(index_dir: str) -> str | None:
    """Find the form4 XML document inside a filing's directory listing."""
    try:
        listing = json.loads(_get(index_dir + "/index.json"))
    except Exception:
        return None
    names = [i.get("name", "") for i in listing.get("directory", {}).get("item", [])]
    # Prefer an explicit form4*.xml; fall back to any .xml that isn't the R/header.
    xmls = [n for n in names if n.lower().endswith(".xml")]
    for n in xmls:
        if "form4" in n.lower() or n.lower().startswith("wk-form4") or n.lower() == "primary_doc.xml":
            return f"{index_dir}/{n}"
    return f"{index_dir}/{xmls[0]}" if xmls else None


def _parse_form4(xml_bytes: bytes, meta: dict) -> list[dict]:
    """Parse a Form 4 XML into a list of open-market (P/S) transaction dicts."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []

    ticker = _value(root, "issuer", "issuerTradingSymbol").strip().upper()

    owner = _find(root, "reportingOwner")
    insider = _text(owner, "reportingOwnerId", "rptOwnerName") if owner is not None else ""
    role_parts = []
    rel = _find(owner, "reportingOwnerRelationship") if owner is not None else None
    if rel is not None:
        if _text(rel, "isDirector") in ("1", "true"):
            role_parts.append("Director")
        if _text(rel, "isOfficer") in ("1", "true"):
            title = _text(rel, "officerTitle")
            role_parts.append(f"Officer ({title})" if title else "Officer")
        if _text(rel, "isTenPercentOwner") in ("1", "true"):
            role_parts.append("10% Owner")
    role = ", ".join(role_parts)

    rows = []
    nd_table = _find(root, "nonDerivativeTable")
    if nd_table is None:
        return rows
    for txn in nd_table:
        if _strip_ns(txn.tag) != "nonDerivativeTransaction":
            continue
        code = _value(txn, "transactionCoding", "transactionCode").upper()
        direction = _DIRECTION_BY_CODE.get(code)
        if direction is None:
            continue  # not an open-market buy/sell

        try:
            shares = float(_value(txn, "transactionAmounts", "transactionShares") or 0)
            price  = float(_value(txn, "transactionAmounts", "transactionPricePerShare") or 0)
        except ValueError:
            continue
        value = round(shares * price, 2)
        if value < INSIDER_MIN_VALUE:
            continue

        txn_date = _value(txn, "transactionDate")[:10]
        if not _ticker_ok(ticker):
            continue

        rows.append({
            "accession": meta["accession"],
            "cik": meta["cik"],
            "insider": insider,
            "role": role,
            "ticker": ticker,
            "txn_code": code,
            "direction": direction,
            "shares": shares,
            "price": price,
            "value": value,
            "txn_date": txn_date,
            "disclosure_date": meta.get("filed", ""),
        })
    return rows


def _ticker_ok(sym: str) -> bool:
    return bool(sym) and sym.replace(".", "").isalnum() and 1 <= len(sym) <= 6


def _dedup_key(r: dict) -> str:
    return hashlib.sha1("|".join([
        r["accession"], r["ticker"], r["insider"], r["txn_code"],
        r["txn_date"], str(r["shares"]), str(r["price"]),
    ]).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def ingest(dry_run: bool = False, limit: int = INSIDER_MAX_FILINGS) -> int:
    if engine is None and not dry_run:
        log.error("DATABASE_URL not set.")
        sys.exit(1)
    if "set SEC_USER_AGENT" in SEC_USER_AGENT:
        log.warning("SEC_USER_AGENT not set — using a generic UA. SEC asks for a "
                    "descriptive identifier (e.g. 'Name email'). Set it in .env.")

    if not dry_run:
        with engine.begin() as conn:
            conn.execute(text(INSIDER_TRADES_DDL))

    try:
        filings = _latest_filings(min(limit, 100))
    except Exception as e:
        log.error("EDGAR feed fetch failed: %s", e)
        return 0
    log.info("EDGAR: %d recent Form 4 filing(s) to scan", len(filings))

    parsed: list[dict] = []
    for f in filings[:limit]:
        time.sleep(_REQUEST_PAUSE_S)
        xml_url = _form4_xml_url(f["index_dir"])
        if not xml_url:
            continue
        time.sleep(_REQUEST_PAUSE_S)
        try:
            rows = _parse_form4(_get(xml_url), f)
        except Exception as e:
            log.debug("parse failed for %s: %s", f["accession"], e)
            continue
        parsed.extend(rows)

    log.info("Parsed %d open-market (P/S) insider transaction(s) >= $%.0f",
             len(parsed), INSIDER_MIN_VALUE)

    if dry_run:
        for r in parsed:
            log.info("  %s  %-6s %-4s %-5s %s sh @ $%.2f = $%s  | %s (%s)",
                     r["disclosure_date"] or "????-??-??", r["ticker"], r["txn_code"],
                     r["direction"], f"{r['shares']:,.0f}", r["price"],
                     f"{r['value']:,.0f}", r["insider"], r["role"])
        return len(parsed)

    new = 0
    with engine.begin() as conn:
        for r in parsed:
            res = conn.execute(
                text("""
                    INSERT INTO insider_trades
                        (dedup_key, accession, cik, insider, role, ticker, txn_code,
                         direction, shares, price, value, txn_date, disclosure_date)
                    VALUES
                        (:k, :acc, :cik, :ins, :role, :tk, :code, :dir,
                         :sh, :pr, :val, :td, :dd)
                    ON CONFLICT (dedup_key) DO NOTHING
                """),
                {"k": _dedup_key(r), "acc": r["accession"], "cik": r["cik"],
                 "ins": r["insider"], "role": r["role"], "tk": r["ticker"],
                 "code": r["txn_code"], "dir": r["direction"], "sh": r["shares"],
                 "pr": r["price"], "val": r["value"], "td": r["txn_date"],
                 "dd": r["disclosure_date"]},
            )
            if res.rowcount:
                new += 1

    log.info("Done — %d new insider trade(s) ingested", new)
    return new


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Ingest latest SEC Form 4 insider trades.")
    ap.add_argument("--dry-run", action="store_true", help="parse + print, no DB writes")
    ap.add_argument("--limit", type=int, default=INSIDER_MAX_FILINGS,
                    help="max number of recent filings to scan")
    args = ap.parse_args()
    ingest(dry_run=args.dry_run, limit=args.limit)
