#!/usr/bin/env python3
"""
Reddit (WallStreetBets / stocks) sentiment ingester + spike-signal builder.

Two-stage, mirroring the structured-signal pattern but for an *experimental*
crowd source:

  1. Ingest — pull recent posts from finance subreddits, extract ticker mentions,
     score each with VADER (FinBERT optional), and aggregate per (date, ticker)
     into `reddit_sentiment`: mention_count + avg sentiment.

  2. Spike heuristic — compare today's mention_count for each ticker against its
     own trailing baseline (z-score). A statistically unusual *spike* in chatter
     combined with a clearly directional average sentiment becomes a candidate
     signal in `reddit_signals` (Up if bullish, Down if bearish).

Unlike insider/congress trades, Reddit has no inherent direction, so this is
explicitly a *hypothesis to test*: run event_study.py --source reddit before ever
letting the watcher act on it.

Reddit API is free. Create a "script" app at https://www.reddit.com/prefs/apps
and set REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET / REDDIT_USER_AGENT in .env.
Read-only (application-only) auth is sufficient — no Reddit password needed.

Usage:
    python3 reddit_ingest.py              # ingest + build today's spike signals
    python3 reddit_ingest.py --dry-run    # fetch + print aggregates, no DB writes
    python3 reddit_ingest.py --finbert    # also score with FinBERT (slower)

The ticker-extraction, sentiment, and spike-detection logic are pure functions
(unit-tested in tests/); only ingest() touches the network and DB.
"""
import os
import re
import sys
import argparse
import logging
from collections import defaultdict
from datetime import datetime, timezone, date, timedelta

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from classifier import get_sentiment_score

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("reddit_ingest")

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if DATABASE_URL.startswith("postgres://") and not DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
engine = create_engine(DATABASE_URL, pool_pre_ping=True) if DATABASE_URL else None

SUBREDDITS    = os.getenv("REDDIT_SUBREDDITS", "wallstreetbets,stocks,StockMarket,investing")
POST_LIMIT    = int(os.getenv("REDDIT_POST_LIMIT", "150"))
SPIKE_Z       = float(os.getenv("REDDIT_SPIKE_Z", "2.0"))      # mention z-score to flag a spike
SPIKE_SENT    = float(os.getenv("REDDIT_SPIKE_SENT", "0.20"))  # |avg sentiment| to call direction
SPIKE_MIN_N   = int(os.getenv("REDDIT_SPIKE_MIN_MENTIONS", "5"))
BASELINE_DAYS = int(os.getenv("REDDIT_BASELINE_DAYS", "14"))

# Cashtag ($TSLA) or bare uppercase token. Bare tokens are validated against a
# ticker universe + this blacklist of common all-caps words that look like tickers.
_CASHTAG  = re.compile(r"\$([A-Za-z]{1,5})\b")
_BARE     = re.compile(r"\b([A-Z]{2,5})\b")
_STOPWORDS = {
    "CEO", "CFO", "IPO", "ATH", "YOLO", "FOMO", "DD", "USA", "USD", "GDP", "FED",
    "SEC", "ETF", "EPS", "AI", "EV", "PE", "TA", "WSB", "OTM", "ITM", "FD", "PR",
    "EOD", "AH", "PM", "RH", "IRA", "LOL", "IMO", "TLDR", "EDIT", "NFA", "GG",
    "OK", "USD", "CALL", "PUT", "BUY", "SELL", "HOLD", "LONG", "BIG", "ALL",
}


# ---------------------------------------------------------------------------
# Pure functions (unit-tested) — no network, no DB
# ---------------------------------------------------------------------------

def extract_tickers(text: str, universe: set[str]) -> set[str]:
    """Return the set of tickers mentioned in `text`.

    Cashtags ($AAPL) are always accepted. Bare uppercase tokens are accepted only
    if they're in `universe` (a set of real symbols) and not a common stopword.
    """
    found: set[str] = set()
    for m in _CASHTAG.findall(text or ""):
        found.add(m.upper())
    for m in _BARE.findall(text or ""):
        if m in _STOPWORDS:
            continue
        if m in universe:
            found.add(m)
    return found


def detect_spikes(today: pd.DataFrame, history: pd.DataFrame) -> pd.DataFrame:
    """Flag tickers whose mention_count today is an unusual spike vs their baseline.

    today:   columns [ticker, mention_count, avg_sentiment]
    history: columns [ticker, mention_count] over the trailing baseline window
    Returns rows [ticker, direction, mention_count, baseline_avg, z_score, avg_sentiment].
    """
    if today.empty:
        return pd.DataFrame(columns=["ticker", "direction", "mention_count",
                                     "baseline_avg", "z_score", "avg_sentiment"])
    stats = (history.groupby("ticker")["mention_count"]
             .agg(["mean", "std"]).rename(columns={"mean": "baseline_avg", "std": "baseline_std"})
             if not history.empty else
             pd.DataFrame(columns=["baseline_avg", "baseline_std"]))

    out = []
    for _, r in today.iterrows():
        if r["mention_count"] < SPIKE_MIN_N:
            continue
        base = stats.loc[r["ticker"]] if r["ticker"] in stats.index else None
        mean = float(base["baseline_avg"]) if base is not None and pd.notna(base["baseline_avg"]) else 0.0
        std  = float(base["baseline_std"]) if base is not None and pd.notna(base["baseline_std"]) else 0.0
        # New/quiet ticker (no usable std): treat any count >= min as a spike.
        z = (r["mention_count"] - mean) / std if std > 0 else float("inf")
        if z < SPIKE_Z:
            continue
        sent = float(r["avg_sentiment"])
        if abs(sent) < SPIKE_SENT:
            continue  # spike with no clear directional lean — skip
        out.append({
            "ticker": r["ticker"],
            "direction": "Up" if sent > 0 else "Down",
            "mention_count": int(r["mention_count"]),
            "baseline_avg": round(mean, 2),
            "z_score": round(z, 2) if np.isfinite(z) else None,
            "avg_sentiment": round(sent, 4),
        })
    return pd.DataFrame(out)


# ---------------------------------------------------------------------------
# Universe + DDL
# ---------------------------------------------------------------------------

REDDIT_SENTIMENT_DDL = """
CREATE TABLE IF NOT EXISTS reddit_sentiment (
    id            SERIAL PRIMARY KEY,
    date          TEXT,
    ticker        TEXT,
    mention_count INTEGER,
    avg_sentiment DOUBLE PRECISION,
    avg_finbert   DOUBLE PRECISION,
    subreddits    TEXT,
    ingested_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (date, ticker)
);
"""
REDDIT_SIGNALS_DDL = """
CREATE TABLE IF NOT EXISTS reddit_signals (
    id            SERIAL PRIMARY KEY,
    date          TEXT,
    ticker        TEXT,
    direction     TEXT,             -- 'Up' | 'Down'
    mention_count INTEGER,
    baseline_avg  DOUBLE PRECISION,
    z_score       DOUBLE PRECISION,
    avg_sentiment DOUBLE PRECISION,
    processed     BOOLEAN DEFAULT FALSE,
    ingested_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (date, ticker)
);
"""


def _ticker_universe() -> set[str]:
    """Build a symbol universe from the tickers we already track + SEC's list."""
    universe: set[str] = set()
    try:
        from targets import HANDLE_TO_TICKER
        universe |= {t.upper() for t in HANDLE_TO_TICKER.values()}
    except Exception:
        pass
    if engine is not None:
        try:
            with engine.connect() as c:
                for tbl, col in (("insider_trades", "ticker"),
                                 ("congress_trades", "ticker"),
                                 ("merged_data", "stock_ticker")):
                    try:
                        rows = c.execute(text(f"SELECT DISTINCT {col} FROM {tbl}")).fetchall()
                        universe |= {(r[0] or "").upper() for r in rows if r[0]}
                    except Exception:
                        continue
        except Exception:
            pass
    return {t for t in universe if t.isalpha()}


# ---------------------------------------------------------------------------
# Ingestion (network + DB)
# ---------------------------------------------------------------------------

def _reddit_client():
    import praw
    cid, csec = os.getenv("REDDIT_CLIENT_ID"), os.getenv("REDDIT_CLIENT_SECRET")
    ua = os.getenv("REDDIT_USER_AGENT", "MoneyMaker:reddit_ingest:v1 (by u/unknown)")
    if not cid or not csec:
        log.error("REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET not set — create a "
                  "'script' app at reddit.com/prefs/apps and add them to .env.")
        sys.exit(1)
    return praw.Reddit(client_id=cid, client_secret=csec, user_agent=ua, read_only=True)


def ingest(dry_run: bool = False, use_finbert: bool = False) -> int:
    today = date.today().isoformat()
    universe = _ticker_universe()
    log.info("Ticker universe: %d symbols", len(universe))

    reddit = _reddit_client()
    # ticker -> list of sentiment scores; ticker -> set of subreddits
    scores: dict[str, list[float]] = defaultdict(list)
    subs:   dict[str, set]         = defaultdict(set)
    texts:  dict[str, list[str]]   = defaultdict(list)

    for sub in SUBREDDITS.split(","):
        sub = sub.strip()
        try:
            for post in reddit.subreddit(sub).hot(limit=POST_LIMIT):
                blob = f"{post.title}\n{getattr(post, 'selftext', '') or ''}"
                tickers = extract_tickers(blob, universe)
                if not tickers:
                    continue
                s = get_sentiment_score(blob)
                for tk in tickers:
                    scores[tk].append(s)
                    subs[tk].add(sub)
                    if use_finbert:
                        texts[tk].append(blob[:400])
        except Exception as e:
            log.warning("subreddit %s fetch failed: %s", sub, e)

    if not scores:
        log.info("No ticker mentions found this run.")
        return 0

    finbert_avg: dict[str, float] = {}
    if use_finbert:
        from classifier import get_finbert_scores_batch
        for tk, blobs in texts.items():
            try:
                fs = get_finbert_scores_batch(blobs)
                finbert_avg[tk] = float(np.mean(fs)) if fs else None
            except Exception:
                finbert_avg[tk] = None

    today_rows = [{
        "ticker": tk,
        "mention_count": len(sc),
        "avg_sentiment": round(float(np.mean(sc)), 4),
        "avg_finbert": finbert_avg.get(tk),
        "subreddits": ",".join(sorted(subs[tk])),
    } for tk, sc in scores.items()]
    today_df = pd.DataFrame(today_rows).sort_values("mention_count", ascending=False)

    log.info("Aggregated %d ticker(s) mentioned today", len(today_df))

    if dry_run:
        print(today_df.head(25).to_string(index=False))

    # Spike detection vs trailing baseline
    history = pd.DataFrame(columns=["ticker", "mention_count"])
    if engine is not None and not dry_run:
        with engine.begin() as conn:
            conn.execute(text(REDDIT_SENTIMENT_DDL))
            conn.execute(text(REDDIT_SIGNALS_DDL))
        since = (date.today() - timedelta(days=BASELINE_DAYS)).isoformat()
        with engine.connect() as conn:
            history = pd.read_sql(
                text("SELECT ticker, mention_count FROM reddit_sentiment "
                     "WHERE date >= :s AND date < :t"),
                conn, params={"s": since, "t": today})

    signals = detect_spikes(today_df, history)
    log.info("%d spike signal(s) detected", len(signals))

    if dry_run:
        if not signals.empty:
            print("\nSpike signals:")
            print(signals.to_string(index=False))
        return len(today_df)

    with engine.begin() as conn:
        for _, r in today_df.iterrows():
            conn.execute(text("""
                INSERT INTO reddit_sentiment
                    (date, ticker, mention_count, avg_sentiment, avg_finbert, subreddits)
                VALUES (:d, :tk, :mc, :s, :fb, :sub)
                ON CONFLICT (date, ticker) DO UPDATE SET
                    mention_count = EXCLUDED.mention_count,
                    avg_sentiment = EXCLUDED.avg_sentiment,
                    avg_finbert   = EXCLUDED.avg_finbert,
                    subreddits    = EXCLUDED.subreddits
            """), {"d": today, "tk": r["ticker"], "mc": int(r["mention_count"]),
                   "s": r["avg_sentiment"], "fb": r["avg_finbert"], "sub": r["subreddits"]})
        for _, r in signals.iterrows():
            conn.execute(text("""
                INSERT INTO reddit_signals
                    (date, ticker, direction, mention_count, baseline_avg, z_score, avg_sentiment)
                VALUES (:d, :tk, :dir, :mc, :ba, :z, :s)
                ON CONFLICT (date, ticker) DO NOTHING
            """), {"d": today, "tk": r["ticker"], "dir": r["direction"],
                   "mc": int(r["mention_count"]), "ba": r["baseline_avg"],
                   "z": r["z_score"], "s": r["avg_sentiment"]})

    log.info("Done — %d ticker-days, %d spike signal(s) written", len(today_df), len(signals))
    return len(today_df)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Ingest Reddit finance-sub sentiment + spike signals.")
    ap.add_argument("--dry-run", action="store_true", help="fetch + print, no DB writes")
    ap.add_argument("--finbert", action="store_true", help="also score with FinBERT (slower)")
    args = ap.parse_args()
    ingest(dry_run=args.dry_run, use_finbert=args.finbert)
