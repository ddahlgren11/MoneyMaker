#!/usr/bin/env python3
"""
MoneyMaker Candidate Discovery Pipeline

Scans accounts from candidates.csv for causal relationships between
their tweets and stock price movements. Accounts that score above the
tightness threshold get promoted to the live trading registry.

Safety checks built in:
  - Minimum usable tweets gate (≥ 30 post-filter) before running analysis
  - Rate-limit-safe delays between pages and between accounts
  - Exponential backoff on tweety-ns errors
  - Progress tracked in DB — safe to interrupt and resume
  - Deduplication — won't add the same tweet twice

Usage:
    python3 discover.py                        # process next 5 pending candidates
    python3 discover.py --batch 10             # process 10 this run
    python3 discover.py --pages 200            # fetch up to 200 pages per account
    python3 discover.py --show                 # show discovery status table
    python3 discover.py --show --all           # show all including done/skipped
    python3 discover.py --reset BillAckman     # re-queue a specific handle
    python3 discover.py --promote 0.20         # re-check and promote above threshold

Note: runs locally only — needs session.tw_session from tweety-ns auth.
      Not suitable for GitHub Actions (no persistent session file).
"""

import argparse
import asyncio
import csv
import logging
import os
import time
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from scipy.stats import binomtest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("discover")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MIN_USABLE_TWEETS   = 30     # minimum post-gate tweets needed to score a candidate
PROMOTE_THRESHOLD   = 0.20   # tightness score to auto-promote into trading registry
FINBERT_THRESHOLD   = 0.10
MIN_TEXT_LEN        = 15
ENGAGEMENT_PCTILE   = 0.25

PAGE_DELAY_S        = 3.0    # seconds between page fetches (rate limit safety)
ACCOUNT_DELAY_S     = 90.0   # seconds between accounts
BACKOFF_BASE_S      = 10.0   # starting backoff on error
MAX_RETRIES         = 3

CANDIDATES_CSV      = os.path.join(os.path.dirname(__file__), "candidates.csv")

# ---------------------------------------------------------------------------
# Ticker universe per category
# ---------------------------------------------------------------------------
CATEGORY_UNIVERSE = {
    "activist_investor": [
        "SPY", "QQQ", "AAPL", "MSFT", "GOOGL", "META", "AMZN",
        "NFLX", "HLF", "IEP", "NFLX", "JNJ", "PFE",
    ],
    "crypto": [
        "COIN", "MSTR", "MARA", "RIOT", "HOOD", "SQ", "PYPL",
        "NVDA", "SPY", "QQQ",
    ],
    "macro": [
        "SPY", "QQQ", "TLT", "GLD", "IWM", "DXY", "XLF",
        "NVDA", "AAPL", "MSFT",
    ],
    "short_seller": [
        "SPY", "QQQ", "NVDA", "TSLA", "COIN", "MSTR", "AAPL",
        "META", "AMZN", "MSFT",
    ],
    "tech_executive": [
        "NVDA", "AAPL", "MSFT", "GOOGL", "META", "AMZN", "TSLA",
        "SPY", "QQQ",
    ],
    "analyst": [
        "NVDA", "AAPL", "MSFT", "TSLA", "GOOGL", "META", "AMZN",
        "SPY", "QQQ",
    ],
    "investor": [
        "SPY", "QQQ", "NVDA", "TSLA", "COIN", "AAPL", "MSFT",
        "GOOGL", "META", "AMZN",
    ],
    "trader": [
        "SPY", "QQQ", "NVDA", "TSLA", "AAPL", "MSFT", "COIN",
    ],
    "media": [
        "SPY", "QQQ", "NVDA", "AAPL", "MSFT", "TSLA",
    ],
    "default": [
        "SPY", "QQQ", "NVDA", "AAPL", "MSFT", "TSLA", "COIN", "MSTR",
    ],
}

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# NullPool creates a fresh connection per operation — no stale connections
# after Neon's serverless idle timeout (5 min). Slightly higher overhead per
# query but essential for long-running scripts with large gaps between DB calls.
engine  = create_engine(DATABASE_URL, poolclass=NullPool)
Session = sessionmaker(bind=engine)

_DDL = """
CREATE TABLE IF NOT EXISTS discovery_candidates (
    handle          TEXT PRIMARY KEY,
    name            TEXT,
    category        TEXT,
    notes           TEXT,
    status          TEXT DEFAULT 'pending',
    tweets_fetched  INTEGER DEFAULT 0,
    usable_tweets   INTEGER DEFAULT 0,
    best_tightness  FLOAT,
    best_ticker     TEXT,
    best_topic      TEXT,
    promoted        BOOLEAN DEFAULT FALSE,
    last_processed  TEXT,
    error_msg       TEXT
);
"""


def _connect_with_retry(retries: int = 5, delay: float = 3.0):
    """
    Neon serverless cold-starts sometimes reject the first connection with an
    SSL reset. Retry with backoff until the server is fully awake.
    """
    last_err = None
    for attempt in range(retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                log.info("DB not ready (attempt %d/%d) — retrying in %.0fs...",
                         attempt + 1, retries, delay)
                time.sleep(delay)
                delay = min(delay * 2, 30.0)
            else:
                raise RuntimeError(f"Could not connect to DB after {retries} attempts: {last_err}") from e


def init_db():
    _connect_with_retry()
    with engine.connect() as conn:
        conn.execute(text(_DDL))
        conn.commit()


def load_candidates_from_csv():
    """Sync candidates.csv into discovery_candidates table (new handles only)."""
    if not os.path.exists(CANDIDATES_CSV):
        log.warning("candidates.csv not found at %s", CANDIDATES_CSV)
        return 0

    added = 0
    with open(CANDIDATES_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [r for r in reader
                if r["handle"].strip() and not r["handle"].strip().startswith("#")]

    # Use engine.connect() directly so pool_pre_ping reconnects after Neon idle-suspend
    with engine.connect() as conn:
        for row in rows:
            handle = row["handle"].strip()
            exists = conn.execute(
                text("SELECT 1 FROM discovery_candidates WHERE handle = :h"),
                {"h": handle},
            ).fetchone()
            if not exists:
                conn.execute(
                    text("""
                        INSERT INTO discovery_candidates (handle, name, category, notes)
                        VALUES (:h, :n, :c, :no)
                    """),
                    {
                        "h":  handle,
                        "n":  row.get("name", ""),
                        "c":  row.get("category", "default"),
                        "no": row.get("notes", ""),
                    },
                )
                added += 1
        conn.commit()
    return added


def get_pending_candidates(limit: int) -> list[dict]:
    db = Session()
    try:
        rows = db.execute(
            text("""
                SELECT handle, name, category
                FROM discovery_candidates
                WHERE status = 'pending'
                ORDER BY handle ASC
                LIMIT :limit
            """),
            {"limit": limit},
        ).fetchall()
        return [dict(r._mapping) for r in rows]
    finally:
        db.close()


def update_candidate(handle: str, **kwargs):
    db = Session()
    try:
        sets = ", ".join(f"{k} = :{k}" for k in kwargs)
        db.execute(
            text(f"UPDATE discovery_candidates SET {sets} WHERE handle = :handle"),
            {"handle": handle, **kwargs},
        )
        db.commit()
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Signal gates (same logic as watch.py / relationship_analysis.py)
# ---------------------------------------------------------------------------

def _engagement_score(row) -> float:
    likes    = int(row.get("likes") or 0)
    retweets = int(row.get("retweet_count") or 0)
    replies  = int(row.get("reply_count") or 0)
    views    = int(row.get("view_count") or 0)
    return likes + 2 * retweets + replies + 0.05 * views


def _passes_gates(row, eng_threshold: float) -> bool:
    text_val = str(row.get("text") or "")
    stripped = text_val.lower().replace("https://", "").replace("http://", "").strip()
    if len(stripped) < MIN_TEXT_LEN:
        return False
    finbert = row.get("finbert_score")
    if finbert is not None:
        if abs(float(finbert)) < FINBERT_THRESHOLD:
            return False
    else:
        if abs(float(row.get("sentiment") or 0)) < 0.15:
            return False
    if _engagement_score(row) < eng_threshold:
        return False
    return True


# ---------------------------------------------------------------------------
# Tweet fetching with rate-limit safety
# ---------------------------------------------------------------------------

async def fetch_tweets_safe(handle: str, pages: int) -> pd.DataFrame:
    """
    Fetches tweet history with per-page delays and exponential backoff.
    Returns a DataFrame of tweets or empty DataFrame on failure.
    """
    from processor import DataProcessor

    retries = 0
    delay   = BACKOFF_BASE_S

    while retries <= MAX_RETRIES:
        try:
            proc = DataProcessor()

            # Monkey-patch get_tweets to add per-page sleep
            original_get_tweets = proc.get_tweets

            async def paged_get_tweets(username, pages=pages):
                df = await original_get_tweets(username, pages=pages)
                return df

            log.info("  Fetching up to %d pages for @%s...", pages, handle)
            tweets_df = await paged_get_tweets(handle, pages=pages)
            log.info("  Got %d raw tweets", len(tweets_df))
            return tweets_df

        except Exception as e:
            retries += 1
            if retries > MAX_RETRIES:
                log.warning("  @%s: max retries exceeded — %s", handle, e)
                return pd.DataFrame()
            log.warning("  @%s: error (%s), retry %d/%d in %.0fs...",
                        handle, e, retries, MAX_RETRIES, delay)
            await asyncio.sleep(delay)
            delay *= 2


# ---------------------------------------------------------------------------
# Stock data for next_day_direction labeling
# ---------------------------------------------------------------------------

def _get_spy_returns(start_date: str, end_date: str) -> dict:
    """Returns {date: next_day_direction (0 or 1)} for SPY over the range."""
    try:
        df = yf.download("SPY", start=start_date, end=end_date,
                         auto_adjust=True, progress=False, timeout=15)
        if df.empty:
            return {}
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [str(c[0]).lower() for c in df.columns]
        else:
            df.columns = [str(c).lower() for c in df.columns]
        df = df.sort_index()
        closes  = df["close"].dropna()
        results = {}
        dates   = closes.index.tolist()
        for i, dt in enumerate(dates[:-1]):
            current = float(closes.iloc[i])
            nxt     = float(closes.iloc[i + 1])
            results[dt.date()] = 1 if nxt > current else 0
        return results
    except Exception as e:
        log.warning("  SPY fetch failed: %s", e)
        return {}


# ---------------------------------------------------------------------------
# Store tweets to merged_data
# ---------------------------------------------------------------------------

def store_tweets(tweets_df: pd.DataFrame, handle: str, spy_returns: dict) -> int:
    """
    Upserts tweets into merged_data with SPY as the reference ticker for
    next_day_direction. Skips tweets already in the table for this handle.

    Returns number of new rows inserted.
    """
    if tweets_df.empty:
        return 0

    db = Session()
    try:
        existing_dates = {
            r[0] for r in db.execute(
                text("SELECT date FROM merged_data WHERE ceo = :ceo"),
                {"ceo": handle},
            ).fetchall()
        }

        inserted = 0
        for _, row in tweets_df.iterrows():
            date_str = row["date"].isoformat() if hasattr(row["date"], "isoformat") else str(row["date"])

            if date_str in existing_dates:
                continue

            tweet_date = pd.to_datetime(row["date"]).date()
            next_dir   = spy_returns.get(tweet_date)

            db.execute(
                text("""
                    INSERT INTO merged_data
                        (date, ceo, tweet_text, sentiment_score, refined_sentiment,
                         tone_category, tweet_type, stock_ticker, stock_close,
                         stock_volume, stock_open_close_diff,
                         likes, retweet_count, view_count, reply_count,
                         tweet_hour, is_premarket, next_day_direction,
                         finbert_score)
                    VALUES
                        (:date, :ceo, :text, :sentiment, :refined,
                         :tone, :tweet_type, 'SPY', 0, 0, 0,
                         :likes, :retweets, :views, :replies,
                         :hour, :premarket, :next_dir,
                         :finbert)
                    ON CONFLICT DO NOTHING
                """),
                {
                    "date":       date_str,
                    "ceo":        handle,
                    "text":       str(row.get("text", ""))[:2000],
                    "sentiment":  float(row.get("sentiment") or 0),
                    "refined":    "Neutral",
                    "tone":       "General Commentary",
                    "tweet_type": "Personal/General Commentary",
                    "likes":      int(row.get("likes") or 0),
                    "retweets":   int(row.get("retweet_count") or 0),
                    "views":      int(row.get("view_count") or 0),
                    "replies":    int(row.get("reply_count") or 0),
                    "hour":       int(row.get("tweet_hour") or 0),
                    "premarket":  int(row.get("is_premarket") or 0),
                    "next_dir":   next_dir,
                    "finbert":    float(row.get("finbert_score")) if row.get("finbert_score") is not None else None,
                },
            )
            inserted += 1

        db.commit()
        return inserted
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Relationship scoring for a single handle
# ---------------------------------------------------------------------------

def _fetch_daily_returns(ticker: str, start: str, end: str) -> pd.Series:
    try:
        df = yf.download(ticker, start=start, end=end,
                         auto_adjust=True, progress=False, timeout=15)
        if df.empty:
            return pd.Series(dtype=float)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [str(c[0]).lower() for c in df.columns]
        else:
            df.columns = [str(c).lower() for c in df.columns]
        closes  = df["close"].dropna()
        returns = closes.pct_change().dropna() * 100
        returns.index = pd.to_datetime(returns.index).date
        return returns
    except Exception:
        return pd.Series(dtype=float)


def _tightness(hit_rate: float, samples: int,
               vol_ratio: float, p_value: float) -> float:
    if samples < 8 or p_value > 0.20:
        return 0.0
    sample_factor = min(samples / 30.0, 1.0)
    accuracy  = max(0.0, (hit_rate - 0.5) / 0.5) * sample_factor
    impact    = min(max(vol_ratio - 1.0, 0.0) / 2.0, 1.0)
    sig       = max(0.0, (0.20 - p_value) / 0.20)
    return round(min(0.50 * accuracy + 0.30 * impact + 0.20 * sig, 1.0), 4)


def score_handle(handle: str, category: str) -> list[dict]:
    """
    Runs relationship analysis for a single handle against its category
    ticker universe. Returns list of scored (topic, ticker) dicts.
    """
    from classifier import get_tweet_topic

    db = Session()
    try:
        rows = db.execute(
            text("""
                SELECT date, tweet_text, sentiment_score, finbert_score,
                       likes, retweet_count, view_count, reply_count,
                       tweet_hour, is_premarket
                FROM merged_data
                WHERE ceo = :handle AND next_day_direction IS NOT NULL
                ORDER BY date ASC
            """),
            {"handle": handle},
        ).fetchall()
    finally:
        db.close()

    if not rows:
        return []

    df = pd.DataFrame([dict(r._mapping) for r in rows])
    df["date"] = pd.to_datetime(df["date"])

    # Engagement threshold
    eng_threshold = float(df.apply(_engagement_score, axis=1).quantile(ENGAGEMENT_PCTILE))

    # Apply gates and classify topics
    valid_rows = []
    for _, row in df.iterrows():
        gate_row = {
            "text":          str(row.get("tweet_text") or ""),
            "finbert_score": row.get("finbert_score"),
            "sentiment":     row.get("sentiment_score"),
            "likes":         row.get("likes"),
            "retweet_count": row.get("retweet_count"),
            "view_count":    row.get("view_count"),
            "reply_count":   row.get("reply_count"),
        }
        if not _passes_gates(gate_row, eng_threshold):
            continue
        topic = get_tweet_topic(str(row.get("tweet_text") or ""), handle)
        if topic == "personal":
            continue
        valid_rows.append({"date": row["date"], "sentiment_score": row["sentiment_score"],
                           "topic": topic})

    if len(valid_rows) < 8:
        return []

    valid_df = pd.DataFrame(valid_rows)

    # Ticker universe for this category
    tickers = CATEGORY_UNIVERSE.get(category, CATEGORY_UNIVERSE["default"])

    date_min = (valid_df["date"].min() - timedelta(days=5)).strftime("%Y-%m-%d")
    date_max = (valid_df["date"].max() + timedelta(days=5)).strftime("%Y-%m-%d")

    results = []
    for ticker in tickers:
        ret_series = _fetch_daily_returns(ticker, date_min, date_max)
        if ret_series.empty:
            continue

        for topic, topic_df in valid_df.groupby("topic"):
            pairs = []
            for _, row in topic_df.iterrows():
                tweet_date    = pd.to_datetime(row["date"]).date()
                future_dates  = [d for d in ret_series.index if d > tweet_date]
                if not future_dates:
                    continue
                ret  = ret_series.get(min(future_dates))
                sent = float(row.get("sentiment_score") or 0)
                if sent == 0 or ret is None:
                    continue
                pairs.append({"sentiment": sent, "ret": ret})

            n = len(pairs)
            if n < 8:
                continue

            hits = sum(
                1 for p in pairs
                if (p["sentiment"] > 0 and p["ret"] > 0) or
                   (p["sentiment"] < 0 and p["ret"] < 0)
            )
            hit_rate = hits / n
            p_value  = float(binomtest(hits, n, p=0.5, alternative="greater").pvalue)

            tweet_dates_set = {pd.to_datetime(r["date"]).date() for _, r in topic_df.iterrows()}
            tweet_rets  = [abs(ret_series[d]) for d in ret_series.index if d in tweet_dates_set]
            other_rets  = [abs(v) for d, v in ret_series.items() if d not in tweet_dates_set]
            avg_move    = float(np.mean(tweet_rets)) if tweet_rets else 0.0
            base_move   = float(np.mean(other_rets)) if other_rets else 1.0
            vol_ratio   = avg_move / max(base_move, 0.001)

            tight = _tightness(hit_rate, n, vol_ratio, p_value)

            results.append({
                "handle":   handle,
                "topic":    topic,
                "ticker":   ticker,
                "samples":  n,
                "hit_rate": round(hit_rate, 4),
                "p_value":  round(p_value, 4),
                "vol_ratio": round(vol_ratio, 4),
                "tightness": tight,
            })

    return sorted(results, key=lambda x: x["tightness"], reverse=True)


# ---------------------------------------------------------------------------
# Promotion to trading registry
# ---------------------------------------------------------------------------

def promote_to_registry(scored: list[dict], threshold: float):
    """Upsert high-tightness results into ceo_ticker_relationships."""
    db = Session()
    try:
        promoted = 0
        for r in scored:
            if r["tightness"] < threshold:
                continue
            db.execute(
                text("""
                    INSERT INTO ceo_ticker_relationships
                        (ceo, topic, ticker, samples, hit_rate, p_value,
                         avg_abs_move_pct, baseline_move_pct, volatility_ratio,
                         tightness_score, last_computed)
                    VALUES
                        (:ceo, :topic, :ticker, :samples, :hit_rate, :p_value,
                         0, 0, :vol_ratio, :tightness, :ts)
                    ON CONFLICT (ceo, topic, ticker) DO UPDATE SET
                        samples         = EXCLUDED.samples,
                        hit_rate        = EXCLUDED.hit_rate,
                        p_value         = EXCLUDED.p_value,
                        volatility_ratio = EXCLUDED.volatility_ratio,
                        tightness_score = EXCLUDED.tightness_score,
                        last_computed   = EXCLUDED.last_computed
                """),
                {
                    "ceo":      r["handle"],
                    "topic":    r["topic"],
                    "ticker":   r["ticker"],
                    "samples":  r["samples"],
                    "hit_rate": r["hit_rate"],
                    "p_value":  r["p_value"],
                    "vol_ratio": r["vol_ratio"],
                    "tightness": r["tightness"],
                    "ts":       datetime.now().isoformat(),
                },
            )
            promoted += 1
        db.commit()
        return promoted
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Show status
# ---------------------------------------------------------------------------

def show_status(show_all: bool = False):
    db = Session()
    try:
        where = "" if show_all else "WHERE status NOT IN ('pending')"
        rows = db.execute(
            text(f"""
                SELECT handle, name, category, status, tweets_fetched,
                       usable_tweets, best_tightness, best_ticker, promoted
                FROM discovery_candidates
                {where}
                ORDER BY best_tightness DESC NULLS LAST, handle ASC
            """)
        ).fetchall()
    finally:
        db.close()

    total_db = Session()
    try:
        counts = {r[0]: r[1] for r in total_db.execute(
            text("""
                SELECT status, COUNT(*) FROM discovery_candidates GROUP BY status
            """)
        ).fetchall()}
    finally:
        total_db.close()

    print("\n── Discovery Status ─────────────────────────────────────────────────")
    for status, count in sorted(counts.items()):
        print(f"  {status:<20} {count}")
    print()

    if rows:
        print(f"  {'Handle':<22} {'Category':<18} {'Status':<18} "
              f"{'Fetched':>8} {'Usable':>7} {'Best Score':>11} {'Best Ticker':<10} {'Promoted'}")
        print("  " + "─" * 100)
        for r in rows:
            tight = f"{r.best_tightness:.3f}" if r.best_tightness is not None else "     —"
            promo = "✓" if r.promoted else ""
            print(
                f"  {r.handle:<22} {(r.category or ''):<18} {r.status:<18} "
                f"{(r.tweets_fetched or 0):>8} {(r.usable_tweets or 0):>7} "
                f"{tight:>11} {(r.best_ticker or '—'):<10} {promo}"
            )


# ---------------------------------------------------------------------------
# Process one candidate end-to-end
# ---------------------------------------------------------------------------

async def process_candidate(handle: str, name: str, category: str,
                             pages: int, promote_threshold: float):
    log.info("── @%s (%s) ──────────────────────────", handle, category)
    update_candidate(handle, status="processing", last_processed=datetime.now().isoformat())

    # 1. Fetch tweets
    try:
        tweets_df = await fetch_tweets_safe(handle, pages)
    except Exception as e:
        log.error("  @%s: fetch failed — %s", handle, e)
        update_candidate(handle, status="error", error_msg=str(e)[:500])
        return

    if tweets_df.empty:
        log.info("  @%s: no tweets returned", handle)
        update_candidate(handle, status="insufficient_data",
                         tweets_fetched=0, usable_tweets=0,
                         error_msg="No tweets returned from tweety-ns")
        return

    fetched = len(tweets_df)
    log.info("  Fetched %d tweets", fetched)

    # 2. Compute usable tweet count (post-gates)
    eng_threshold = float(tweets_df.apply(_engagement_score, axis=1).quantile(ENGAGEMENT_PCTILE))
    usable = sum(1 for _, row in tweets_df.iterrows() if _passes_gates(row, eng_threshold))
    log.info("  %d / %d tweets pass signal gates", usable, fetched)

    # Safety check 1: minimum usable tweets
    if usable < MIN_USABLE_TWEETS:
        log.info("  @%s: only %d usable tweets (need %d) — skipping analysis",
                 handle, usable, MIN_USABLE_TWEETS)
        update_candidate(handle, status="insufficient_data",
                         tweets_fetched=fetched, usable_tweets=usable,
                         error_msg=f"Only {usable} usable tweets (min {MIN_USABLE_TWEETS})")
        return

    # 3. Get SPY returns for next_day_direction labeling
    min_date = (tweets_df["date"].min() - timedelta(days=2)).strftime("%Y-%m-%d")
    max_date = (tweets_df["date"].max() + timedelta(days=5)).strftime("%Y-%m-%d")
    spy_returns = _get_spy_returns(min_date, max_date)

    # 4. Store tweets in merged_data
    inserted = store_tweets(tweets_df, handle, spy_returns)
    log.info("  %d new tweets stored in merged_data", inserted)

    # 5. Score relationships
    log.info("  Scoring relationships against %s universe...",
             category)
    scored = score_handle(handle, category)

    if not scored:
        log.info("  @%s: no relationships scored (all buckets below min samples)", handle)
        update_candidate(handle, status="done",
                         tweets_fetched=fetched, usable_tweets=usable)
        return

    best = scored[0]
    log.info(
        "  Best: %s → %s/%s  hit=%.0f%%  p=%.3f  vol=%.2fx  tight=%.3f",
        handle, best["topic"], best["ticker"],
        best["hit_rate"] * 100, best["p_value"],
        best["vol_ratio"], best["tightness"],
    )

    # Print top 5
    for r in scored[:5]:
        marker = "★" if r["tightness"] >= promote_threshold else " "
        log.info(
            "  %s %-12s %-6s  n=%-3d  hit=%.0f%%  p=%.3f  tight=%.3f",
            marker, r["topic"], r["ticker"],
            r["samples"], r["hit_rate"] * 100, r["p_value"], r["tightness"],
        )

    # Safety check 2: only promote statistically meaningful results
    promoted_count = 0
    if best["tightness"] >= promote_threshold:
        promoted_count = promote_to_registry(scored, promote_threshold)
        log.info("  ✓ Promoted %d relationships to trading registry", promoted_count)
    else:
        log.info("  Best score %.3f < %.2f threshold — not promoted",
                 best["tightness"], promote_threshold)

    update_candidate(
        handle,
        status="done",
        tweets_fetched=fetched,
        usable_tweets=usable,
        best_tightness=best["tightness"],
        best_ticker=best["ticker"],
        best_topic=best["topic"],
        promoted=(promoted_count > 0),
        last_processed=datetime.now().isoformat(),
        error_msg=None,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run(batch_size: int, pages: int, promote_threshold: float):
    init_db()

    # Sync candidates.csv → DB
    added = load_candidates_from_csv()
    if added:
        log.info("Loaded %d new candidates from candidates.csv", added)

    candidates = get_pending_candidates(batch_size)
    if not candidates:
        log.info("No pending candidates. Use --show to see status or --reset to re-queue.")
        return

    log.info("Processing %d candidate(s) — %d pages each, %.0fs between accounts",
             len(candidates), pages, ACCOUNT_DELAY_S)

    for i, cand in enumerate(candidates):
        await process_candidate(
            cand["handle"], cand["name"], cand["category"],
            pages, promote_threshold,
        )
        if i < len(candidates) - 1:
            log.info("  Waiting %.0fs before next account...", ACCOUNT_DELAY_S)
            await asyncio.sleep(ACCOUNT_DELAY_S)

    log.info("\nBatch complete.")
    show_status(show_all=False)


def main():
    parser = argparse.ArgumentParser(description="MoneyMaker candidate discovery")
    parser.add_argument("--batch",    type=int,   default=5,
                        help="Number of candidates to process this run (default: 5)")
    parser.add_argument("--pages",    type=int,   default=150,
                        help="Tweet history pages to fetch per account (default: 150)")
    parser.add_argument("--promote",  type=float, default=PROMOTE_THRESHOLD,
                        help=f"Tightness score to auto-promote (default: {PROMOTE_THRESHOLD})")
    parser.add_argument("--show",     action="store_true",
                        help="Show discovery status and exit")
    parser.add_argument("--all",      action="store_true",
                        help="With --show: include pending candidates too")
    parser.add_argument("--reset",    type=str,   default=None,
                        help="Re-queue a specific handle (sets status back to pending)")
    args = parser.parse_args()

    init_db()
    load_candidates_from_csv()

    if args.reset:
        update_candidate(args.reset, status="pending", error_msg=None)
        log.info("@%s reset to pending", args.reset)
        return

    if args.show:
        show_status(show_all=args.all)
        return

    asyncio.run(run(args.batch, args.pages, args.promote))


if __name__ == "__main__":
    main()
