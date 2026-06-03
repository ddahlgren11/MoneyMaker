"""
Causal relationship discovery: which (CEO, tweet_topic, ticker) triples have
a statistically meaningful, consistent link between tweet sentiment and next-day
stock direction?

For each CEO in the database this script:
  1. Loads stored tweets and applies three signal gates:
       - Engagement gate  : tweet in top 75th percentile of that CEO's reach
       - Content gate     : |finbert_score| > 0.10  (has financial language)
       - Text gate        : stripped text length > 15 chars (not URL-only)
  2. Classifies each passing tweet into a topic bucket via get_tweet_topic()
  3. Builds the ticker test universe for each (CEO, topic) pair from
     CEO_TOPIC_UNIVERSE in classifier.py (cross-asset) plus SPY as a baseline
  4. Fetches daily returns for every ticker in the universe over the tweet date range
  5. For each (CEO, topic, ticker) group computes:
       hit_rate         — % of tweets where sentiment direction matched ticker direction
       p_value          — one-sided binomial test vs 50% null
       avg_abs_move     — mean |% return| on tweet days for this ticker
       baseline_move    — mean |% return| on ALL non-tweet days
       volatility_ratio — avg_abs_move / baseline_move
       tightness_score  — composite 0–1 score (0 = no relationship)
  6. Writes results to the ceo_ticker_relationships table (upserts)

Usage:
    python3 relationship_analysis.py

Rerun any time new tweets are ingested to refresh the registry.
"""

import os
import math
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from scipy.stats import binomtest
from sqlalchemy import create_engine, text

from classifier import get_tweet_topic, CEO_TOPIC_UNIVERSE

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MIN_SAMPLES        = 8      # minimum valid tweets to score a relationship
FINBERT_THRESHOLD  = 0.10   # |finbert_score| must exceed this
MIN_TEXT_LEN       = 15     # stripped text chars (filters URL-only tweets)
ENGAGEMENT_PCTILE  = 0.25   # tweet must be above this percentile of CEO's own reach

# Base tickers always added to every universe regardless of topic
BASE_UNIVERSE = ["SPY", "QQQ"]

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ceo_ticker_relationships (
    id                SERIAL PRIMARY KEY,
    ceo               TEXT NOT NULL,
    topic             TEXT NOT NULL,
    ticker            TEXT NOT NULL,
    samples           INTEGER,
    hit_rate          FLOAT,
    p_value           FLOAT,
    avg_abs_move_pct  FLOAT,
    baseline_move_pct FLOAT,
    volatility_ratio  FLOAT,
    tightness_score   FLOAT,
    last_computed     TEXT,
    UNIQUE (ceo, topic, ticker)
);
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _engagement_score(row) -> float:
    """Weighted reach proxy. Falls back gracefully when views = 0 (missing data)."""
    likes     = int(row.get("likes") or 0)
    retweets  = int(row.get("retweet_count") or 0)
    replies   = int(row.get("reply_count") or 0)
    views     = int(row.get("view_count") or 0)
    return likes + 2 * retweets + replies + 0.05 * views


def _engagement_threshold(df: pd.DataFrame) -> float:
    """Return the ENGAGEMENT_PCTILE engagement score for this CEO's tweet set."""
    scores = df.apply(_engagement_score, axis=1)
    return float(scores.quantile(ENGAGEMENT_PCTILE))


def _passes_gates(row, eng_threshold: float) -> bool:
    text = str(row.get("tweet_text") or "")
    stripped = text.lower().replace("https://", "").replace("http://", "").strip()
    if len(stripped) < MIN_TEXT_LEN:
        return False
    if abs(float(row.get("finbert_score") or 0)) < FINBERT_THRESHOLD:
        return False
    if _engagement_score(row) < eng_threshold:
        return False
    return True


def _fetch_daily_returns(ticker: str, start: str, end: str) -> pd.Series:
    """
    Returns a Series of daily close-to-close % returns indexed by date,
    for the given ticker over [start, end].
    Returns empty Series on failure.
    """
    try:
        df = yf.download(ticker, start=start, end=end,
                         auto_adjust=True, progress=False, timeout=15)
        if df.empty:
            return pd.Series(dtype=float)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [str(c[0]).lower() for c in df.columns]
        else:
            df.columns = [str(c).lower() for c in df.columns]
        closes = df["close"].dropna()
        returns = closes.pct_change().dropna() * 100       # in %
        returns.index = pd.to_datetime(returns.index).date  # date objects
        return returns
    except Exception as e:
        log.warning("yfinance failed for %s: %s", ticker, e)
        return pd.Series(dtype=float)


def _tightness(hit_rate: float, samples: int,
               volatility_ratio: float, p_value: float) -> float:
    """
    Composite relationship strength score in [0, 1].

    Components:
      accuracy  — how far above 50% the hit rate is, scaled by sample size
      impact    — how much the ticker moves more on tweet days vs baseline
      significance — how far below 0.10 the p-value is
    """
    if samples < MIN_SAMPLES or p_value > 0.20:
        return 0.0

    # Accuracy: normalised edge above random, damped for small N
    sample_factor = min(samples / 30.0, 1.0)
    accuracy = max(0.0, (hit_rate - 0.5) / 0.5) * sample_factor

    # Impact: excess volatility (capped at 3x)
    impact = min(max(volatility_ratio - 1.0, 0.0) / 2.0, 1.0)

    # Significance: linear from p=0.20 (0.0) to p=0.00 (1.0)
    sig = max(0.0, (0.20 - p_value) / 0.20)

    score = 0.50 * accuracy + 0.30 * impact + 0.20 * sig
    return round(min(score, 1.0), 4)


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def _compute_relationship(tweet_rows: pd.DataFrame,
                          ticker_returns: pd.Series) -> dict | None:
    """
    Given filtered tweet rows (already past gates) and a Series of daily returns
    for one ticker, compute all relationship metrics.

    tweet_rows must have columns: date (datetime), sentiment_score
    """
    pairs = []
    for _, row in tweet_rows.iterrows():
        tweet_date = pd.to_datetime(row["date"]).date()
        # next-day return: find the first trading day after the tweet date
        future_dates = [d for d in ticker_returns.index if d > tweet_date]
        if not future_dates:
            continue
        next_date = min(future_dates)
        ret = ticker_returns.get(next_date)
        if ret is None or math.isnan(ret):
            continue
        sent = float(row.get("sentiment_score") or 0)
        if sent == 0.0:
            continue    # skip genuinely neutral tweets — no directional signal
        pairs.append({"sentiment": sent, "next_return": ret})

    n = len(pairs)
    if n < MIN_SAMPLES:
        return None

    # Directional hit rate
    hits = sum(
        1 for p in pairs
        if (p["sentiment"] > 0 and p["next_return"] > 0) or
           (p["sentiment"] < 0 and p["next_return"] < 0)
    )
    hit_rate = hits / n

    # Binomial p-value (one-sided: is hit_rate > 0.5?)
    result   = binomtest(hits, n, p=0.5, alternative="greater")
    p_value  = float(result.pvalue)

    # Volatility amplification
    tweet_dates_set = {pd.to_datetime(r["date"]).date() for _, r in tweet_rows.iterrows()}
    tweet_returns   = [abs(ticker_returns[d]) for d in ticker_returns.index
                       if d in tweet_dates_set and not math.isnan(ticker_returns[d])]
    other_returns   = [abs(v) for d, v in ticker_returns.items()
                       if d not in tweet_dates_set and not math.isnan(v)]

    avg_abs_move  = float(np.mean(tweet_returns)) if tweet_returns else 0.0
    baseline_move = float(np.mean(other_returns)) if other_returns else 1.0
    vol_ratio     = avg_abs_move / max(baseline_move, 0.001)

    return {
        "samples":           n,
        "hit_rate":          round(hit_rate, 4),
        "p_value":           round(p_value, 4),
        "avg_abs_move_pct":  round(avg_abs_move, 4),
        "baseline_move_pct": round(baseline_move, 4),
        "volatility_ratio":  round(vol_ratio, 4),
        "tightness_score":   _tightness(hit_rate, n, vol_ratio, p_value),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    # Ensure table exists with the unique constraint needed for upserts
    with engine.connect() as conn:
        conn.execute(text(CREATE_TABLE_SQL))
        conn.execute(text("""
            DO $$ BEGIN
                ALTER TABLE ceo_ticker_relationships
                    ADD CONSTRAINT ceo_ticker_uq UNIQUE (ceo, topic, ticker);
            EXCEPTION WHEN duplicate_table THEN NULL;
            END $$;
        """))
        conn.commit()

    # Load all merged records from DB
    df = pd.read_sql(
        text("""
            SELECT ceo, date, tweet_text, sentiment_score, finbert_score,
                   likes, retweet_count, view_count, reply_count,
                   next_day_direction
            FROM merged_data
            WHERE next_day_direction IS NOT NULL
            ORDER BY date ASC
        """),
        engine,
    )
    log.info("Loaded %d labeled rows from merged_data", len(df))

    if df.empty:
        log.error("No data found. Run the ingestion pipeline first.")
        return

    df["date"] = pd.to_datetime(df["date"])
    all_results = []

    for ceo, ceo_df in df.groupby("ceo"):
        log.info("--- %s (%d tweets)", ceo, len(ceo_df))

        eng_threshold = _engagement_threshold(ceo_df)

        # Apply gates
        valid_mask = ceo_df.apply(lambda r: _passes_gates(r, eng_threshold), axis=1)
        valid_df   = ceo_df[valid_mask].copy()
        log.info("  %d / %d tweets pass gates", len(valid_df), len(ceo_df))

        if valid_df.empty:
            continue

        # Classify topics
        valid_df["topic"] = valid_df.apply(
            lambda r: get_tweet_topic(str(r["tweet_text"]), ceo), axis=1
        )

        # Build ticker universe for this CEO
        ticker_universe: set[str] = set(BASE_UNIVERSE)
        ceo_universe = CEO_TOPIC_UNIVERSE.get(ceo, {})
        for topic_tickers in ceo_universe.values():
            ticker_universe.update(topic_tickers)

        # Date range for fetching returns (add buffer for baseline days)
        date_min = (valid_df["date"].min() - timedelta(days=30)).strftime("%Y-%m-%d")
        date_max = (valid_df["date"].max() + timedelta(days=5)).strftime("%Y-%m-%d")

        # Fetch returns once per ticker (covers all topics)
        returns_cache: dict[str, pd.Series] = {}
        for ticker in sorted(ticker_universe):
            log.info("  fetching %s  [%s → %s]", ticker, date_min, date_max)
            returns_cache[ticker] = _fetch_daily_returns(ticker, date_min, date_max)

        # Compute metrics per (topic, ticker)
        for topic, topic_df in valid_df.groupby("topic"):
            if topic == "personal":
                continue   # no expected causal signal

            # Which tickers to test for this topic?
            topic_tickers = set(BASE_UNIVERSE)
            topic_tickers.update(ceo_universe.get(topic, []))

            for ticker in sorted(topic_tickers):
                ret_series = returns_cache.get(ticker, pd.Series(dtype=float))
                if ret_series.empty:
                    continue

                metrics = _compute_relationship(topic_df, ret_series)
                if metrics is None:
                    continue

                all_results.append({
                    "ceo":               ceo,
                    "topic":             topic,
                    "ticker":            ticker,
                    "last_computed":     datetime.now().isoformat(),
                    **metrics,
                })
                log.info(
                    "  %-14s %-12s %-6s  n=%-3d  hit=%.0f%%  p=%.3f  vol=%.2fx  tight=%.3f",
                    ceo, topic, ticker,
                    metrics["samples"],
                    metrics["hit_rate"] * 100,
                    metrics["p_value"],
                    metrics["volatility_ratio"],
                    metrics["tightness_score"],
                )

    if not all_results:
        log.warning("No relationships computed — check gates or data volume.")
        return

    results_df = pd.DataFrame(all_results)

    # Upsert into DB
    with engine.connect() as conn:
        for _, row in results_df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO ceo_ticker_relationships
                        (ceo, topic, ticker, samples, hit_rate, p_value,
                         avg_abs_move_pct, baseline_move_pct, volatility_ratio,
                         tightness_score, last_computed)
                    VALUES
                        (:ceo, :topic, :ticker, :samples, :hit_rate, :p_value,
                         :avg_abs_move_pct, :baseline_move_pct, :volatility_ratio,
                         :tightness_score, :last_computed)
                    ON CONFLICT (ceo, topic, ticker) DO UPDATE SET
                        samples           = EXCLUDED.samples,
                        hit_rate          = EXCLUDED.hit_rate,
                        p_value           = EXCLUDED.p_value,
                        avg_abs_move_pct  = EXCLUDED.avg_abs_move_pct,
                        baseline_move_pct = EXCLUDED.baseline_move_pct,
                        volatility_ratio  = EXCLUDED.volatility_ratio,
                        tightness_score   = EXCLUDED.tightness_score,
                        last_computed     = EXCLUDED.last_computed
                """),
                row.to_dict(),
            )
        conn.commit()

    log.info(
        "\nDone. %d relationships written to ceo_ticker_relationships.",
        len(results_df),
    )

    # Print top 20 by tightness for a quick sanity check
    top = results_df.sort_values("tightness_score", ascending=False).head(20)
    print("\n── Top relationships by tightness score ──")
    print(top[["ceo", "topic", "ticker", "samples", "hit_rate",
               "p_value", "volatility_ratio", "tightness_score"]].to_string(index=False))


if __name__ == "__main__":
    run()
