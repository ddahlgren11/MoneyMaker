#!/usr/bin/env python3
"""
MoneyMaker Tweet Watcher — continuous intraday trading agent.

Monitors all CEOs in the relationship registry for new tweets, evaluates
each one against the signal pipeline, and places Alpaca paper trades
immediately when a valid signal is detected.

Flow (runs every POLL_INTERVAL seconds):
  For each CEO in the registry:
    1. Fetch recent tweets via tweety-ns
    2. Identify tweets newer than last processed (dedup by date)
    3. Apply signal gates: engagement · FinBERT content · text length
    4. Classify tweet topic
    5. Look up relationship registry — find best (topic, ticker) match
    6. Run ML model prediction
    7. Confidence gate (≥ 55%)
    8. If market open  → place order immediately
       If market closed → queue signal for next open

Market hours:
  - Polls continuously 24/7 but only places orders Mon–Fri 9:31am–3:55pm ET
  - Queued signals (found outside market hours) are placed at next open
  - Pre-market tweets (before 9:30am) are evaluated and queued

Usage:
  python3 watch.py                   # run with defaults
  python3 watch.py --dry-run         # evaluate signals, no orders
  python3 watch.py --interval 900    # poll every 15 minutes
  python3 watch.py --once            # single pass then exit (useful for testing)
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
POLL_MARKET_HOURS_S  = 20 * 60   # 20 min during market hours
POLL_EXTENDED_S      = 60 * 60   # 60 min pre/post market
POLL_OVERNIGHT_S     = 4  * 60 * 60  # 4 hours overnight

CONFIDENCE_THRESHOLD = 55.0
TIGHTNESS_THRESHOLD  = 0.20
TRADE_NOTIONAL       = 1000.0
TWEET_PAGES          = 2          # pages of tweets to fetch per CEO per cycle (~40 tweets)
FINBERT_THRESHOLD    = 0.10
MIN_TEXT_LEN         = 15

ET = ZoneInfo("America/New_York")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("watcher")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine  = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
Session = sessionmaker(bind=engine)

_WATCHER_STATE_DDL = """
CREATE TABLE IF NOT EXISTS watcher_state (
    ceo              TEXT PRIMARY KEY,
    last_tweet_at    TIMESTAMPTZ,
    last_polled_at   TIMESTAMPTZ,
    tweets_seen      INTEGER DEFAULT 0,
    trades_placed    INTEGER DEFAULT 0
);
"""

_SIGNAL_QUEUE_DDL = """
CREATE TABLE IF NOT EXISTS signal_queue (
    id               SERIAL PRIMARY KEY,
    queued_at        TIMESTAMPTZ DEFAULT NOW(),
    ceo              TEXT,
    tweet_text       TEXT,
    tweet_date       TIMESTAMPTZ,
    topic            TEXT,
    ticker           TEXT,
    tightness_score  FLOAT,
    predicted_direction TEXT,
    confidence_pct   FLOAT,
    sentiment_score  FLOAT,
    finbert_score    FLOAT,
    processed        BOOLEAN DEFAULT FALSE,
    processed_at     TIMESTAMPTZ,
    alpaca_order_id  TEXT
);
"""


def init_db():
    with engine.connect() as conn:
        conn.execute(text(_WATCHER_STATE_DDL))
        conn.execute(text(_SIGNAL_QUEUE_DDL))
        conn.commit()


def get_last_tweet_at(db, ceo: str) -> datetime | None:
    row = db.execute(
        text("SELECT last_tweet_at FROM watcher_state WHERE ceo = :ceo"),
        {"ceo": ceo},
    ).fetchone()
    if row and row[0]:
        dt = pd.to_datetime(row[0])
        return dt.to_pydatetime() if hasattr(dt, "to_pydatetime") else dt
    # Fall back to latest tweet already in merged_data
    row2 = db.execute(
        text("SELECT MAX(date) FROM merged_data WHERE ceo = :ceo"),
        {"ceo": ceo},
    ).fetchone()
    if row2 and row2[0]:
        dt = pd.to_datetime(row2[0])
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    return None


def update_watcher_state(db, ceo: str, latest_tweet_at: datetime):
    db.execute(
        text("""
            INSERT INTO watcher_state (ceo, last_tweet_at, last_polled_at, tweets_seen)
            VALUES (:ceo, :tweet_at, NOW(), 1)
            ON CONFLICT (ceo) DO UPDATE SET
                last_tweet_at  = GREATEST(watcher_state.last_tweet_at, EXCLUDED.last_tweet_at),
                last_polled_at = NOW(),
                tweets_seen    = watcher_state.tweets_seen + 1
        """),
        {"ceo": ceo, "tweet_at": latest_tweet_at},
    )


def increment_trades(db, ceo: str):
    db.execute(
        text("""
            INSERT INTO watcher_state (ceo, trades_placed)
            VALUES (:ceo, 1)
            ON CONFLICT (ceo) DO UPDATE SET
                trades_placed = watcher_state.trades_placed + 1
        """),
        {"ceo": ceo},
    )


def enqueue_signal(db, signal: dict):
    db.execute(
        text("""
            INSERT INTO signal_queue
                (ceo, tweet_text, tweet_date, topic, ticker, tightness_score,
                 predicted_direction, confidence_pct, sentiment_score, finbert_score)
            VALUES
                (:ceo, :tweet_text, :tweet_date, :topic, :ticker, :tightness,
                 :direction, :confidence, :sentiment, :finbert)
        """),
        signal,
    )


def pop_queued_signals(db) -> list[dict]:
    rows = db.execute(
        text("""
            SELECT id, ceo, tweet_text, tweet_date, topic, ticker,
                   tightness_score, predicted_direction, confidence_pct,
                   sentiment_score, finbert_score
            FROM signal_queue
            WHERE processed = FALSE
            ORDER BY queued_at ASC
        """)
    ).fetchall()
    return [dict(r._mapping) for r in rows]


def mark_signal_processed(db, signal_id: int, order_id: str | None):
    db.execute(
        text("""
            UPDATE signal_queue
            SET processed = TRUE, processed_at = NOW(), alpaca_order_id = :oid
            WHERE id = :id
        """),
        {"id": signal_id, "oid": order_id},
    )


# ---------------------------------------------------------------------------
# Market hours
# ---------------------------------------------------------------------------

_NYSE_HOLIDAYS = {
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-04-18",
    "2025-05-26", "2025-06-19", "2025-07-04", "2025-09-01",
    "2025-11-27", "2025-12-25",
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
    "2026-11-26", "2026-12-25",
}


def market_status(now_et: datetime) -> str:
    """
    Returns one of:
      'open'       — NYSE trading hours (9:31am–3:55pm ET, Mon–Fri)
      'pre'        — pre-market (4am–9:30am ET)
      'post'       — post-market (4pm–8pm ET)
      'closed'     — overnight or weekend/holiday
    """
    if now_et.weekday() >= 5 or now_et.strftime("%Y-%m-%d") in _NYSE_HOLIDAYS:
        return "closed"
    h, m = now_et.hour, now_et.minute
    t = h * 60 + m
    if 9 * 60 + 31 <= t <= 15 * 60 + 55:
        return "open"
    if 4 * 60 <= t < 9 * 60 + 30:
        return "pre"
    if 16 * 60 <= t <= 20 * 60:
        return "post"
    return "closed"


def next_market_open(now_et: datetime) -> datetime:
    """Return the next NYSE market open (9:31am ET) from now."""
    candidate = now_et.replace(hour=9, minute=31, second=0, microsecond=0)
    if candidate <= now_et:
        candidate += timedelta(days=1)
    while candidate.weekday() >= 5 or candidate.strftime("%Y-%m-%d") in _NYSE_HOLIDAYS:
        candidate += timedelta(days=1)
    return candidate


# ---------------------------------------------------------------------------
# Signal gates
# ---------------------------------------------------------------------------

def _engagement_score(row: pd.Series) -> float:
    likes    = int(row.get("likes") or 0)
    retweets = int(row.get("retweet_count") or 0)
    replies  = int(row.get("reply_count") or 0)
    views    = int(row.get("view_count") or 0)
    return likes + 2 * retweets + replies + 0.05 * views


def passes_gates(row: pd.Series, eng_threshold: float) -> bool:
    text = str(row.get("text") or row.get("tweet_text") or "")
    stripped = text.lower().replace("https://", "").replace("http://", "").strip()
    if len(stripped) < MIN_TEXT_LEN:
        return False
    finbert = row.get("finbert_score")
    if finbert is not None:
        if abs(float(finbert)) < FINBERT_THRESHOLD:
            return False
    else:
        # No FinBERT score available — fall back to VADER magnitude
        if abs(float(row.get("sentiment") or row.get("sentiment_score") or 0)) < 0.15:
            return False
    if _engagement_score(row) < eng_threshold:
        return False
    return True


# ---------------------------------------------------------------------------
# Trade placement (shared by immediate + queued paths)
# ---------------------------------------------------------------------------

def place_order(ticker: str, direction: str, dry_run: bool) -> str | None:
    """Place a paper market order. Returns Alpaca order ID or None on dry-run."""
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import MarketOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce
    from pipeline_utils import compute_technicals
    from processor import DataProcessor

    if dry_run:
        return None

    tc   = TradingClient(
        os.getenv("ALPACA_PAPER_API_KEY"),
        os.getenv("ALPACA_PAPER_SECRET_KEY"),
        paper=True,
    )
    side = OrderSide.BUY if direction == "Up" else OrderSide.SELL

    # Position management
    positions = {p.symbol: p for p in tc.get_all_positions()}
    if ticker in positions:
        existing_side = str(positions[ticker].side).lower()
        if (existing_side == "long" and direction == "Up") or \
           (existing_side == "short" and direction == "Down"):
            log.info("  already %s %s — skipping duplicate", existing_side, ticker)
            return "DUPLICATE"
        log.info("  closing existing %s position in %s before reversing", existing_side, ticker)
        tc.close_position(ticker)

    if side == OrderSide.BUY:
        order_req = MarketOrderRequest(
            symbol=ticker, notional=TRADE_NOTIONAL,
            side=side, time_in_force=TimeInForce.DAY,
        )
    else:
        proc = DataProcessor()
        end_dt   = datetime.now(timezone.utc)
        stocks   = proc.get_stocks(ticker, start_date=end_dt - timedelta(days=5), end_date=end_dt)
        price    = float(stocks["close"].iloc[-1]) if not stocks.empty else 100.0
        order_req = MarketOrderRequest(
            symbol=ticker, qty=max(1, int(TRADE_NOTIONAL / price)),
            side=side, time_in_force=TimeInForce.DAY,
        )

    order = tc.submit_order(order_req)
    return str(order.id)


def log_trade(db, ceo: str, tweet_text: str, tweet_date, topic: str,
              ticker: str, direction: str, confidence: float,
              tightness: float | None, sentiment: float,
              order_id: str | None, status: str,
              side_str: str, skip_reason: str | None = None):
    db.execute(
        text("""
            INSERT INTO paper_trades
                (timestamp, ceo, tweet_text, tweet_date, topic, ticker, side,
                 notional, predicted_direction, confidence_pct, sentiment_score,
                 tightness_score, alpaca_order_id, status, skip_reason)
            VALUES
                (:ts, :ceo, :tweet, :tweet_date, :topic, :ticker, :side,
                 :notional, :direction, :conf, :sent, :tight, :oid, :status, :reason)
        """),
        {
            "ts":         datetime.now().isoformat(),
            "ceo":        ceo,
            "tweet":      tweet_text[:500],
            "tweet_date": str(tweet_date)[:19] if tweet_date else None,
            "topic":      topic,
            "ticker":     ticker,
            "side":       side_str,
            "notional":   TRADE_NOTIONAL,
            "direction":  direction,
            "conf":       confidence,
            "sent":       sentiment,
            "tight":      tightness,
            "oid":        order_id,
            "status":     status,
            "reason":     skip_reason,
        },
    )


# ---------------------------------------------------------------------------
# Signal evaluation pipeline for a single tweet row
# ---------------------------------------------------------------------------

def evaluate_tweet(row: pd.Series, ceo: str, db) -> dict | None:
    """
    Run the full signal pipeline on one tweet.
    Returns a signal dict if the tweet passes all gates, else None.
    """
    from classifier import get_tweet_topic
    from pipeline_utils import compute_technicals
    from model.predict import predict_tweets
    from targets import HANDLE_TO_TICKER
    from processor import DataProcessor

    tweet_text = str(row.get("text") or "")
    sentiment  = float(row.get("sentiment") or 0)
    finbert    = row.get("finbert_score")
    tweet_date = row.get("date")

    topic = get_tweet_topic(tweet_text, ceo)
    if topic == "personal":
        return None

    # Registry lookup
    rel = db.execute(
        text("""
            SELECT ticker, tightness_score FROM ceo_ticker_relationships
            WHERE ceo = :ceo AND topic = :topic
              AND tightness_score >= :min_t
            ORDER BY tightness_score DESC LIMIT 1
        """),
        {"ceo": ceo, "topic": topic, "min_t": TIGHTNESS_THRESHOLD},
    ).fetchone()

    ticker    = rel.ticker          if rel else HANDLE_TO_TICKER.get(ceo, "")
    tightness = rel.tightness_score if rel else None

    if not ticker:
        return None

    # ML prediction
    end_dt    = datetime.now(timezone.utc)
    start_dt  = end_dt - timedelta(days=60)
    proc      = DataProcessor()
    stocks_df = proc.get_stocks(ticker, start_date=start_dt, end_date=end_dt)
    stocks_df = compute_technicals(stocks_df)

    dt = pd.to_datetime(tweet_date)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    tweet_row = {
        "date":          dt,
        "text":          tweet_text,
        "sentiment":     sentiment,
        "finbert_score": finbert,
        "likes":         int(row.get("likes") or 0),
        "retweet_count": int(row.get("retweet_count") or 0),
        "view_count":    int(row.get("view_count") or 0),
        "reply_count":   int(row.get("reply_count") or 0),
        "tweet_hour":    int(row.get("tweet_hour") or dt.hour),
        "is_premarket":  int(row.get("is_premarket") or 0),
    }

    try:
        result_df = predict_tweets(pd.DataFrame([tweet_row]), stocks_df, ticker=ticker)
    except FileNotFoundError:
        log.warning("  model not found — run python3 model/baseline.py to train it")
        return None

    if result_df.empty:
        return None

    direction  = str(result_df.iloc[0]["predicted_direction"])
    confidence = float(result_df.iloc[0]["confidence_pct"])

    if confidence < CONFIDENCE_THRESHOLD:
        log.debug("  %s → %s: confidence %.1f%% < threshold, skip", ceo, ticker, confidence)
        return None

    return {
        "ceo":        ceo,
        "tweet_text": tweet_text,
        "tweet_date": tweet_date,
        "topic":      topic,
        "ticker":     ticker,
        "tightness":  tightness,
        "direction":  direction,
        "confidence": confidence,
        "sentiment":  sentiment,
        "finbert":    float(finbert) if finbert is not None else None,
    }


# ---------------------------------------------------------------------------
# Process queued signals (runs at market open)
# ---------------------------------------------------------------------------

def process_signal_queue(dry_run: bool):
    db = Session()
    try:
        queued = pop_queued_signals(db)
        if not queued:
            return
        log.info("Processing %d queued signal(s) from overnight/pre-market", len(queued))
        for sig in queued:
            _execute_signal(sig, db, dry_run, source="queue")
        db.commit()
    except Exception as e:
        db.rollback()
        log.error("Queue processing error: %s", e)
    finally:
        db.close()


def _execute_signal(sig: dict, db, dry_run: bool, source: str = "live"):
    """Place order for a signal dict and log the result."""
    ceo       = sig["ceo"]
    ticker    = sig["ticker"]
    direction = sig.get("predicted_direction") or sig.get("direction", "")
    confidence = float(sig.get("confidence_pct") or sig.get("confidence", 0))
    tightness  = sig.get("tightness_score") or sig.get("tightness")
    topic      = sig.get("topic", "")
    sentiment  = float(sig.get("sentiment_score") or sig.get("sentiment", 0))
    tweet_text = sig.get("tweet_text", "")
    tweet_date = sig.get("tweet_date")
    side_str   = "buy" if direction == "Up" else "sell_short"
    prefix     = "[DRY-RUN] " if dry_run else ""

    try:
        order_id = place_order(ticker, direction, dry_run)

        if order_id == "DUPLICATE":
            log.info(
                "%s%s %s → %s (%s) conf=%.1f%% tight=%s — SKIPPED: already positioned",
                prefix, ceo, topic, ticker, direction, confidence,
                f"{tightness:.3f}" if tightness else "n/a",
            )
            log_trade(db, ceo, tweet_text, tweet_date, topic, ticker,
                      direction, confidence, tightness, sentiment,
                      None, "skipped", side_str,
                      skip_reason="already positioned same side")
        else:
            log.info(
                "%s%s %s → %s (%s) conf=%.1f%% tight=%s — ORDER %s%s",
                prefix, ceo, topic, ticker, direction, confidence,
                f"{tightness:.3f}" if tightness else "n/a",
                "PLACED" if not dry_run else "WOULD PLACE",
                f" id={order_id}" if order_id else "",
            )
            log_trade(db, ceo, tweet_text, tweet_date, topic, ticker,
                      direction, confidence, tightness, sentiment,
                      order_id, "placed" if not dry_run else "dry-run", side_str)
            if not dry_run:
                increment_trades(db, ceo)

        # Mark queue entry processed if it came from the queue
        if "id" in sig:
            mark_signal_processed(db, sig["id"], order_id)

    except Exception as e:
        log.error("  Order failed for %s/%s: %s", ceo, ticker, e)
        log_trade(db, ceo, tweet_text, tweet_date, topic, ticker,
                  direction, confidence, tightness, sentiment,
                  None, "error", side_str, skip_reason=str(e))


# ---------------------------------------------------------------------------
# DB-only polling cycle (used by GitHub Actions — no Twitter auth needed)
# Reads recently ingested tweets straight from merged_data.
# ---------------------------------------------------------------------------

def poll_from_db(ceo_list: list[str], dry_run: bool):
    """
    Reads tweets from merged_data that are newer than watcher_state.last_tweet_at
    for each CEO. Used in --db-only mode where Twitter session auth isn't available
    (e.g. GitHub Actions).

    DB rows already have finbert_score, sentiment_score, engagement columns —
    no transformers or tweety-ns required.
    """
    now_et = datetime.now(ET)
    status = market_status(now_et)
    db     = Session()

    try:
        new_signal_count = 0

        for ceo in ceo_list:
            try:
                last_seen = get_last_tweet_at(db, ceo)

                # Pull tweets newer than last processed for this CEO
                query_params = {"ceo": ceo}
                if last_seen:
                    if last_seen.tzinfo is None:
                        last_seen = last_seen.replace(tzinfo=timezone.utc)
                    query_params["since"] = last_seen
                    rows = db.execute(
                        text("""
                            SELECT date, tweet_text, sentiment_score, finbert_score,
                                   likes, retweet_count, view_count, reply_count,
                                   tweet_hour, is_premarket
                            FROM merged_data
                            WHERE ceo = :ceo AND date > :since
                            ORDER BY date ASC
                        """),
                        query_params,
                    ).fetchall()
                else:
                    # First run — look at last 24 hours only to avoid flooding
                    rows = db.execute(
                        text("""
                            SELECT date, tweet_text, sentiment_score, finbert_score,
                                   likes, retweet_count, view_count, reply_count,
                                   tweet_hour, is_premarket
                            FROM merged_data
                            WHERE ceo = :ceo
                              AND date >= NOW() - INTERVAL '24 hours'
                            ORDER BY date ASC
                        """),
                        query_params,
                    ).fetchall()

                if not rows:
                    log.debug("%s — no new tweets in DB", ceo)
                    continue

                log.info("%s — %d new tweet(s) in DB", ceo, len(rows))

                # Build a DataFrame with consistent column names for evaluate_tweet
                new_df = pd.DataFrame([dict(r._mapping) for r in rows])
                new_df = new_df.rename(columns={
                    "tweet_text":      "text",
                    "sentiment_score": "sentiment",
                })

                # Engagement threshold relative to all stored tweets for this CEO
                all_eng = db.execute(
                    text("""
                        SELECT likes, retweet_count, view_count, reply_count
                        FROM merged_data WHERE ceo = :ceo
                    """),
                    {"ceo": ceo},
                ).fetchall()
                all_eng_df = pd.DataFrame([dict(r._mapping) for r in all_eng])
                eng_threshold = float(
                    all_eng_df.apply(_engagement_score, axis=1).quantile(0.25)
                ) if not all_eng_df.empty else 0.0

                for _, row in new_df.iterrows():
                    if not passes_gates(row, eng_threshold):
                        continue

                    signal = evaluate_tweet(row, ceo, db)
                    if signal is None:
                        continue

                    new_signal_count += 1

                    if status == "open":
                        log.info(
                            "  SIGNAL: %s → %s/%s  conf=%.1f%%  tight=%s  [MARKET OPEN — trading]",
                            ceo, signal["topic"], signal["ticker"],
                            signal["confidence"],
                            f"{signal['tightness']:.3f}" if signal["tightness"] else "n/a",
                        )
                        _execute_signal(signal, db, dry_run)
                    else:
                        log.info(
                            "  SIGNAL: %s → %s/%s  conf=%.1f%%  tight=%s  [market %s — queuing]",
                            ceo, signal["topic"], signal["ticker"],
                            signal["confidence"],
                            f"{signal['tightness']:.3f}" if signal["tightness"] else "n/a",
                            status,
                        )
                        enqueue_signal(db, {
                            "ceo":        signal["ceo"],
                            "tweet_text": signal["tweet_text"],
                            "tweet_date": signal["tweet_date"],
                            "topic":      signal["topic"],
                            "ticker":     signal["ticker"],
                            "tightness":  signal["tightness"],
                            "direction":  signal["direction"],
                            "confidence": signal["confidence"],
                            "sentiment":  signal["sentiment"],
                            "finbert":    signal["finbert"],
                        })

                # Advance the watermark
                latest = pd.to_datetime(new_df["date"].max())
                if latest.tzinfo is None:
                    latest = latest.replace(tzinfo=timezone.utc)
                update_watcher_state(db, ceo, latest)

            except Exception as e:
                log.warning("  Error processing %s: %s", ceo, e)
                continue

        db.commit()

        if new_signal_count == 0:
            log.info("DB poll complete — no new signals (market: %s)", status)

    except Exception as e:
        db.rollback()
        log.error("DB poll error: %s", e)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# One polling cycle (Twitter mode)
# ---------------------------------------------------------------------------

async def poll_once(ceo_list: list[str], dry_run: bool):
    from processor import DataProcessor
    from classifier import get_finbert_scores_batch

    now_et = datetime.now(ET)
    status = market_status(now_et)

    db = Session()
    try:
        proc = DataProcessor()
        new_signal_count = 0

        for ceo in ceo_list:
            try:
                last_seen = get_last_tweet_at(db, ceo)

                log.debug("Fetching tweets for %s (last seen: %s)", ceo,
                          last_seen.strftime("%Y-%m-%d %H:%M") if last_seen else "never")

                tweets_df = await proc.get_tweets(ceo, pages=TWEET_PAGES)

                if tweets_df.empty:
                    continue

                # Filter to genuinely new tweets
                if last_seen is not None:
                    if tweets_df["date"].dt.tz is None:
                        tweets_df["date"] = tweets_df["date"].dt.tz_localize("UTC")
                    if last_seen.tzinfo is None:
                        last_seen = last_seen.replace(tzinfo=timezone.utc)
                    new_df = tweets_df[tweets_df["date"] > last_seen].copy()
                else:
                    new_df = tweets_df.copy()

                if new_df.empty:
                    log.debug("  %s — no new tweets", ceo)
                    continue

                log.info("%s — %d new tweet(s)", ceo, len(new_df))

                # Engagement threshold for this CEO's current tweet set
                eng_threshold = float(
                    tweets_df.apply(_engagement_score, axis=1).quantile(0.25)
                )

                for _, row in new_df.iterrows():
                    if not passes_gates(row, eng_threshold):
                        log.debug("  tweet failed gates, skip")
                        continue

                    signal = evaluate_tweet(row, ceo, db)
                    if signal is None:
                        continue

                    new_signal_count += 1

                    if status == "open":
                        log.info(
                            "  SIGNAL: %s → %s/%s  conf=%.1f%%  tight=%s  [MARKET OPEN — trading]",
                            ceo, signal["topic"], signal["ticker"],
                            signal["confidence"],
                            f"{signal['tightness']:.3f}" if signal["tightness"] else "n/a",
                        )
                        _execute_signal(signal, db, dry_run)
                    else:
                        log.info(
                            "  SIGNAL: %s → %s/%s  conf=%.1f%%  tight=%s  [market %s — queuing]",
                            ceo, signal["topic"], signal["ticker"],
                            signal["confidence"],
                            f"{signal['tightness']:.3f}" if signal["tightness"] else "n/a",
                            status,
                        )
                        enqueue_signal(db, {
                            "ceo":        signal["ceo"],
                            "tweet_text": signal["tweet_text"],
                            "tweet_date": signal["tweet_date"],
                            "topic":      signal["topic"],
                            "ticker":     signal["ticker"],
                            "tightness":  signal["tightness"],
                            "direction":  signal["direction"],
                            "confidence": signal["confidence"],
                            "sentiment":  signal["sentiment"],
                            "finbert":    signal["finbert"],
                        })

                # Update last seen to newest tweet in this batch
                latest = new_df["date"].max()
                if latest.tzinfo is None:
                    latest = latest.replace(tzinfo=timezone.utc)
                update_watcher_state(db, ceo, latest)

            except Exception as e:
                log.warning("  Error processing %s: %s", ceo, e)
                continue

        db.commit()

        if new_signal_count == 0:
            log.info("Cycle complete — no new signals (market: %s)", status)

    except Exception as e:
        db.rollback()
        log.error("Poll cycle error: %s", e)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def run(ceo_list: list[str], dry_run: bool, interval_override: int | None,
              once: bool, db_only: bool):

    init_db()

    mode = "DB-only (GitHub Actions)" if db_only else "live Twitter"
    log.info("MoneyMaker Watcher started  [mode: %s]", mode)
    log.info("Tracking %d CEOs: %s", len(ceo_list), ", ".join(ceo_list))
    if dry_run:
        log.info("DRY-RUN mode — signals will be evaluated but no orders placed")

    _queued_open_processed = False

    while True:
        now_et = datetime.now(ET)
        status = market_status(now_et)

        # At market open: flush any overnight/pre-market queued signals first
        if status == "open" and not _queued_open_processed:
            process_signal_queue(dry_run)
            _queued_open_processed = True
        elif status != "open":
            _queued_open_processed = False

        # Poll — DB-only skips Twitter entirely
        if db_only:
            poll_from_db(ceo_list, dry_run)
        else:
            await poll_once(ceo_list, dry_run)

        if once:
            break

        # Dynamic sleep
        if interval_override:
            sleep_s = interval_override
        elif status == "open":
            sleep_s = POLL_MARKET_HOURS_S
        elif status in ("pre", "post"):
            sleep_s = POLL_EXTENDED_S
        else:
            next_open = next_market_open(now_et)
            wake_time = next_open - timedelta(minutes=30)
            sleep_s   = max(300, (wake_time - now_et).total_seconds())
            log.info(
                "Market closed. Sleeping until %s ET (%dh %dm)",
                wake_time.strftime("%H:%M"),
                int(sleep_s // 3600), int((sleep_s % 3600) // 60),
            )

        await asyncio.sleep(sleep_s)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="MoneyMaker tweet watcher")
    parser.add_argument("ceos", nargs="*",
                        help="CEO handles to watch (default: all with registry entries)")
    parser.add_argument("--dry-run",   action="store_true",
                        help="Evaluate signals without placing orders")
    parser.add_argument("--interval",  type=int, default=None,
                        help="Override poll interval in seconds")
    parser.add_argument("--once",      action="store_true",
                        help="Run one poll cycle then exit (for testing)")
    parser.add_argument("--db-only",   action="store_true",
                        help="Read from DB instead of fetching Twitter (no auth needed — used by GitHub Actions)")
    args = parser.parse_args()

    # Resolve CEO list
    if args.ceos:
        ceo_list = args.ceos
    else:
        db = Session()
        try:
            rows = db.execute(
                text("""
                    SELECT DISTINCT ceo FROM ceo_ticker_relationships
                    WHERE tightness_score >= :min_t ORDER BY ceo
                """),
                {"min_t": TIGHTNESS_THRESHOLD},
            ).fetchall()
            ceo_list = [r[0] for r in rows]
        finally:
            db.close()

    if not ceo_list:
        print(
            "No CEOs with registered relationships found.\n"
            "Run `python3 relationship_analysis.py` first."
        )
        sys.exit(1)

    asyncio.run(run(ceo_list, args.dry_run, args.interval, args.once, args.db_only))


if __name__ == "__main__":
    main()
