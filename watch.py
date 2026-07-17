#!/usr/bin/env python3
"""
MoneyMaker Tweet Watcher — continuous intraday trading agent.

Monitors all CEOs in the relationship registry for new tweets, evaluates
each one against the signal pipeline, and places Alpaca paper trades
immediately when a valid signal is detected.

Flow (runs every POLL_INTERVAL seconds):
  For each CEO in the registry:
    1. Fetch recent tweets via twikit
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
POLL_MARKET_HOURS_S  = 20 * 60   # 20 min — full sweep of all accounts
POLL_HIGH_PRIORITY_S =  3 * 60   # 3 min — fast lane for time-sensitive accounts
POLL_EXTENDED_S      = 60 * 60   # 60 min pre/post market
POLL_OVERNIGHT_S     = 4  * 60 * 60  # 4 hours overnight

# Accounts polled every 3 min during market hours.
# Short sellers publish rarely but move stocks 20-50% within minutes of posting.
# Congressional aggregators and presidential accounts also move fast.
HIGH_PRIORITY_HANDLES = {
    # Short sellers — reports cause immediate, large moves
    "HindenburgRes", "muddywaters", "CitronResearch", "GothamResearch", "PrestigeEconom1",
    # Congressional trade aggregators — disclosures post continuously during market hours
    "unusual_whales", "capitoltrades",
    # Presidential / treasury — tariff tweets move sectors within minutes
    "realDonaldTrump", "POTUS", "ScottBessent",
    # Highest-signal CEO (tightness 0.57, 4.8% avg move)
    "george_kurtz",
}

CONFIDENCE_THRESHOLD = 55.0
TIGHTNESS_THRESHOLD  = 0.20
TWEET_PAGES          = 2          # pages of tweets to fetch per CEO per cycle (~40 tweets)
FINBERT_THRESHOLD    = 0.10
MIN_TEXT_LEN         = 15

# ── Position sizing (conviction-scaled) ────────────────────────────────────
# Notional scales between MIN and MAX by a conviction score built from the
# model confidence and the relationship tightness. TRADE_NOTIONAL is the
# fallback/base used for logging defaults and non-signal paths.
TRADE_NOTIONAL = float(os.getenv("TRADE_NOTIONAL", "1000"))
MIN_NOTIONAL   = float(os.getenv("MIN_NOTIONAL",   "500"))
MAX_NOTIONAL   = float(os.getenv("MAX_NOTIONAL",   "2500"))
CONF_SIZING_CEILING = 90.0   # confidence at/above which the confidence factor maxes out

# ── Portfolio risk caps ────────────────────────────────────────────────────
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "15"))
MAX_DAILY_LOSS     = float(os.getenv("MAX_DAILY_LOSS", "1500"))  # halt new entries past this loss

# ── Congressional trades (ingested by congress_ingest.py into congress_trades) ──
# Only act on disclosures filed within this many days, so the first poll doesn't
# trade the entire recent backlog. The signal is the disclosure becoming public.
CONGRESS_RECENCY_DAYS = int(os.getenv("CONGRESS_RECENCY_DAYS", "4"))
CONGRESS_CONFIDENCE   = 80.0
# Event study + backtest both show congressional BUYS have no edge (they
# underperform just holding SPY), while the SELLS carry the only (weak, unproven)
# edge. When true, skip the buys and trade sells only. See the strategy bake-off.
CONGRESS_SELLS_ONLY   = os.getenv("CONGRESS_SELLS_ONLY", "false").lower() == "true"

# ── SEC Form 4 insider trades (ingested by insider_ingest.py into insider_trades) ─
# Structured corporate-insider analogue of congressional trades: open-market buy
# (code P) → Up, sale (code S) → Down. Same fast-path treatment (no registry/ML).
# Insider BUYS are the well-documented predictive signal; SELLS are noisy
# (diversification/tax/10b5-1), so they're OFF by default — run event_study.py to
# decide whether to enable them via INSIDER_BUYS_ONLY=false.
# Trading is OFF by default: insider signals are collected/queryable but not
# acted on until validated via event_study.py and explicitly enabled. This keeps
# a merge to main inert until you flip the switch.
INSIDER_TRADING_ENABLED = os.getenv("INSIDER_TRADING_ENABLED", "false").lower() == "true"
INSIDER_RECENCY_DAYS  = int(os.getenv("INSIDER_RECENCY_DAYS", "3"))
INSIDER_CONFIDENCE    = float(os.getenv("INSIDER_CONFIDENCE", "70.0"))
INSIDER_BUYS_ONLY     = os.getenv("INSIDER_BUYS_ONLY", "true").lower() != "false"
INSIDER_MIN_VALUE     = float(os.getenv("INSIDER_MIN_TRADE_VALUE", "50000"))

# ── Reddit spike signals (ingested by reddit_ingest.py into reddit_signals) ──────
# EXPERIMENTAL and UNPROVEN — Reddit chatter has no inherent direction, so live
# trading is OFF by default. Signals are still collected for event_study.py; only
# flip REDDIT_TRADING_ENABLED=true once the study shows the spike heuristic has edge.
REDDIT_TRADING_ENABLED = os.getenv("REDDIT_TRADING_ENABLED", "false").lower() == "true"
REDDIT_RECENCY_DAYS    = int(os.getenv("REDDIT_RECENCY_DAYS", "2"))
REDDIT_CONFIDENCE      = float(os.getenv("REDDIT_CONFIDENCE", "60.0"))

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


_MANAGED_POSITIONS_DDL = """
CREATE TABLE IF NOT EXISTS managed_positions (
    ticker            TEXT PRIMARY KEY,
    side              TEXT,            -- 'long' or 'short'
    ceo               TEXT,
    topic             TEXT,
    opened_at         TIMESTAMPTZ DEFAULT NOW(),
    exit_after        TIMESTAMPTZ,    -- close at/after this time (next-day horizon)
    entry_confidence  FLOAT
);
"""


_CONGRESS_TRADES_DDL = """
CREATE TABLE IF NOT EXISTS congress_trades (
    id              SERIAL PRIMARY KEY,
    dedup_key       TEXT UNIQUE,
    chamber         TEXT,
    member          TEXT,
    ticker          TEXT,
    asset_type      TEXT,
    txn_type        TEXT,
    direction       TEXT,
    txn_date        TEXT,
    disclosure_date TEXT,
    amount          TEXT,
    owner           TEXT,
    ingested_at     TIMESTAMPTZ DEFAULT NOW(),
    processed       BOOLEAN DEFAULT FALSE
);
"""


def init_db():
    with engine.connect() as conn:
        conn.execute(text(_WATCHER_STATE_DDL))
        conn.execute(text(_SIGNAL_QUEUE_DDL))
        conn.execute(text(_MANAGED_POSITIONS_DDL))
        conn.execute(text(_CONGRESS_TRADES_DDL))
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
    existing = db.execute(
        text("""
            SELECT id FROM signal_queue
            WHERE ceo = :ceo AND tweet_date = :tweet_date AND ticker = :ticker
              AND processed = FALSE
        """),
        {"ceo": signal["ceo"], "tweet_date": signal["tweet_date"], "ticker": signal["ticker"]},
    ).fetchone()
    if existing:
        log.debug("  signal already queued for %s/%s — skipping duplicate enqueue", signal["ceo"], signal["ticker"])
        return
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


def next_day_exit_time(entry_et: datetime) -> datetime:
    """
    Exit horizon for a trade: the next trading day after entry, at 15:30 ET.

    The model predicts next-day direction, so positions are held until the close
    of the following session. 15:30 (not 16:00) ensures the last pre-close poll
    cycle catches the exit while the market is still open.
    """
    d = entry_et + timedelta(days=1)
    while d.weekday() >= 5 or d.strftime("%Y-%m-%d") in _NYSE_HOLIDAYS:
        d += timedelta(days=1)
    return d.replace(hour=15, minute=30, second=0, microsecond=0)


# ---------------------------------------------------------------------------
# Signal gates
# ---------------------------------------------------------------------------

def _engagement_score(row: pd.Series) -> float:
    likes    = int(row.get("likes") or 0)
    retweets = int(row.get("retweet_count") or 0)
    replies  = int(row.get("reply_count") or 0)
    views    = int(row.get("view_count") or 0)
    return likes + 2 * retweets + replies + 0.05 * views


def passes_gates(row: pd.Series, eng_threshold: float, ceo: str | None = None) -> bool:
    from classifier import get_tweet_topic

    text = str(row.get("text") or row.get("tweet_text") or "")
    stripped = text.lower().replace("https://", "").replace("http://", "").strip()
    if len(stripped) < MIN_TEXT_LEN:
        return False

    # Congressional disclosures and policy statements are factual and low-sentiment
    # by nature (e.g. "Rep. Pelosi purchased $1M-$5M of $NVDA"). The sentiment gate
    # was tuned for opinionated CEO tweets and would wrongly discard them, so exempt
    # those topics — the ticker/direction comes from the post itself, not sentiment.
    topic = get_tweet_topic(text, ceo) if ceo else None
    if topic not in ("congressional_trade", "policy", "short_report", "insider_trade"):
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

def _trading_client():
    """Construct an Alpaca paper TradingClient from env credentials."""
    from alpaca.trading.client import TradingClient
    return TradingClient(
        os.getenv("ALPACA_PAPER_API_KEY"),
        os.getenv("ALPACA_PAPER_SECRET_KEY"),
        paper=True,
    )


def position_notional(confidence: float, tightness: float | None) -> float:
    """
    Conviction-scaled trade size. Blends model confidence and relationship
    tightness into a 0–1 conviction score, then maps it onto [MIN, MAX] notional.

    Confidence contributes from the gate (55%) up to CONF_SIZING_CEILING (90%);
    tightness contributes directly (None → 0.5, the neutral mid-point used by
    fast-path signals that skip the registry).
    """
    span = max(CONF_SIZING_CEILING - CONFIDENCE_THRESHOLD, 1.0)
    conf_frac = max(0.0, min((confidence - CONFIDENCE_THRESHOLD) / span, 1.0))
    tight = tightness if tightness is not None else 0.5
    conviction = 0.6 * conf_frac + 0.4 * tight
    return round(MIN_NOTIONAL + conviction * (MAX_NOTIONAL - MIN_NOTIONAL), 2)


def risk_gate(db, ticker: str, dry_run: bool) -> tuple[bool, str]:
    """
    Portfolio-level guardrails checked before opening a new position.
    Returns (allowed, reason). Exits and same-ticker reversals are not gated
    here — they don't add a new position slot.
    """
    already_tracked = db.execute(
        text("SELECT 1 FROM managed_positions WHERE ticker = :t"), {"t": ticker}
    ).fetchone()

    if not already_tracked:
        open_count = db.execute(
            text("SELECT COUNT(*) FROM managed_positions")
        ).scalar() or 0
        if open_count >= MAX_OPEN_POSITIONS:
            return False, f"max open positions reached ({open_count}/{MAX_OPEN_POSITIONS})"

    if dry_run:
        return True, ""

    # Daily-loss kill switch — Alpaca tracks equity vs the prior close.
    try:
        acct = _trading_client().get_account()
        daily_pl = float(acct.equity) - float(acct.last_equity)
        if daily_pl <= -MAX_DAILY_LOSS:
            return False, f"daily loss limit hit (P&L ${daily_pl:,.0f})"
    except Exception as e:
        log.warning("  risk check: could not read account (%s) — allowing trade", e)

    return True, ""


def place_order(ticker: str, direction: str, notional: float,
                dry_run: bool) -> str | None:
    """Place a paper market order. Returns Alpaca order ID or None on dry-run."""
    from alpaca.trading.requests import MarketOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce

    if dry_run:
        return None

    tc   = _trading_client()
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
            symbol=ticker, notional=notional,
            side=side, time_in_force=TimeInForce.DAY,
        )
    else:
        # Shorts need qty not notional — use yfinance for price (no auth needed)
        try:
            import yfinance as _yf
            _df = _yf.download(ticker, period="2d", auto_adjust=True, progress=False)
            if isinstance(_df.columns, pd.MultiIndex):
                _df.columns = [str(c[0]).lower() for c in _df.columns]
            else:
                _df.columns = [str(c).lower() for c in _df.columns]
            price = float(_df["close"].iloc[-1]) if not _df.empty else 100.0
        except Exception:
            price = 100.0
        order_req = MarketOrderRequest(
            symbol=ticker, qty=max(1, int(notional / price)),
            side=side, time_in_force=TimeInForce.DAY,
        )

    order = tc.submit_order(order_req)
    return str(order.id)


def log_trade(db, ceo: str, tweet_text: str, tweet_date, topic: str,
              ticker: str, direction: str, confidence: float,
              tightness: float | None, sentiment: float,
              order_id: str | None, status: str,
              side_str: str, skip_reason: str | None = None,
              notional: float = TRADE_NOTIONAL):
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
            "notional":   notional,
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
# Position lifecycle — scheduled exit at the next-day prediction horizon
# ---------------------------------------------------------------------------

def register_position(db, ticker: str, side: str, ceo: str, topic: str,
                      confidence: float, exit_after: datetime):
    """Record (or refresh) an open position so it can be closed at its horizon."""
    db.execute(
        text("""
            INSERT INTO managed_positions
                (ticker, side, ceo, topic, opened_at, exit_after, entry_confidence)
            VALUES (:ticker, :side, :ceo, :topic, NOW(), :exit_after, :conf)
            ON CONFLICT (ticker) DO UPDATE SET
                side             = EXCLUDED.side,
                ceo              = EXCLUDED.ceo,
                topic            = EXCLUDED.topic,
                opened_at        = NOW(),
                exit_after       = EXCLUDED.exit_after,
                entry_confidence = EXCLUDED.entry_confidence
        """),
        {"ticker": ticker, "side": side, "ceo": ceo, "topic": topic,
         "exit_after": exit_after, "conf": confidence},
    )


def close_due_positions(dry_run: bool):
    """
    Close any tracked position whose next-day exit horizon has passed.
    Runs only during market hours (a market order needs an open market).
    """
    now_et = datetime.now(ET)
    if market_status(now_et) != "open":
        return

    db = Session()
    try:
        rows = db.execute(
            text("""
                SELECT ticker, side, ceo, topic FROM managed_positions
                WHERE exit_after <= NOW()
            """)
        ).fetchall()
        if not rows:
            return

        tc = None if dry_run else _trading_client()
        for r in rows:
            ticker = r.ticker
            exit_side = "sell" if r.side == "long" else "buy_to_cover"
            try:
                if not dry_run:
                    tc.close_position(ticker)
                log.info(
                    "%sEXIT %s (%s/%s) — closed at next-day horizon",
                    "[DRY-RUN] " if dry_run else "", ticker, r.ceo, r.topic,
                )
                log_trade(db, r.ceo, "", None, r.topic, ticker,
                          "exit", 0.0, None, 0.0, None,
                          "exit" if not dry_run else "dry-run-exit", exit_side,
                          skip_reason="scheduled next-day horizon exit")
                db.execute(text("DELETE FROM managed_positions WHERE ticker = :t"),
                           {"t": ticker})
            except Exception as e:
                msg = str(e).lower()
                log.error("  Exit failed for %s: %s", ticker, e)
                # Position already gone at the broker — stop tracking it.
                if "position does not exist" in msg or "not found" in msg or "404" in msg:
                    db.execute(text("DELETE FROM managed_positions WHERE ticker = :t"),
                               {"t": ticker})
        db.commit()
    except Exception as e:
        db.rollback()
        log.error("Exit sweep error: %s", e)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Signal evaluation pipeline for a single tweet row
# ---------------------------------------------------------------------------

def evaluate_tweet(row: pd.Series, ceo: str, db,
                   proc=None, stocks_cache: dict | None = None) -> dict | None:
    """
    Run the full signal pipeline on one tweet.
    Returns a signal dict if the tweet passes all gates, else None.

    proc         — shared DataProcessor instance (created once per poll cycle)
    stocks_cache — dict keyed by ticker; avoids re-fetching the same 60-day
                   window for multiple tweets mapped to the same stock
    """
    from classifier import get_tweet_topic
    from pipeline_utils import compute_technicals
    from model.predict import predict_tweets
    from targets import HANDLE_TO_TICKER
    from processor import DataProcessor

    if stocks_cache is None:
        stocks_cache = {}

    tweet_text = str(row.get("text") or "")
    sentiment  = float(row.get("sentiment") or 0)
    finbert    = row.get("finbert_score")
    tweet_date = row.get("date")

    topic = get_tweet_topic(tweet_text, ceo)
    if topic == "personal":
        return None

    # Fast path — congressional trade disclosures: skip registry and ML entirely.
    # The ticker and direction are explicit in the post; confidence is fixed at 80%.
    if topic == "congressional_trade":
        from classifier import parse_congressional_trade
        trade = parse_congressional_trade(tweet_text)
        if trade is None:
            return None
        log.info("  CONGRESSIONAL TRADE: %s → %s (%s)", ceo, trade["ticker"], trade["direction"])
        return {
            "ceo":        ceo,
            "tweet_text": tweet_text,
            "tweet_date": tweet_date,
            "topic":      topic,
            "ticker":     trade["ticker"],
            "tightness":  1.0,
            "direction":  trade["direction"],
            "confidence": 80.0,
            "sentiment":  sentiment,
            "finbert":    float(finbert) if finbert is not None else None,
        }

    # Fast path — short-seller report: a published report is a strong DOWN signal
    # on the named ticker. Skip registry and ML; the ticker comes from the post.
    if topic == "short_report":
        from classifier import parse_short_seller_report
        rep = parse_short_seller_report(tweet_text)
        if rep is None:
            return None
        log.info("  SHORT REPORT: %s → %s (Down)", ceo, rep["ticker"])
        return {
            "ceo":        ceo,
            "tweet_text": tweet_text,
            "tweet_date": tweet_date,
            "topic":      topic,
            "ticker":     rep["ticker"],
            "tightness":  1.0,
            "direction":  "Down",
            "confidence": 75.0,
            "sentiment":  sentiment,
            "finbert":    float(finbert) if finbert is not None else None,
        }

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

    # Fetch technicals once per ticker per cycle, reuse across tweets
    if ticker not in stocks_cache:
        end_dt   = datetime.now(timezone.utc) - timedelta(days=1)
        start_dt = end_dt - timedelta(days=60)
        try:
            _proc = proc if proc is not None else DataProcessor()
            df = _proc.get_stocks(ticker, start_date=start_dt, end_date=end_dt)
            stocks_cache[ticker] = compute_technicals(df)
        except Exception as _stock_err:
            log.debug("  Stock data unavailable for %s (%s) — using empty DF", ticker, _stock_err)
            stocks_cache[ticker] = pd.DataFrame(
                columns=["date_only", "close", "open", "high", "low",
                         "volume", "rsi_14", "atr_14"]
            )
    stocks_df = stocks_cache[ticker]

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

    # Idempotency — never trade the same tweet twice. Guards against retries and
    # against two watcher deployments (the Render worker and the GitHub Actions
    # db-only run) both processing the same signal.
    if not dry_run and tweet_date is not None:
        td_str = str(tweet_date)[:19]
        dup = db.execute(
            text("""
                SELECT 1 FROM paper_trades
                WHERE ceo = :ceo AND ticker = :ticker AND tweet_date = :td
                  AND status = 'placed'
                LIMIT 1
            """),
            {"ceo": ceo, "ticker": ticker, "td": td_str},
        ).fetchone()
        if dup:
            log.info("  %s %s/%s already traded for this tweet — skipping duplicate",
                     ceo, topic, ticker)
            if "id" in sig:
                mark_signal_processed(db, sig["id"], None)
            return

    # Market-regime gate (top-level, Part II). Project policy: gate LONG entries
    # only — shorts (congress/insider sales) fire regardless of regime. Inert
    # unless REGIME_GATE_ENABLED, so it's validated via backtest.py before it
    # touches live trades. A blocked long is logged skipped; an allowed long's
    # size is scaled by the regime's exposure confidence.
    import regime as _regime
    regime_scale = 1.0
    if _regime.REGIME_GATE_ENABLED:
        reg_ok, regime_scale, reg_reason = _regime.gate_for_direction(direction)
        if not reg_ok:
            log.info("  REGIME GATE — %s %s/%s skipped: %s", ceo, topic, ticker, reg_reason)
            log_trade(db, ceo, tweet_text, tweet_date, topic, ticker,
                      direction, confidence, tightness, sentiment,
                      None, "skipped", side_str, skip_reason=f"regime: {reg_reason}",
                      notional=0.0)
            if "id" in sig:
                mark_signal_processed(db, sig["id"], None)
            return

    # Portfolio risk caps — block new entries past position/loss limits.
    allowed, reason = risk_gate(db, ticker, dry_run)
    if not allowed:
        log.warning("  RISK HALT — %s %s/%s skipped: %s", ceo, topic, ticker, reason)
        log_trade(db, ceo, tweet_text, tweet_date, topic, ticker,
                  direction, confidence, tightness, sentiment,
                  None, "skipped", side_str, skip_reason=f"risk: {reason}",
                  notional=0.0)
        if "id" in sig:
            mark_signal_processed(db, sig["id"], None)
        return

    # Sector reactivity weight (Part I): down-weight sentiment signals in
    # low-reactivity sectors (e.g. Energy); structured signals are unscaled.
    import sector_map as _sector
    sector_scale = (_sector.signal_weight(ticker, topic)
                    if _sector.SECTOR_WEIGHTING_ENABLED else 1.0)

    # Conviction-scaled position size, then scaled by regime + sector confidence
    notional = round(position_notional(confidence, tightness) * regime_scale * sector_scale, 2)

    try:
        order_id = place_order(ticker, direction, notional, dry_run)

        if order_id == "DUPLICATE":
            log.info(
                "%s%s %s → %s (%s) conf=%.1f%% tight=%s — SKIPPED: already positioned",
                prefix, ceo, topic, ticker, direction, confidence,
                f"{tightness:.3f}" if tightness else "n/a",
            )
            log_trade(db, ceo, tweet_text, tweet_date, topic, ticker,
                      direction, confidence, tightness, sentiment,
                      None, "skipped", side_str,
                      skip_reason="already positioned same side", notional=0.0)
        else:
            log.info(
                "%s%s %s → %s (%s) conf=%.1f%% tight=%s size=$%.0f — ORDER %s%s",
                prefix, ceo, topic, ticker, direction, confidence,
                f"{tightness:.3f}" if tightness else "n/a", notional,
                "PLACED" if not dry_run else "WOULD PLACE",
                f" id={order_id}" if order_id else "",
            )
            log_trade(db, ceo, tweet_text, tweet_date, topic, ticker,
                      direction, confidence, tightness, sentiment,
                      order_id, "placed" if not dry_run else "dry-run", side_str,
                      notional=notional)
            if not dry_run:
                increment_trades(db, ceo)
                # Track the open position so it's closed at the next-day horizon
                register_position(
                    db, ticker,
                    "long" if direction == "Up" else "short",
                    ceo, topic, confidence,
                    next_day_exit_time(datetime.now(ET)),
                )

        # Mark queue entry processed if it came from the queue
        if "id" in sig:
            mark_signal_processed(db, sig["id"], order_id)

    except Exception as e:
        msg = str(e).lower()
        # Flag Alpaca's intraday-margin pre-trade rejection distinctly so it's
        # obvious in the logs (vs. a generic API error). See the 2026 intraday
        # margin framework — orders that would cause a margin deficit are rejected.
        if any(s in msg for s in ("margin", "buying power", "insufficient")):
            log.warning("  MARGIN REJECTION — %s/%s order rejected (intraday margin / "
                        "buying power): %s. Consider lowering MAX_OPEN_POSITIONS or "
                        "MAX_NOTIONAL.", ceo, ticker, e)
            reason = f"margin rejection: {e}"
        else:
            log.error("  Order failed for %s/%s: %s", ceo, ticker, e)
            reason = str(e)
        log_trade(db, ceo, tweet_text, tweet_date, topic, ticker,
                  direction, confidence, tightness, sentiment,
                  None, "error", side_str, skip_reason=reason, notional=notional)
        # Always retire the queue entry on failure — don't retry indefinitely
        if "id" in sig:
            mark_signal_processed(db, sig["id"], None)


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
    no transformers or twikit required.
    """
    now_et = datetime.now(ET)
    status = market_status(now_et)
    db     = Session()

    try:
        from processor import DataProcessor
        proc         = DataProcessor()
        stocks_cache = {}
        new_signal_count = 0

        for ceo in ceo_list:
            try:
                last_seen = get_last_tweet_at(db, ceo)

                # Pull tweets newer than last processed for this CEO
                query_params = {"ceo": ceo}
                if last_seen:
                    if last_seen.tzinfo is None:
                        last_seen = last_seen.replace(tzinfo=timezone.utc)
                    # date column is VARCHAR — cast both sides via ISO string so
                    # PostgreSQL can compare them without a type mismatch
                    query_params["since"] = last_seen.strftime("%Y-%m-%dT%H:%M:%S")
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
                    if not passes_gates(row, eng_threshold, ceo):
                        continue

                    signal = evaluate_tweet(row, ceo, db, proc=proc, stocks_cache=stocks_cache)
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
                db.rollback()   # reset aborted transaction so next CEO can query cleanly
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
# Congressional trades — trade newly disclosed filings from congress_trades
# (populated by congress_ingest.py). Structured data: ticker + direction are
# explicit, so this bypasses the registry/ML entirely, like the tweet fast-path.
# ---------------------------------------------------------------------------

def poll_congress_trades(dry_run: bool):
    now_et = datetime.now(ET)
    status = market_status(now_et)
    cutoff = (now_et.date() - timedelta(days=CONGRESS_RECENCY_DAYS)).isoformat()

    db = Session()
    try:
        rows = db.execute(
            text("""
                SELECT id, chamber, member, ticker, direction, txn_type,
                       disclosure_date, amount
                FROM congress_trades
                WHERE processed = FALSE AND disclosure_date >= :cutoff
                ORDER BY disclosure_date ASC
            """),
            {"cutoff": cutoff},
        ).fetchall()

        # Retire disclosures too old to act on so they don't linger unprocessed.
        db.execute(
            text("""
                UPDATE congress_trades SET processed = TRUE
                WHERE processed = FALSE AND disclosure_date < :cutoff
            """),
            {"cutoff": cutoff},
        )

        if not rows:
            db.commit()
            return

        log.info("Congress: %d newly disclosed trade(s) to act on", len(rows))
        for r in rows:
            # Buys have no measured edge (they underperform SPY); trade sells only
            # when configured. Mark processed so the skipped buy doesn't linger.
            if CONGRESS_SELLS_ONLY and r.direction == "Up":
                log.info("  CONGRESS %s → skip BUY %s (sells-only mode)", r.member, r.ticker)
                db.execute(text("UPDATE congress_trades SET processed = TRUE WHERE id = :id"),
                           {"id": r.id})
                continue
            sig = {
                "ceo":        f"congress:{r.member}"[:64],
                "tweet_text": f"{r.member} {r.txn_type} {r.ticker} ({r.amount}) disclosed {r.disclosure_date}",
                "tweet_date": r.disclosure_date,
                "topic":      "congressional_trade",
                "ticker":     r.ticker,
                "tightness":  1.0,
                "direction":  r.direction,
                "confidence": CONGRESS_CONFIDENCE,
                "sentiment":  0.0,
                "finbert":    None,
            }
            if status == "open":
                log.info("  CONGRESS %s → %s %s [MARKET OPEN — trading]",
                         r.member, r.direction, r.ticker)
                _execute_signal(sig, db, dry_run)
            else:
                log.info("  CONGRESS %s → %s %s [market %s — queuing]",
                         r.member, r.direction, r.ticker, status)
                enqueue_signal(db, sig)
            db.execute(text("UPDATE congress_trades SET processed = TRUE WHERE id = :id"),
                       {"id": r.id})

        db.commit()
    except Exception as e:
        db.rollback()
        log.error("Congress poll error: %s", e)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# SEC Form 4 insider trades — trade newly disclosed filings from insider_trades
# (populated by insider_ingest.py). Structured corporate-insider analogue of the
# congressional path: ticker + direction are explicit, so it bypasses registry/ML.
# ---------------------------------------------------------------------------

def poll_insider_trades(dry_run: bool):
    if not INSIDER_TRADING_ENABLED:
        return  # collected/queryable, but not traded until validated + enabled

    now_et = datetime.now(ET)
    status = market_status(now_et)
    cutoff = (now_et.date() - timedelta(days=INSIDER_RECENCY_DAYS)).isoformat()

    db = Session()
    try:
        direction_clause = "AND direction = 'Up'" if INSIDER_BUYS_ONLY else ""
        rows = db.execute(
            text(f"""
                SELECT id, insider, role, ticker, direction, txn_code,
                       shares, price, value, disclosure_date
                FROM insider_trades
                WHERE processed = FALSE AND disclosure_date >= :cutoff
                  AND value >= :minval
                  {direction_clause}
                ORDER BY disclosure_date ASC, value DESC
            """),
            {"cutoff": cutoff, "minval": INSIDER_MIN_VALUE},
        ).fetchall()

        # Retire disclosures too old to act on so they don't linger unprocessed.
        db.execute(
            text("""
                UPDATE insider_trades SET processed = TRUE
                WHERE processed = FALSE AND disclosure_date < :cutoff
            """),
            {"cutoff": cutoff},
        )

        if not rows:
            db.commit()
            return

        log.info("Insider: %d newly disclosed Form 4 trade(s) to act on", len(rows))
        for r in rows:
            sig = {
                "ceo":        f"insider:{r.insider}"[:64],
                "tweet_text": (f"{r.insider} ({r.role}) {r.txn_code} "
                               f"{r.shares:.0f} {r.ticker} @ ${r.price:.2f} "
                               f"= ${r.value:,.0f} disclosed {r.disclosure_date}"),
                "tweet_date": r.disclosure_date,
                "topic":      "insider_trade",
                "ticker":     r.ticker,
                "tightness":  1.0,
                "direction":  r.direction,
                "confidence": INSIDER_CONFIDENCE,
                "sentiment":  0.0,
                "finbert":    None,
            }
            if status == "open":
                log.info("  INSIDER %s → %s %s [MARKET OPEN — trading]",
                         r.insider, r.direction, r.ticker)
                _execute_signal(sig, db, dry_run)
            else:
                log.info("  INSIDER %s → %s %s [market %s — queuing]",
                         r.insider, r.direction, r.ticker, status)
                enqueue_signal(db, sig)
            db.execute(text("UPDATE insider_trades SET processed = TRUE WHERE id = :id"),
                       {"id": r.id})

        db.commit()
    except Exception as e:
        db.rollback()
        log.error("Insider poll error: %s", e)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Reddit spike signals — EXPERIMENTAL. Trade freshly detected mention/sentiment
# spikes from reddit_signals (populated by reddit_ingest.py). Disabled by default
# (REDDIT_TRADING_ENABLED) until event_study.py shows the heuristic has edge.
# ---------------------------------------------------------------------------

def poll_reddit_signals(dry_run: bool):
    if not REDDIT_TRADING_ENABLED:
        return  # collected for the event study, but not traded yet

    now_et = datetime.now(ET)
    status = market_status(now_et)
    cutoff = (now_et.date() - timedelta(days=REDDIT_RECENCY_DAYS)).isoformat()

    db = Session()
    try:
        rows = db.execute(
            text("""
                SELECT id, ticker, direction, mention_count, z_score, avg_sentiment, date
                FROM reddit_signals
                WHERE processed = FALSE AND date >= :cutoff
                ORDER BY z_score DESC NULLS LAST
            """),
            {"cutoff": cutoff},
        ).fetchall()

        db.execute(
            text("UPDATE reddit_signals SET processed = TRUE "
                 "WHERE processed = FALSE AND date < :cutoff"),
            {"cutoff": cutoff},
        )

        if not rows:
            db.commit()
            return

        log.info("Reddit: %d fresh spike signal(s) to act on", len(rows))
        for r in rows:
            sig = {
                "ceo":        f"reddit:{r.ticker}",
                "tweet_text": (f"Reddit spike {r.ticker} {r.direction} "
                               f"(mentions={r.mention_count}, z={r.z_score}, "
                               f"sent={r.avg_sentiment}) on {r.date}"),
                "tweet_date": r.date,
                "topic":      "reddit_spike",
                "ticker":     r.ticker,
                "tightness":  None,
                "direction":  r.direction,
                "confidence": REDDIT_CONFIDENCE,
                "sentiment":  float(r.avg_sentiment or 0.0),
                "finbert":    None,
            }
            if status == "open":
                log.info("  REDDIT %s → %s [MARKET OPEN — trading]", r.ticker, r.direction)
                _execute_signal(sig, db, dry_run)
            else:
                log.info("  REDDIT %s → %s [market %s — queuing]", r.ticker, r.direction, status)
                enqueue_signal(db, sig)
            db.execute(text("UPDATE reddit_signals SET processed = TRUE WHERE id = :id"),
                       {"id": r.id})

        db.commit()
    except Exception as e:
        db.rollback()
        log.error("Reddit poll error: %s", e)
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
        proc         = DataProcessor()
        stocks_cache = {}
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
                    if not passes_gates(row, eng_threshold, ceo):
                        log.debug("  tweet failed gates, skip")
                        continue

                    signal = evaluate_tweet(row, ceo, db, proc=proc, stocks_cache=stocks_cache)
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
    _last_full_poll_et     = None  # tracks when we last ran the full account sweep

    while True:
        now_et = datetime.now(ET)
        status = market_status(now_et)

        # At market open: flush any overnight/pre-market queued signals first
        if status == "open" and not _queued_open_processed:
            process_signal_queue(dry_run)
            _queued_open_processed = True
        elif status != "open":
            _queued_open_processed = False

        # Two-tier polling during market hours:
        #   Fast lane  (every 3 min)  — HIGH_PRIORITY_HANDLES only
        #   Full sweep (every 20 min) — all accounts
        # Outside market hours we always do a full sweep at the slower cadence.
        if status == "open" and not interval_override:
            full_due = (
                _last_full_poll_et is None
                or (now_et - _last_full_poll_et).total_seconds() >= POLL_MARKET_HOURS_S
            )
            if full_due:
                poll_list = ceo_list
                _last_full_poll_et = now_et
                log.info("Full sweep (%d accounts)", len(poll_list))
            else:
                poll_list = [c for c in ceo_list if c in HIGH_PRIORITY_HANDLES]
                if poll_list:
                    log.info("Fast-lane poll (%d HP accounts: %s)",
                             len(poll_list), ", ".join(poll_list))
        else:
            poll_list = ceo_list

        if poll_list:
            if db_only:
                poll_from_db(poll_list, dry_run)
            else:
                await poll_once(poll_list, dry_run)

        # Trade newly disclosed congressional filings (from congress_ingest.py)
        poll_congress_trades(dry_run)

        # Trade newly disclosed SEC Form 4 insider filings (from insider_ingest.py)
        poll_insider_trades(dry_run)

        # Trade Reddit spike signals (experimental; disabled unless REDDIT_TRADING_ENABLED)
        poll_reddit_signals(dry_run)

        # Close any positions that have reached their next-day exit horizon
        close_due_positions(dry_run)

        if once:
            break

        # Sleep duration
        if interval_override:
            sleep_s = interval_override
        elif status == "open":
            sleep_s = POLL_HIGH_PRIORITY_S   # wake every 3 min; full sweep governed by _last_full_poll_et
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
