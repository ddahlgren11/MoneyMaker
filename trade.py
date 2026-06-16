#!/usr/bin/env python3
"""
Morning trading script — runs the full signal-to-order pipeline.

Flow:
  1. Market-hours guard  — exits immediately on weekends / US holidays
  2. Tweet freshness     — fetches new tweets via /process/all if last ingestion
                           was more than 12 hours ago (skip with --no-refresh)
  3. Relationship lookup — queries the registry for (CEO, topic) pairs with
                           tightness >= TIGHTNESS_THRESHOLD
  4. Model prediction    — runs the trained ML model for each matched ticker
  5. Confidence gate     — skips if model confidence < CONFIDENCE_THRESHOLD
  6. Position check      — closes opposite position before opening new one
  7. Order placement     — $1,000 market order via Alpaca paper trading
  8. Summary             — prints a table of all signals and outcomes

Usage:
    python3 trade.py                       # trade all CEOs with registry entries
    python3 trade.py elonmusk LisaSu       # specific CEO handles only
    python3 trade.py --dry-run             # preview signals, no orders placed
    python3 trade.py --no-refresh          # skip tweet pipeline refresh
    python3 trade.py --portfolio           # print account + positions and exit
    python3 trade.py --history             # print recent trade log and exit
    python3 trade.py --force-stale         # ignore the 48h tweet staleness guard
"""

import argparse
import os
import sys
import math
from datetime import datetime, timedelta, timezone

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

load_dotenv()

# ---------------------------------------------------------------------------
# Config — mirrors the constants in main.py
# ---------------------------------------------------------------------------
TRADE_NOTIONAL       = 1000.0
CONFIDENCE_THRESHOLD = 55.0
TIGHTNESS_THRESHOLD  = 0.20
STALENESS_HOURS      = 48
REFRESH_IF_OLDER_H   = 12   # trigger pipeline refresh if last tweet is this old

# US market holidays (NYSE) — add the current year's dates as needed
_NYSE_HOLIDAYS_2025 = {
    "2025-01-01", "2025-01-20", "2025-02-17", "2025-04-18",
    "2025-05-26", "2025-06-19", "2025-07-04", "2025-09-01",
    "2025-11-27", "2025-12-25",
}
_NYSE_HOLIDAYS_2026 = {
    "2026-01-01", "2026-01-19", "2026-02-16", "2026-04-03",
    "2026-05-25", "2026-06-19", "2026-07-03", "2026-09-07",
    "2026-11-26", "2026-12-25",
}
NYSE_HOLIDAYS = _NYSE_HOLIDAYS_2025 | _NYSE_HOLIDAYS_2026


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine  = create_engine(DATABASE_URL, pool_pre_ping=True)
Session = sessionmaker(bind=engine)


def _is_trading_day(dt: datetime) -> bool:
    if dt.weekday() >= 5:
        return False
    return dt.strftime("%Y-%m-%d") not in NYSE_HOLIDAYS


def _get_latest_tweet_age_hours(db, ceo: str) -> float | None:
    row = db.execute(
        text("SELECT date FROM merged_data WHERE ceo = :ceo ORDER BY date DESC LIMIT 1"),
        {"ceo": ceo},
    ).fetchone()
    if not row:
        return None
    dt = pd.to_datetime(row[0])
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt).total_seconds() / 3600


# ---------------------------------------------------------------------------
# Pipeline refresh
# ---------------------------------------------------------------------------
def refresh_tweets():
    """Call the FastAPI /process/all endpoint to ingest fresh tweets."""
    import requests
    api_url = os.getenv("API_URL", "http://localhost:8000")
    print(f"  Refreshing tweets via {api_url}/process/all ...")
    try:
        resp = requests.post(f"{api_url}/process/all", timeout=300)
        data = resp.json()
        added = data.get("records_added", "?")
        skipped = len(data.get("skipped", []))
        print(f"  Done — {added} new records, {skipped} CEOs skipped")
    except Exception as e:
        print(f"  Warning: tweet refresh failed ({e}). Trading on cached data.")


# ---------------------------------------------------------------------------
# Portfolio / history display
# ---------------------------------------------------------------------------
def show_portfolio():
    from alpaca.trading.client import TradingClient
    tc = TradingClient(
        os.getenv("ALPACA_PAPER_API_KEY"),
        os.getenv("ALPACA_PAPER_SECRET_KEY"),
        paper=True,
    )
    acct = tc.get_account()
    print("\n── Paper Account ──────────────────────────────")
    print(f"  Portfolio Value : ${float(acct.portfolio_value):>12,.2f}")
    print(f"  Cash            : ${float(acct.cash):>12,.2f}")
    print(f"  Equity          : ${float(acct.equity):>12,.2f}")
    print(f"  Today's P&L     : ${float(acct.equity) - float(acct.last_equity):>+12,.2f}")
    print(f"  Buying Power    : ${float(acct.buying_power):>12,.2f}")

    positions = tc.get_all_positions()
    if positions:
        total_unreal = sum(float(p.unrealized_pl) for p in positions)
        print(f"  Open P&L        : ${total_unreal:>+12,.2f}  across {len(positions)} positions")
        print("\n── Open Positions ─────────────────────────────")
        print(f"  {'Symbol':<8} {'Side':<6} {'Qty':>6}  {'Entry':>8}  {'Now':>8}  {'P&L':>10}  {'P&L%':>7}")
        print("  " + "─" * 60)
        for p in positions:
            print(
                f"  {p.symbol:<8} {str(p.side):<6} {float(p.qty):>6.2f}  "
                f"${float(p.avg_entry_price):>7.2f}  ${float(p.current_price):>7.2f}  "
                f"${float(p.unrealized_pl):>+9.2f}  {float(p.unrealized_plpc)*100:>+6.1f}%"
            )
    else:
        print("\n  No open positions.")


def show_history(limit: int = 20):
    db = Session()
    try:
        rows = db.execute(
            text("""
                SELECT timestamp, ceo, topic, ticker, side, predicted_direction,
                       confidence_pct, tightness_score, status, skip_reason
                FROM paper_trades
                ORDER BY timestamp DESC
                LIMIT :limit
            """),
            {"limit": limit},
        ).fetchall()
    finally:
        db.close()

    if not rows:
        print("No trades logged yet.")
        return

    print(f"\n── Last {len(rows)} trades ────────────────────────────────────────────")
    print(f"  {'Time':<19}  {'CEO':<16}  {'Topic':<12}  {'Ticker':<6}  "
          f"{'Side':<11}  {'Conf':>5}  {'Tight':>6}  {'Status'}")
    print("  " + "─" * 100)
    for r in rows:
        status_icon = "✓" if r.status == "placed" else ("✗" if r.status == "error" else "–")
        tight = f"{r.tightness_score:.3f}" if r.tightness_score is not None else "  n/a"
        print(
            f"  {str(r.timestamp)[:19]}  {r.ceo:<16}  {str(r.topic):<12}  "
            f"{r.ticker:<6}  {str(r.side):<11}  "
            f"{r.confidence_pct:>4.1f}%  {tight}  "
            f"{status_icon} {r.status}"
            + (f"  ({r.skip_reason})" if r.skip_reason else "")
        )


# ---------------------------------------------------------------------------
# Core trade execution (mirrors main.py execute_paper_trade, no HTTP)
# ---------------------------------------------------------------------------
def execute_for_ceo(ceo: str, dry_run: bool, force_stale: bool) -> dict:
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import MarketOrderRequest
    from alpaca.trading.enums import OrderSide, TimeInForce
    from classifier import get_tweet_topic
    from pipeline_utils import compute_technicals
    from model.predict import predict_tweets
    from targets import HANDLE_TO_TICKER

    db = Session()
    try:
        ticker_for_ceo = HANDLE_TO_TICKER.get(ceo)
        if not ticker_for_ceo:
            return {"status": "skipped", "reason": "unknown CEO handle"}

        # Latest tweet
        row = db.execute(
            text("""
                SELECT ceo, tweet_text, date, sentiment_score, finbert_score,
                       likes, retweet_count, view_count, reply_count,
                       tweet_hour, is_premarket
                FROM merged_data WHERE ceo = :ceo ORDER BY date DESC LIMIT 1
            """),
            {"ceo": ceo},
        ).fetchone()

        if not row:
            return {"status": "skipped", "reason": "no stored tweets — run pipeline first"}

        tweet_text = row.tweet_text or ""
        tweet_dt   = pd.to_datetime(row.date)
        if tweet_dt.tzinfo is None:
            tweet_dt = tweet_dt.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - tweet_dt).total_seconds() / 3600

        if age_hours > STALENESS_HOURS and not force_stale:
            return {
                "status": "skipped",
                "reason": f"tweet is {age_hours:.0f}h old (>{STALENESS_HOURS}h) — run pipeline or pass --force-stale",
                "tweet_date": str(row.date)[:10],
            }

        # Topic + relationship lookup
        topic = get_tweet_topic(tweet_text, ceo)

        rel = db.execute(
            text("""
                SELECT ticker, tightness_score FROM ceo_ticker_relationships
                WHERE ceo = :ceo AND topic = :topic
                  AND tightness_score >= :min_t
                ORDER BY tightness_score DESC LIMIT 1
            """),
            {"ceo": ceo, "topic": topic, "min_t": TIGHTNESS_THRESHOLD},
        ).fetchone()

        ticker    = rel.ticker          if rel else ticker_for_ceo
        tightness = rel.tightness_score if rel else None

        # ML model
        from processor import DataProcessor
        proc = DataProcessor()
        end_dt   = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=60)
        stocks_df = proc.get_stocks(ticker, start_date=start_dt, end_date=end_dt)
        stocks_df = compute_technicals(stocks_df)

        tweet_row = {
            "date":          tweet_dt,
            "text":          tweet_text,
            "sentiment":     float(row.sentiment_score or 0),
            "finbert_score": row.finbert_score,
            "likes":         int(row.likes or 0),
            "retweet_count": int(row.retweet_count or 0),
            "view_count":    int(row.view_count or 0),
            "reply_count":   int(row.reply_count or 0),
            "tweet_hour":    int(row.tweet_hour or 12),
            "is_premarket":  int(row.is_premarket or 0),
        }

        result_df = predict_tweets(pd.DataFrame([tweet_row]), stocks_df, ticker=ticker)
        if result_df.empty:
            return {"status": "skipped", "reason": "model returned no prediction"}

        direction  = str(result_df.iloc[0]["predicted_direction"])
        confidence = float(result_df.iloc[0]["confidence_pct"])

        if confidence < CONFIDENCE_THRESHOLD:
            return {
                "status":    "skipped",
                "ticker":    ticker,
                "topic":     topic,
                "direction": direction,
                "confidence": confidence,
                "tightness": tightness,
                "reason":    f"confidence {confidence:.1f}% < {CONFIDENCE_THRESHOLD}%",
            }

        if dry_run:
            return {
                "status":    "dry-run",
                "ticker":    ticker,
                "topic":     topic,
                "direction": direction,
                "confidence": confidence,
                "tightness": tightness,
                "side":      "buy" if direction == "Up" else "sell_short",
                "age_hours": round(age_hours, 1),
            }

        # Live trade
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
                return {
                    "status": "skipped",
                    "ticker": ticker,
                    "reason": f"already {existing_side} — signal matches existing position",
                }
            tc.close_position(ticker)

        if side == OrderSide.BUY:
            order_req = MarketOrderRequest(
                symbol=ticker, notional=TRADE_NOTIONAL,
                side=side, time_in_force=TimeInForce.DAY,
            )
        else:
            price = float(stocks_df["close"].iloc[-1]) if not stocks_df.empty else 100.0
            order_req = MarketOrderRequest(
                symbol=ticker, qty=max(1, int(TRADE_NOTIONAL / price)),
                side=side, time_in_force=TimeInForce.DAY,
            )

        order    = tc.submit_order(order_req)
        order_id = str(order.id)

        # Log to DB
        db.execute(
            text("""
                INSERT INTO paper_trades
                    (timestamp, ceo, tweet_text, tweet_date, topic, ticker, side,
                     notional, predicted_direction, confidence_pct, sentiment_score,
                     tightness_score, alpaca_order_id, status)
                VALUES
                    (:ts, :ceo, :tweet, :tweet_date, :topic, :ticker, :side,
                     :notional, :direction, :conf, :sent, :tight, :order_id, 'placed')
            """),
            {
                "ts":         datetime.now().isoformat(),
                "ceo":        ceo,
                "tweet":      tweet_text[:500],
                "tweet_date": str(row.date)[:10],
                "topic":      topic,
                "ticker":     ticker,
                "side":       "buy" if direction == "Up" else "sell_short",
                "notional":   TRADE_NOTIONAL,
                "direction":  direction,
                "conf":       confidence,
                "sent":       float(row.sentiment_score or 0),
                "tight":      tightness,
                "order_id":   order_id,
            },
        )
        db.commit()

        return {
            "status":    "placed",
            "ticker":    ticker,
            "topic":     topic,
            "direction": direction,
            "confidence": confidence,
            "tightness": tightness,
            "side":      "buy" if direction == "Up" else "sell_short",
            "order_id":  order_id,
            "age_hours": round(age_hours, 1),
        }

    except Exception as e:
        db.rollback()
        return {"status": "error", "reason": str(e)}
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="MoneyMaker paper trading CLI")
    parser.add_argument("ceos", nargs="*", help="CEO handles to trade (default: all with registry entries)")
    parser.add_argument("--dry-run",     action="store_true", help="Show signals without placing orders")
    parser.add_argument("--no-refresh",  action="store_true", help="Skip tweet pipeline refresh")
    parser.add_argument("--force-stale", action="store_true", help="Trade even on tweets older than 48h")
    parser.add_argument("--portfolio",   action="store_true", help="Show account + positions and exit")
    parser.add_argument("--history",     action="store_true", help="Show recent trade log and exit")
    args = parser.parse_args()

    now_et = datetime.now(timezone.utc) - timedelta(hours=5)   # approx ET
    print(f"\nMoneyMaker Trade Runner — {now_et.strftime('%Y-%m-%d %H:%M')} ET")

    if args.portfolio:
        show_portfolio()
        return

    if args.history:
        show_history()
        return

    # Market-hours guard
    if not _is_trading_day(now_et) and not args.force_stale and not args.dry_run:
        print(f"  Today ({now_et.strftime('%A %Y-%m-%d')}) is not a trading day. Exiting.")
        sys.exit(0)

    # Which CEOs to trade?
    db = Session()
    try:
        if args.ceos:
            ceo_list = args.ceos
        else:
            rows = db.execute(
                text("""
                    SELECT DISTINCT ceo FROM ceo_ticker_relationships
                    WHERE tightness_score >= :min_t
                    ORDER BY ceo
                """),
                {"min_t": TIGHTNESS_THRESHOLD},
            ).fetchall()
            ceo_list = [r[0] for r in rows]
    finally:
        db.close()

    if not ceo_list:
        print(
            "  No CEOs with registered relationships found.\n"
            "  Run `python3 relationship_analysis.py` first."
        )
        sys.exit(0)

    print(f"  CEOs to process: {', '.join(ceo_list)}")

    # Refresh tweets if stale
    if not args.no_refresh:
        db = Session()
        try:
            oldest_age = max(
                (_get_latest_tweet_age_hours(db, ceo) or 999)
                for ceo in ceo_list
            )
        finally:
            db.close()

        if oldest_age > REFRESH_IF_OLDER_H:
            print(f"  Oldest tweet is {oldest_age:.0f}h old — refreshing...")
            refresh_tweets()
        else:
            print(f"  Tweets are fresh ({oldest_age:.0f}h old) — skipping refresh")

    # Execute
    if args.dry_run:
        print("\n  ── DRY RUN — no orders will be placed ──")

    print(
        f"\n  {'CEO':<18}  {'Topic':<12}  {'Ticker':<6}  "
        f"{'Dir':<5}  {'Conf':>5}  {'Tight':>6}  {'Result'}"
    )
    print("  " + "─" * 80)

    placed = skipped = errors = 0
    for ceo in ceo_list:
        result = execute_for_ceo(ceo, dry_run=args.dry_run, force_stale=args.force_stale)
        status = result["status"]

        ticker    = result.get("ticker", "—")
        topic     = result.get("topic",  "—")
        direction = result.get("direction", "—")
        conf      = result.get("confidence")
        tight     = result.get("tightness")
        reason    = result.get("reason", "")

        conf_str  = f"{conf:>4.1f}%" if conf is not None else "   —  "
        tight_str = f"{tight:.3f}"   if tight is not None else "   n/a"

        icon = "✓" if status == "placed" else ("~" if status == "dry-run" else ("✗" if status == "error" else "–"))

        print(
            f"  {ceo:<18}  {topic:<12}  {ticker:<6}  "
            f"{direction:<5}  {conf_str}  {tight_str}  "
            f"{icon} {status}"
            + (f"  ({reason})" if reason and status != "placed" else "")
        )

        if status == "placed":      placed  += 1
        elif status == "error":     errors  += 1
        else:                       skipped += 1

    print(f"\n  Done — {placed} placed  ·  {skipped} skipped  ·  {errors} errors")

    if args.dry_run:
        print("  (dry-run: no real orders placed)")


if __name__ == "__main__":
    main()
