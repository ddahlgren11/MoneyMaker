"""
Standalone pipeline runner — fetches fresh tweets + stocks + news for all
CEOs, writes merged records to Neon, then exits.

Daily run (GitHub Actions):
    python3 run_pipeline.py

One-time historical backfill (run manually):
    python3 run_pipeline.py --pages 50

--pages controls how many pages of tweets are fetched per CEO.
Each page is ~20 tweets.  Default is 20 (~400 tweets per CEO).
For a full backfill use 50-100 to pull years of history.
"""
import argparse
import asyncio
import logging
import os
import sys
import time
from datetime import timedelta, datetime, date as date_type

parser = argparse.ArgumentParser()
parser.add_argument("--pages", type=int, default=20,
                    help="Pages of tweets to fetch per CEO (each page ~20 tweets)")
args = parser.parse_args()
PAGES = args.pages

import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

load_dotenv()

# ── Validate required env vars before going further ───────────────────────────
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
# Guard against secret being stored as "DATABASE_URL=postgresql://..." instead of just the URL
if "=" in DATABASE_URL and not DATABASE_URL.startswith(("postgres://", "postgresql://")):
    DATABASE_URL = DATABASE_URL.split("=", 1)[1].strip()
if not DATABASE_URL:
    print("FATAL: DATABASE_URL is not set.", file=sys.stderr)
    sys.exit(1)
# Neon/Heroku connection strings often use postgres:// — SQLAlchemy requires postgresql://
if DATABASE_URL.startswith("postgres://") and not DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# ── DB setup ──────────────────────────────────────────────────────────────────
engine       = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
SessionLocal = sessionmaker(bind=engine)
Base         = declarative_base()

class MergedRecord(Base):
    __tablename__ = "merged_data"
    id                   = Column(Integer, primary_key=True, index=True)
    date                 = Column(String)
    ceo                  = Column(String)
    tweet_text           = Column(String)
    sentiment_score      = Column(Float)
    refined_sentiment    = Column(String)
    tone_category        = Column(String)
    tweet_type           = Column(String)
    stock_ticker         = Column(String)
    stock_close          = Column(Float)
    stock_volume         = Column(Float)
    stock_open_close_diff= Column(Float)
    likes                = Column(Integer)
    retweet_count        = Column(Integer)
    view_count           = Column(Integer)
    reply_count          = Column(Integer)
    tweet_hour           = Column(Integer)
    is_premarket         = Column(Integer)
    next_day_direction   = Column(Integer, nullable=True)
    rsi_at_tweet         = Column(Float,   nullable=True)
    atr_at_tweet         = Column(Float,   nullable=True)
    news_sentiment_score = Column(Float,   nullable=True)
    vix_at_tweet         = Column(Float,   nullable=True)
    days_to_earnings     = Column(Integer, nullable=True)

class NewsSentimentCache(Base):
    __tablename__ = "news_sentiment_cache"
    ticker          = Column(String, primary_key=True)
    date_str        = Column(String, primary_key=True)
    sentiment_score = Column(Float)

Base.metadata.create_all(bind=engine)

# ── App imports (after env is validated) ─────────────────────────────────────
from processor  import DataProcessor
from classifier import get_refined_sentiment, get_tone_category, get_tweet_type, get_sentiment_score
from context    import get_earnings_dates, build_news_sentiment_lookup

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s:%(name)s:%(message)s",
    stream=sys.stdout,
)
logging.getLogger("context").setLevel(logging.INFO)

proc = DataProcessor()

# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_vix_lookup(start_date, end_date):
    try:
        vix = yf.download("^VIX", start=start_date, end=end_date + timedelta(days=1),
                          auto_adjust=True, progress=False)
        if vix.empty:
            return {}
        if isinstance(vix.columns, pd.MultiIndex):
            vix.columns = vix.columns.get_level_values(0)
        return {d.date(): float(v) for d, v in zip(vix.index, vix["Close"])}
    except Exception:
        return {}


def _days_to_nearest_earnings(target_date, earnings_set):
    if not earnings_set:
        return None
    if isinstance(target_date, datetime):
        target_date = target_date.date()
    diffs = [abs((datetime.strptime(d, "%Y-%m-%d").date() - target_date).days)
             for d in earnings_set]
    return min(diffs)


# ── Pipeline ──────────────────────────────────────────────────────────────────

TARGETS = {
    "elonmusk":       "TSLA",
    "tim_cook":       "AAPL",
    "satyanadella":   "MSFT",
    "sundarpichai":   "GOOGL",
    "MichaelDell":    "DELL",
    "LisaSu":         "AMD",
    "ajassy":         "AMZN",
    "bchesky":        "ABNB",
    "dkhos":          "UBER",
    "RobertIger":     "DIS",
    "zuck":           "META",
    "jensenhuang":    "NVDA",
    "jack":           "SQ",
    "tobi":           "SHOP",
    "brian_armstrong": "COIN",
}


async def run():
    db            = SessionLocal()
    total_records = 0
    skipped       = []

    print(f"Pages per CEO: {PAGES}  (~{PAGES * 20} tweets max per CEO)")
    print(f"Mode: {'BACKFILL' if PAGES > 20 else 'daily'}\n")

    try:
        for username, ticker in TARGETS.items():
            print(f"\n── {username} / {ticker} ──")

            try:
                tweets_df = await proc.get_tweets(username, pages=PAGES)
            except Exception as exc:
                skipped.append({"username": username, "reason": str(exc)})
                print(f"  SKIP: {exc}")
                continue

            if tweets_df.empty:
                print("  SKIP: no tweets returned")
                continue

            tweets_df = tweets_df.sort_values(by="date", ascending=False).dropna(subset=["date"])
            if tweets_df.empty:
                continue

            # Load timestamps already in the DB for this CEO so we don't double-insert
            existing_dates = {
                r.date for r in
                db.query(MergedRecord.date).filter(MergedRecord.ceo == username).all()
            }
            tweets_df = tweets_df[
                ~tweets_df["date"].apply(lambda d: d.isoformat()).isin(existing_dates)
            ]
            if tweets_df.empty:
                print("  SKIP: all tweets already in DB")
                continue

            print(f"  {len(tweets_df)} new tweets (skipped {len(existing_dates)} already stored)")

            min_date    = tweets_df["date"].min() - timedelta(days=30)
            max_date    = tweets_df["date"].max() + timedelta(days=5)
            vix_lookup  = _build_vix_lookup(min_date, max_date)
            earnings_set = get_earnings_dates(ticker)
            stocks_df   = proc.get_stocks(ticker, start_date=min_date, end_date=max_date)

            if not stocks_df.empty:
                stocks_df = stocks_df.sort_index()
                stocks_df["date_only"] = (
                    stocks_df.index.get_level_values("timestamp").date
                    if isinstance(stocks_df.index, pd.MultiIndex)
                    else stocks_df.index.date
                )
                stocks_df["prev_close"] = stocks_df["close"].shift(1)
                stocks_df["tr"] = stocks_df[["high", "low", "prev_close"]].apply(
                    lambda r: max(r["high"] - r["low"],
                                  abs(r["high"] - r["prev_close"]),
                                  abs(r["low"]  - r["prev_close"])), axis=1
                )
                stocks_df["atr_14"] = stocks_df["tr"].rolling(14).mean()
                delta = stocks_df["close"].diff()
                gain  = delta.where(delta > 0, 0.0).rolling(14).mean()
                loss  = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
                stocks_df["rsi_14"] = 100 - (100 / (1 + gain / loss))

            # News sentiment — load cache, fetch only uncached tweet dates.
            tweet_date_objects = set(tweets_df["date"].dt.date)
            cached_rows  = db.query(NewsSentimentCache).filter(
                NewsSentimentCache.ticker == ticker
            ).all()
            news_lookup  = {r.date_str: r.sentiment_score for r in cached_rows}
            uncached_dates = tweet_date_objects - {date_type.fromisoformat(d) for d in news_lookup}

            if uncached_dates:
                new_lookup = build_news_sentiment_lookup(
                    ticker,
                    tweet_dates=uncached_dates,
                    get_sentiment_fn=get_sentiment_score,
                )
                time.sleep(1.1)
                if new_lookup:
                    for date_str, score in new_lookup.items():
                        cached = db.get(NewsSentimentCache, (ticker, date_str))
                        if cached is None:
                            db.add(NewsSentimentCache(ticker=ticker, date_str=date_str, sentiment_score=score))
                        else:
                            cached.sentiment_score = score
                    db.flush()
                    news_lookup.update(new_lookup)

            if news_lookup:
                lookup_dates = sorted(news_lookup.keys())
                overlap = {d.isoformat() for d in tweet_date_objects} & set(lookup_dates)
                print(f"  news: {len(lookup_dates)} dates, {len(overlap)} overlap")
            else:
                print("  news: no coverage")

            ticker_records = 0
            for _, row in tweets_df.iterrows():
                sentiment  = float(row["sentiment"])
                text       = str(row["text"])
                tweet_date = row["date"]

                target_date = tweet_date
                if target_date.weekday() == 5:
                    target_date += timedelta(days=2)
                elif target_date.weekday() == 6:
                    target_date += timedelta(days=1)
                target_date_only = target_date.date()

                stock_close = stock_volume = stock_open_close_diff = 0.0
                next_day_direction = rsi_at_tweet = atr_at_tweet = None

                if not stocks_df.empty:
                    valid = stocks_df[stocks_df["date_only"] >= target_date_only]
                    if not valid.empty:
                        stock_close           = float(valid["close"].iloc[0])
                        stock_volume          = float(valid["volume"].iloc[0])
                        stock_open            = float(valid["open"].iloc[0])
                        stock_open_close_diff = float(stock_open - stock_close)
                        if len(valid) >= 2:
                            next_close = float(valid["close"].iloc[1])
                            next_day_direction = 1 if next_close > stock_close else 0
                        rsi_val = valid["rsi_14"].iloc[0]
                        atr_val = valid["atr_14"].iloc[0]
                        rsi_at_tweet = float(rsi_val) if not pd.isna(rsi_val) else None
                        atr_at_tweet = float(atr_val) if not pd.isna(atr_val) else None

                vix_at_tweet = vix_lookup.get(target_date_only)
                if vix_at_tweet is None:
                    for offset in range(1, 4):
                        vix_at_tweet = vix_lookup.get(target_date_only - timedelta(days=offset))
                        if vix_at_tweet is not None:
                            break

                db.add(MergedRecord(
                    date=tweet_date.isoformat(),
                    ceo=username,
                    tweet_text=text,
                    sentiment_score=sentiment,
                    refined_sentiment=get_refined_sentiment(sentiment),
                    tone_category=get_tone_category(text, sentiment),
                    tweet_type=get_tweet_type(text),
                    stock_ticker=ticker,
                    stock_close=stock_close,
                    stock_volume=stock_volume,
                    stock_open_close_diff=stock_open_close_diff,
                    likes=int(row.get("likes", 0)),
                    retweet_count=int(row.get("retweet_count", 0)),
                    view_count=int(row.get("view_count", 0)),
                    reply_count=int(row.get("reply_count", 0)),
                    tweet_hour=int(row.get("tweet_hour", 0)),
                    is_premarket=int(row.get("is_premarket", 0)),
                    next_day_direction=next_day_direction,
                    rsi_at_tweet=rsi_at_tweet,
                    atr_at_tweet=atr_at_tweet,
                    news_sentiment_score=news_lookup.get(target_date_only.isoformat()),
                    vix_at_tweet=vix_at_tweet,
                    days_to_earnings=_days_to_nearest_earnings(target_date_only, earnings_set),
                ))
                ticker_records += 1

            total_records += ticker_records
            print(f"  {ticker_records} new records written")

        db.commit()
        print(f"\nDone — {total_records} total records, {len(skipped)} skipped")
        for s in skipped:
            print(f"  skipped {s['username']}: {s['reason']}")

    except Exception as exc:
        db.rollback()
        print(f"\nFATAL: {exc}", file=sys.stderr)
        raise
    finally:
        db.close()


if __name__ == "__main__":
    asyncio.run(run())
