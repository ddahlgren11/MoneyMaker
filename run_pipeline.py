"""
Standalone pipeline runner — fetches fresh tweets + stocks + news for all
CEOs, writes merged records to Neon, then exits.

Used by the GitHub Actions daily workflow so no FastAPI server is needed:
    python3 run_pipeline.py

Imports DB models and helpers directly from main.py to avoid duplication.
"""
import asyncio
import logging
import time
import sys
import pandas as pd
from datetime import timedelta, date as date_type
from dotenv import load_dotenv

load_dotenv()

# Import DB setup and helpers from main without starting the HTTP server.
# main.py's module-level code (create_engine, create_all, DataProcessor init)
# runs on import — that's fine for a CLI script.
from main import (
    SessionLocal,
    MergedRecord,
    NewsSentimentCache,
    _build_vix_lookup,
    _days_to_nearest_earnings,
    proc,
)
from classifier import (
    get_refined_sentiment, get_tone_category,
    get_tweet_type, get_sentiment_score,
)
from context import get_earnings_dates, build_news_sentiment_lookup

logging.basicConfig(
    level=logging.WARNING,
    format="%(levelname)s:%(name)s:%(message)s",
    stream=sys.stdout,
)
logging.getLogger("context").setLevel(logging.INFO)

TARGETS = {
    "elonmusk":    "TSLA",
    "tim_cook":    "AAPL",
    "satyanadella":"MSFT",
    "sundarpichai":"GOOGL",
    "MichaelDell": "DELL",
    "LisaSu":      "AMD",
    "ajassy":      "AMZN",
    "bchesky":     "ABNB",
    "dkhos":       "UBER",
    "RobertIger":  "DIS",
}


async def run():
    db = SessionLocal()
    total_records = 0
    skipped = []

    try:
        # Clear existing merged records so re-runs don't create duplicates.
        db.query(MergedRecord).delete()
        db.flush()

        for username, ticker in TARGETS.items():
            print(f"\n── {username} / {ticker} ──")

            try:
                tweets_df = await proc.get_tweets(username)
            except Exception as exc:
                reason = str(exc)
                skipped.append({"username": username, "reason": reason})
                print(f"  SKIP: {reason}")
                continue

            if tweets_df.empty:
                print("  SKIP: no tweets returned")
                continue

            tweets_df = tweets_df.sort_values(by="date", ascending=False).dropna(subset=["date"])
            if tweets_df.empty:
                continue

            print(f"  {len(tweets_df)} tweets fetched")

            min_date = tweets_df["date"].min() - timedelta(days=30)
            max_date = tweets_df["date"].max() + timedelta(days=5)

            vix_lookup  = _build_vix_lookup(min_date, max_date)
            earnings_set = get_earnings_dates(ticker)
            stocks_df   = proc.get_stocks(ticker, start_date=min_date, end_date=max_date)

            if not stocks_df.empty:
                stocks_df = stocks_df.sort_index()
                if isinstance(stocks_df.index, pd.MultiIndex):
                    stock_dates = stocks_df.index.get_level_values("timestamp").date
                else:
                    stock_dates = stocks_df.index.date
                stocks_df["date_only"] = stock_dates

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

            # News sentiment — load cache, fetch only uncached tweet years.
            tweet_date_objects = set(tweets_df["date"].dt.date)
            cached_rows = db.query(NewsSentimentCache).filter(
                NewsSentimentCache.ticker == ticker
            ).all()
            news_lookup = {r.date_str: r.sentiment_score for r in cached_rows}

            uncached_dates = tweet_date_objects - {date_type.fromisoformat(d) for d in news_lookup}
            if uncached_dates:
                new_lookup = build_news_sentiment_lookup(
                    ticker,
                    tweet_dates=uncached_dates,
                    get_sentiment_fn=get_sentiment_score,
                )
                time.sleep(1.1)  # respect AV 1-req/sec burst limit
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
                tweet_date_strs = {d.isoformat() for d in tweet_date_objects}
                overlap = tweet_date_strs & set(lookup_dates)
                print(f"  news: {len(lookup_dates)} dates in lookup, {len(overlap)} overlap with tweets")
            else:
                print("  news: no coverage (news_sentiment_score will be NULL)")

            # Merge tweets with stock/context data and write to DB.
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
            print(f"  {ticker_records} records written")

        db.commit()
        print(f"\nDone — {total_records} total records, {len(skipped)} skipped")
        if skipped:
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
