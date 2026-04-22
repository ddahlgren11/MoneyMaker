import os
import asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
from tweety import Twitter
import tweety.transaction

# Runtime patch to fix tweety-ns throwing "Couldn't get animation key indices" error
original_get_indices = tweety.transaction.TransactionGenerator.get_indices

def patched_get_indices(self, home_page_html=None):
    try:
        return original_get_indices(self, home_page_html)
    except Exception as e:
        if "Couldn't get animation key indices" in str(e):
            return 0, [1, 2, 3, 4, 5]
        raise

tweety.transaction.TransactionGenerator.get_indices = patched_get_indices

from classifier import get_sentiment_score
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from dotenv import load_dotenv

load_dotenv()

class DataProcessor:
    def __init__(self):
        self.twitter_client = Twitter("session")
        self.stock_client = StockHistoricalDataClient(
            os.getenv("ALPACA_API_KEY"),
            os.getenv("ALPACA_SECRET_KEY")
        )

    @staticmethod
    def _safe_int(value):
        """Convert engagement counts to int, returning 0 for None/'Unavailable'/etc."""
        try:
            return int(value or 0)
        except (ValueError, TypeError):
            return 0

    async def get_tweets(self, username, pages=20):
        all_tweets = []
        user_tweets = await self.twitter_client.get_tweets(username, pages=pages)
        for tweet in user_tweets:
            # Skip retweets — they reflect someone else's words, not the CEO's
            if tweet.is_retweet:
                continue

            created = tweet.created_on
            tweet_hour = created.hour if hasattr(created, 'hour') else 0
            tweet_minute = created.minute if hasattr(created, 'minute') else 0
            # NYSE opens 9:30 ET = 14:30 UTC, closes 16:00 ET = 21:00 UTC
            is_premarket = tweet_hour < 14 or (tweet_hour == 14 and tweet_minute < 30)

            all_tweets.append({
                'ceo': username,
                'text': tweet.text,
                'sentiment': get_sentiment_score(tweet.text),
                'date': created,
                'likes': self._safe_int(tweet.likes),
                'retweet_count': self._safe_int(tweet.retweet_counts),
                'view_count': self._safe_int(tweet.views),
                'reply_count': self._safe_int(tweet.reply_counts),
                'tweet_hour': tweet_hour,
                'is_premarket': is_premarket,
            })
        return pd.DataFrame(all_tweets)

    def get_market_context(self, ticker, start_date, end_date):
        """Fetch SPY and sector ETF bars for the same date range."""
        from context import get_sector_etf
        sector_etf = get_sector_etf(ticker)
        spy_df = self.get_stocks("SPY", start_date, end_date)
        sector_df = self.get_stocks(sector_etf, start_date, end_date)
        return spy_df, sector_df, sector_etf

    def get_stocks(self, symbol, start_date=None, end_date=None):
        if end_date is None:
            end_date = datetime.now(timezone.utc) - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=7)

        request = StockBarsRequest(symbol_or_symbols=symbol, timeframe=TimeFrame.Day, start=start_date, end=end_date)
        bars = self.stock_client.get_stock_bars(request)
        return bars.df if bars else pd.DataFrame()
