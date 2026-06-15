import os
import asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
from twikit import Client as TwikitClient
from classifier import get_sentiment_score, get_finbert_scores_batch
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from dotenv import load_dotenv

load_dotenv()

COOKIES_PATH = os.path.join(os.path.dirname(__file__), "twitter_cookies.json")

class DataProcessor:
    def __init__(self):
        self.twitter_client = TwikitClient("en-US")
        if os.path.exists(COOKIES_PATH):
            self.twitter_client.load_cookies(COOKIES_PATH)
        else:
            print(f"WARNING: Twitter cookies not found at {COOKIES_PATH}. "
                  "Tweet fetching will fail. See README for cookie setup.")
        # Market-data client works with either live or paper keys (free IEX feed).
        # Prefer live data keys (set in the retrain workflow); fall back to paper
        # keys (set in the watcher workflow) so both environments work.
        self.stock_client = StockHistoricalDataClient(
            os.getenv("ALPACA_API_KEY") or os.getenv("ALPACA_PAPER_API_KEY"),
            os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_PAPER_SECRET_KEY"),
        )

    @staticmethod
    def _safe_int(value):
        """Convert engagement counts to int, returning 0 for None/'Unavailable'/etc."""
        try:
            return int(value or 0)
        except (ValueError, TypeError):
            return 0

    async def get_tweets(self, username, pages=50):
        all_tweets = []
        user = await self.twitter_client.get_user_by_screen_name(username)
        result = await self.twitter_client.get_user_tweets(user.id, tweet_type="Tweets", count=20)

        for page_num in range(pages):
            for tweet in result:
                # Skip retweets — they reflect someone else's words, not the CEO's
                if tweet.retweeted_tweet is not None:
                    continue

                created = tweet.created_at_datetime
                if created and created.tzinfo is None:
                    created = created.replace(tzinfo=timezone.utc)
                tweet_hour = created.hour if created else 0
                tweet_minute = created.minute if created else 0
                # NYSE opens 9:30 ET = 14:30 UTC, closes 16:00 ET = 21:00 UTC
                is_premarket = tweet_hour < 14 or (tweet_hour == 14 and tweet_minute < 30)

                all_tweets.append({
                    'ceo': username,
                    'text': tweet.full_text or tweet.text,
                    'sentiment': get_sentiment_score(tweet.full_text or tweet.text),
                    'date': created,
                    'likes': self._safe_int(tweet.favorite_count),
                    'retweet_count': self._safe_int(tweet.retweet_count),
                    'view_count': self._safe_int(tweet.view_count),
                    'reply_count': self._safe_int(tweet.reply_count),
                    'tweet_hour': tweet_hour,
                    'is_premarket': is_premarket,
                })

            if page_num < pages - 1:
                try:
                    result = await result.next()
                except Exception:
                    break  # no more pages

        df = pd.DataFrame(all_tweets)
        if not df.empty:
            df['finbert_score'] = get_finbert_scores_batch(df['text'].tolist())
        return df

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
