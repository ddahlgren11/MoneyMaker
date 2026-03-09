import os
import asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
from tweety import Twitter
from textblob import TextBlob
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
        self.ceo_map = {"elonmusk": "TSLA", "tim_cook": "AAPL", "satyanadella": "MSFT", "sundarpichai": "GOOGL", "MichaelDell": "DELL"}

    async def get_tweets(self, username):
        all_tweets = []
        user_tweets = await self.twitter_client.get_tweets(username, pages=2)
        for tweet in user_tweets:
            analysis = TextBlob(tweet.text)
            all_tweets.append({
                'ceo': username,
                'text': tweet.text,
                'sentiment': analysis.sentiment.polarity,
                'created_at': tweet.created_on
            })
        return pd.DataFrame(all_tweets)

    def get_stocks(self, symbol, start_date=None, end_date=None):
        # Fetching data from 1 day ago to avoid SIP restrictions if not provided
        if end_date is None:
            end_date = datetime.now(timezone.utc) - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=7)

        request = StockBarsRequest(symbol_or_symbols=symbol, timeframe=TimeFrame.Day, start=start_date, end=end_date)
        bars = self.stock_client.get_stock_bars(request)
        return bars.df if bars else pd.DataFrame()