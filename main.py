import os
import pandas as pd
from datetime import timedelta
from typing import List
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
import pandas as pd

# Import the logic from your new files
from processor import DataProcessor
from classifier import get_refined_sentiment, get_tone_category, get_tweet_type, get_sentiment_score
from context import get_news_for_date

load_dotenv()

# --- DATABASE SETUP ---
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=300)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- DATABASE MODELS ---
class TweetRecord(Base):
    __tablename__ = "tweets"
    id = Column(Integer, primary_key=True, index=True)
    date = Column(String)
    ceo = Column(String)
    text = Column(String)
    sentiment_score = Column(Float)
    refined_sentiment = Column(String)

class StockRecord(Base):
    __tablename__ = "stocks"
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String)
    timestamp = Column(String)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)

class MergedRecord(Base):
    __tablename__ = "merged_data"
    id = Column(Integer, primary_key=True, index=True)
    date = Column(String)
    ceo = Column(String)
    tweet_text = Column(String)
    sentiment_score = Column(Float)
    refined_sentiment = Column(String)
    tone_category = Column(String)
    tweet_type = Column(String)
    stock_ticker = Column(String)
    stock_close = Column(Float)
    stock_volume = Column(Float)
    stock_open_close_diff = Column(Float)
    # Engagement signals
    likes = Column(Integer)
    retweet_count = Column(Integer)
    view_count = Column(Integer)
    reply_count = Column(Integer)
    # Timing signals
    tweet_hour = Column(Integer)
    is_premarket = Column(Integer)  # stored as 0/1
    # Prediction target
    next_day_direction = Column(Integer, nullable=True)  # 1 = up, 0 = down, NULL = no next-day data
    # Technical state at tweet time
    rsi_at_tweet = Column(Float, nullable=True)
    atr_at_tweet = Column(Float, nullable=True)
    # News sentiment on tweet day
    news_sentiment_score = Column(Float, nullable=True)

# Create tables if they don't exist
Base.metadata.create_all(bind=engine)

# --- PYDANTIC SCHEMAS ---
class TweetSchema(BaseModel):
    date: str
    ceo: str
    text: str
    sentiment_score: float
    refined_sentiment: str

class StockSchema(BaseModel):
    symbol: str
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: float

class MergedSchema(BaseModel):
    date: str
    ceo: str
    text: str
    sentiment_score: float
    refined_sentiment: str
    tone_category: str
    tweet_type: str
    stock_ticker: str
    close: float
    volume: float
    open_close_diff: float

app = FastAPI()
proc = DataProcessor()

# --- ACTIVE "CONTROLLER" ENDPOINT ---

@app.post("/process/all")
async def process_and_save_all(db: Session = Depends(get_db)):
    """
    Fetches data for multiple CEOs, classifies it, and saves it to Neon.
    This replaces the manual looping previously done in Colab.
    """
    # Map of CEO usernames to their respective Stock Tickers
    targets = {
        "elonmusk": "TSLA",
        "tim_cook": "AAPL",
        "satyanadella": "MSFT",
        "sundarpichai": "GOOGL",
        "MichaelDell": "DELL",
        "LisaSu": "AMD",
        "ajassy": "AMZN",
        "bchesky": "ABNB",
        "dkhos": "UBER",
        "RobertIger": "DIS",
    }

    # Company names for news queries
    ticker_company = {
        "TSLA": "Tesla",
        "AAPL": "Apple",
        "MSFT": "Microsoft",
        "GOOGL": "Google",
        "DELL": "Dell",
        "AMD": "AMD Advanced Micro Devices",
        "AMZN": "Amazon",
        "ABNB": "Airbnb",
        "UBER": "Uber",
        "DIS": "Disney",
    }

    # Cache news scores by (ticker, date) — one NewsAPI call per day, not per tweet
    news_cache = {}
    
    total_records = 0

    try:
        # Clear existing records so re-runs don't create duplicates
        db.query(MergedRecord).delete()
        db.flush()

        skipped = []
        for username, ticker in targets.items():
            # 1. Fetch Tweets using processor logic
            try:
                tweets_df = await proc.get_tweets(username)
            except Exception as e:
                skipped.append({"username": username, "reason": str(e)})
                continue
            if tweets_df.empty:
                continue

            tweets_df = tweets_df.sort_values(by='date', ascending=False)

            # Extra 30-day lookback so RSI/ATR rolling windows are valid from the first tweet
            min_date = tweets_df['date'].min() - timedelta(days=30)
            max_date = tweets_df['date'].max() + timedelta(days=5)

            # 2. Fetch Stock Data for the associated ticker
            stocks_df = proc.get_stocks(ticker, start_date=min_date, end_date=max_date)

            if not stocks_df.empty:
                stocks_df = stocks_df.sort_index()
                if isinstance(stocks_df.index, pd.MultiIndex):
                    stock_dates = stocks_df.index.get_level_values('timestamp').date
                else:
                    stock_dates = stocks_df.index.date
                stocks_df['date_only'] = stock_dates

                # ATR (14-period)
                stocks_df['prev_close'] = stocks_df['close'].shift(1)
                stocks_df['tr'] = stocks_df[['high', 'low', 'prev_close']].apply(
                    lambda r: max(r['high'] - r['low'],
                                  abs(r['high'] - r['prev_close']),
                                  abs(r['low'] - r['prev_close'])), axis=1
                )
                stocks_df['atr_14'] = stocks_df['tr'].rolling(14).mean()

                # RSI (14-period)
                delta = stocks_df['close'].diff()
                gain = delta.where(delta > 0, 0.0).rolling(14).mean()
                loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
                stocks_df['rsi_14'] = 100 - (100 / (1 + gain / loss))
            
            # 3. Process and merge each tweet
            for _, row in tweets_df.iterrows():
                sentiment = float(row['sentiment'])
                text = str(row['text'])
                tweet_date = row['date']

                # Match weekend tweets to following Monday
                target_date = tweet_date
                if target_date.weekday() == 5:  # Saturday
                    target_date += timedelta(days=2)
                elif target_date.weekday() == 6:  # Sunday
                    target_date += timedelta(days=1)

                target_date_only = target_date.date()

                stock_close = 0.0
                stock_volume = 0.0
                stock_open_close_diff = 0.0
                next_day_direction = None
                rsi_at_tweet = None
                atr_at_tweet = None
                if not stocks_df.empty:
                    valid_stocks = stocks_df[stocks_df['date_only'] >= target_date_only]
                    if not valid_stocks.empty:
                        stock_close = float(valid_stocks['close'].iloc[0])
                        stock_volume = float(valid_stocks['volume'].iloc[0])
                        stock_open = float(valid_stocks['open'].iloc[0])
                        stock_open_close_diff = float(stock_open - stock_close)
                        if len(valid_stocks) >= 2:
                            next_close = float(valid_stocks['close'].iloc[1])
                            next_day_direction = 1 if next_close > stock_close else 0
                        rsi_val = valid_stocks['rsi_14'].iloc[0]
                        atr_val = valid_stocks['atr_14'].iloc[0]
                        rsi_at_tweet = float(rsi_val) if not pd.isna(rsi_val) else None
                        atr_at_tweet = float(atr_val) if not pd.isna(atr_val) else None

                # News sentiment — cached per (ticker, date) to stay within API rate limits
                news_key = (ticker, target_date_only.isoformat())
                if news_key not in news_cache:
                    company = ticker_company.get(ticker, ticker)
                    articles = get_news_for_date(ticker, company, target_date_only)
                    if articles:
                        scores = [get_sentiment_score(a['title']) for a in articles]
                        news_cache[news_key] = round(sum(scores) / len(scores), 4)
                    else:
                        news_cache[news_key] = None
                news_sentiment_score = news_cache[news_key]

                new_record = MergedRecord(
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
                    likes=int(row.get('likes', 0)),
                    retweet_count=int(row.get('retweet_count', 0)),
                    view_count=int(row.get('view_count', 0)),
                    reply_count=int(row.get('reply_count', 0)),
                    tweet_hour=int(row.get('tweet_hour', 0)),
                    is_premarket=int(row.get('is_premarket', 0)),
                    next_day_direction=next_day_direction,
                    rsi_at_tweet=rsi_at_tweet,
                    atr_at_tweet=atr_at_tweet,
                    news_sentiment_score=news_sentiment_score,
                )
                db.add(new_record)
                total_records += 1
        
        db.commit()
        return {"status": "Success", "records_added": total_records, "skipped": skipped}
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# --- MANUAL INGESTION ENDPOINTS (BACKUPS) ---

@app.post("/ingest/tweets")
async def ingest_tweets(tweets: List[TweetSchema], db: Session = Depends(get_db)):
    for t in tweets:
        db.add(TweetRecord(**t.model_dump()))
    db.commit()
    return {"status": "success", "count": len(tweets)}

@app.post("/ingest/stocks")
async def ingest_stocks(stocks: List[StockSchema], db: Session = Depends(get_db)):
    for s in stocks:
        db.add(StockRecord(**s.model_dump()))
    db.commit()
    return {"status": "success", "count": len(stocks)}

@app.get("/", response_class=HTMLResponse)
def read_root():
    with open("index.html", "r") as f:
        return f.read()

@app.get("/api/tweets/{ceo}")
async def api_get_tweets(ceo: str):
    try:
        tweets_df = await proc.get_tweets(ceo)
        if tweets_df.empty:
            return {"status": "success", "data": []}

        tweets_df = tweets_df.sort_values(by='date', ascending=False)

        # Convert timestamp to string for JSON serialization
        if 'date' in tweets_df.columns:
            tweets_df['date'] = tweets_df['date'].astype(str)

        return {"status": "success", "data": tweets_df.to_dict(orient='records')}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/stocks/{ticker}")
def api_get_stocks(ticker: str):
    try:
        stocks_df = proc.get_stocks(ticker)
        if stocks_df.empty:
            return {"status": "success", "data": []}

        # Reset index to make timestamp a column, and convert to string
        stocks_df = stocks_df.reset_index()
        if 'timestamp' in stocks_df.columns:
            stocks_df['timestamp'] = stocks_df['timestamp'].astype(str)

        return {"status": "success", "data": stocks_df.to_dict(orient='records')}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/merged/{ceo}/{ticker}")
async def api_get_merged(ceo: str, ticker: str):
    try:
        # 1. Fetch Tweets
        tweets_df = await proc.get_tweets(ceo)
        if tweets_df.empty:
            return {"status": "success", "data": []}

        tweets_df = tweets_df.sort_values(by='date', ascending=False)

        # Extra 30-day lookback so RSI/ATR rolling windows are valid from the first tweet
        min_date = tweets_df['date'].min() - timedelta(days=30)
        max_date = tweets_df['date'].max() + timedelta(days=5)

        # 2. Fetch Stock Data
        stocks_df = proc.get_stocks(ticker, start_date=min_date, end_date=max_date)

        if not stocks_df.empty:
            stocks_df = stocks_df.sort_index()
            if isinstance(stocks_df.index, pd.MultiIndex):
                stock_dates = stocks_df.index.get_level_values('timestamp').date
            else:
                stock_dates = stocks_df.index.date
            stocks_df['date_only'] = stock_dates

            # ATR (14-period)
            stocks_df['prev_close'] = stocks_df['close'].shift(1)
            stocks_df['tr'] = stocks_df[['high', 'low', 'prev_close']].apply(
                lambda r: max(r['high'] - r['low'],
                              abs(r['high'] - r['prev_close']),
                              abs(r['low'] - r['prev_close'])), axis=1
            )
            stocks_df['atr_14'] = stocks_df['tr'].rolling(14).mean()

            # RSI (14-period)
            delta = stocks_df['close'].diff()
            gain = delta.where(delta > 0, 0.0).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
            stocks_df['rsi_14'] = 100 - (100 / (1 + gain / loss))

        ticker_company = {
            "TSLA": "Tesla", "AAPL": "Apple", "MSFT": "Microsoft",
            "GOOGL": "Google", "DELL": "Dell",
        }
        news_cache = {}
        merged_data = []

        # 3. Process and merge each tweet
        for _, row in tweets_df.iterrows():
            sentiment = float(row['sentiment'])
            text = str(row['text'])
            tweet_date = row['date']

            # Match weekend tweets to following Monday
            target_date = tweet_date
            if target_date.weekday() == 5:  # Saturday
                target_date += timedelta(days=2)
            elif target_date.weekday() == 6:  # Sunday
                target_date += timedelta(days=1)

            target_date_only = target_date.date()

            stock_close = None
            stock_volume = None
            stock_open_close_diff = None
            next_day_direction = None
            rsi_at_tweet = None
            atr_at_tweet = None
            if not stocks_df.empty:
                valid_stocks = stocks_df[stocks_df['date_only'] >= target_date_only]
                if not valid_stocks.empty:
                    stock_close = float(valid_stocks['close'].iloc[0])
                    stock_volume = float(valid_stocks['volume'].iloc[0])
                    stock_open = float(valid_stocks['open'].iloc[0])
                    stock_open_close_diff = float(stock_open - stock_close)
                    if len(valid_stocks) >= 2:
                        next_close = float(valid_stocks['close'].iloc[1])
                        next_day_direction = 1 if next_close > stock_close else 0
                    rsi_val = valid_stocks['rsi_14'].iloc[0]
                    atr_val = valid_stocks['atr_14'].iloc[0]
                    rsi_at_tweet = float(rsi_val) if not pd.isna(rsi_val) else None
                    atr_at_tweet = float(atr_val) if not pd.isna(atr_val) else None

            news_key = (ticker, target_date_only.isoformat())
            if news_key not in news_cache:
                company = ticker_company.get(ticker.upper(), ticker)
                articles = get_news_for_date(ticker, company, target_date_only)
                if articles:
                    scores = [get_sentiment_score(a['title']) for a in articles]
                    news_cache[news_key] = round(sum(scores) / len(scores), 4)
                else:
                    news_cache[news_key] = None
            news_sentiment_score = news_cache[news_key]

            merged_data.append({
                "date": tweet_date.isoformat(),
                "ceo": ceo,
                "tweet_text": text,
                "sentiment_score": sentiment,
                "refined_sentiment": get_refined_sentiment(sentiment),
                "tone_category": get_tone_category(text, sentiment),
                "tweet_type": get_tweet_type(text),
                "stock_close": stock_close,
                "stock_volume": stock_volume,
                "stock_open_close_diff": stock_open_close_diff,
                "likes": int(row.get('likes', 0)),
                "retweet_count": int(row.get('retweet_count', 0)),
                "view_count": int(row.get('view_count', 0)),
                "reply_count": int(row.get('reply_count', 0)),
                "tweet_hour": int(row.get('tweet_hour', 0)),
                "is_premarket": bool(row.get('is_premarket', False)),
                "next_day_direction": next_day_direction,
                "rsi_at_tweet": rsi_at_tweet,
                "atr_at_tweet": atr_at_tweet,
                "news_sentiment_score": news_sentiment_score,
            })

        return {"status": "success", "data": merged_data}
    except Exception as e:
        return {"status": "error", "message": str(e)}