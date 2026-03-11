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
from classifier import get_refined_sentiment, get_tone_category, get_tweet_type

load_dotenv()

# --- DATABASE SETUP ---
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
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
        "MichaelDell": "DELL"
    }
    
    total_records = 0
    
    try:
        for username, ticker in targets.items():
            # 1. Fetch Tweets using processor logic
            tweets_df = await proc.get_tweets(username)
            if tweets_df.empty:
                continue

            # Calculate date range with padding for weekends/holidays
            min_date = tweets_df['created_at'].min() - timedelta(days=5)
            max_date = tweets_df['created_at'].max() + timedelta(days=5)
            
            # 2. Fetch Stock Data for the associated ticker
            stocks_df = proc.get_stocks(ticker, start_date=min_date, end_date=max_date)

            if not stocks_df.empty:
                if isinstance(stocks_df.index, pd.MultiIndex):
                    stock_dates = stocks_df.index.get_level_values('timestamp').date
                else:
                    stock_dates = stocks_df.index.date
                stocks_df['date_only'] = stock_dates
            
            # 3. Process and merge each tweet
            for _, row in tweets_df.iterrows():
                sentiment = float(row['sentiment'])
                text = str(row['text'])
                tweet_date = row['created_at']

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
                if not stocks_df.empty:
                    valid_stocks = stocks_df[stocks_df['date_only'] >= target_date_only]
                    if not valid_stocks.empty:
                        stock_close = float(valid_stocks['close'].iloc[0])
                        stock_volume = float(valid_stocks['volume'].iloc[0])
                        stock_open = float(valid_stocks['open'].iloc[0])
                        stock_open_close_diff = float(stock_open - stock_close)
                
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
                    stock_open_close_diff=stock_open_close_diff
                )
                db.add(new_record)
                total_records += 1
        
        db.commit()
        return {"status": "Success", "records_added": total_records}
        
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

        # Convert timestamp to string for JSON serialization
        if 'created_at' in tweets_df.columns:
            tweets_df['created_at'] = tweets_df['created_at'].astype(str)

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

        # Calculate date range with padding for weekends/holidays
        min_date = tweets_df['created_at'].min() - timedelta(days=5)
        max_date = tweets_df['created_at'].max() + timedelta(days=5)

        # 2. Fetch Stock Data
        stocks_df = proc.get_stocks(ticker, start_date=min_date, end_date=max_date)

        if not stocks_df.empty:
            if isinstance(stocks_df.index, pd.MultiIndex):
                stock_dates = stocks_df.index.get_level_values('timestamp').date
            else:
                stock_dates = stocks_df.index.date
            stocks_df['date_only'] = stock_dates

        merged_data = []

        # 3. Process and merge each tweet
        for _, row in tweets_df.iterrows():
            sentiment = float(row['sentiment'])
            text = str(row['text'])
            tweet_date = row['created_at']

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
            if not stocks_df.empty:
                valid_stocks = stocks_df[stocks_df['date_only'] >= target_date_only]
                if not valid_stocks.empty:
                    stock_close = float(valid_stocks['close'].iloc[0])
                    stock_volume = float(valid_stocks['volume'].iloc[0])
                    stock_open = float(valid_stocks['open'].iloc[0])
                    stock_open_close_diff = float(stock_open - stock_close)

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
                "stock_open_close_diff": stock_open_close_diff
            })

        return {"status": "success", "data": merged_data}
    except Exception as e:
        return {"status": "error", "message": str(e)}