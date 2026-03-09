import os
from typing import List
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel

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
    stock_close = Column(Float)
    stock_volume = Column(Float)

# Create tables if they don't exist
Base.metadata.create_all(bind=engine)

# --- PYDANTIC SCHEMAS ---
class TweetSchema(BaseModel):
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
    close: float
    volume: float

app = FastAPI()
proc = DataProcessor()

# --- ACTIVE "CONTROLLER" ENDPOINT ---

@app.post("/process/all")
async def process_and_save_all(db: Session = Depends(get_db)):
    """Fetches data, classifies it, and saves it to Neon automatically."""
    try:
        # 1. Fetch Tweets using processor logic
        tweets_df = await proc.get_tweets("elonmusk")
        
        # 2. Fetch Stock Data
        stocks_df = proc.get_stocks("TSLA")
        
        # 3. Process each tweet and save to merged_data
        for _, row in tweets_df.iterrows():
            sentiment = row['sentiment']
            text = row['text']
            
            new_record = MergedRecord(
                date=row['created_at'].isoformat(),
                ceo=row['ceo'],
                tweet_text=text,
                sentiment_score=sentiment,
                refined_sentiment=get_refined_sentiment(sentiment),
                tone_category=get_tone_category(text, sentiment),
                tweet_type=get_tweet_type(text),
                stock_close=stocks_df['close'].iloc[-1] if not stocks_df.empty else 0.0,
                stock_volume=stocks_df['volume'].iloc[-1] if not stocks_df.empty else 0.0
            )
            db.add(new_record)
        
        db.commit()
        return {"status": "Success", "records_added": len(tweets_df)}
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

@app.post("/ingest/merged")
async def ingest_merged(data: List[MergedSchema], db: Session = Depends(get_db)):
    for item in data:
        db_item = MergedRecord(
            date=item.date, ceo=item.ceo, tweet_text=item.text,
            sentiment_score=item.sentiment_score, refined_sentiment=item.refined_sentiment,
            tone_category=item.tone_category, tweet_type=item.tweet_type,
            stock_close=item.close, stock_volume=item.volume
        )
        db.add(db_item)
    db.commit()
    return {"status": "success", "count": len(data)}

@app.get("/")
def read_root():
    return {"message": "MoneyMaker Active Controller API"}