import os
from dotenv import load_dotenv
from fastapi import FastAPI, Depends
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from typing import List

load_dotenv()

# Setup Database
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# Dependency to handle DB sessions
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app = FastAPI()

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

class ClassifiedTweetSchema(BaseModel):
    text: str
    refined_sentiment: str
    tone_category: str
    tweet_type: str

# --- ENDPOINTS ---

@app.post("/ingest/tweets")
async def ingest_tweets(tweets: List[TweetSchema], db: Session = Depends(get_db)):
    for t in tweets:
        # .model_dump() is the modern version of .dict()
        db.add(TweetRecord(**t.model_dump()))
    db.commit()
    return {"status": "success", "count": len(tweets)}

# Updated Stock endpoint
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
            date=item.date,
            ceo=item.ceo,
            tweet_text=item.text,
            sentiment_score=item.sentiment_score,
            refined_sentiment=item.refined_sentiment,
            tone_category=item.tone_category,
            tweet_type=item.tweet_type,
            stock_close=item.close,
            stock_volume=item.volume
        )
        db.add(db_item)
    db.commit()
    return {"status": "success", "count": len(data)}

@app.post("/ingest/classified_tweets")
async def ingest_classified_tweets(tweets: List[ClassifiedTweetSchema], db: Session = Depends(get_db)):
    for t in tweets:
        # Reusing the MergedRecord table or create a new 'ClassifiedRecord' if preferred
        db_item = MergedRecord(
            tweet_text=t.text,
            refined_sentiment=t.refined_sentiment,
            tone_category=t.tone_category,
            tweet_type=t.tweet_type
        )
        db.add(db_item)
    db.commit()
    return {"status": "success", "count": len(tweets)}

# This creates the tables in Neon if they don't exist
Base.metadata.create_all(bind=engine)