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
DATABASE_URL = os.getenv("DATABASE_URL") # This comes from Render/Secrets
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

engine = create_engine(DATABASE_URL)

# Define your Tweet table
class TweetRecord(Base):
    __tablename__ = "tweets"
    id = Column(Integer, primary_key=True, index=True)
    ceo = Column(String)
    text = Column(String)
    sentiment_score = Column(Float)
    refined_sentiment = Column(String)

Base.metadata.create_all(bind=engine)

app = FastAPI()

# Pydantic model for data coming from Colab
class TweetSchema(BaseModel):
    ceo: str
    text: str
    sentiment_score: float
    refined_sentiment: str

@app.post("/ingest/tweets")
async def ingest_tweets(tweets: List[TweetSchema]):
    db = SessionLocal()
    for t in tweets:
        new_tweet = TweetRecord(**t.dict())
        db.add(new_tweet)
    db.commit()
    return {"status": "success", "count": len(tweets)}

class StockRecord(Base):
    __tablename__ = "stocks"
    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String)
    timestamp = Column(String)  # We store as string to match the Colab conversion
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)

# Add this to your Pydantic models section
class StockSchema(BaseModel):
    symbol: str
    timestamp: str
    open: float
    high: float
    low: float
    close: float
    volume: float

@app.post("/ingest/stocks")
async def ingest_stocks(stocks: List[StockSchema]):
    db = SessionLocal()
    try:
        for s in stocks:
            new_stock = StockRecord(**s.dict())
            db.add(new_stock)
        db.commit()
        return {"status": "success", "message": f"Saved {len(stocks)} stock records"}
    except Exception as e:
        db.rollback()
        return {"status": "error", "message": str(e)}
    finally:
        db.close()