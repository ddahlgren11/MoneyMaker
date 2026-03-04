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

@app.post("/ingest")
def ingest_tweets(tweets: List[TweetSchema]):
    db = SessionLocal()
    for t in tweets:
        new_tweet = TweetRecord(**t.dict())
        db.add(new_tweet)
    db.commit()
    return {"status": "success", "count": len(tweets)}