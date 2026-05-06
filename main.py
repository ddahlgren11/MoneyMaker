import os
import time
import logging
import pandas as pd
from datetime import timedelta, datetime, date as date_type
from typing import List, Optional
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from sqlalchemy import create_engine, Column, Integer, String, Float, func, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
# Import the logic from your new files
from processor import DataProcessor
from classifier import get_refined_sentiment, get_tone_category, get_tweet_type, get_sentiment_score
from context import get_news_for_date, get_earnings_dates, build_news_sentiment_lookup
from model.predict import predict_tweets
from targets import CEO_TARGETS, HANDLE_TO_TICKER
from pipeline_utils import build_vix_lookup, days_to_nearest_earnings, shift_weekend_to_monday, compute_technicals

load_dotenv()

logging.basicConfig(level=logging.WARNING)
logging.getLogger("context").setLevel(logging.DEBUG)


# --- DATABASE SETUP ---
DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
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

class NewsSentimentCache(Base):
    __tablename__ = "news_sentiment_cache"
    ticker   = Column(String, primary_key=True)
    date_str = Column(String, primary_key=True)  # YYYY-MM-DD
    sentiment_score = Column(Float)

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
    # FinBERT financial sentiment (independent of VADER sentiment_score)
    finbert_score = Column(Float, nullable=True)
    # Market context
    vix_at_tweet = Column(Float, nullable=True)       # VIX on tweet day — market fear level
    days_to_earnings = Column(Integer, nullable=True)  # calendar days to nearest earnings date

# Create tables if they don't exist
Base.metadata.create_all(bind=engine)

# Add any new columns that don't exist yet (safe to run on every startup)
with engine.connect() as _conn:
    for _col, _type in [("finbert_score", "FLOAT")]:
        try:
            _conn.execute(text(f"ALTER TABLE merged_data ADD COLUMN {_col} {_type}"))
            _conn.commit()
        except Exception:
            pass  # column already exists

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
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
proc = DataProcessor()

# --- ACTIVE "CONTROLLER" ENDPOINT ---

@app.post("/process/all")
async def process_and_save_all(db: Session = Depends(get_db)):
    """
    Fetches data for multiple CEOs, classifies it, and saves it to Neon.
    This replaces the manual looping previously done in Colab.
    """
    targets = HANDLE_TO_TICKER
    total_records = 0

    try:
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
            tweets_df = tweets_df.dropna(subset=['date'])
            if tweets_df.empty:
                continue

            # Skip tweets already stored for this CEO
            existing_dates = {
                r.date for r in
                db.query(MergedRecord.date).filter(MergedRecord.ceo == username).all()
            }
            tweets_df = tweets_df[
                ~tweets_df['date'].apply(lambda d: d.isoformat()).isin(existing_dates)
            ]
            if tweets_df.empty:
                continue

            # Extra 30-day lookback so RSI/ATR rolling windows are valid from the first tweet
            min_date = tweets_df['date'].min() - timedelta(days=30)
            max_date = tweets_df['date'].max() + timedelta(days=5)

            # 2a. Fetch VIX for this date range (market fear context)
            vix_lookup = build_vix_lookup(min_date, max_date)

            # 2b. Fetch earnings dates for this ticker
            earnings_set = get_earnings_dates(ticker)

            # 2c. Fetch Stock Data for the associated ticker
            stocks_df = proc.get_stocks(ticker, start_date=min_date, end_date=max_date)

            stocks_df = compute_technicals(stocks_df)

            # 2d. Build news sentiment lookup — one AV call per ticker (sort=EARLIEST),
            # cached to DB so re-runs don't burn the 25 req/day limit.
            tweet_date_objects = set(tweets_df['date'].dt.date)

            cached_rows = db.query(NewsSentimentCache).filter(
                NewsSentimentCache.ticker == ticker
            ).all()
            news_lookup = {r.date_str: r.sentiment_score for r in cached_rows}

            cached_date_objs = {date_type.fromisoformat(d) for d in news_lookup}
            uncached_dates = tweet_date_objects - cached_date_objs

            if uncached_dates:
                new_lookup = build_news_sentiment_lookup(
                    ticker,
                    tweet_dates=uncached_dates,
                    get_sentiment_fn=get_sentiment_score,
                )
                # Respect AV's 1-req/sec burst limit between tickers.
                time.sleep(1.1)
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
                hits = tweet_date_strs & set(lookup_dates)
                logging.warning("News lookup %s: %d dates in lookup (%s → %s), %d tweet dates, %d overlap",
                                ticker, len(lookup_dates),
                                lookup_dates[0], lookup_dates[-1],
                                len(tweet_date_strs), len(hits))
            else:
                logging.warning("News lookup %s: empty — all news_sentiment_score will be NULL", ticker)

            # 3. Process and merge each tweet
            for _, row in tweets_df.iterrows():
                sentiment = float(row['sentiment'])
                text = str(row['text'])
                tweet_date = row['date']

                target_date = shift_weekend_to_monday(tweet_date)
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

                # News sentiment — look up from the bulk fetch done once per ticker
                news_sentiment_score = news_lookup.get(target_date_only.isoformat())

                # VIX on tweet day — fall back to nearest available day if weekend/holiday
                vix_at_tweet = vix_lookup.get(target_date_only)
                if vix_at_tweet is None:
                    for offset in range(1, 4):
                        vix_at_tweet = vix_lookup.get(target_date_only - timedelta(days=offset))
                        if vix_at_tweet is not None:
                            break

                # Days to nearest earnings date
                days_to_earnings = days_to_nearest_earnings(target_date_only, earnings_set)

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
                    vix_at_tweet=vix_at_tweet,
                    days_to_earnings=days_to_earnings,
                    finbert_score=float(row.get('finbert_score')) if row.get('finbert_score') is not None else None,
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

        # Add pre-classification so the UI doesn't need classifier imports
        tweets_df['refined_sentiment'] = tweets_df['sentiment'].apply(get_refined_sentiment)
        tweets_df['tone_category'] = tweets_df.apply(
            lambda r: get_tone_category(str(r['text']), float(r['sentiment'])), axis=1
        )
        tweets_df['tweet_type'] = tweets_df['text'].apply(get_tweet_type)

        if 'date' in tweets_df.columns:
            tweets_df['date'] = tweets_df['date'].astype(str)

        return {"status": "success", "data": tweets_df.to_dict(orient='records')}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/stocks/{ticker}")
def api_get_stocks(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    try:
        from datetime import timezone
        start_dt = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc) if start_date else None
        end_dt = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc) if end_date else None
        stocks_df = proc.get_stocks(ticker, start_date=start_dt, end_date=end_dt)
        if stocks_df.empty:
            return {"status": "success", "data": []}

        stocks_df = stocks_df.reset_index()
        # Flatten symbol column from MultiIndex reset if present
        if 'symbol' in stocks_df.columns:
            stocks_df = stocks_df.drop(columns=['symbol'])
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
        tweets_df = tweets_df.dropna(subset=['date'])
        if tweets_df.empty:
            return {"status": "success", "data": []}

        # Extra 30-day lookback so RSI/ATR rolling windows are valid from the first tweet
        min_date = tweets_df['date'].min() - timedelta(days=30)
        max_date = tweets_df['date'].max() + timedelta(days=5)

        # 2. Fetch Stock Data
        stocks_df = proc.get_stocks(ticker, start_date=min_date, end_date=max_date)

        stocks_df = compute_technicals(stocks_df)
        news_cache = {}
        merged_data = []

        # 3. Process and merge each tweet
        for _, row in tweets_df.iterrows():
            sentiment = float(row['sentiment'])
            text = str(row['text'])
            tweet_date = row['date']

            target_date = shift_weekend_to_monday(tweet_date)
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
                company = next((v["name"] for v in CEO_TARGETS.values() if v["ticker"] == ticker.upper()), ticker)
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


# ---------------------------------------------------------------------------
# THREE-TIER API ENDPOINTS
# ---------------------------------------------------------------------------

CEO_INFO = CEO_TARGETS


@app.get("/api/ceos")
def api_get_ceos():
    """Return full CEO list for UI dropdowns."""
    return {
        "status": "success",
        "data": [
            {"handle": handle, "name": info["name"], "ticker": info["ticker"]}
            for handle, info in CEO_INFO.items()
        ],
    }


@app.get("/api/merged")
def api_get_merged_db(
    ceo: Optional[str] = None,
    ticker: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 2000,
    db: Session = Depends(get_db),
):
    """Read merged records from the DB with optional filters."""
    q = db.query(MergedRecord)
    if ceo:
        q = q.filter(MergedRecord.ceo == ceo)
    if ticker:
        q = q.filter(MergedRecord.stock_ticker == ticker)
    if start_date:
        q = q.filter(MergedRecord.date >= start_date)
    if end_date:
        q = q.filter(MergedRecord.date <= end_date)
    records = q.order_by(MergedRecord.date.desc()).limit(limit).all()
    return {
        "status": "success",
        "data": [
            {
                "id": r.id,
                "date": r.date,
                "ceo": r.ceo,
                "tweet_text": r.tweet_text,
                "sentiment_score": r.sentiment_score,
                "refined_sentiment": r.refined_sentiment,
                "tone_category": r.tone_category,
                "tweet_type": r.tweet_type,
                "stock_ticker": r.stock_ticker,
                "stock_close": r.stock_close,
                "stock_volume": r.stock_volume,
                "stock_open_close_diff": r.stock_open_close_diff,
                "likes": r.likes,
                "retweet_count": r.retweet_count,
                "view_count": r.view_count,
                "reply_count": r.reply_count,
                "tweet_hour": r.tweet_hour,
                "is_premarket": r.is_premarket,
                "next_day_direction": r.next_day_direction,
                "rsi_at_tweet": r.rsi_at_tweet,
                "atr_at_tweet": r.atr_at_tweet,
                "news_sentiment_score": r.news_sentiment_score,
                "finbert_score": r.finbert_score,
                "vix_at_tweet": r.vix_at_tweet,
                "days_to_earnings": r.days_to_earnings,
            }
            for r in records
        ],
    }


@app.get("/api/merged/summary")
def api_get_merged_summary(db: Session = Depends(get_db)):
    """High-level stats about what's in the DB."""
    total = db.query(func.count(MergedRecord.id)).scalar()
    ceos = sorted(r[0] for r in db.query(MergedRecord.ceo).distinct().all() if r[0])
    tickers = sorted(r[0] for r in db.query(MergedRecord.stock_ticker).distinct().all() if r[0])
    date_range = db.query(func.min(MergedRecord.date), func.max(MergedRecord.date)).first()
    return {
        "status": "success",
        "total_records": total,
        "ceos": ceos,
        "tickers": tickers,
        "date_min": date_range[0] if date_range else None,
        "date_max": date_range[1] if date_range else None,
    }


# --- Predict endpoint ---

class PredictRequest(BaseModel):
    tweet_text: str
    ticker: str
    sentiment_score: Optional[float] = None
    finbert_score: Optional[float] = None
    likes: int = 0
    retweet_count: int = 0
    view_count: int = 0
    reply_count: int = 0
    tweet_hour: int = 12
    is_premarket: bool = False


@app.post("/api/predict")
def api_predict(req: PredictRequest):
    """Run the trained model on a single tweet + optional engagement context."""
    try:
        sentiment = req.sentiment_score if req.sentiment_score is not None else get_sentiment_score(req.tweet_text)
        row = {
            "date": datetime.now(),
            "text": req.tweet_text,
            "sentiment": sentiment,
            "finbert_score": req.finbert_score,
            "likes": req.likes,
            "retweet_count": req.retweet_count,
            "view_count": req.view_count,
            "reply_count": req.reply_count,
            "tweet_hour": req.tweet_hour,
            "is_premarket": int(req.is_premarket),
        }
        tweets_df = pd.DataFrame([row])
        empty_stocks = pd.DataFrame(
            columns=["date_only", "close", "open", "high", "low", "volume", "rsi_14", "atr_14"]
        )
        result_df = predict_tweets(tweets_df, empty_stocks, ticker=req.ticker)
        if result_df.empty:
            return {"status": "error", "message": "Model returned no predictions"}
        first = result_df.iloc[0]
        return {
            "status": "success",
            "data": {
                "predicted_direction": first["predicted_direction"],
                "confidence_pct": float(first["confidence_pct"]),
                "sentiment_score": round(float(sentiment), 4),
                "ticker": req.ticker,
            },
        }
    except FileNotFoundError as e:
        return {"status": "error", "message": str(e)}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# --- Analysis endpoints ---


def _compute_impact(r) -> float:
    import math
    sentiment_mag = abs(r.sentiment_score or 0) * 50
    reach = (r.likes or 0) + 2 * (r.retweet_count or 0) + 0.1 * (r.view_count or 0) + (r.reply_count or 0)
    engagement = min(math.log1p(reach) / math.log1p(100_000) * 50, 50)
    return round(sentiment_mag + engagement, 1)


@app.get("/api/analysis/price-swing/{ceo}/{ticker}")
def api_price_swing(ceo: str, ticker: str, db: Session = Depends(get_db)):
    """Average absolute price swing and next-day direction split for tweet days."""
    records = db.query(MergedRecord).filter(
        MergedRecord.ceo == ceo,
        MergedRecord.stock_ticker == ticker,
    ).all()
    if not records:
        return {"status": "success", "data": None}

    swings = [abs(r.stock_open_close_diff) for r in records if r.stock_open_close_diff is not None]
    avg_swing = round(sum(swings) / len(swings), 4) if swings else 0.0
    up = sum(1 for r in records if r.next_day_direction == 1)
    down = sum(1 for r in records if r.next_day_direction == 0)
    total_labeled = up + down

    return {
        "status": "success",
        "data": {
            "avg_abs_swing": avg_swing,
            "total_tweets": len(records),
            "next_day_up": up,
            "next_day_down": down,
            "next_day_up_pct": round(up / total_labeled * 100, 1) if total_labeled else 0,
        },
    }


@app.get("/api/analysis/tweet-impact/{ceo}/{ticker}")
def api_tweet_impact(ceo: str, ticker: str, db: Session = Depends(get_db)):
    """Per-tweet impact scores with next-day direction label."""
    records = (
        db.query(MergedRecord)
        .filter(MergedRecord.ceo == ceo, MergedRecord.stock_ticker == ticker)
        .order_by(MergedRecord.date.desc())
        .limit(300)
        .all()
    )
    if not records:
        return {"status": "success", "data": []}

    data = [
        {
            "date": r.date,
            "tweet_text": r.tweet_text[:140] if r.tweet_text else "",
            "sentiment_score": r.sentiment_score,
            "impact_score": _compute_impact(r),
            "next_day_direction": r.next_day_direction,
            "likes": r.likes,
            "retweet_count": r.retweet_count,
        }
        for r in records
    ]
    return {"status": "success", "data": data}


@app.get("/api/analysis/post-tweet-trend/{ceo}/{ticker}")
def api_post_tweet_trend(ceo: str, ticker: str, db: Session = Depends(get_db)):
    """Next-day direction distribution bucketed by tweet sentiment."""
    records = db.query(MergedRecord).filter(
        MergedRecord.ceo == ceo,
        MergedRecord.stock_ticker == ticker,
        MergedRecord.next_day_direction != None,  # noqa: E711
    ).all()

    if not records:
        return {"status": "success", "data": {}}

    buckets = {
        "very_negative": {"label": "< -0.5",       "up": 0, "down": 0},
        "negative":      {"label": "-0.5 to -0.1",  "up": 0, "down": 0},
        "neutral":       {"label": "-0.1 to 0.1",   "up": 0, "down": 0},
        "positive":      {"label": "0.1 to 0.5",    "up": 0, "down": 0},
        "very_positive": {"label": "> 0.5",         "up": 0, "down": 0},
    }

    for r in records:
        s = r.sentiment_score or 0
        if s < -0.5:
            key = "very_negative"
        elif s < -0.1:
            key = "negative"
        elif s <= 0.1:
            key = "neutral"
        elif s <= 0.5:
            key = "positive"
        else:
            key = "very_positive"
        buckets[key]["up" if r.next_day_direction == 1 else "down"] += 1

    for b in buckets.values():
        total = b["up"] + b["down"]
        b["total"] = total
        b["up_pct"] = round(b["up"] / total * 100, 1) if total else 0

    return {"status": "success", "data": buckets}