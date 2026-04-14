# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run FastAPI backend (http://localhost:8000, docs at /docs)
uvicorn main:app --reload

# Run Streamlit UI
streamlit run app.py

# Environment variables required in .env
# DATABASE_URL, ALPACA_API_KEY, ALPACA_SECRET_KEY, FINNHUB_API_KEY
```

No test framework or linter is currently configured.

## Architecture

MoneyMaker correlates CEO tweets with stock market data. It fetches tweets via the `tweety-ns` library, runs sentiment analysis, fetches corresponding stock data from Alpaca, merges the results, and stores them in a Neon PostgreSQL database.

**Core files:**
- `main.py` — FastAPI app with all endpoints, SQLAlchemy models (`TweetRecord`, `StockRecord`, `MergedRecord`), and the primary orchestration endpoint `POST /process/all`
- `processor.py` — `DataProcessor` class that calls tweety-ns (Twitter) and alpaca-py (stocks); contains a runtime monkey-patch to bypass a tweety-ns animation key index error
- `classifier.py` — TextBlob-based sentiment utilities: `get_refined_sentiment()`, `get_tone_category()`, `get_tweet_type()`
- `app.py` — Streamlit dashboard with 4 tabs: Pull Tweets, Pull Stock Data, Pull Merged Data, ATR Analysis

**Data flow:** User inputs CEO handle + ticker + date range → `DataProcessor` fetches tweets and stock bars → `Classifier` annotates sentiment/tone/type → records merged and saved to Neon → returned to UI.

**CEO→ticker mappings** are hardcoded in `processor.py`: elonmusk→TSLA, tim_cook→AAPL, satyanadella→MSFT, sundarpichai→GOOGL, MichaelDell→DELL.

**Weekend handling:** Tweet dates that fall on weekends are automatically shifted to the following Monday for stock price correlation.

**ATR Analysis:** The Streamlit UI computes a 14-day rolling Average True Range to correlate high-sentiment tweet periods with stock volatility.

## Database Schema

Three tables (SQLAlchemy + Neon PostgreSQL):
- `tweets` — id, date, ceo, text, sentiment_score, refined_sentiment
- `stocks` — id, symbol, timestamp, open, high, low, close, volume
- `merged_data` — id, date, ceo, tweet_text, sentiment_score, refined_sentiment, tone_category, tweet_type, stock_ticker, stock_close, stock_volume, stock_open_close_diff
