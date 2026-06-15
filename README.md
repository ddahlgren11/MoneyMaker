# MoneyMaker

**Do CEO tweets move stock prices?**

MoneyMaker answers that question with real data. It collects tweets from 24 high-profile CEOs, scores each one for sentiment using two NLP models (VADER + FinBERT), pairs every tweet with that day's stock data and broader market context, and runs a machine learning model that predicts whether the stock will close higher or lower the following trading day.

**[Live demo →](https://moneymaker-ddahlgren.streamlit.app)** &nbsp;|&nbsp; Built by [Dillon Dahlgren](https://github.com/ddahlgren11)

---

## Tech Stack

| Layer | Technologies |
|-------|-------------|
| Language | Python 3.11 |
| Backend API | FastAPI, SQLAlchemy (ORM), Pydantic v2, Uvicorn |
| Frontend | Streamlit, Plotly |
| ML / NLP | scikit-learn, VADER, FinBERT (HuggingFace `ProsusAI/finbert`), pandas, numpy |
| Database | Neon (serverless PostgreSQL), psycopg2 |
| Data sources | Twitter/X (twikit, cookie auth), Alpaca Markets, Alpha Vantage, Finnhub, yfinance |
| CI/CD | GitHub Actions (scheduled daily retraining) |
| Hosting | Streamlit Community Cloud |

---

## Architecture

The project has two operating modes:

**Full local pipeline** — three-tier, for data ingestion and model training:
```
Twitter/X · Alpaca · Alpha Vantage · Finnhub · yfinance
        ↓
  processor.py  →  FastAPI (main.py)  →  Neon PostgreSQL
                                              ↑ HTTP
                                    Streamlit UI (app.py)
```

**Hosted demo** — Streamlit connects directly to the database, stock data via yfinance (no FastAPI needed for read-only display):
```
Neon PostgreSQL  ←→  Streamlit (app.py)  ←→  yfinance
```

---

## Key Engineering Highlights

- **Walk-forward cross-validation** — uses `TimeSeriesSplit` (5 folds) so each fold trains on all data before its test window. No lookahead. Standard k-fold would leak future data and report inflated accuracy.

- **Three competing models** — Logistic Regression, Random Forest (200 trees), and Gradient Boosting (200 estimators) are trained and evaluated; the best CV mean accuracy wins. Each gets appropriate scaling and class-balance treatment.

- **Calibrated probability estimates** — `CalibratedClassifierCV` with isotonic regression maps raw model scores to true probabilities. The confidence % shown in the UI is meaningful: a "70% confident" prediction should be correct ~70% of the time.

- **Exponential decay sample weights** (half-life 180 days) — tweets from 6 months ago carry 50% the weight of today's tweets. Keeps the model current without discarding historical signal.

- **22-feature input vector** spanning sentiment (VADER + FinBERT), engagement (likes/retweets/views, log-transformed), timing (tweet hour, pre-market flag), technicals (RSI, ATR), and market context (VIX, days to earnings, prior-day news sentiment).

- **Deduplicating daily pipeline** — `run_pipeline.py` loads existing tweet timestamps from the DB before fetching and only inserts new records. History accumulates over time; a partial failure doesn't wipe data.

- **Automated daily retraining** — GitHub Actions runs at 8am EST on weekdays, ingests fresh data, retrains the model, and commits the updated `.pkl` back to the repo with a date-stamped message.

- **116-test suite** across 6 files — all external dependencies (Twitter, Alpaca, Alpha Vantage, Finnhub) are mocked. Tests cover sentiment scoring, API response schemas, engagement parsing edge cases, ML feature contract enforcement, and inference-time correctness.

---

## ML Model — Feature Set

| Category | Features |
|----------|----------|
| Sentiment | `sentiment_score` (VADER), `finbert_score` (FinBERT), `sentiment_magnitude` |
| Tweet substance | `tweet_length`, `word_count` |
| Engagement | `log_likes`, `log_retweets`, `log_views`, `log_replies`, `engagement_rate` |
| Timing | `tweet_hour`, `is_premarket` |
| Technical indicators | `rsi_at_tweet`, `atr_at_tweet`, `rsi_overbought`, `rsi_oversold` |
| Market context | `vix_at_tweet`, `days_to_earnings`, `prev_day_direction`, `news_sentiment_score` |
| Categorical (one-hot) | `refined_sentiment` (5 labels), `tone_category` (5 labels), `tweet_type` (4 labels) |

Engagement counts are log-transformed (`log1p`) to compress the heavy right skew from viral tweets. `engagement_rate` normalizes for follower-count differences across CEOs — Musk's 200M followers vs. most others' 1–5M.

---

## Database Schema

Four tables, auto-created by SQLAlchemy on startup.

**`merged_data`** — the main analysis table used for training and display

| Column | Type | Notes |
|--------|------|-------|
| `date` | String | Tweet timestamp |
| `ceo` | String | Twitter handle |
| `tweet_text` | String | |
| `sentiment_score` | Float | VADER [-1, 1] |
| `finbert_score` | Float | FinBERT [-1, 1] |
| `refined_sentiment` | String | Very Negative → Very Positive |
| `tone_category` | String | 5 labels |
| `tweet_type` | String | 4 labels |
| `stock_ticker` | String | |
| `stock_close` | Float | Close on tweet day (shifted to Monday if weekend) |
| `stock_volume` | Float | |
| `stock_open_close_diff` | Float | |
| `likes / retweet_count / view_count / reply_count` | Integer | Engagement signals |
| `tweet_hour` | Integer | UTC hour |
| `is_premarket` | Integer | 1 if posted before NYSE open (14:30 UTC) |
| `next_day_direction` | Integer | 1 = up, 0 = down, NULL = no next-day data yet |
| `rsi_at_tweet` | Float | 14-period RSI |
| `atr_at_tweet` | Float | 14-period ATR |
| `vix_at_tweet` | Float | VIX close on tweet day |
| `news_sentiment_score` | Float | Avg headline sentiment for the ticker that day |
| `days_to_earnings` | Integer | Calendar days to nearest earnings date |

Additional tables: `tweets` (raw), `stocks` (raw OHLCV), `news_sentiment_cache` (API cache keyed by ticker + date).

---

## API Endpoints

All responses: `{"status": "success"|"error", ...}`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Serves `index.html` |
| `GET` | `/api/ceos` | All tracked CEOs with handle, name, ticker |
| `GET` | `/api/tweets/{ceo}` | Live-fetch tweets from Twitter + classify |
| `GET` | `/api/stocks/{ticker}` | Live-fetch OHLCV from Alpaca |
| `GET` | `/api/merged/{ceo}/{ticker}` | Live-fetch + merge (no DB read) |
| `GET` | `/api/merged` | DB read with filters: `ceo`, `ticker`, `start_date`, `end_date`, `limit` |
| `GET` | `/api/merged/summary` | DB stats: record count, CEO list, date range |
| `POST` | `/process/all` | Full ingestion pipeline for all CEOs, deduplicates to DB |
| `POST` | `/api/predict` | Single-tweet prediction via trained model |
| `GET` | `/api/analysis/price-swing/{ceo}/{ticker}` | Avg price swing + direction split on tweet days |
| `GET` | `/api/analysis/tweet-impact/{ceo}/{ticker}` | Per-tweet impact scores (sentiment × engagement) |
| `GET` | `/api/analysis/post-tweet-trend/{ceo}/{ticker}` | Next-day direction bucketed by sentiment range |

---

## Local Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Create .env in the project root (see .env.example)
DATABASE_URL=your_neon_postgres_connection_url
ALPACA_API_KEY=your_alpaca_api_key                  # market data (live keys)
ALPACA_SECRET_KEY=your_alpaca_secret_key
ALPACA_PAPER_API_KEY=your_alpaca_paper_api_key      # paper trading (watch.py / trade.py)
ALPACA_PAPER_SECRET_KEY=your_alpaca_paper_secret_key
FINNHUB_API_KEY=your_finnhub_api_key          # optional
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_key  # optional

# 2b. Generate Twitter cookies for twikit (no paid API needed).
#     Grab auth_token and ct0 from your logged-in x.com browser cookies, then:
python3 test_twitter_cookies.py <auth_token> <ct0>   # writes twitter_cookies.json
#     For GitHub Actions, paste the file's contents into the TWITTER_COOKIES repo secret.

# 3. Start the FastAPI backend
uvicorn main:app --reload           # http://localhost:8000 · docs at /docs

# 4. Start the Streamlit dashboard
streamlit run app.py                # http://localhost:8501

# 5. Train the ML model (required before the Predict tab works)
python3 model/baseline.py

# 6. Run the data pipeline manually
python3 run_pipeline.py             # daily incremental
python3 run_pipeline.py --pages 50  # historical backfill

# 7. Run all tests
python3 -m pytest tests/ -v
```

---

## Tests

```
116 tests · 6 files · all external APIs mocked · no credentials needed
```

| File | What it covers |
|------|----------------|
| `test_sentiment_score.py` | VADER return type, range [-1,1], polarity direction, edge cases |
| `test_classifier.py` | All sentiment/tone/type label boundaries and fallbacks |
| `test_api.py` | All three live-fetch endpoints — schema, value ranges, error handling |
| `test_processor.py` | `_safe_int()` — handles `None`, `"Unavailable"`, float strings, large ints |
| `test_context.py` | Alpha Vantage and Finnhub parsing, fallback logic, rate-limit handling, caching |
| `test_predict.py` | Feature contract (23 exact features), weekend date shifting, RSI flags, inference correctness |

---

## CEO Coverage (24)

| Handle | Name | Ticker | Handle | Name | Ticker |
|--------|------|--------|--------|------|--------|
| elonmusk | Elon Musk | TSLA | LisaSu | Lisa Su | AMD |
| tim_cook | Tim Cook | AAPL | jack | Jack Dorsey | SQ |
| satyanadella | Satya Nadella | MSFT | tobi | Tobi Lütke | SHOP |
| sundarpichai | Sundar Pichai | GOOGL | brian_armstrong | Brian Armstrong | COIN |
| MichaelDell | Michael Dell | DELL | CathieDWood | Cathie Wood | ARKK |
| ajassy | Andy Jassy | AMZN | mtbarra | Mary Barra | GM |
| bchesky | Brian Chesky | ABNB | JimFarley98 | Jim Farley | F |
| dkhos | Dara Khosrowshahi | UBER | AnthonyNoto | Anthony Noto | SOFI |
| RobertIger | Bob Iger | DIS | reedhastings | Reed Hastings | NFLX |
| Benioff | Marc Benioff | CRM | PGelsinger | Pat Gelsinger | INTC |
| george_kurtz | George Kurtz | CRWD | levie | Aaron Levie | BOX |
| eldsjal | Daniel Ek | SPOT | RJScaringe | RJ Scaringe | RIVN |
