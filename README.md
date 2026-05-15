# MoneyMaker

Correlates CEO tweets with stock market data. Fetches tweets, scores sentiment two ways (VADER + FinBERT), pulls corresponding OHLCV bars and market context signals, merges everything, stores it in Neon PostgreSQL, and runs a machine learning model that predicts whether a stock will go up or down the day after a CEO posts.

---

## Quick Start

**1. Install dependencies**
```bash
pip install -r requirements.txt
```

**2. Create a `.env` file in the project root**
```env
DATABASE_URL=your_neon_postgres_connection_url
ALPACA_API_KEY=your_alpaca_api_key
ALPACA_SECRET_KEY=your_alpaca_secret_key
FINNHUB_API_KEY=your_finnhub_api_key
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_api_key
```

**3. Start the FastAPI backend** (Terminal 1)
```bash
uvicorn main:app --reload
```
- Runs at http://localhost:8000
- Interactive API docs at http://localhost:8000/docs

**4. Start the Streamlit dashboard** (Terminal 2)
```bash
streamlit run app.py
```
- Runs at http://localhost:8501

**5. Train the ML model** (one-time, or after new data is ingested)
```bash
python3 model/baseline.py
```
- Reads from the database, trains, and saves `model/trained_model.pkl`
- Must be run before the Predict tab in the UI will work

**6. Backfill FinBERT scores** (one-time, if you have existing NULL rows)
```bash
python3 model/backfill_finbert.py
```

**7. Run the standalone data pipeline** (daily use or historical backfill)
```bash
python3 run_pipeline.py           # daily — ~400 tweets per CEO
python3 run_pipeline.py --pages 50  # historical backfill — ~1000 tweets per CEO
```

**8. Run all tests**
```bash
python3 -m pytest tests/ -v
```

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | Neon PostgreSQL connection string |
| `ALPACA_API_KEY` | Yes | Alpaca Markets API key (stock data) |
| `ALPACA_SECRET_KEY` | Yes | Alpaca Markets secret key |
| `FINNHUB_API_KEY` | No | Finnhub news API key (fallback news source) |
| `ALPHA_VANTAGE_API_KEY` | No | Alpha Vantage key (primary news sentiment, 25 req/day free) |
| `API_BASE_URL` | No | Override for Streamlit to find the backend (default: http://localhost:8000) |

The app runs without Finnhub/Alpha Vantage — those fields will be NULL in the database.

---

## Languages & Technologies

### Languages
- **Python 3.11** — entire backend, ML pipeline, and Streamlit frontend
- **SQL** — Neon PostgreSQL, accessed through SQLAlchemy ORM; raw SQL only in `model/baseline.py` for the training query
- **HTML** — `index.html` served by FastAPI's root route

### Backend Framework
- **FastAPI** — REST API layer. Async-capable, auto-generates OpenAPI docs at `/docs`. Used with `uvicorn` as the ASGI server and `gunicorn` for production deployments.
- **SQLAlchemy** — ORM for all database interaction. Models are defined in Python classes; the engine handles table creation on startup.
- **Pydantic** (v2) — request/response schema validation for all API endpoints.

### Frontend
- **Streamlit** — dashboard UI. All data flows in via HTTP calls to the FastAPI backend — the UI imports no database libraries directly.
- **Plotly** — interactive charts (OHLCV candlesticks, ATR overlays, sentiment time series, scatter plots). Uses both `go.Figure` and `make_subplots`.

### Data & ML Libraries
- **pandas** — primary data structure throughout; DataFrames passed between every layer
- **numpy** — log transforms, exponential decay weights, NaN handling in the model pipeline
- **scikit-learn** — full ML pipeline: preprocessing (imputation, scaling, one-hot encoding), three classifiers, walk-forward CV, calibration
- **joblib** — model serialization (saves/loads `trained_model.pkl`)
- **vaderSentiment** — rule-based sentiment analyzer; fast, no GPU needed, great for social media text
- **transformers** (HuggingFace) — hosts `ProsusAI/finbert`, a BERT model fine-tuned on financial text; lazy-loaded to avoid slow startup
- **yfinance** — pulls VIX data and earnings dates; no API key required
- **alpaca-py** — official Alpaca Markets SDK for historical OHLCV stock bars
- **tweety-ns** — unofficial Twitter/X scraping library; session-based, no API key
- **textblob** — downloaded corpora used by GitHub Actions step; imported in legacy code
- **pytz** — timezone handling in the Streamlit dashboard

### External APIs
| API | Purpose | Rate Limit |
|-----|---------|------------|
| Twitter/X (tweety-ns) | CEO tweet fetching | Session-based, no hard limit |
| Alpaca Markets | Historical daily OHLCV bars | Generous on free tier |
| Alpha Vantage | News sentiment scores (primary) | 25 req/day free |
| Finnhub | Company news headlines (fallback) | 60 req/min free, ~1yr history |
| yfinance (Yahoo Finance) | VIX + earnings dates | Unofficial, no key |

### Infrastructure & Hosting
- **Neon** — serverless PostgreSQL. The database lives in the cloud; the app connects via a `DATABASE_URL` connection string. Neon auto-suspends when idle, so `pool_pre_ping=True` and `pool_recycle=300` are set to handle reconnects gracefully.
- **GitHub Actions** — CI/CD. A scheduled workflow (`.github/workflows/daily_retrain.yml`) runs at 8am EST on weekdays, fetches fresh data, retrains the model, and commits the updated `.pkl` file back to the repo automatically.
- **Local development** — FastAPI + Streamlit both run locally via terminal. There is no Docker setup; the two processes communicate over `localhost:8000`.

---

## Architecture

```
Twitter/X (tweety-ns)
Alpaca (OHLCV bars)
Alpha Vantage (news)         →   FastAPI backend   →   Neon PostgreSQL
Finnhub (news fallback)           (main.py)              (4 tables)
VIX + Earnings (yfinance)
                                       ↑ HTTP
                               Streamlit UI (app.py)
                               (no direct DB access)
```

The Streamlit UI is completely decoupled from the database — it calls FastAPI endpoints and renders what comes back. This is the "three-tier" architecture established in the major refactor (see Major Changes below).

---

## Major Changes & Product Evolution

This section documents the significant development milestones in the order they happened, based on the git history.

### 1. Monolith → Three-Tier Architecture

The original codebase was a single Jupyter notebook (`actualcapstone.ipynb`) and a Streamlit app that directly imported `DataProcessor` and hit the database inline. Every time a user clicked a button, the app was fetching tweets, querying the DB, and running classifiers all in the same Python process.

The three-tier refactor moved all data access and external API calls behind FastAPI. The Streamlit app now calls HTTP endpoints and renders responses — it doesn't import `processor.py` or `sqlalchemy` directly. This separation means:
- The backend can be deployed independently of the UI
- The API is documented and callable from anywhere (Postman, curl, other services)
- External Twitter/Alpaca calls fail gracefully and return structured `{"status": "error"}` responses instead of crashing the whole UI

### 2. Database Pipeline: Delete-All → Deduplication

The original `POST /process/all` wiped the entire `merged_data` table before re-inserting everything. This worked for initial loading but destroyed history if the pipeline partially failed.

`run_pipeline.py` was introduced as a standalone daily runner that **deduplicates** — it loads the set of tweet ISO timestamps already in the database for each CEO before fetching, and only inserts tweets it hasn't seen before. This means the database accumulates history over time rather than being reset on every run.

### 3. CEO Coverage Expansion

Started with 5 CEOs (Musk, Cook, Nadella, Pichai, Dell). Expanded to 26 total over multiple iterations:
- Added Andy Jassy, Brian Chesky, Dara Khosrowshahi, Robert Iger, Marc Benioff (replaced Zuckerberg whose account is protected)
- Added Jack Dorsey, Tobi Lütke, Brian Armstrong, Lisa Su, Eric Yuan
- Added Cathie Wood, Alex Karp, Mary Barra, Jim Farley, Anthony Noto, Reed Hastings, Pat Gelsinger, Aaron Levie, George Kurtz, Daniel Ek, RJ Scaringe
- Jensen Huang (`jensenhuang`) is commented out — his account returned too few tweets to be useful at the time

### 4. Dual Sentiment Scoring: VADER + FinBERT

Originally only VADER scored sentiment. VADER is a rule-based system tuned for social media — it handles sarcasm poorly and doesn't understand finance-specific language ("revenue miss", "guidance raised", "margin compression").

FinBERT (`ProsusAI/finbert`) was added as a second sentiment signal. It's a BERT model fine-tuned on financial text from Reuters and analyst reports. It understands the domain. The two scores (`sentiment_score` and `finbert_score`) are kept as separate features so the model can learn how much each one matters relative to the other.

FinBERT is lazy-loaded on first use (the `transformers` pipeline is expensive to initialize) and called in batches during ingestion to minimize runtime. A backfill script (`model/backfill_finbert.py`) was written to retroactively score all existing records that had NULL.

### 5. Market Context Signals Added

The original merge only had tweet text + sentiment + stock price. Several market context columns were added to the `merged_data` table to give the model information about what was happening in the broader market when each tweet landed:

- **VIX** (`vix_at_tweet`) — the CBOE Volatility Index on tweet day. A tweet from Musk during a VIX spike (market panic) probably has a different impact than the same tweet during a calm market. VIX is fetched from yfinance and falls back to the nearest prior day if the tweet landed on a weekend or holiday.
- **Days to earnings** (`days_to_earnings`) — how many calendar days until or since the company's most recent earnings date. Tweets land harder near earnings because investors are already paying close attention.
- **RSI (14-period)** (`rsi_at_tweet`) — Relative Strength Index computed on the stock's daily bars. Indicates whether the stock was overbought (>70) or oversold (<30) when the tweet came out. Two binary flags (`rsi_overbought`, `rsi_oversold`) derived from this.
- **ATR (14-period)** (`atr_at_tweet`) — Average True Range. How volatile the stock already was on the day of the tweet. A tweet dropping during an already volatile stretch has more context than one in a flat market.
- **News sentiment** (`news_sentiment_score`) — Average sentiment of news headlines for that ticker on tweet day. Alpha Vantage provides pre-computed per-ticker scores; Finnhub is the fallback with VADER applied to raw headlines. Results are cached to the `news_sentiment_cache` table so re-runs don't burn the 25 req/day AV limit.

### 6. Engagement Signals

Added four engagement count fields to every tweet record: `likes`, `retweet_count`, `view_count`, `reply_count`. Also derived:
- `tweet_hour` — UTC hour of posting
- `is_premarket` — 1 if posted before NYSE open (14:30 UTC). Pre-market tweets have more time to move prices before trading opens.

The `_safe_int()` helper was written specifically because tweety-ns returns the literal string `"Unavailable"` for some engagement counts rather than a number or None — without this guard, every upstream parse would throw an exception or silently zero out.

### 7. ML Model: From Simple Classifier → Walk-Forward CV with Calibration

The first iteration of the model was a single logistic regression with an 80/20 train/test split. Problems with that approach:
- A single split is unstable — one hard market period (like a crash or rally) in the test window can swing reported accuracy by several points in either direction
- The model had no way to express how confident it was — it just output a class label

Replacements and additions:
- **Walk-forward cross-validation** (`TimeSeriesSplit`, 5 folds) — each fold trains on all data before its test window. No lookahead. Model selection uses CV mean accuracy across all 5 folds, not any single window.
- **Three competing models** — Logistic Regression, Random Forest (200 trees), Gradient Boosting (200 estimators) — best CV mean wins
- **Exponential decay sample weights** (half-life 180 days) — tweets from 6 months ago carry 50% the weight of today's tweets. The model adapts to how CEOs tweet now, not two years ago. Combined with class-balance weights so Up/Down classes stay balanced during training.
- **Probability calibration** (`CalibratedClassifierCV`, isotonic regression) — raw model probability scores are often overconfident or underconfident. Calibration maps them to true probabilities. The confidence percentage shown in the UI is meaningful: "70% confident" should be right ~70% of the time.
- **Confidence bucket analysis** — printed at the end of training to verify calibration actually worked. Shows accuracy broken down by confidence range (50-55%, 55-60%, etc.).

### 8. Automated Daily Pipeline (GitHub Actions)

GitHub Actions was configured to run the full pipeline automatically every weekday at 8am EST. The workflow:
1. Checks out the repo
2. Installs Python 3.11 and all dependencies
3. Downloads TextBlob corpora (needed for some classifiers)
4. Runs `run_pipeline.py` — fetches new tweets + stocks + news, deduplicates, writes to Neon
5. Runs `model/baseline.py` — retrains on fresh data
6. Commits the updated `model/trained_model.pkl` back to the repo with a date-stamped commit message

Several fixes were required to make this work reliably:
- SQLAlchemy requires `postgresql://` but Neon/Heroku connection strings often start with `postgres://` — a normalization step was added
- GitHub Secrets sometimes get stored as `DATABASE_URL=postgresql://...` with the key included — a strip step was added
- The workflow needed explicit `contents: write` permission to push the retrained model back to the repo
- scikit-learn 1.8 removed `fit_params` from `cross_val_score` — updated to the new API

### 9. Analysis Endpoints

Three new read-only API endpoints were added to power chart widgets in the dashboard without requiring the UI to run its own data aggregation:

- `GET /api/analysis/price-swing/{ceo}/{ticker}` — average absolute price swing on tweet days, plus next-day direction split (% up vs % down)
- `GET /api/analysis/tweet-impact/{ceo}/{ticker}` — per-tweet "impact score" (50% from sentiment magnitude + 50% from log-normalized engagement reach), returned with next-day direction for scatter plot visualization
- `GET /api/analysis/post-tweet-trend/{ceo}/{ticker}` — next-day direction bucketed by five sentiment ranges (Very Negative through Very Positive), with up/down counts and percentages for each bucket

### 10. UI Cleanup

Several rounds of UI simplification:
- Removed technical jargon and glossary expanders — terms like "ATR", "RSI", and "FinBERT" were explained inline rather than buried in expandable sections
- Moved query parameters (CEO selector, ticker, date range) from the main content area into the sidebar
- Applied a consistent dark theme across all tabs (`#12161f` background, gradient red/orange header, navy tab bar)

---

## File-by-File Breakdown

### `main.py` — FastAPI Backend

The central hub. Handles database setup, defines all ORM models and Pydantic schemas, and hosts every API endpoint.

On startup: creates all 4 tables if they don't exist, then runs a safe `ALTER TABLE merged_data ADD COLUMN finbert_score FLOAT` (silently skipped if the column already exists).

### `processor.py` — Data Fetcher

`DataProcessor` class with two public methods:

**`get_tweets(username, pages=50)`** — fetches CEO tweets via tweety-ns, skips retweets, extracts engagement signals, computes `is_premarket` flag (NYSE opens 14:30 UTC), scores VADER sentiment per tweet, then runs FinBERT batch scoring across the full set. Contains a runtime monkey-patch at module load time that intercepts a specific tweety-ns error (`"Couldn't get animation key indices"`) and returns a safe default rather than crashing the whole fetch.

**`get_stocks(symbol, start_date, end_date)`** — fetches daily OHLCV bars from Alpaca for any ticker. Returns a DataFrame indexed by timestamp.

### `classifier.py` — Sentiment & Classification

All text annotation. No network calls, no GPU required at import time.

- `get_sentiment_score(text)` — VADER compound score, [-1, 1]
- `get_finbert_score(text)` — single-text FinBERT score, lazy-loaded
- `get_finbert_scores_batch(texts)` — batch FinBERT, much faster than one-by-one
- `get_refined_sentiment(score)` → Very Negative / Negative / Neutral / Positive / Very Positive
- `get_tone_category(text, score)` → keyword-matched: Emotional (Joyful/Excited), Emotional (Angry/Frustrated), Informational (Promotional/Update), Informational (Mixed), General Commentary
- `get_tweet_type(text)` → keyword-matched: Poll/Vote, Discussion Starter, Company Milestone, Personal/General Commentary

### `context.py` — Market Context

External enrichment sources. Two news APIs with fallback logic, sector ETF mapping, earnings dates, VIX lookup builder.

`build_news_sentiment_lookup()` is the main entry point used by the pipeline — makes one AV call per ticker over the full tweet date range (`sort=EARLIEST` so the 1000-article cap covers oldest history first), caches results to DB, falls back to a single Finnhub call if AV is unavailable.

### `app.py` — Streamlit Dashboard

Dark-themed multi-tab dashboard. Calls the FastAPI backend via `api_get()` / `api_post()` HTTP helpers — no direct database or external API access. Renders Plotly charts for tweet sentiment, stock prices, ATR overlays, engagement breakdowns, and model predictions.

### `run_pipeline.py` — Standalone CLI Pipeline

Used by GitHub Actions for daily ingestion. Unlike `POST /process/all`, it never clears existing records — it deduplicates by loading existing tweet timestamps from the DB before inserting. Accepts `--pages` (default 20, use 50-100 for historical backfill). Validates and normalizes `DATABASE_URL` format early before any imports that might fail silently.

### `model/baseline.py` — ML Training Script

Loads all labeled rows from `merged_data`, engineers features, trains three classifiers with walk-forward CV, calibrates the best-performing model, and saves it to `model/trained_model.pkl`. Run manually or triggered by GitHub Actions. Full output includes per-fold accuracy, confusion matrices, feature importances, confidence bucket analysis.

### `model/predict.py` — Inference

Loads `trained_model.pkl` and exposes `predict_tweets(tweets_df, stocks_df, ticker)`. Builds the exact same feature vector that was used during training — including VIX lookups and earnings date proximity. Returns the original DataFrame with two new columns: `predicted_direction` ("Up" or "Down") and `confidence_pct` (e.g. 64.2).

### `model/backfill_finbert.py` — One-Time Backfill

Fetches all `merged_data` rows where `finbert_score IS NULL`, runs FinBERT in batches of 64, updates in place. Run once after adding FinBERT to an existing database.

### `model/trained_model.pkl`

The serialized calibrated classifier. Committed to git and automatically updated every weekday by GitHub Actions.

---

## Database Schema

Four tables (auto-created by SQLAlchemy on startup):

**`tweets`** — raw tweet storage
| Column | Type |
|--------|------|
| id | Integer PK |
| date | String (ISO timestamp) |
| ceo | String (Twitter handle) |
| text | String |
| sentiment_score | Float |
| refined_sentiment | String |

**`stocks`** — raw OHLCV bars
| Column | Type |
|--------|------|
| id | Integer PK |
| symbol | String |
| timestamp | String |
| open / high / low / close | Float |
| volume | Float |

**`news_sentiment_cache`** — API call cache (ticker + date as composite PK)
| Column | Type |
|--------|------|
| ticker | String PK |
| date_str | String PK (YYYY-MM-DD) |
| sentiment_score | Float |

**`merged_data`** — main analysis table (20+ columns)
| Column | Type | Notes |
|--------|------|-------|
| id | Integer PK | |
| date | String | Tweet timestamp |
| ceo | String | Twitter handle |
| tweet_text | String | |
| sentiment_score | Float | VADER [-1, 1] |
| refined_sentiment | String | One of 5 labels |
| tone_category | String | One of 5 labels |
| tweet_type | String | One of 4 labels |
| stock_ticker | String | |
| stock_close | Float | Close on tweet day (or next Monday if weekend) |
| stock_volume | Float | |
| stock_open_close_diff | Float | open - close |
| likes | Integer | |
| retweet_count | Integer | |
| view_count | Integer | |
| reply_count | Integer | |
| tweet_hour | Integer | UTC hour |
| is_premarket | Integer | 0 or 1 |
| next_day_direction | Integer | 1=up, 0=down, NULL=no data yet |
| rsi_at_tweet | Float | 14-period RSI |
| atr_at_tweet | Float | 14-period ATR |
| news_sentiment_score | Float | Avg headline sentiment on tweet day |
| finbert_score | Float | FinBERT financial sentiment [-1, 1] |
| vix_at_tweet | Float | VIX close on tweet day |
| days_to_earnings | Integer | Calendar days to nearest earnings date |

---

## API Endpoints

All responses: `{"status": "success"|"error", ...}`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Serves `index.html` |
| `GET` | `/api/ceos` | All 26 CEOs with handle, name, ticker |
| `GET` | `/api/tweets/{ceo}` | Live-fetch tweets from Twitter + classify |
| `GET` | `/api/stocks/{ticker}` | Live-fetch OHLCV from Alpaca |
| `GET` | `/api/merged/{ceo}/{ticker}` | Live-fetch + merge (no DB read) |
| `GET` | `/api/merged` | DB read with filters: ceo, ticker, start_date, end_date, limit |
| `GET` | `/api/merged/summary` | DB stats: record count, CEO list, date range |
| `POST` | `/process/all` | Full pipeline for all 26 CEOs, saves to DB (clears first) |
| `POST` | `/api/predict` | Single-tweet prediction via trained model |
| `POST` | `/ingest/tweets` | Manual tweet ingestion |
| `POST` | `/ingest/stocks` | Manual stock ingestion |
| `GET` | `/api/analysis/price-swing/{ceo}/{ticker}` | Avg price swing + direction split |
| `GET` | `/api/analysis/tweet-impact/{ceo}/{ticker}` | Per-tweet impact scores |
| `GET` | `/api/analysis/post-tweet-trend/{ceo}/{ticker}` | Direction split bucketed by sentiment |

---

## CEO Coverage (26 total)

| Handle | Name | Ticker |
|--------|------|--------|
| elonmusk | Elon Musk | TSLA |
| tim_cook | Tim Cook | AAPL |
| satyanadella | Satya Nadella | MSFT |
| sundarpichai | Sundar Pichai | GOOGL |
| MichaelDell | Michael Dell | DELL |
| LisaSu | Lisa Su | AMD |
| ajassy | Andy Jassy | AMZN |
| bchesky | Brian Chesky | ABNB |
| dkhos | Dara Khosrowshahi | UBER |
| RobertIger | Robert Iger | DIS |
| Benioff | Marc Benioff | CRM |
| jack | Jack Dorsey | SQ |
| tobi | Tobi Lütke | SHOP |
| brian_armstrong | Brian Armstrong | COIN |
| ericyuan | Eric Yuan | ZM |
| CathieDWood | Cathie Wood | ARKK |
| AlexKarp | Alex Karp | PLTR |
| mtbarra | Mary Barra | GM |
| JimFarley98 | Jim Farley | F |
| AnthonyNoto | Anthony Noto | SOFI |
| reedhastings | Reed Hastings | NFLX |
| PGelsinger | Pat Gelsinger | INTC |
| levie | Aaron Levie | BOX |
| george_kurtz | George Kurtz | CRWD |
| eldsjal | Daniel Ek | SPOT |
| RJScaringe | RJ Scaringe | RIVN |

---

## Predictive Modeling — Deep Dive

### What the model predicts

Binary classification: will the stock close **higher** or **lower** the day after a CEO tweets? `next_day_direction = 1` (Up) or `0` (Down). This label is computed at ingestion time by comparing the closing price on the tweet day to the closing price on the following trading day.

### Features (23 total)

**Sentiment signals**
- `sentiment_score` — VADER compound score in [-1, 1]. Rule-based, tuned for social media. Fast.
- `sentiment_magnitude` — `abs(sentiment_score)`. Captures extreme tweets regardless of direction. A tweet at -0.9 and one at +0.9 both carry strong signal even though they point opposite ways.
- `finbert_score` — FinBERT score in [-1, 1]. Domain-trained on financial text. More reliable than VADER for earnings language, analyst references, product announcements. Kept as a separate feature rather than replacing VADER.

**Tweet substance**
- `tweet_length` — character count. Longer tweets tend to be more deliberate.
- `word_count` — word count. Dense tweets carry more information.

**Engagement signals (all log-transformed)**
- `log_likes`, `log_retweets`, `log_views`, `log_replies` — raw engagement counts are heavily right-skewed (one viral tweet can have 10M views, most have 10K). `log1p` compresses the scale so the outliers don't dominate.
- `engagement_rate` — `(likes + retweets + replies) / max(views, 1)`. Normalizes for audience size differences across CEOs. Musk has 200M followers; most other CEOs have 1-5M. Engagement rate makes their numbers comparable.

**Timing signals**
- `tweet_hour` — UTC hour of posting. Pre-market tweets have more time to move prices. After-hours tweets reach investors before next-day open.
- `is_premarket` — binary flag (1 if posted before 14:30 UTC / NYSE open).

**Technical indicators**
- `rsi_at_tweet` — 14-period RSI on tweet day. Momentum indicator. The model can learn that a tweet during an overbought stock (RSI > 70) has a different impact than the same tweet during an oversold stock.
- `atr_at_tweet` — 14-period Average True Range. How much the stock was already moving before the tweet landed. A company in a volatile stretch will move more on a tweet than one in a quiet period.
- `rsi_overbought` — binary flag (RSI > 70). Discrete regime signal in addition to the raw value.
- `rsi_oversold` — binary flag (RSI < 30).

**Market context**
- `vix_at_tweet` — VIX close on tweet day. Market-wide fear level. High VIX means everything is volatile and individual tweet signal is noisier. Falls back to the nearest prior trading day if tweet landed on a weekend or holiday.
- `days_to_earnings` — calendar days to the nearest earnings date. Tweets during earnings season land in a more attentive market.
- `prev_day_direction` — was the stock up or down yesterday? Simple momentum feature. Stocks that went up yesterday are more likely to continue up (short-term momentum).
- `news_sentiment_score` — average headline sentiment for the ticker on tweet day. Separates tweet signal from confounding news events.

**Categorical (one-hot encoded)**
- `refined_sentiment` — 5 labels (Very Negative through Very Positive)
- `tone_category` — 5 labels (Emotional Joyful, Emotional Angry, Informational Promotional, Informational Mixed, General Commentary)
- `tweet_type` — 4 labels (Poll/Vote, Discussion Starter, Company Milestone, Personal/General Commentary)

### Three competing models

**Logistic Regression** — linear, interpretable, scaled features (StandardScaler applied). Good baseline. Class-balanced (`class_weight='balanced'`).

**Random Forest** — 200 trees, unscaled features (trees don't need scaling), `class_weight='balanced'`. Non-linear, handles feature interactions, gives feature importances. Generally more stable than logistic regression on tabular data.

**Gradient Boosting** — 200 estimators, learning rate 0.05, max depth 4, 80% subsampling. Typically strongest on tabular data. Doesn't natively support `class_weight=`, so class balance is handled through sample weights instead.

### Training strategy

**Walk-forward cross-validation** (`TimeSeriesSplit`, 5 folds). Each fold trains on all data before its test window — earlier tweets train, later tweets test. This mirrors real deployment: you always predict forward in time, never backward. Standard k-fold would let future data leak into training folds and report inflated accuracy.

**Sample weights — exponential decay.** Tweets are not all equally relevant. A tweet from 2022 reflects how Musk was posting during a different market regime than 2024. The decay formula gives each tweet a weight of `exp(-ln(2)/180 * days_ago)` — so a tweet from 180 days ago has half the weight of a tweet from today. These weights are combined with class-balance weights so Gradient Boosting (which doesn't support `class_weight=`) gets the same Up/Down balance that LR and RF get via `class_weight='balanced'`.

**Model selection.** The model with the highest mean CV accuracy across all 5 folds is saved. A final 80/20 holdout split is printed for human review (confusion matrix, classification report, feature importances), but does not influence which model is picked.

### Probability calibration

Raw classifiers output probability estimates, but those estimates are often overconfident or underconfident. A random forest that says "75% confident" might actually be right only 60% of the time.

`CalibratedClassifierCV` with isotonic regression is applied to the winning model — it remaps the raw probability scores to true probabilities using the same `TimeSeriesSplit` folds, so the calibration also respects time ordering. After calibration, the printed confidence bucket analysis shows whether "70% confident" predictions actually come true at a 70% rate in the holdout set.

### Preprocessing pipeline

Two scikit-learn pipelines are defined (one scaled, one unscaled):
- **Numeric features** → `SimpleImputer(strategy='median')` to fill NULLs, then `StandardScaler` for LR or raw for tree models
- **Categorical features** → `OneHotEncoder(handle_unknown='ignore')` — unknown labels at inference time are silently treated as zero rather than throwing an error

### Prediction at inference time

`model/predict.py` builds the exact same feature vector from a live tweet row. It fetches VIX from yfinance (cached by month), looks up earnings dates (cached by ticker), applies the same weekend-to-Monday shift, and computes RSI flags from whatever stock data is available. Missing values for technical indicators are None, which `SimpleImputer` fills with the training-set median at prediction time.

---

## Tests — Deep Dive

The test suite has **6 test files** and **~80 individual test cases** across the following areas. All tests run with no network access and no credentials — everything external is mocked.

```bash
python3 -m pytest tests/ -v        # run everything
python3 -m pytest tests/test_classifier.py -v
python3 -m pytest tests/test_api.py -v
python3 -m pytest tests/test_sentiment_score.py -v
python3 -m pytest tests/test_processor.py -v
python3 -m pytest tests/test_context.py -v
python3 -m pytest tests/test_predict.py -v
```

---

### `test_sentiment_score.py` — VADER Scoring (10 tests)

Tests `get_sentiment_score()` in isolation. This function runs on every single tweet in the pipeline — a regression here silently corrupts every sentiment value written to the database.

- **Return type** — always returns a Python `float`
- **Range** — VADER's compound score is always in `[-1.0, 1.0]`. Tested against four different inputs including empty string and a tweet with emoji/hashtags
- **Polarity direction** — a strongly positive tweet scores positive, strongly negative scores negative
- **Neutral text** — a factual statement with no emotional language should score within `[-0.3, 0.3]`
- **Magnitude ordering** — a very enthusiastic tweet should score higher than a mildly positive one; a catastrophic tweet should score lower than a mildly negative one
- **Edge cases** — empty string, whitespace-only string, and a tweet with emoji + hashtags + dollar signs all return a float without crashing

---

### `test_classifier.py` — Classification Logic (27 tests)

Tests the three label functions in `classifier.py` — no external dependencies.

**`get_refined_sentiment` (12 tests)**
- Exact boundary values: `0.6` → "Very Positive", `0.2` → "Positive", `-0.2` (exclusive) → "Neutral", `-0.6` → "Very Negative"
- The boundaries in the code use `<` vs `<=` inconsistently; boundary tests catch any regressions in which side is inclusive
- `1.0` (max) → "Very Positive", `-1.0` (min) → "Very Negative"
- Sweep of 11 evenly-spaced values across the full range — every one must return one of the 5 known labels with no gaps

**`get_tone_category` (8 tests)**
- Returns a non-empty string
- Result is always one of the 5 known tone labels
- "excited" keyword + positive score → "Emotional (Joyful/Excited)"
- "angry" keyword + negative score → "Emotional (Angry/Frustrated)"
- "announcement" / "launch" keywords → "Informational (Promotional/Update)"
- No keywords → "General Commentary" (fallback)
- Both emotional and informational keywords + positive score → "Emotional (Joyful/Excited)" (emotional wins)
- Both emotional and informational keywords + negative score → "Informational (Mixed)"

**`get_tweet_type` (7 tests)**
- Returns a non-empty string
- Result is always one of the 4 known type labels
- "vote" → "Poll/Vote", "thoughts" → "Discussion Starter", "launch" → "Company Milestone"
- No keywords → "Personal/General Commentary" (fallback)
- Case insensitivity: "VOTE NOW!" and "LAUNCH EVENT" both match correctly

---

### `test_api.py` — API Endpoints (18 tests)

Tests all three live-fetch API endpoints using FastAPI's `TestClient`. Twitter (`proc.get_tweets`) and Alpaca (`proc.get_stocks`) are mocked with realistic sample DataFrames — no network needed, no credentials.

**`GET /api/tweets/{ceo}` (6 tests)**
- HTTP 200 + `status: "success"` on a normal response
- `data` field is a list with one item per tweet
- Every record has `date`, `text`, and `sentiment` fields
- Sentiment scores are always in `[-1.0, 1.0]`
- Unknown CEO returns `{"status": "success", "data": []}` — not a crash
- If Twitter raises a `RuntimeError`, response still has `status: "error"` (structured failure)

**`GET /api/stocks/{ticker}` (6 tests)**
- HTTP 200 + `status: "success"`
- Every bar has all 5 OHLCV fields: `open`, `high`, `low`, `close`, `volume`
- All price fields are `> 0`
- `high >= low` on every bar — an impossible bar means corrupted data
- `volume >= 0` — volume can be zero (halted stock) but never negative
- Unknown ticker returns `{"status": "success", "data": []}` — not a crash

**`GET /api/merged/{ceo}/{ticker}` (6 tests)**
- HTTP 200 + `status: "success"`
- Every record has all 10 required merge fields
- Sentiment scores stay in `[-1.0, 1.0]`
- `refined_sentiment` is always one of the 5 known labels
- `stock_close > 0` whenever a stock bar was matched
- `ceo` field in every record matches the handle passed in the URL
- Empty tweet set returns `{"status": "success", "data": []}` — not a crash

---

### `test_processor.py` — Engagement Parsing (12 tests)

Tests `DataProcessor._safe_int()` — the static method that converts raw engagement counts from tweety-ns into integers. This is called on every tweet's likes, retweets, views, and replies. A broken parser silently zeros out all engagement features for every record.

tweety-ns doesn't consistently return numbers — it sometimes returns `None`, `"Unavailable"`, `"N/A"`, `"--"`, or float strings like `"1.5"`.

- Normal integer → passes through
- Integer string `"1500"` → converts correctly
- `None` → 0
- Empty string `""` → 0
- Literal string `"Unavailable"` → 0 (the specific value tweety-ns returns for some fields)
- `0` and `"0"` → 0
- Float `3.9` → 3 (truncates, doesn't crash)
- Float string `"1.5"` → 0 (can't be passed to `int()` directly — must return 0, not raise)
- Arbitrary non-numeric strings `"N/A"`, `"--"` → 0
- Large number `50_000_000` → passes through (viral tweets can have tens of millions of views)
- Return type is always `int` (not float, not None)

---

### `test_context.py` — News Sentiment Lookups (24 tests)

Tests `context.py` — the news sentiment API layer. All HTTP calls are mocked. The goal is to catch bugs in the parsing logic before they burn the 25 req/day Alpha Vantage quota or silently return zero coverage for all tweet dates.

**`get_sector_etf` (4 tests)**
- Tech tickers (AAPL, MSFT, GOOGL, DELL, AMD, NVDA, META) all map to XLK
- Consumer tickers (TSLA, AMZN, ABNB, UBER, DIS) all map to XLY
- Unknown ticker falls back to SPY
- Lowercase input works — ticker normalization is case-insensitive

**`build_news_sentiment_lookup` (6 tests)**
- Empty tweet dates → empty dict (no API calls made)
- Empty set → empty dict
- Uses AV result when available, doesn't call Finnhub
- Falls back to Finnhub when AV returns empty
- Skips AV entirely when `_AV_KEY` is None (no env var set)
- Returns empty dict when both keys are missing
- Calls AV with `start_date=min(tweet_dates)` and `end_date=max(tweet_dates)` — verifies the request window is the exact tweet date range, not something arbitrary

**`_av_news_sentiment_lookup` (8 tests)**
- Returns empty dict when no AV key is configured
- Parses per-ticker sentiment scores and averages them correctly (0.6 + 0.4 = 0.5)
- Falls back to `overall_sentiment_score` when no per-ticker entry exists for the queried ticker
- Groups multiple articles by date correctly (two different days → two entries)
- Returns empty dict on AV `"Note"` field (rate limit exceeded)
- Returns empty dict on AV `"Information"` field (invalid API key)
- Returns empty dict on HTTP 500
- Returns empty dict on empty feed
- Skips articles with malformed `time_published` timestamps (less than 8 chars) while still processing valid articles in the same response

**`_finnhub_bulk_lookup` (6 tests)**
- Returns empty dict when no Finnhub key is configured
- Parses headlines, applies sentiment function, averages result (0.6 + 0.4 = 0.5)
- Skips articles with empty headline, missing headline key — only the valid headline gets scored
- Returns empty dict on HTTP 429 (rate limit)
- Returns empty dict on empty article list
- Multiple articles on the same day are averaged correctly

---

### `test_predict.py` — ML Feature Contract (22 tests)

Tests `model/predict.py`, specifically `_build_feature_row()`. The critical invariant this file enforces: **the feature vector produced at inference time must exactly match what the model was trained on**. A mismatch causes scikit-learn to silently predict on wrong or missing values — no exception is raised, just bad numbers.

**Feature contract (4 tests)**
- All 23 expected features are present in the returned dict
- No extra features are returned — extra keys would be silently dropped by sklearn but indicate drift between training and serving code
- All numeric features are `float`, `int`, or `None` (never a string or DataFrame)
- All categorical features are strings (for the OneHotEncoder)

**Weekend date shifting (3 tests)**
- Saturday tweet (2024-01-06) → shifted to Monday (2024-01-08) → picks up Monday's RSI from stocks_df
- Sunday tweet (2024-01-07) → shifted to Monday (2024-01-08) → same
- Friday tweet (2024-01-05) → not shifted → picks up Friday's RSI

This mirrors the same shift logic in `run_pipeline.py` at ingestion time. If either side drifts, tweets will be trained on stock data from one day but predicted against stock data from a different day.

**RSI zone flags (4 tests)**
- RSI 75.0 → `rsi_overbought=1`, `rsi_oversold=0`
- RSI 25.0 → `rsi_oversold=1`, `rsi_overbought=0`
- RSI 50.0 → both flags 0
- NaN RSI → fills to 50 for flag computation (neither fires), `rsi_at_tweet` stored as `None`

**Engagement features (4 tests)**
- `engagement_rate = (likes + retweets + replies) / views` — formula verified numerically
- `view_count=0` → denominator clamped to 1, no division by zero
- All log features (`log_likes`, `log_retweets`, etc.) are `>= 0` even when counts are zero (`log1p(0) = 0`)
- Sentiment magnitude of `-0.7` == sentiment magnitude of `0.7` (absolute value confirmed for both positive and negative inputs)

**Empty stocks (2 tests)**
- When `stocks_df` is empty (no stock data available for that ticker/date), `rsi_at_tweet`, `atr_at_tweet`, and `prev_day_direction` are all `None` — `SimpleImputer` fills them at prediction time
- All 23 features are still present even with empty stocks — no KeyError at inference time

**`prev_day_direction` (3 tests)**
- Three rows in stocks (oldest=180, middle=190, tweet day=192) → previous day went up → `1`
- Three rows (oldest=190, middle=180, tweet day=178) → previous day went down → `0`
- Only one prior row → can't compute direction → `None`

**Error handling (1 test)**
- `predict_tweets()` raises `FileNotFoundError` with a message containing "No trained model found" when `trained_model.pkl` doesn't exist — instead of returning garbage predictions

---

## Automation — GitHub Actions

`.github/workflows/daily_retrain.yml` triggers at 8am EST on every weekday (cron: `0 13 * * 1-5`) and on manual dispatch.

Steps:
1. Checkout repo with `GITHUB_TOKEN` (needs `contents: write` permission to push back)
2. Python 3.11 setup with pip dependency caching
3. Install from `requirements.txt`
4. Download TextBlob corpora
5. Debug step: prints DATABASE_URL prefix for diagnosing secret-format issues
6. Run `python3 run_pipeline.py` — fetches new tweets + stocks + news, deduplicates, writes to Neon
7. Run `python3 model/baseline.py` — retrains on full database
8. Commit updated `model/trained_model.pkl` with message `chore: retrain model [YYYY-MM-DD]`

Required GitHub secrets: `DATABASE_URL`, `ALPACA_API_KEY`, `ALPACA_SECRET_KEY`, `FINNHUB_API_KEY`, `ALPHA_VANTAGE_API_KEY`.

---

## Other Files

- `index.html` — Served by `GET /`. Landing page for the FastAPI backend.
- `actualcapstone.ipynb` — Exploratory notebook from early development. Not part of the active app.
- `cookies.json` / `session.tw_session` — tweety-ns session state. Auto-managed by the library.
- `streamlit.log` — Streamlit runtime output.
- `MODEL_EXPLAINED.md` — Plain-English writeup of how the prediction model works.
- `SESSION_REPORT.md`, `TEST_COVERAGE.md` — Development documentation.
