# MoneyMaker

**Turn public market signals into automated paper trades.**

MoneyMaker started by asking whether CEO tweets move stock prices — it collects tweets from high-profile CEOs, scores them with two NLP models (VADER + FinBERT), pairs each with stock and market context, and trains a model to predict next-day direction. It has since grown into a multi-source trading-signal pipeline: alongside CEO sentiment it ingests **congressional trade disclosures** (official House/Senate filings), **short-seller reports**, and **policy/macro accounts**, then a continuous **watcher** evaluates each signal and places Alpaca **paper trades** with conviction-based sizing, portfolio risk caps, and scheduled next-day exits.

**[Live demo →](https://moneymaker-ddahlgren.streamlit.app)** &nbsp;|&nbsp; Built by [Dillon Dahlgren](https://github.com/ddahlgren11)

---

## Tech Stack

| Layer | Technologies |
|-------|-------------|
| Language | Python 3.11 |
| Backend API | FastAPI, SQLAlchemy (ORM), Pydantic v2, Uvicorn |
| Frontend | Streamlit, Plotly |
| ML / NLP | scikit-learn, VADER, FinBERT (HuggingFace `ProsusAI/finbert`), pandas, numpy |
| Trading / execution | Alpaca paper trading, conviction-based sizing, portfolio risk caps, scheduled exits |
| Database | Neon (serverless PostgreSQL), psycopg2 |
| Data sources | Twitter/X (free syndication endpoint, twikit fallback), SEC EDGAR (Form 4 insiders), Financial Modeling Prep (congressional disclosures), Reddit (praw), Alpaca Markets, Alpha Vantage, Finnhub, yfinance |
| CI/CD | GitHub Actions — daily retrain, intraday market watcher, congressional ingest |
| Hosting | Streamlit Community Cloud; watcher worker on Render |

---

## Architecture

The project has three operating modes:

**Data + model pipeline** — ingestion and training:
```
Twitter/X · FMP (congress) · Alpaca · Alpha Vantage · Finnhub · yfinance
        ↓
  run_pipeline.py / congress_ingest.py  →  Neon PostgreSQL  →  model/baseline.py (retrain)
```

**Live trading loop** — `watch.py` runs continuously (Render worker) or per-cycle (GitHub Actions, `--db-only`):
```
new signal (tweet topic | congressional disclosure | short-seller report)
        ↓
classify → registry/ML or fast-path → confidence gate → conviction sizing → risk caps
        ↓
Alpaca paper order  →  managed_positions (scheduled next-day exit)  →  paper_trades log
```

**Dashboard** — Streamlit connects directly to the database, stock data via yfinance:
```
Neon PostgreSQL  ←→  Streamlit (app.py)  ←→  yfinance
```

---

## Key Engineering Highlights

- **Walk-forward cross-validation** — uses `TimeSeriesSplit` (5 folds) so each fold trains on all data before its test window. No lookahead. Standard k-fold would leak future data and report inflated accuracy.

- **Three competing models** — Logistic Regression, Random Forest (200 trees), and Gradient Boosting (200 estimators) are trained and evaluated; the best CV mean accuracy wins. Each gets appropriate scaling and class-balance treatment.

- **Calibrated probability estimates** — `CalibratedClassifierCV` with isotonic regression maps raw model scores to true probabilities. The confidence % shown in the UI is meaningful: a "70% confident" prediction should be correct ~70% of the time.

- **Exponential decay sample weights** (half-life 180 days) — tweets from 6 months ago carry 50% the weight of today's tweets. Keeps the model current without discarding historical signal.

- **23-feature input vector** spanning sentiment (VADER + FinBERT), engagement (likes/retweets/views, log-transformed), timing (tweet hour, pre-market flag), technicals (RSI, ATR), and market context (VIX, days to earnings, prior-day news sentiment).

- **Deduplicating daily pipeline** — `run_pipeline.py` loads existing tweet timestamps from the DB before fetching and only inserts new records. History accumulates over time; a partial failure doesn't wipe data.

- **Automated daily retraining** — GitHub Actions runs at 8am EST on weekdays, ingests fresh data, retrains the model, and commits the updated `.pkl` back to the repo with a date-stamped message.

- **Market-regime gate** — a top-level trend (SPY vs 200-day SMA) + volatility (VIX) overlay that gates long entries and scales position size by regime confidence, implementing the two-stage "gate over ranker" design from the research brief. Disabled until `backtest.py` shows it cuts drawdown ~25–30% out-of-sample.

- **178-test suite** across 14 files — all external dependencies (Twitter/syndication, SEC EDGAR, Reddit, Alpaca, Alpha Vantage, Finnhub) are mocked. Tests cover sentiment scoring, API response schemas, engagement parsing edge cases, ML feature contract enforcement, inference-time correctness, the Form 4 parser, Reddit ticker/spike logic, event-study return math, regime-gate states, sector reactivity, and the bot/pump-dump risk filters.

---

## Trading Signals & Execution

The watcher (`watch.py`) turns signals into Alpaca paper trades. It runs continuously as a Render worker (live Twitter mode) and per-cycle in GitHub Actions (`--db-only`, reads `merged_data`).

**Signal sources**

| Source | Path | Direction logic |
|--------|------|-----------------|
| CEO sentiment tweets | topic classify → relationship registry → ML model → ≥55% confidence gate | model prediction |
| Congressional trades | `congress_ingest.py` (FMP) → `congress_trades` → fast-path | Purchase → Up, Sale → Down |
| SEC Form 4 insider trades | `insider_ingest.py` (EDGAR) → `insider_trades` → fast-path | Buy (P) → Up, Sale (S) → Down |
| Short-seller reports | `_SHORT_SELLER_HANDLES` tweet → fast-path | report → Down |
| Policy / macro accounts | Trump / POTUS / Treasury → `policy` topic → registry/ML | sector-ETF mapping |
| Reddit spikes *(experimental)* | `reddit_ingest.py` → `reddit_signals` → fast-path | mention spike + sentiment lean; **trading off by default** |

Congressional and policy posts are exempt from the sentiment gate (they're factual, not opinionated); congressional and short-seller signals skip the ML model entirely since the ticker and direction are explicit.

**Execution controls**

- **Conviction-based sizing** — trade notional scales between `MIN_NOTIONAL` and `MAX_NOTIONAL` by a blend of model confidence and relationship tightness.
- **Portfolio risk caps** — `MAX_OPEN_POSITIONS` limit and a `MAX_DAILY_LOSS` kill switch (Alpaca equity vs. prior close) block new entries.
- **Scheduled exits** — every entry is recorded in `managed_positions` and closed at the next trading day's close (the model's prediction horizon), not left open until a reversing signal.
- **Idempotency** — a signal is never traded twice (guards retries and the Render-worker + GitHub-Actions overlap).
- **Market-hours aware** — signals found outside market hours are queued in `signal_queue` and executed at the next open.
- **Regime gate (longs-only, opt-in)** — when `REGIME_GATE_ENABLED=true`, long entries fire only when SPY is above its 200-day SMA (and VIX isn't in a crisis regime), with size scaled by regime confidence; shorts are ungated. Off by default until validated by `backtest.py`.

The relationship registry (`ceo_ticker_relationships`, built by `relationship_analysis.py`) scores each (account, topic, ticker) link by directional hit rate, statistical significance, and volatility amplification into a `tightness_score`, which gates and sizes the tweet-based trades.

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

Tables are auto-created by SQLAlchemy on startup and by the watcher/ingester on first run.

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

**Operational tables** (auto-created by the watcher / ingester):

| Table | Purpose |
|-------|---------|
| `paper_trades` | Log of every placed / skipped / errored / exit trade |
| `watcher_state` | Per-account last-seen-tweet watermark and counters |
| `insider_trades` | SEC Form 4 corporate-insider filings from `insider_ingest.py` (ticker, P/S, shares, price, role) |
| `reddit_sentiment` / `reddit_signals` | Reddit mention counts + sentiment per (date, ticker), and the spike-derived signals (experimental) from `reddit_ingest.py` |

Additional cache/raw tables: `tweets` (raw), `stocks` (raw OHLCV), `news_sentiment_cache` (API cache keyed by ticker + date).

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
FINNHUB_API_KEY=your_finnhub_api_key          # news sentiment
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_key  # fallback news sentiment (optional)
FMP_API_KEY=your_fmp_api_key                  # congressional disclosures (free tier)
SEC_USER_AGENT="Your Name your@email.com"     # required by SEC EDGAR (Form 4 insiders)
REDDIT_CLIENT_ID=your_reddit_app_id           # reddit_ingest.py (free 'script' app)
REDDIT_CLIENT_SECRET=your_reddit_app_secret
REDDIT_USER_AGENT="MoneyMaker:reddit_ingest:v1 (by u/you)"
# TWEET_SOURCE=syndication                    # default free tweet backend; set to 'twikit' to use cookies
# INSIDER_TRADING_ENABLED=false                # opt-in: trade SEC Form 4 insider signals (validate with event_study.py first)
# REGIME_GATE_ENABLED=false                    # opt-in market-regime gate (validate with backtest.py first)
# SECTOR_WEIGHTING_ENABLED=false               # opt-in sector reactivity sizing for sentiment signals

# 2b. (Optional) twikit cookies — only needed if TWEET_SOURCE=twikit.
#     The default 'syndication' backend needs no cookies. To use twikit instead:
#     Grab auth_token and ct0 from your logged-in x.com browser cookies, then:
python3 test_twitter_cookies.py <auth_token> <ct0>   # writes twitter_cookies.json
#     For GitHub Actions, paste the file's contents into the TWITTER_COOKIES repo secret.

# 3. Start the FastAPI backend
uvicorn main:app --reload           # http://localhost:8000 · docs at /docs

# 4. Start the Streamlit dashboard
streamlit run app.py                # http://localhost:8501

# 5. Train the ML model (required before the Predict tab works)
python3 model/baseline.py

# 6. Run the data pipelines manually
python3 run_pipeline.py             # tweets + stocks + news (daily incremental)
python3 run_pipeline.py --pages 50  # historical backfill
python3 congress_ingest.py          # latest House + Senate disclosures (FMP)
python3 insider_ingest.py           # latest SEC Form 4 insider trades (EDGAR)
python3 insider_ingest.py --dry-run # parse + print, no DB writes
python3 reddit_ingest.py            # WSB/stocks mention + sentiment spikes (needs Reddit app creds)

# 6b. Validate whether a signal source actually has edge (read-only, no trades)
python3 event_study.py --source insider     # insider trades, 1/3/5-day forward abnormal returns
python3 event_study.py --source congress     # congressional trades
python3 event_study.py --source reddit        # reddit spikes

# 6c. Validate the market-regime gate before enabling it (read-only, no trades)
python3 backtest.py --source congress --hold 3   # ungated vs regime-gated vs benchmarks
#     Enable live only if it cuts drawdown ~25-30% without Sharpe loss:
#     set REGIME_GATE_ENABLED=true (gates LONG entries; shorts stay ungated)

# 7. Run the trading watcher
python3 watch.py --once --db-only --dry-run   # one cycle, no orders (safe smoke test)
python3 watch.py --db-only                     # continuous, places paper trades

# 8. Run the one-shot morning trader (registry → ML → paper orders)
python3 trade.py --dry-run          # preview signals, no orders placed
python3 trade.py                    # trade all CEOs with registry entries
python3 trade.py --portfolio        # print Alpaca account + open positions and exit
python3 trade.py --history          # print recent trade log and exit

# 9. Run all tests
python3 -m pytest tests/ -v
```

### Twitter cookie setup

Tweet fetching uses [twikit](https://github.com/d60/twikit), which authenticates
with browser cookies instead of a paid X API key. Without valid cookies the
pipeline runs but ingests **no new tweets** (it falls back to whatever is already
in `merged_data`).

1. **Use a dedicated burner X account** — the `auth_token` cookie is full account
   access (no password/2FA gate), and automated access can get an account
   rate-limited or suspended. Don't use your personal account.
2. Log into x.com in a browser, open DevTools → Application/Storage → Cookies →
   `https://x.com`, and copy the **`auth_token`** and **`ct0`** values.
3. Generate and validate the cookie file:
   ```bash
   python3 test_twitter_cookies.py <auth_token> <ct0>   # writes twitter_cookies.json
   ```
4. For GitHub Actions, paste the contents of `twitter_cookies.json` into a repo
   secret named **`TWITTER_COOKIES`** (the daily pipeline writes it back to a file).

**Cookies expire** (on logout or X's rotation) — when fetching suddenly returns
nothing, regenerate them. `processor.py` now logs a clear error and
`run_pipeline.py` exits non-zero in that case, so the failure is visible instead
of silent.

> **Datacenter-IP note:** cookies generated on your home IP and used from
> GitHub Actions / Render may occasionally be flagged. If that happens, run
> ingestion locally (cron/launchd, where the IP matches the browser session) and
> keep the cloud watcher in `--db-only` mode reading `merged_data`.

---

## Tests

```
178 tests · 14 files · all external APIs mocked · no credentials needed
```

| File | What it covers |
|------|----------------|
| `test_sentiment_score.py` | VADER return type, range [-1,1], polarity direction, edge cases |
| `test_classifier.py` | All sentiment/tone/type label boundaries and fallbacks |
| `test_api.py` | All three live-fetch endpoints — schema, value ranges, error handling |
| `test_processor.py` | `_safe_int()` — handles `None`, `"Unavailable"`, float strings, large ints |
| `test_context.py` | Alpha Vantage and Finnhub parsing, fallback logic, rate-limit handling, caching |
| `test_predict.py` | Feature contract (23 exact features), weekend date shifting, RSI flags, inference correctness |
| `test_insider.py` | SEC Form 4 parser — P/S extraction, direction mapping, role/value, dedup keys, malformed XML |
| `test_reddit.py` | Reddit ticker extraction (cashtags, stopwords) and mention/sentiment spike detection |
| `test_tweet_sources.py` | Syndication `__NEXT_DATA__` parsing, retweet skipping, timestamp parsing |
| `test_event_study.py` | Forward-return math, market-abnormal orientation, strategy-return aggregation |
| `test_regime.py` | Trend (200d SMA) + VIX regime states, gate long/short logic, crisis-vol override, causal lag |
| `test_sector_map.py` | Ticker→sector lookup, reactivity ordering (tech>energy), structured-vs-sentiment weighting |
| `test_risk_filters.py` | Bot/coordination heuristics, duplicate-ratio, pump-and-dump detection, micro-cap guard |
| `test_backtest.py` | Equity-metric math (return/Sharpe/drawdown), causal regime lookup / fail-open |

---

## Signal Coverage

Beyond the CEO roster below, the watcher also acts on **congressional trades** (all disclosing House & Senate members, via FMP), **short-seller reports** (Hindenburg, Muddy Waters, Citron, and peers), and **policy/macro accounts** (President, POTUS, Treasury) mapped to sector ETFs.

### Tracked CEOs (26)

| Handle | Name | Ticker | Handle | Name | Ticker |
|--------|------|--------|--------|------|--------|
| elonmusk | Elon Musk | TSLA | LisaSu | Lisa Su | AMD |
| tim_cook | Tim Cook | AAPL | jack | Jack Dorsey | SQ |
| satyanadella | Satya Nadella | MSFT | tobi | Tobi Lütke | SHOP |
| sundarpichai | Sundar Pichai | GOOGL | brian_armstrong | Brian Armstrong | COIN |
| MichaelDell | Michael Dell | DELL | ericyuan | Eric Yuan | ZM |
| ajassy | Andy Jassy | AMZN | CathieDWood | Cathie Wood | ARKK |
| bchesky | Brian Chesky | ABNB | AlexKarp | Alex Karp | PLTR |
| dkhos | Dara Khosrowshahi | UBER | mtbarra | Mary Barra | GM |
| RobertIger | Robert Iger | DIS | JimFarley98 | Jim Farley | F |
| Benioff | Marc Benioff | CRM | AnthonyNoto | Anthony Noto | SOFI |
| george_kurtz | George Kurtz | CRWD | reedhastings | Reed Hastings | NFLX |
| eldsjal | Daniel Ek | SPOT | PGelsinger | Pat Gelsinger | INTC |
| RJScaringe | RJ Scaringe | RIVN | levie | Aaron Levie | BOX |
