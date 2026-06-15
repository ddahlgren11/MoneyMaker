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
# DATABASE_URL, ALPACA_API_KEY, ALPACA_SECRET_KEY,
# ALPACA_PAPER_API_KEY, ALPACA_PAPER_SECRET_KEY, FINNHUB_API_KEY
```

**Twitter auth:** Tweet fetching uses the `twikit` library, which authenticates with
browser cookies (no paid API). Generate `twitter_cookies.json` once with
`python test_twitter_cookies.py <auth_token> <ct0>` (the file is gitignored). In
GitHub Actions the same JSON is supplied via the `TWITTER_COOKIES` repo secret.

No linter is configured. Tests live in `tests/` and run with `pytest`.

## Architecture

MoneyMaker is an ML-driven trading-signal pipeline: it ingests tweets from CEOs,
congressional-trade aggregators, and policy accounts, scores them, predicts a
short-term stock direction, and places Alpaca paper trades. Data is stored in Neon
PostgreSQL.

**Core files:**
- `main.py` — FastAPI app with endpoints and SQLAlchemy models; orchestration endpoint `POST /process/all`
- `processor.py` — `DataProcessor`: fetches tweets via `twikit` (cookie auth) and stock bars via `alpaca-py`. The market-data client accepts live or paper keys (free IEX feed)
- `classifier.py` — VADER + FinBERT sentiment, tone/type helpers, the `get_tweet_topic()` bucket classifier, and `parse_congressional_trade()` (extracts ticker + buy/sell direction from disclosure posts)
- `model/predict.py` — builds the 22-feature vector and runs the trained model (`model/trained_model.pkl`); see `22Features.md`
- `run_pipeline.py` — daily ingestion: fetch tweets+stocks+news for every target, write `merged_data`
- `congress_ingest.py` — pulls latest House+Senate STOCK Act disclosures from Financial Modeling Prep (free tier, `FMP_API_KEY`) into the `congress_trades` table. No Twitter/PDF needed — structured ticker/direction/date/amount. 2 API calls per run
- `watch.py` — intraday watcher: polls accounts, evaluates signals, places paper trades. Two-tier polling (fast lane for `HIGH_PRIORITY_HANDLES`, full sweep otherwise). `--db-only` mode (GitHub Actions) reads `merged_data` instead of fetching Twitter
- `discover.py` — candidate-discovery pipeline; tests `candidates.csv` accounts for tweet→price links and promotes them to the registry (local only — needs cookies)
- `targets.py` — single source of truth for handle→ticker mappings (`CEO_TARGETS`, `HANDLE_TO_TICKER`)
- `relationship_analysis.py` — builds the `ceo_ticker_relationships` registry (per-CEO, per-topic best ticker + tightness score)
- `app.py` — Streamlit dashboard

**Signal flow (watch.py):** new tweet → `get_tweet_topic()` → if `congressional_trade` or `short_report`, parse ticker/direction directly (fast path, bypasses ML); else registry lookup for best ticker → ML prediction → confidence gate (≥55%) → trade if market open, else queue for next open.

**Exit horizon:** the model predicts *next-day* direction, so every entry is registered in `managed_positions` with an `exit_after` time (next trading day, 15:30 ET) and closed by `close_due_positions()` on the first poll cycle past that time. Without this, positions would otherwise only close on a reversing signal.

**Double-trade guard:** `_execute_signal()` skips a signal if `paper_trades` already has a `placed` row for the same (ceo, ticker, tweet_date) — protects against retries and against the Render worker + GitHub Actions both processing the same tweet.

**Position sizing & risk caps:** trade size is conviction-scaled by `position_notional()` (blends model confidence and tightness onto [`MIN_NOTIONAL`, `MAX_NOTIONAL`]). Before opening a new position, `risk_gate()` enforces `MAX_OPEN_POSITIONS` and a `MAX_DAILY_LOSS` kill switch (via Alpaca equity vs. prior close). All sizing/risk limits are env-overridable.

**Congressional trades (primary path):** sourced from official disclosures via `congress_ingest.py` → `congress_trades` table → `poll_congress_trades()` in the watcher, which builds `congressional_trade` signals (Purchase→Up, Sale→Down) and runs them through `_execute_signal()` so they get sizing, risk caps, the next-day exit, and idempotency. Gated by `CONGRESS_RECENCY_DAYS` so only freshly disclosed trades are acted on. This replaces the old, fragile tweet path; the `parse_congressional_trade()` tweet parser remains only as a fallback for any aggregator tweets still ingested.

**Special account types (tweet path):**
- policy accounts (`realDonaldTrump`, `POTUS`, `ScottBessent`) are exempt from the sentiment gate in `passes_gates()` — their posts are factual/low-sentiment, so the signal comes from content, not tone.
- short-seller accounts (`HindenburgRes`, `muddywaters`, etc. — see `_SHORT_SELLER_HANDLES`) get a `short_report` fast-path: a report naming a ticker is a fixed DOWN signal, since these posts move stocks sharply on publication.

**Weekend handling:** tweet dates on weekends shift to the following Monday for stock correlation.

## Database Schema

Neon PostgreSQL (SQLAlchemy). Key tables:
- `merged_data` — the training/inference table: one row per tweet with sentiment, engagement, technicals, news sentiment, and the `next_day_direction` label (see `run_pipeline.py` `MergedRecord`)
- `news_sentiment_cache` — cached Finnhub news sentiment per (ticker, date)
- `ceo_ticker_relationships` — registry of best (ceo, topic) → ticker with `tightness_score` (built by `relationship_analysis.py`)
- `signal_queue` — signals found outside market hours, executed at next open (`watch.py`)
- `managed_positions` — open positions with their scheduled next-day exit time (`watch.py`)
- `congress_trades` — structured House/Senate disclosures from `congress_ingest.py`; the watcher trades unprocessed rows
- `paper_trades` — log of every placed/skipped/errored/exit trade
- `watcher_state` — per-CEO last-seen tweet watermark and counters
- `tweets` / `stocks` — legacy tables used by the original FastAPI/Streamlit flow
