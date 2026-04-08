# MoneyMaker

## Description
MoneyMaker correlates CEO tweets with stock market data. It fetches tweets, runs sentiment analysis, pulls corresponding stock prices from Alpaca, merges the results, and stores everything in a Neon PostgreSQL database. A Streamlit dashboard lets you explore the data visually.

*(Note: `actualcapstone.ipynb` is an exploratory scratchpad and is not part of the active app.)*

---

## Project Structure
- `main.py` — FastAPI backend: all API endpoints and database models
- `processor.py` — Fetches tweets (tweety-ns) and stock bars (Alpaca)
- `classifier.py` — Sentiment scoring, tone categorization, and tweet type detection
- `app.py` — Streamlit dashboard UI
- `context.py` — Market context helpers (earnings dates, news, sector ETFs)
- `tests/` — Automated tests for the API endpoints and classifier

---

## Required Environment Variables

Create a `.env` file in the root directory with the following:

```env
DATABASE_URL=your_neon_postgres_connection_url
ALPACA_API_KEY=your_alpaca_api_key
ALPACA_SECRET_KEY=your_alpaca_secret_key
```

---

## Setup & Running

**1. Install dependencies**
```bash
pip install -r requirements.txt
```

**2. Start the FastAPI backend** (runs at http://localhost:8000)
```bash
uvicorn main:app --reload
```
Interactive API docs are available at http://localhost:8000/docs

**3. Start the Streamlit dashboard** (runs at http://localhost:8501)
```bash
streamlit run app.py
```

Run both in separate terminal tabs.

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Homepage |
| `GET` | `/api/tweets/{ceo}` | Fetch tweets for a CEO handle |
| `GET` | `/api/stocks/{ticker}` | Fetch stock bars for a ticker |
| `GET` | `/api/merged/{ceo}/{ticker}` | Fetch tweets merged with stock data |
| `POST` | `/process/all` | Fetch, classify, and save data for all hardcoded CEOs |
| `POST` | `/ingest/tweets` | Manually ingest a list of tweets |
| `POST` | `/ingest/stocks` | Manually ingest a list of stock records |

---

## Tests

The `tests/` folder contains automated checks that verify the API is returning the right data in the right shape, and that all values are within expected bounds.

**Run all tests:**
```bash
python3 -m pytest tests/ -v
```

**Run just one file:**
```bash
python3 -m pytest tests/test_classifier.py -v
python3 -m pytest tests/test_api.py -v
```

### What the tests check

**`tests/test_classifier.py`** — Tests the sentiment and classification logic in isolation. No internet connection or credentials needed. Checks things like:
- A score of `0.9` maps to `"Very Positive"`, `-0.8` maps to `"Very Negative"`, etc.
- Every possible sentiment score returns one of the five known labels
- Tweet tone and type detection work for each keyword category

**`tests/test_api.py`** — Tests the API endpoints without hitting Twitter or Alpaca (external calls are replaced with fake data). Checks things like:
- Every response has a `status` field that says `"success"` or `"error"`
- Every tweet response contains `date`, `text`, and `sentiment` fields
- Sentiment scores are always between `-1.0` and `1.0`
- Stock prices (`open`, `high`, `low`, `close`) are always greater than zero
- `high` is always greater than or equal to `low` — a basic sanity check on OHLC data
- `volume` is never negative
- The merged endpoint echoes back the correct CEO name
- Unknown CEOs and tickers return an empty list instead of crashing
