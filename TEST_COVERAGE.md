# Test Coverage

**116 tests — all passing**

Run the suite:
```bash
python3 -m pytest tests/ -v
```

---

## Coverage by File

### `classifier.py`
**34 tests across 2 files**

| Test File | Class | What It Covers |
|---|---|---|
| `test_classifier.py` | `TestGetRefinedSentiment` | All 5 sentiment labels, exact boundary values (±0.2, ±0.6), extreme inputs (±1.0), full-range sweep |
| `test_classifier.py` | `TestGetToneCategory` | Emotional vs informational keyword matching, conflict resolution, fallback to General Commentary |
| `test_classifier.py` | `TestGetTweetType` | Poll/Vote, Discussion Starter, Company Milestone keywords, fallback, case insensitivity |
| `test_sentiment_score.py` | `TestGetSentimentScore` | Returns float in [-1, 1], positive/negative/neutral behavior, empty string, whitespace, special characters, relative strength |

---

### `processor.py`
**12 tests**

| Test File | Class | What It Covers |
|---|---|---|
| `test_processor.py` | `TestSafeInt` | `None`, empty string, `"Unavailable"`, numeric strings, float strings, non-numeric strings, zero, large numbers, return type |

---

### `context.py`
**26 tests**

| Test File | Class | What It Covers |
|---|---|---|
| `test_context.py` | `TestGetSectorEtf` | All mapped tickers (XLK/XLY), unknown ticker fallback to SPY, lowercase input |
| `test_context.py` | `TestBuildNewsSentimentLookup` | Empty dates, AV used first, Finnhub fallback when AV empty, no-key guards, correct date window passed |
| `test_context.py` | `TestAvNewsSentimentLookup` | Per-ticker score preferred over overall score, grouping by date, rate-limit Note/Information fields, HTTP errors, empty feed, malformed timestamps |
| `test_context.py` | `TestFinnhubBulkLookup` | Headline scoring, skipping articles without headlines, per-day averaging, HTTP 429, empty response |

---

### `model/predict.py`
**21 tests**

| Test File | Class | What It Covers |
|---|---|---|
| `test_predict.py` | `TestFeatureContract` | Exact feature names match `baseline.py` training features, no extra features, numeric/categorical types |
| `test_predict.py` | `TestWeekendShift` | Saturday → Monday, Sunday → Monday, weekday not shifted |
| `test_predict.py` | `TestRsiFlags` | Overbought (>70), oversold (<30), normal range, NaN fills to 50 and clears both flags |
| `test_predict.py` | `TestEngagementFeatures` | Rate formula, zero-views guard, log transforms non-negative, sentiment magnitude |
| `test_predict.py` | `TestEmptyStocks` | Returns None for technical features, still returns all feature keys |
| `test_predict.py` | `TestPrevDayDirection` | Up day → 1, down day → 0, insufficient history → None |
| `test_predict.py` | `TestPredictTweetsMissingModel` | `FileNotFoundError` raised cleanly when model file is absent |

---

### FastAPI endpoints (`main.py`)
**23 tests**

| Test File | Class | What It Covers |
|---|---|---|
| `test_api.py` | `TestApiTweets` | HTTP 200, data is list, required fields present, sentiment in [-1, 1], empty result for unknown CEO, error returns `status: "error"` |
| `test_api.py` | `TestApiStocks` | HTTP 200, all OHLCV fields present, prices > 0, high ≥ low, volume ≥ 0, empty result for unknown ticker, error handling |
| `test_api.py` | `TestApiMerged` | HTTP 200, all 10 fields present, sentiment in range, valid refined sentiment label, stock close > 0, CEO field matches URL, empty tweets |

---

## What Is Not Yet Covered

| File / Area | Reason Not Covered |
|---|---|
| `model/baseline.py` | Reads from database at module level — needs a DB fixture or integration test setup |
| `run_pipeline.py` — weekend shift | `create_engine` runs at module level, making import require a live `DATABASE_URL` |
| `POST /process/all` in `main.py` | Requires mocking both processors and a DB session; mid-complexity integration test |
| `predict_tweets` end-to-end | Requires a trained model fixture — needs a small dummy model trained in the test setup |
| `context.py` — `get_earnings_dates` | Calls yfinance over the network; needs a mock |
| `context.py` — `get_news_for_date` / `get_news_for_range` | Thin wrappers over `_finnhub_bulk_lookup`, which is already covered |
