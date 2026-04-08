"""
API endpoint tests for main.py using FastAPI TestClient.
External dependencies (Twitter via tweety-ns, Alpaca stocks) are mocked so
tests run fast without credentials.

Tolerance rules enforced:
  - sentiment_score: float in [-1.0, 1.0]
  - refined_sentiment: one of the five known labels
  - stock prices (open/high/low/close): > 0
  - high >= low for every bar
  - volume: >= 0
  - Response always has {"status": "success"|"error", ...}
"""
import pytest
import pandas as pd
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient

from main import app

client = TestClient(app)

# ── Realistic sample data ──────────────────────────────────────────────────────

SAMPLE_TWEETS_DF = pd.DataFrame([
    {
        "date": pd.Timestamp("2024-01-15 10:00:00", tz="UTC"),
        "text": "Excited about our new product launch! Big things coming.",
        "sentiment": 0.8,
        "likes": 45000, "retweet_count": 8200, "view_count": 3100000, "reply_count": 1200,
        "tweet_hour": 10, "is_premarket": True,
    },
    {
        "date": pd.Timestamp("2024-01-10 16:00:00", tz="UTC"),
        "text": "Quarterly results exceeded expectations.",
        "sentiment": 0.55,
        "likes": 12000, "retweet_count": 2100, "view_count": 800000, "reply_count": 430,
        "tweet_hour": 16, "is_premarket": False,
    },
    {
        "date": pd.Timestamp("2024-01-06 09:00:00", tz="UTC"),  # Saturday → Monday shift tested
        "text": "Weekend thoughts on innovation.",
        "sentiment": 0.1,
        "likes": 3000, "retweet_count": 500, "view_count": 200000, "reply_count": 90,
        "tweet_hour": 9, "is_premarket": True,
    },
])

SAMPLE_STOCKS_DF = pd.DataFrame(
    {
        "open":   [185.0, 187.5, 190.0],
        "high":   [188.0, 192.0, 193.0],
        "low":    [184.0, 186.0, 188.5],
        "close":  [187.0, 191.0, 192.0],
        "volume": [50_000_000.0, 45_000_000.0, 55_000_000.0],
    },
    index=pd.DatetimeIndex(
        ["2024-01-08", "2024-01-15", "2024-01-16"], name="timestamp"
    ).tz_localize("UTC"),
)

VALID_REFINED_SENTIMENTS = {"Very Positive", "Positive", "Neutral", "Negative", "Very Negative"}


# ── GET /api/tweets/{ceo} ──────────────────────────────────────────────────────

class TestApiTweets:
    @patch("main.proc.get_tweets", new_callable=AsyncMock)
    def test_success_status(self, mock_tweets):
        mock_tweets.return_value = SAMPLE_TWEETS_DF
        r = client.get("/api/tweets/elonmusk")
        assert r.status_code == 200
        assert r.json()["status"] == "success"

    @patch("main.proc.get_tweets", new_callable=AsyncMock)
    def test_data_is_list(self, mock_tweets):
        mock_tweets.return_value = SAMPLE_TWEETS_DF
        r = client.get("/api/tweets/elonmusk")
        assert isinstance(r.json()["data"], list)
        assert len(r.json()["data"]) == len(SAMPLE_TWEETS_DF)

    @patch("main.proc.get_tweets", new_callable=AsyncMock)
    def test_required_fields_present(self, mock_tweets):
        mock_tweets.return_value = SAMPLE_TWEETS_DF
        r = client.get("/api/tweets/elonmusk")
        record = r.json()["data"][0]
        for field in ("date", "text", "sentiment"):
            assert field in record, f"Missing field: {field!r}"

    @patch("main.proc.get_tweets", new_callable=AsyncMock)
    def test_sentiment_within_tolerance(self, mock_tweets):
        mock_tweets.return_value = SAMPLE_TWEETS_DF
        r = client.get("/api/tweets/elonmusk")
        for rec in r.json()["data"]:
            s = rec["sentiment"]
            assert -1.0 <= s <= 1.0, f"Sentiment {s} out of range [-1, 1]"

    @patch("main.proc.get_tweets", new_callable=AsyncMock)
    def test_empty_response_for_unknown_ceo(self, mock_tweets):
        mock_tweets.return_value = pd.DataFrame()
        r = client.get("/api/tweets/unknownuser999")
        assert r.json() == {"status": "success", "data": []}

    @patch("main.proc.get_tweets", new_callable=AsyncMock)
    def test_error_returns_status_field(self, mock_tweets):
        mock_tweets.side_effect = RuntimeError("Twitter unavailable")
        r = client.get("/api/tweets/elonmusk")
        body = r.json()
        assert "status" in body
        assert body["status"] == "error"


# ── GET /api/stocks/{ticker} ───────────────────────────────────────────────────

class TestApiStocks:
    @patch("main.proc.get_stocks")
    def test_success_status(self, mock_stocks):
        mock_stocks.return_value = SAMPLE_STOCKS_DF
        r = client.get("/api/stocks/AAPL")
        assert r.status_code == 200
        assert r.json()["status"] == "success"

    @patch("main.proc.get_stocks")
    def test_ohlcv_fields_present(self, mock_stocks):
        mock_stocks.return_value = SAMPLE_STOCKS_DF
        r = client.get("/api/stocks/AAPL")
        record = r.json()["data"][0]
        for field in ("open", "high", "low", "close", "volume"):
            assert field in record, f"Missing OHLCV field: {field!r}"

    @patch("main.proc.get_stocks")
    def test_prices_positive(self, mock_stocks):
        mock_stocks.return_value = SAMPLE_STOCKS_DF
        r = client.get("/api/stocks/AAPL")
        for rec in r.json()["data"]:
            for field in ("open", "high", "low", "close"):
                assert rec[field] > 0, f"{field} must be > 0, got {rec[field]}"

    @patch("main.proc.get_stocks")
    def test_high_gte_low(self, mock_stocks):
        mock_stocks.return_value = SAMPLE_STOCKS_DF
        r = client.get("/api/stocks/AAPL")
        for rec in r.json()["data"]:
            assert rec["high"] >= rec["low"], (
                f"high={rec['high']} < low={rec['low']} — impossible OHLC bar"
            )

    @patch("main.proc.get_stocks")
    def test_volume_non_negative(self, mock_stocks):
        mock_stocks.return_value = SAMPLE_STOCKS_DF
        r = client.get("/api/stocks/AAPL")
        for rec in r.json()["data"]:
            assert rec["volume"] >= 0, f"Negative volume: {rec['volume']}"

    @patch("main.proc.get_stocks")
    def test_empty_response_for_unknown_ticker(self, mock_stocks):
        mock_stocks.return_value = pd.DataFrame()
        r = client.get("/api/stocks/FAKE")
        assert r.json() == {"status": "success", "data": []}

    @patch("main.proc.get_stocks")
    def test_error_returns_status_field(self, mock_stocks):
        mock_stocks.side_effect = RuntimeError("Alpaca unavailable")
        r = client.get("/api/stocks/AAPL")
        body = r.json()
        assert "status" in body
        assert body["status"] == "error"


# ── GET /api/merged/{ceo}/{ticker} ────────────────────────────────────────────

class TestApiMerged:
    @patch("main.proc.get_stocks")
    @patch("main.proc.get_tweets", new_callable=AsyncMock)
    def test_success_status(self, mock_tweets, mock_stocks):
        mock_tweets.return_value = SAMPLE_TWEETS_DF
        mock_stocks.return_value = SAMPLE_STOCKS_DF
        r = client.get("/api/merged/elonmusk/TSLA")
        assert r.status_code == 200
        assert r.json()["status"] == "success"

    @patch("main.proc.get_stocks")
    @patch("main.proc.get_tweets", new_callable=AsyncMock)
    def test_all_fields_present(self, mock_tweets, mock_stocks):
        mock_tweets.return_value = SAMPLE_TWEETS_DF
        mock_stocks.return_value = SAMPLE_STOCKS_DF
        r = client.get("/api/merged/elonmusk/TSLA")
        record = r.json()["data"][0]
        expected = (
            "date", "ceo", "tweet_text", "sentiment_score",
            "refined_sentiment", "tone_category", "tweet_type",
            "stock_close", "stock_volume", "stock_open_close_diff",
        )
        for field in expected:
            assert field in record, f"Missing field: {field!r}"

    @patch("main.proc.get_stocks")
    @patch("main.proc.get_tweets", new_callable=AsyncMock)
    def test_sentiment_score_in_range(self, mock_tweets, mock_stocks):
        mock_tweets.return_value = SAMPLE_TWEETS_DF
        mock_stocks.return_value = SAMPLE_STOCKS_DF
        r = client.get("/api/merged/elonmusk/TSLA")
        for rec in r.json()["data"]:
            s = rec["sentiment_score"]
            assert -1.0 <= s <= 1.0, f"sentiment_score {s} out of [-1, 1]"

    @patch("main.proc.get_stocks")
    @patch("main.proc.get_tweets", new_callable=AsyncMock)
    def test_refined_sentiment_valid(self, mock_tweets, mock_stocks):
        mock_tweets.return_value = SAMPLE_TWEETS_DF
        mock_stocks.return_value = SAMPLE_STOCKS_DF
        r = client.get("/api/merged/elonmusk/TSLA")
        for rec in r.json()["data"]:
            assert rec["refined_sentiment"] in VALID_REFINED_SENTIMENTS, (
                f"Unexpected refined_sentiment: {rec['refined_sentiment']!r}"
            )

    @patch("main.proc.get_stocks")
    @patch("main.proc.get_tweets", new_callable=AsyncMock)
    def test_stock_close_positive_when_matched(self, mock_tweets, mock_stocks):
        mock_tweets.return_value = SAMPLE_TWEETS_DF
        mock_stocks.return_value = SAMPLE_STOCKS_DF
        r = client.get("/api/merged/elonmusk/TSLA")
        for rec in r.json()["data"]:
            if rec["stock_close"] is not None:
                assert rec["stock_close"] > 0, f"stock_close must be > 0, got {rec['stock_close']}"

    @patch("main.proc.get_stocks")
    @patch("main.proc.get_tweets", new_callable=AsyncMock)
    def test_ceo_field_matches_request(self, mock_tweets, mock_stocks):
        mock_tweets.return_value = SAMPLE_TWEETS_DF
        mock_stocks.return_value = SAMPLE_STOCKS_DF
        r = client.get("/api/merged/elonmusk/TSLA")
        for rec in r.json()["data"]:
            assert rec["ceo"] == "elonmusk"

    @patch("main.proc.get_stocks")
    @patch("main.proc.get_tweets", new_callable=AsyncMock)
    def test_empty_tweets_returns_empty_list(self, mock_tweets, mock_stocks):
        mock_tweets.return_value = pd.DataFrame()
        mock_stocks.return_value = SAMPLE_STOCKS_DF
        r = client.get("/api/merged/elonmusk/TSLA")
        assert r.json() == {"status": "success", "data": []}
