"""
Tests for context.py — news sentiment lookups and sector ETF mapping.

All HTTP calls are mocked so these run fast with no API keys or network.
The goal is to catch bugs in the AV/Finnhub parsing logic before they burn
the daily API quota or silently return zero coverage for all tweet dates.
"""
import pytest
import pandas as pd
from datetime import date
from unittest.mock import patch, MagicMock

from context import (
    get_sector_etf,
    build_news_sentiment_lookup,
    _av_news_sentiment_lookup,
    _finnhub_bulk_lookup,
)


# ── get_sector_etf ────────────────────────────────────────────────────────────

class TestGetSectorEtf:
    def test_tech_tickers_map_to_xlk(self):
        for ticker in ["AAPL", "MSFT", "GOOGL", "GOOG", "DELL", "AMD", "NVDA", "META"]:
            assert get_sector_etf(ticker) == "XLK", f"{ticker} should map to XLK"

    def test_consumer_tickers_map_to_xly(self):
        for ticker in ["TSLA", "AMZN", "ABNB", "UBER", "DIS"]:
            assert get_sector_etf(ticker) == "XLY", f"{ticker} should map to XLY"

    def test_unknown_ticker_falls_back_to_spy(self):
        assert get_sector_etf("FAKE") == "SPY"
        assert get_sector_etf("XYZ123") == "SPY"

    def test_lowercase_input_works(self):
        # Tickers can arrive in any case from user input
        assert get_sector_etf("aapl") == "XLK"
        assert get_sector_etf("tsla") == "XLY"
        assert get_sector_etf("fake") == "SPY"


# ── build_news_sentiment_lookup ───────────────────────────────────────────────

class TestBuildNewsSentimentLookup:
    def test_empty_tweet_dates_returns_empty(self):
        result = build_news_sentiment_lookup("TSLA", [], lambda t: 0.5)
        assert result == {}

    def test_empty_set_returns_empty(self):
        result = build_news_sentiment_lookup("TSLA", set(), lambda t: 0.5)
        assert result == {}

    @patch("context._AV_KEY", "fake-key")
    @patch("context._av_news_sentiment_lookup", return_value={"2024-01-15": 0.3})
    def test_returns_av_result_when_available(self, mock_av):
        dates = {date(2024, 1, 15)}
        result = build_news_sentiment_lookup("TSLA", dates, lambda t: 0.5)
        assert result == {"2024-01-15": 0.3}
        mock_av.assert_called_once()

    @patch("context._AV_KEY", "fake-key")
    @patch("context._finnhub_bulk_lookup", return_value={"2024-01-15": 0.1})
    @patch("context._av_news_sentiment_lookup", return_value={})
    def test_falls_back_to_finnhub_when_av_empty(self, mock_av, mock_fh):
        dates = {date(2024, 1, 15)}
        result = build_news_sentiment_lookup("TSLA", dates, lambda t: 0.5)
        assert result == {"2024-01-15": 0.1}
        mock_fh.assert_called_once()

    @patch("context._AV_KEY", None)
    @patch("context._finnhub_bulk_lookup", return_value={"2024-01-15": 0.2})
    def test_skips_av_entirely_when_no_av_key(self, mock_fh):
        dates = {date(2024, 1, 15)}
        result = build_news_sentiment_lookup("TSLA", dates, lambda t: 0.5)
        assert result == {"2024-01-15": 0.2}
        mock_fh.assert_called_once()

    @patch("context._AV_KEY", None)
    @patch("context._FINNHUB_KEY", None)
    def test_returns_empty_when_no_api_keys(self):
        dates = {date(2024, 1, 15)}
        result = build_news_sentiment_lookup("TSLA", dates, lambda t: 0.5)
        assert result == {}

    @patch("context._AV_KEY", "fake-key")
    @patch("context._av_news_sentiment_lookup", return_value={"2024-01-10": 0.4, "2024-01-15": 0.3})
    def test_window_uses_min_and_max_of_tweet_dates(self, mock_av):
        # AV should be called with the exact range spanned by tweet dates
        dates = {date(2024, 1, 10), date(2024, 1, 15), date(2024, 1, 12)}
        build_news_sentiment_lookup("TSLA", dates, lambda t: 0.5)
        args = mock_av.call_args[0]
        assert args[1] == date(2024, 1, 10), "start_date should be min of tweet dates"
        assert args[2] == date(2024, 1, 15), "end_date should be max of tweet dates"


# ── _av_news_sentiment_lookup ─────────────────────────────────────────────────

class TestAvNewsSentimentLookup:
    def test_returns_empty_when_no_key(self):
        with patch("context._AV_KEY", None):
            result = _av_news_sentiment_lookup("TSLA", date(2024, 1, 1), date(2024, 1, 31))
        assert result == {}

    @patch("context._AV_KEY", "fake-key")
    def test_parses_per_ticker_sentiment_score(self):
        # AV provides per-ticker scores — these should be preferred over overall score
        feed = [
            {
                "time_published": "20240115T120000",
                "overall_sentiment_score": "0.1",
                "ticker_sentiment": [{"ticker": "TSLA", "ticker_sentiment_score": "0.6"}],
            },
            {
                "time_published": "20240115T160000",
                "overall_sentiment_score": "0.1",
                "ticker_sentiment": [{"ticker": "TSLA", "ticker_sentiment_score": "0.4"}],
            },
        ]
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"feed": feed}

        with patch("context.requests.get", return_value=mock_resp):
            result = _av_news_sentiment_lookup("TSLA", date(2024, 1, 15), date(2024, 1, 15))

        assert "2024-01-15" in result
        assert abs(result["2024-01-15"] - 0.5) < 0.001  # avg of 0.6 and 0.4

    @patch("context._AV_KEY", "fake-key")
    def test_falls_back_to_overall_score_when_no_ticker_match(self):
        feed = [{
            "time_published": "20240115T120000",
            "overall_sentiment_score": "0.3",
            "ticker_sentiment": [],  # no per-ticker entry
        }]
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"feed": feed}

        with patch("context.requests.get", return_value=mock_resp):
            result = _av_news_sentiment_lookup("TSLA", date(2024, 1, 15), date(2024, 1, 15))

        assert "2024-01-15" in result
        assert abs(result["2024-01-15"] - 0.3) < 0.001

    @patch("context._AV_KEY", "fake-key")
    def test_groups_multiple_articles_by_date(self):
        # Two articles on different days should produce two separate entries
        feed = [
            {
                "time_published": "20240115T120000",
                "overall_sentiment_score": "0.5",
                "ticker_sentiment": [{"ticker": "TSLA", "ticker_sentiment_score": "0.5"}],
            },
            {
                "time_published": "20240116T120000",
                "overall_sentiment_score": "-0.3",
                "ticker_sentiment": [{"ticker": "TSLA", "ticker_sentiment_score": "-0.3"}],
            },
        ]
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"feed": feed}

        with patch("context.requests.get", return_value=mock_resp):
            result = _av_news_sentiment_lookup("TSLA", date(2024, 1, 15), date(2024, 1, 16))

        assert "2024-01-15" in result
        assert "2024-01-16" in result

    @patch("context._AV_KEY", "fake-key")
    def test_handles_rate_limit_note_field(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"Note": "API call frequency limit reached."}

        with patch("context.requests.get", return_value=mock_resp):
            result = _av_news_sentiment_lookup("TSLA", date(2024, 1, 1), date(2024, 1, 31))

        assert result == {}

    @patch("context._AV_KEY", "fake-key")
    def test_handles_information_field(self):
        # AV returns "Information" key when the API key is invalid
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"Information": "Invalid API key."}

        with patch("context.requests.get", return_value=mock_resp):
            result = _av_news_sentiment_lookup("TSLA", date(2024, 1, 1), date(2024, 1, 31))

        assert result == {}

    @patch("context._AV_KEY", "fake-key")
    def test_handles_http_error(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 500

        with patch("context.requests.get", return_value=mock_resp):
            result = _av_news_sentiment_lookup("TSLA", date(2024, 1, 1), date(2024, 1, 31))

        assert result == {}

    @patch("context._AV_KEY", "fake-key")
    def test_handles_empty_feed(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"feed": []}

        with patch("context.requests.get", return_value=mock_resp):
            result = _av_news_sentiment_lookup("TSLA", date(2024, 1, 1), date(2024, 1, 31))

        assert result == {}

    @patch("context._AV_KEY", "fake-key")
    def test_skips_articles_with_short_time_published(self):
        # Articles with malformed timestamps should be silently skipped
        feed = [
            {"time_published": "20240", "overall_sentiment_score": "0.5", "ticker_sentiment": []},
            {"time_published": "20240115T120000", "overall_sentiment_score": "0.4", "ticker_sentiment": []},
        ]
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"feed": feed}

        with patch("context.requests.get", return_value=mock_resp):
            result = _av_news_sentiment_lookup("TSLA", date(2024, 1, 15), date(2024, 1, 15))

        # Only the valid article should be counted
        assert "2024-01-15" in result
        assert abs(result["2024-01-15"] - 0.4) < 0.001


# ── _finnhub_bulk_lookup ──────────────────────────────────────────────────────

class TestFinnhubBulkLookup:
    # Unix timestamp for 2024-01-15 00:00:00 UTC
    _TS_JAN15 = 1705276800

    def test_returns_empty_when_no_key(self):
        with patch("context._FINNHUB_KEY", None):
            result = _finnhub_bulk_lookup("TSLA", date(2024, 1, 1), date(2024, 1, 31), lambda t: 0.5)
        assert result == {}

    @patch("context._FINNHUB_KEY", "fake-key")
    def test_parses_headlines_and_scores_them(self):
        articles = [
            {"headline": "Tesla deliveries beat expectations", "datetime": self._TS_JAN15},
            {"headline": "Tesla stock hits record high",       "datetime": self._TS_JAN15 + 3600},
        ]
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = articles

        scores = iter([0.6, 0.4])
        def fake_sentiment(text):
            return next(scores)

        with patch("context.requests.get", return_value=mock_resp):
            result = _finnhub_bulk_lookup("TSLA", date(2024, 1, 15), date(2024, 1, 15), fake_sentiment)

        assert len(result) == 1
        day = list(result.keys())[0]
        assert abs(result[day] - 0.5) < 0.001  # avg of 0.6 and 0.4

    @patch("context._FINNHUB_KEY", "fake-key")
    def test_skips_articles_without_headline(self):
        articles = [
            {"headline": "",                              "datetime": self._TS_JAN15},  # empty
            {"datetime": self._TS_JAN15},                                               # missing key
            {"headline": "Valid headline for Tesla",      "datetime": self._TS_JAN15},
        ]
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = articles

        call_count = {"n": 0}
        def counting_fn(text):
            call_count["n"] += 1
            return 0.5

        with patch("context.requests.get", return_value=mock_resp):
            _finnhub_bulk_lookup("TSLA", date(2024, 1, 15), date(2024, 1, 15), counting_fn)

        assert call_count["n"] == 1, "Only the valid headline should be scored"

    @patch("context._FINNHUB_KEY", "fake-key")
    def test_handles_http_error(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 429

        with patch("context.requests.get", return_value=mock_resp):
            result = _finnhub_bulk_lookup("TSLA", date(2024, 1, 1), date(2024, 1, 31), lambda t: 0.5)

        assert result == {}

    @patch("context._FINNHUB_KEY", "fake-key")
    def test_handles_empty_article_list(self):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = []

        with patch("context.requests.get", return_value=mock_resp):
            result = _finnhub_bulk_lookup("TSLA", date(2024, 1, 1), date(2024, 1, 31), lambda t: 0.5)

        assert result == {}

    @patch("context._FINNHUB_KEY", "fake-key")
    def test_multiple_articles_same_day_averaged(self):
        articles = [
            {"headline": "Good news", "datetime": self._TS_JAN15},
            {"headline": "Bad news",  "datetime": self._TS_JAN15 + 7200},
        ]
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = articles

        sentiments = iter([0.8, -0.2])
        def fake_sentiment(text):
            return next(sentiments)

        with patch("context.requests.get", return_value=mock_resp):
            result = _finnhub_bulk_lookup("TSLA", date(2024, 1, 15), date(2024, 1, 15), fake_sentiment)

        day = list(result.keys())[0]
        assert abs(result[day] - 0.3) < 0.001  # avg of 0.8 and -0.2
