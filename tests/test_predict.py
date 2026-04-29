"""
Tests for model/predict.py — specifically _build_feature_row().

The critical invariant: _build_feature_row() must produce exactly the same
feature names and types that baseline.py trains on.  A mismatch causes
sklearn to silently predict on wrong/missing values — no error, just bad numbers.

Weekend date-shifting logic is also tested here since _build_feature_row
duplicates the same shift that run_pipeline.py applies during ingestion.
"""
import pytest
import numpy as np
import pandas as pd
from datetime import date, timedelta
from unittest.mock import patch

from model.predict import _build_feature_row, predict_tweets


# ── Feature contract — must stay in sync with baseline.py ────────────────────
# If you add a feature to baseline.py, add it here too. The test will then
# fail until predict.py is updated, preventing a silent training/serving gap.

NUMERIC_FEATURES = [
    "sentiment_score", "sentiment_magnitude",
    "tweet_length", "word_count",
    "log_likes", "log_retweets", "log_views", "log_replies",
    "engagement_rate",
    "tweet_hour", "is_premarket",
    "rsi_at_tweet", "atr_at_tweet",
    "rsi_overbought", "rsi_oversold",
    "vix_at_tweet",
    "days_to_earnings",
    "prev_day_direction",
    "news_sentiment_score", "finbert_score",
]
CATEGORICAL_FEATURES = ["refined_sentiment", "tone_category", "tweet_type"]
ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_tweet(date_val, sentiment=0.5, text="Product launch announcement!", **kwargs):
    return pd.Series({
        "date":          pd.Timestamp(date_val),
        "text":          text,
        "sentiment":     sentiment,
        "likes":         kwargs.get("likes", 1000),
        "retweet_count": kwargs.get("retweet_count", 200),
        "view_count":    kwargs.get("view_count", 50_000),
        "reply_count":   kwargs.get("reply_count", 100),
        "tweet_hour":    kwargs.get("tweet_hour", 10),
        "is_premarket":  kwargs.get("is_premarket", 1),
    })


def _make_stocks(date_close_pairs, rsi=55.0, atr=3.0):
    """
    Build a minimal stocks_df with the columns _build_feature_row needs.
    date_close_pairs: list of (date, close) tuples, oldest first.
    """
    rows = [
        {"date_only": d, "close": c, "rsi_14": rsi, "atr_14": atr}
        for d, c in date_close_pairs
    ]
    return pd.DataFrame(rows)


# ── Feature contract ──────────────────────────────────────────────────────────

class TestFeatureContract:
    @patch("model.predict._get_vix", return_value=18.5)
    @patch("model.predict._get_days_to_earnings", return_value=10)
    def test_all_expected_features_present(self, _earn, _vix):
        row = _make_tweet("2024-03-05")
        stocks = _make_stocks([(date(2024, 3, 5), 185.0)])
        result = _build_feature_row(row, stocks)
        for feat in ALL_FEATURES:
            assert feat in result, f"Feature '{feat}' missing from _build_feature_row"

    @patch("model.predict._get_vix", return_value=18.5)
    @patch("model.predict._get_days_to_earnings", return_value=10)
    def test_no_extra_features(self, _earn, _vix):
        # Extra keys would be silently dropped by sklearn but indicate drift
        # between training and inference code
        row = _make_tweet("2024-03-05")
        stocks = _make_stocks([(date(2024, 3, 5), 185.0)])
        result = _build_feature_row(row, stocks)
        extra = set(result.keys()) - set(ALL_FEATURES)
        assert not extra, f"Unexpected extra features returned: {extra}"

    @patch("model.predict._get_vix", return_value=18.5)
    @patch("model.predict._get_days_to_earnings", return_value=10)
    def test_numeric_features_are_numeric_or_none(self, _earn, _vix):
        row = _make_tweet("2024-03-05")
        stocks = _make_stocks([(date(2024, 3, 5), 185.0)])
        result = _build_feature_row(row, stocks)
        for feat in NUMERIC_FEATURES:
            val = result[feat]
            assert val is None or isinstance(val, (int, float, np.integer, np.floating)), \
                f"Numeric feature '{feat}' has type {type(val).__name__}: {val!r}"

    @patch("model.predict._get_vix", return_value=18.5)
    @patch("model.predict._get_days_to_earnings", return_value=10)
    def test_categorical_features_are_strings(self, _earn, _vix):
        row = _make_tweet("2024-03-05")
        stocks = _make_stocks([(date(2024, 3, 5), 185.0)])
        result = _build_feature_row(row, stocks)
        for feat in CATEGORICAL_FEATURES:
            assert isinstance(result[feat], str), \
                f"Categorical feature '{feat}' is not a string: {result[feat]!r}"


# ── Weekend date shifting ─────────────────────────────────────────────────────

class TestWeekendShift:
    """
    Weekend tweets must shift to Monday for stock lookup.  This mirrors
    the same logic in run_pipeline.py — a regression in either place means
    some tweets get stock_close=0 at training time or wrong predictions at
    inference time, with no error raised.
    """

    @patch("model.predict._get_vix", return_value=15.0)
    @patch("model.predict._get_days_to_earnings", return_value=5)
    def test_saturday_uses_monday_stock_data(self, _earn, _vix):
        # 2024-01-06 is Saturday → should use Monday 2024-01-08
        monday = date(2024, 1, 8)
        stocks = _make_stocks([(monday, 190.0)])
        row = _make_tweet("2024-01-06 10:00:00")
        result = _build_feature_row(row, stocks)
        # If shift is broken, valid rows in stocks will be empty → rsi=None
        assert result["rsi_at_tweet"] == 55.0, \
            "Saturday tweet should resolve to Monday's stock row"

    @patch("model.predict._get_vix", return_value=15.0)
    @patch("model.predict._get_days_to_earnings", return_value=5)
    def test_sunday_uses_monday_stock_data(self, _earn, _vix):
        # 2024-01-07 is Sunday → should also use Monday 2024-01-08
        monday = date(2024, 1, 8)
        stocks = _make_stocks([(monday, 190.0)])
        row = _make_tweet("2024-01-07 10:00:00")
        result = _build_feature_row(row, stocks)
        assert result["rsi_at_tweet"] == 55.0, \
            "Sunday tweet should resolve to Monday's stock row"

    @patch("model.predict._get_vix", return_value=15.0)
    @patch("model.predict._get_days_to_earnings", return_value=5)
    def test_weekday_not_shifted(self, _earn, _vix):
        # 2024-01-05 is Friday — should stay on Friday, not advance to Monday
        friday = date(2024, 1, 5)
        stocks = _make_stocks([(friday, 185.0)])
        row = _make_tweet("2024-01-05 10:00:00")
        result = _build_feature_row(row, stocks)
        assert result["rsi_at_tweet"] == 55.0, \
            "Weekday tweet should not be shifted forward"


# ── RSI zone flags ────────────────────────────────────────────────────────────

class TestRsiFlags:
    def _build_with_rsi(self, rsi_val):
        stocks = pd.DataFrame([{
            "date_only": date(2024, 3, 5),
            "close": 185.0,
            "rsi_14": rsi_val,
            "atr_14": 3.0,
        }])
        row = _make_tweet("2024-03-05")
        with patch("model.predict._get_vix", return_value=15.0), \
             patch("model.predict._get_days_to_earnings", return_value=5):
            return _build_feature_row(row, stocks)

    def test_overbought_above_70(self):
        result = self._build_with_rsi(75.0)
        assert result["rsi_overbought"] == 1
        assert result["rsi_oversold"] == 0

    def test_oversold_below_30(self):
        result = self._build_with_rsi(25.0)
        assert result["rsi_oversold"] == 1
        assert result["rsi_overbought"] == 0

    def test_neither_flag_in_normal_range(self):
        result = self._build_with_rsi(50.0)
        assert result["rsi_overbought"] == 0
        assert result["rsi_oversold"] == 0

    def test_nan_rsi_fills_to_50_and_clears_both_flags(self):
        # NaN RSI → fill 50.0 → neither flag fires; rsi_at_tweet stored as None
        result = self._build_with_rsi(float("nan"))
        assert result["rsi_overbought"] == 0
        assert result["rsi_oversold"] == 0
        assert result["rsi_at_tweet"] is None


# ── Engagement features ───────────────────────────────────────────────────────

class TestEngagementFeatures:
    @patch("model.predict._get_vix", return_value=15.0)
    @patch("model.predict._get_days_to_earnings", return_value=5)
    def test_engagement_rate_formula(self, _earn, _vix):
        # (likes + retweets + replies) / views
        row = _make_tweet("2024-03-05", likes=100, retweet_count=50,
                          view_count=1000, reply_count=50)
        stocks = _make_stocks([(date(2024, 3, 5), 185.0)])
        result = _build_feature_row(row, stocks)
        assert abs(result["engagement_rate"] - (100 + 50 + 50) / 1000) < 1e-9

    @patch("model.predict._get_vix", return_value=15.0)
    @patch("model.predict._get_days_to_earnings", return_value=5)
    def test_zero_views_no_division_error(self, _earn, _vix):
        # view_count=0 → denominator clamped to 1
        row = _make_tweet("2024-03-05", likes=100, retweet_count=50,
                          view_count=0, reply_count=50)
        stocks = _make_stocks([(date(2024, 3, 5), 185.0)])
        result = _build_feature_row(row, stocks)
        assert result["engagement_rate"] == pytest.approx(200.0)

    @patch("model.predict._get_vix", return_value=15.0)
    @patch("model.predict._get_days_to_earnings", return_value=5)
    def test_log_transforms_non_negative_for_zero_counts(self, _earn, _vix):
        # log1p(0) = 0 — never negative
        row = _make_tweet("2024-03-05", likes=0, retweet_count=0,
                          view_count=0, reply_count=0)
        stocks = _make_stocks([(date(2024, 3, 5), 185.0)])
        result = _build_feature_row(row, stocks)
        for feat in ["log_likes", "log_retweets", "log_views", "log_replies"]:
            assert result[feat] >= 0, f"{feat} should be >= 0, got {result[feat]}"

    @patch("model.predict._get_vix", return_value=15.0)
    @patch("model.predict._get_days_to_earnings", return_value=5)
    def test_sentiment_magnitude_is_absolute_value(self, _earn, _vix):
        row_pos = _make_tweet("2024-03-05", sentiment=0.7)
        row_neg = _make_tweet("2024-03-05", sentiment=-0.7)
        stocks = _make_stocks([(date(2024, 3, 5), 185.0)])
        with patch("model.predict._get_vix", return_value=15.0), \
             patch("model.predict._get_days_to_earnings", return_value=5):
            res_pos = _build_feature_row(row_pos, stocks)
            res_neg = _build_feature_row(row_neg, stocks)
        assert res_pos["sentiment_magnitude"] == pytest.approx(0.7)
        assert res_neg["sentiment_magnitude"] == pytest.approx(0.7)


# ── Empty stocks edge case ────────────────────────────────────────────────────

class TestEmptyStocks:
    @patch("model.predict._get_vix", return_value=15.0)
    @patch("model.predict._get_days_to_earnings", return_value=5)
    def test_empty_stocks_returns_none_for_technical_features(self, _earn, _vix):
        # No stock data available — technical features should be None,
        # not raise an exception (SimpleImputer fills them at prediction time)
        row = _make_tweet("2024-03-05")
        result = _build_feature_row(row, pd.DataFrame())
        assert result["rsi_at_tweet"] is None
        assert result["atr_at_tweet"] is None
        assert result["prev_day_direction"] is None

    @patch("model.predict._get_vix", return_value=15.0)
    @patch("model.predict._get_days_to_earnings", return_value=5)
    def test_empty_stocks_still_returns_all_features(self, _earn, _vix):
        row = _make_tweet("2024-03-05")
        result = _build_feature_row(row, pd.DataFrame())
        for feat in ALL_FEATURES:
            assert feat in result, f"Feature '{feat}' missing when stocks_df is empty"


# ── prev_day_direction ────────────────────────────────────────────────────────

class TestPrevDayDirection:
    @patch("model.predict._get_vix", return_value=15.0)
    @patch("model.predict._get_days_to_earnings", return_value=5)
    def test_up_day_returns_1(self, _earn, _vix):
        # prev close=180, last close=190 → up → 1
        stocks = _make_stocks([
            (date(2024, 3, 3), 180.0),
            (date(2024, 3, 4), 190.0),
            (date(2024, 3, 5), 192.0),
        ])
        row = _make_tweet("2024-03-05")
        result = _build_feature_row(row, stocks)
        assert result["prev_day_direction"] == 1

    @patch("model.predict._get_vix", return_value=15.0)
    @patch("model.predict._get_days_to_earnings", return_value=5)
    def test_down_day_returns_0(self, _earn, _vix):
        # prev close=190, last close=180 → down → 0
        stocks = _make_stocks([
            (date(2024, 3, 3), 190.0),
            (date(2024, 3, 4), 180.0),
            (date(2024, 3, 5), 178.0),
        ])
        row = _make_tweet("2024-03-05")
        result = _build_feature_row(row, stocks)
        assert result["prev_day_direction"] == 0

    @patch("model.predict._get_vix", return_value=15.0)
    @patch("model.predict._get_days_to_earnings", return_value=5)
    def test_insufficient_history_returns_none(self, _earn, _vix):
        # Only one prior row — can't compute direction
        stocks = _make_stocks([(date(2024, 3, 5), 185.0)])
        row = _make_tweet("2024-03-05")
        result = _build_feature_row(row, stocks)
        assert result["prev_day_direction"] is None


# ── predict_tweets error handling ─────────────────────────────────────────────

class TestPredictTweetsMissingModel:
    def test_raises_file_not_found_when_no_model(self, tmp_path, monkeypatch):
        import model.predict as predict_module
        monkeypatch.setattr(predict_module, "MODEL_PATH", str(tmp_path / "nonexistent.pkl"))
        tweets = pd.DataFrame([{
            "date": pd.Timestamp("2024-03-05"),
            "text": "Hello world",
            "sentiment": 0.5,
            "likes": 100, "retweet_count": 10,
            "view_count": 1000, "reply_count": 5,
            "tweet_hour": 10, "is_premarket": 1,
        }])
        with pytest.raises(FileNotFoundError, match="No trained model found"):
            predict_tweets(tweets, pd.DataFrame())
