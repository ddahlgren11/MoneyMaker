"""
Prediction helper used by the Streamlit UI.

Loads the trained model from trained_model.pkl and builds the same feature
vector that was used during training, so predictions are consistent.

Usage:
    from model.predict import predict_tweets
    result_df = predict_tweets(tweets_df, stocks_df)
"""
import os
import numpy as np
import pandas as pd
import joblib
import yfinance as yf
from datetime import timedelta, datetime
from classifier import get_refined_sentiment, get_tone_category, get_tweet_type
from context import get_earnings_dates

MODEL_PATH = os.path.join(os.path.dirname(__file__), "trained_model.pkl")


def _model_path_for(ticker=None):
    """Returns the ticker-specific model path if it exists, else the global model."""
    if ticker:
        specific = os.path.join(os.path.dirname(__file__), f"trained_model_{ticker.upper()}.pkl")
        if os.path.exists(specific):
            return specific
    return MODEL_PATH

_vix_cache = {}   # (start_iso, end_iso) → {date: float}
_earnings_cache = {}  # ticker → set of "YYYY-MM-DD"


def _get_vix(target_date):
    """Return VIX close for target_date, fetching from yfinance if needed."""
    key = target_date.isoformat()[:7]  # cache by month
    if key not in _vix_cache:
        import calendar
        year, month = target_date.year, target_date.month
        last_day = calendar.monthrange(year, month)[1]
        start = f"{year}-{month:02d}-01"
        end   = f"{year}-{month:02d}-{last_day}"
        try:
            vix = yf.download("^VIX", start=start, end=end,
                               auto_adjust=True, progress=False)
            if isinstance(vix.columns, pd.MultiIndex):
                vix.columns = vix.columns.get_level_values(0)
            _vix_cache[key] = {d.date(): float(v)
                               for d, v in zip(vix.index, vix["Close"])}
        except Exception:
            _vix_cache[key] = {}
    lookup = _vix_cache[key]
    val = lookup.get(target_date)
    if val is None:
        for offset in range(1, 4):
            val = lookup.get(target_date - timedelta(days=offset))
            if val is not None:
                break
    return val


def _get_days_to_earnings(ticker, target_date):
    """Return calendar days to nearest earnings date for ticker."""
    if ticker not in _earnings_cache:
        _earnings_cache[ticker] = get_earnings_dates(ticker)
    earnings_set = _earnings_cache[ticker]
    if not earnings_set:
        return None
    diffs = [abs((datetime.strptime(d, "%Y-%m-%d").date() - target_date).days)
             for d in earnings_set]
    return min(diffs)


def _build_feature_row(tweet_row, stocks_df, ticker=None, spy_lookup=None):
    """Builds one feature dict for a single tweet row."""
    tweet_date = tweet_row["date"]

    # Match weekend tweets to the following Monday (same logic as ingestion)
    target_date = tweet_date
    if hasattr(target_date, "weekday"):
        if target_date.weekday() == 5:
            target_date += timedelta(days=2)
        elif target_date.weekday() == 6:
            target_date += timedelta(days=1)

    target_date_only = target_date.date() if hasattr(target_date, "date") else target_date

    # Look up RSI, ATR, prev_day_direction, and Tier 1 features for this trading day
    rsi_at_tweet = None
    atr_at_tweet = None
    prev_day_direction = None
    return_1d = None
    return_5d = None
    return_20d = None
    volume_ratio_20d = None
    dist_from_52w_high = None
    dist_from_52w_low = None
    matched_stock_date = None
    if not stocks_df.empty and "date_only" in stocks_df.columns:
        valid = stocks_df[stocks_df["date_only"] >= target_date_only]
        if not valid.empty:
            rsi_val = valid["rsi_14"].iloc[0]
            atr_val = valid["atr_14"].iloc[0]
            rsi_at_tweet = float(rsi_val) if not pd.isna(rsi_val) else None
            atr_at_tweet = float(atr_val) if not pd.isna(atr_val) else None
            matched_stock_date = valid["date_only"].iloc[0]

            def _nn(col):
                if col not in valid.columns:
                    return None
                v = valid[col].iloc[0]
                return float(v) if not pd.isna(v) else None

            return_1d          = _nn("return_1d")
            return_5d          = _nn("return_5d")
            return_20d         = _nn("return_20d")
            volume_ratio_20d   = _nn("volume_ratio_20d")
            dist_from_52w_high = _nn("dist_from_52w_high")
            dist_from_52w_low  = _nn("dist_from_52w_low")

        # prev_day_direction: was the most recent completed trading day up or down?
        before = stocks_df[stocks_df["date_only"] < target_date_only].sort_values("date_only")
        if len(before) >= 2:
            last_close = float(before["close"].iloc[-1])
            prev_close = float(before["close"].iloc[-2])
            prev_day_direction = 1 if last_close > prev_close else 0

    spy_return_same_day = None
    if spy_lookup and matched_stock_date is not None:
        spy_return_same_day = spy_lookup.get(matched_stock_date)

    sentiment = float(tweet_row.get("sentiment", 0))
    text = str(tweet_row.get("text", ""))

    likes        = int(tweet_row.get("likes", 0))
    retweet_count = int(tweet_row.get("retweet_count", 0))
    view_count   = int(tweet_row.get("view_count", 0))
    reply_count  = int(tweet_row.get("reply_count", 0))

    engagement_rate = (likes + retweet_count + reply_count) / max(view_count, 1)
    rsi_fill = rsi_at_tweet if rsi_at_tweet is not None else 50.0

    return {
        "sentiment_score":      sentiment,
        "sentiment_magnitude":  abs(sentiment),
        "tweet_length":         len(text),
        "word_count":           len(text.split()),
        "log_likes":            float(np.log1p(likes)),
        "log_retweets":         float(np.log1p(retweet_count)),
        "log_views":            float(np.log1p(view_count)),
        "log_replies":          float(np.log1p(reply_count)),
        "engagement_rate":      engagement_rate,
        "tweet_hour":           int(tweet_row.get("tweet_hour", 0)),
        "is_premarket":         int(tweet_row.get("is_premarket", 0)),
        "rsi_at_tweet":         rsi_at_tweet,
        "atr_at_tweet":         atr_at_tweet,
        "rsi_overbought":       int(rsi_fill > 70),
        "rsi_oversold":         int(rsi_fill < 30),
        "vix_at_tweet":         _get_vix(target_date_only),
        "days_to_earnings":     _get_days_to_earnings(ticker, target_date_only) if ticker else None,
        "prev_day_direction":   prev_day_direction,
        "news_sentiment_score": None,  # fetched live via get_news_for_date if needed; imputer fills with median
        "return_1d":            return_1d,
        "return_5d":            return_5d,
        "return_20d":           return_20d,
        "volume_ratio_20d":     volume_ratio_20d,
        "dist_from_52w_high":   dist_from_52w_high,
        "dist_from_52w_low":    dist_from_52w_low,
        "spy_return_same_day":  spy_return_same_day,
        "refined_sentiment":    get_refined_sentiment(sentiment),
        "tone_category":        get_tone_category(text, sentiment),
        "tweet_type":           get_tweet_type(text),
    }


def predict_tweets(tweets_df, stocks_df, ticker=None, spy_lookup=None):
    """
    Runs the trained model on a DataFrame of tweets.

    Expects:
        tweets_df  — output of proc.get_tweets(), with sentiment/engagement columns
        stocks_df  — output of proc.get_stocks() with rsi_14, atr_14, return_*d,
                     volume_ratio_20d, dist_from_52w_high/low already computed
        ticker     — stock ticker (e.g. "TSLA") used to look up earnings dates and VIX
        spy_lookup — optional dict {date: SPY_return_1d} for same-day market context

    Returns tweets_df with two new columns added:
        predicted_direction  — "Up" or "Down"
        confidence_pct       — model's confidence as a percentage (e.g. 64.2)
    """
    resolved_path = _model_path_for(ticker)
    if not os.path.exists(resolved_path):
        raise FileNotFoundError(
            f"No trained model found at {resolved_path}. "
            "Run 'python3 model/baseline.py' first to train and save it."
        )

    model = joblib.load(resolved_path)

    feature_rows = [_build_feature_row(row, stocks_df, ticker=ticker, spy_lookup=spy_lookup)
                    for _, row in tweets_df.iterrows()]
    features_df = pd.DataFrame(feature_rows)

    predictions = model.predict(features_df)
    probabilities = model.predict_proba(features_df)

    result_df = tweets_df.copy()
    result_df["predicted_direction"] = ["Up" if p == 1 else "Down" for p in predictions]
    result_df["confidence_pct"] = [round(max(prob) * 100, 1) for prob in probabilities]

    return result_df


# ── Regression model support ─────────────────────────────────────────────────
# Separate path/loader for the XGBoost regressor trained by model/regression.py.
# Predicts abnormal_return_1d (stock next-day return minus SPY next-day return).

REG_MODEL_PATH = os.path.join(os.path.dirname(__file__), "reg_model.pkl")


def _reg_model_path_for(ticker=None):
    """Returns ticker-specific regression model if it exists, else the global one."""
    if ticker:
        specific = os.path.join(os.path.dirname(__file__), f"reg_model_{ticker.upper()}.pkl")
        if os.path.exists(specific):
            return specific
    return REG_MODEL_PATH


def predict_tweets_regression(tweets_df, stocks_df, ticker=None, spy_lookup=None, direction_threshold=0.0):
    """
    Runs the trained XGBoost regressor on a DataFrame of tweets.

    Predicts abnormal_return_1d — the stock's next-day return minus SPY's next-day
    return. This isolates CEO-tweet-driven alpha from broader market moves.

    Args:
        tweets_df:           proc.get_tweets() output
        stocks_df:           proc.get_stocks() output with Tier 1 features precomputed
        ticker:              stock ticker (e.g. "COIN")
        spy_lookup:          dict {date: SPY_return_1d} for same-day context feature
        direction_threshold: predicted magnitude above which we call "Up". Default 0.0.

    Returns tweets_df with three new columns:
        predicted_abnormal_return  — continuous, raw regressor output
        predicted_direction        — "Up" or "Down" based on threshold
        confidence_magnitude       — abs(predicted_abnormal_return) as a conviction score
    """
    resolved_path = _reg_model_path_for(ticker)
    if not os.path.exists(resolved_path):
        raise FileNotFoundError(
            f"No regression model found at {resolved_path}. "
            "Run 'python3 model/regression.py' first."
        )

    model = joblib.load(resolved_path)

    feature_rows = [_build_feature_row(row, stocks_df, ticker=ticker, spy_lookup=spy_lookup)
                    for _, row in tweets_df.iterrows()]
    features_df = pd.DataFrame(feature_rows)

    raw_preds = model.predict(features_df)

    result_df = tweets_df.copy()
    result_df["predicted_abnormal_return"] = raw_preds
    result_df["predicted_direction"] = ["Up" if p > direction_threshold else "Down" for p in raw_preds]
    result_df["confidence_magnitude"] = np.abs(raw_preds)

    return result_df
