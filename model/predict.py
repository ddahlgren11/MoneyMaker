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
from classifier import get_refined_sentiment, get_tone_category, get_tweet_type, get_finbert_score
from context import get_earnings_dates

MODEL_PATH = os.path.join(os.path.dirname(__file__), "trained_model.pkl")

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


def _build_feature_row(tweet_row, stocks_df, ticker=None):
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

    # Look up RSI, ATR, and prev_day_direction for this trading day
    rsi_at_tweet = None
    atr_at_tweet = None
    prev_day_direction = None
    if not stocks_df.empty and "date_only" in stocks_df.columns:
        valid = stocks_df[stocks_df["date_only"] >= target_date_only]
        if not valid.empty:
            rsi_val = valid["rsi_14"].iloc[0]
            atr_val = valid["atr_14"].iloc[0]
            rsi_at_tweet = float(rsi_val) if not pd.isna(rsi_val) else None
            atr_at_tweet = float(atr_val) if not pd.isna(atr_val) else None

        # prev_day_direction: was the most recent completed trading day up or down?
        before = stocks_df[stocks_df["date_only"] < target_date_only].sort_values("date_only")
        if len(before) >= 2:
            last_close = float(before["close"].iloc[-1])
            prev_close = float(before["close"].iloc[-2])
            prev_day_direction = 1 if last_close > prev_close else 0

    sentiment = float(tweet_row.get("sentiment", 0))
    text = str(tweet_row.get("text", ""))
    finbert = tweet_row.get("finbert_score")
    if finbert is None:
        finbert = get_finbert_score(text)

    likes        = int(tweet_row.get("likes", 0))
    retweet_count = int(tweet_row.get("retweet_count", 0))
    view_count   = int(tweet_row.get("view_count", 0))
    reply_count  = int(tweet_row.get("reply_count", 0))

    engagement_rate = (likes + retweet_count + reply_count) / max(view_count, 1)
    rsi_fill = rsi_at_tweet if rsi_at_tweet is not None else 50.0

    return {
        "sentiment_score":      sentiment,
        "sentiment_magnitude":  abs(sentiment),
        "finbert_score":        float(finbert) if finbert is not None else None,
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
        "refined_sentiment":    get_refined_sentiment(sentiment),
        "tone_category":        get_tone_category(text, sentiment),
        "tweet_type":           get_tweet_type(text),
    }


def predict_tweets(tweets_df, stocks_df, ticker=None):
    """
    Runs the trained model on a DataFrame of tweets.

    Expects:
        tweets_df  — output of proc.get_tweets(), with sentiment/engagement columns
        stocks_df  — output of proc.get_stocks() with rsi_14 and atr_14 already computed
        ticker     — stock ticker (e.g. "TSLA") used to look up earnings dates and VIX

    Returns tweets_df with two new columns added:
        predicted_direction  — "Up" or "Down"
        confidence_pct       — model's confidence as a percentage (e.g. 64.2)
    """
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(
            f"No trained model found at {MODEL_PATH}. "
            "Run 'python3 model/baseline.py' first to train and save it."
        )

    model = joblib.load(MODEL_PATH)

    feature_rows = [_build_feature_row(row, stocks_df, ticker=ticker) for _, row in tweets_df.iterrows()]
    features_df = pd.DataFrame(feature_rows)

    predictions = model.predict(features_df)
    probabilities = model.predict_proba(features_df)

    result_df = tweets_df.copy()
    result_df["predicted_direction"] = ["Up" if p == 1 else "Down" for p in predictions]
    result_df["confidence_pct"] = [round(max(prob) * 100, 1) for prob in probabilities]

    return result_df
