"""
Prediction helper used by the Streamlit UI.

Loads the trained model from trained_model.pkl and builds the same feature
vector that was used during training, so predictions are consistent.

Usage:
    from model.predict import predict_tweets
    result_df = predict_tweets(tweets_df, stocks_df)
"""
import os
import pandas as pd
import joblib
from datetime import timedelta
from classifier import get_refined_sentiment, get_tone_category, get_tweet_type

MODEL_PATH = os.path.join(os.path.dirname(__file__), "trained_model.pkl")


def _build_feature_row(tweet_row, stocks_df):
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

    # Look up RSI and ATR for this trading day
    rsi_at_tweet = None
    atr_at_tweet = None
    if not stocks_df.empty and "date_only" in stocks_df.columns:
        valid = stocks_df[stocks_df["date_only"] >= target_date_only]
        if not valid.empty:
            rsi_val = valid["rsi_14"].iloc[0]
            atr_val = valid["atr_14"].iloc[0]
            rsi_at_tweet = float(rsi_val) if not pd.isna(rsi_val) else None
            atr_at_tweet = float(atr_val) if not pd.isna(atr_val) else None

    sentiment = float(tweet_row.get("sentiment", 0))
    text = str(tweet_row.get("text", ""))

    return {
        "sentiment_score":    sentiment,
        "likes":              int(tweet_row.get("likes", 0)),
        "retweet_count":      int(tweet_row.get("retweet_count", 0)),
        "view_count":         int(tweet_row.get("view_count", 0)),
        "reply_count":        int(tweet_row.get("reply_count", 0)),
        "tweet_hour":         int(tweet_row.get("tweet_hour", 0)),
        "is_premarket":       int(tweet_row.get("is_premarket", 0)),
        "rsi_at_tweet":       rsi_at_tweet,
        "atr_at_tweet":       atr_at_tweet,
        "news_sentiment_score": None,  # not available in real-time; imputer fills with median
        "refined_sentiment":  get_refined_sentiment(sentiment),
        "tone_category":      get_tone_category(text, sentiment),
        "tweet_type":         get_tweet_type(text),
    }


def predict_tweets(tweets_df, stocks_df):
    """
    Runs the trained model on a DataFrame of tweets.

    Expects:
        tweets_df  — output of proc.get_tweets(), with sentiment/engagement columns
        stocks_df  — output of proc.get_stocks() with rsi_14 and atr_14 already computed

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

    feature_rows = [_build_feature_row(row, stocks_df) for _, row in tweets_df.iterrows()]
    features_df = pd.DataFrame(feature_rows)

    predictions = model.predict(features_df)
    probabilities = model.predict_proba(features_df)

    result_df = tweets_df.copy()
    result_df["predicted_direction"] = ["Up" if p == 1 else "Down" for p in predictions]
    result_df["confidence_pct"] = [round(max(prob) * 100, 1) for prob in probabilities]

    return result_df
