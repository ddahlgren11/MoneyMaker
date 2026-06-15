The 22 Features Going Into the Model
Sentiment

sentiment_score — VADER compound score (-1 to +1)
finbert_score — FinBERT financial-domain score (-1 to +1)
sentiment_magnitude — absolute value of VADER, captures extreme tweets regardless of direction
Tweet substance

tweet_length, word_count — longer more deliberate tweets carry different signal than short ones
Engagement (all log-scaled so one viral tweet doesn't dominate)

log_likes, log_retweets, log_views, log_replies
engagement_rate — (likes + retweets + replies) / views, normalized for audience size
Timing

tweet_hour — what hour it was posted
is_premarket — whether it was posted before NYSE open (9:30am ET)
Market state at tweet time

rsi_at_tweet — momentum indicator (0–100)
rsi_overbought — flag: RSI > 70
rsi_oversold — flag: RSI < 30
atr_at_tweet — how volatile the stock already was that day
vix_at_tweet — how fearful the overall market was
days_to_earnings — how close to a quarterly earnings report
news_sentiment_score — average headline sentiment for that ticker that day
prev_day_direction — did the stock go up or down yesterday (momentum)
Categorical (one-hot encoded)

refined_sentiment, tone_category, tweet_type