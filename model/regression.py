"""
Regression model — predicts next-day abnormal return (stock return minus SPY return).

Why this instead of direction classification:
    - Direction is ~50% noise; magnitude has more structure.
    - Abnormal return strips out market-wide moves and isolates alpha.
    - Regression gives more info per sample than classification.

Usage:
    python3 model/regression.py                  # all tickers → reg_model.pkl
    python3 model/regression.py --ticker COIN    # single ticker → reg_model_COIN.pkl
    python3 model/regression.py --min-sentiment 0.15

Evaluation reports three layers:
    1. Regression quality  — R², MAE, Spearman rank correlation (IC)
    2. Direction accuracy   — converts predictions to Up/Down via threshold
    3. Trading-aware metrics — hit rate at top-K confidence, naive Sharpe
"""
import os
import sys
import argparse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from scipy.stats import spearmanr
import joblib

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from classifier import get_finbert_scores_batch
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, r2_score
import xgboost as xgb

parser = argparse.ArgumentParser()
parser.add_argument("--ticker", type=str, default=None,
                    help="Train on a single ticker only (e.g. COIN).")
parser.add_argument("--min-sentiment", type=float, default=0.0,
                    help="Drop rows where both |vader| and |finbert| are below this threshold.")
args = parser.parse_args()
TICKER_FILTER = args.ticker.upper() if args.ticker else None
MIN_SENTIMENT = args.min_sentiment

load_dotenv()

# ── Load data ─────────────────────────────────────────────────────────────────

engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

_ticker_clause = "AND stock_ticker = :ticker" if TICKER_FILTER else ""
_params = {"ticker": TICKER_FILTER} if TICKER_FILTER else {}

raw = pd.read_sql(
    text(f"""
        SELECT date, ceo, stock_ticker, tweet_text, sentiment_score,
               likes, retweet_count, view_count, reply_count,
               tweet_hour, is_premarket, refined_sentiment, tone_category, tweet_type,
               rsi_at_tweet, atr_at_tweet, vix_at_tweet, days_to_earnings,
               news_sentiment_score,
               return_1d, return_5d, return_20d,
               volume_ratio_20d,
               dist_from_52w_high, dist_from_52w_low,
               spy_return_same_day,
               abnormal_return_1d,
               next_day_direction,
               LAG(next_day_direction) OVER (
                   PARTITION BY stock_ticker ORDER BY date ASC
               ) AS prev_day_direction
        FROM merged_data
        WHERE abnormal_return_1d IS NOT NULL
        {_ticker_clause}
        ORDER BY date ASC
    """),
    engine,
    params=_params,
)

scope = f"ticker={TICKER_FILTER}" if TICKER_FILTER else "all tickers"
print(f"Loaded {len(raw)} raw rows  ({scope})")

if raw.empty:
    print("\nNo rows with abnormal_return_1d populated.")
    print("Re-run POST /process/all with the updated main.py to backfill the new columns.")
    sys.exit(0)

# ── FinBERT ───────────────────────────────────────────────────────────────────
print("Running FinBERT inference...")
finbert_df = get_finbert_scores_batch(raw['tweet_text'].tolist())
raw = pd.concat([raw.reset_index(drop=True), finbert_df], axis=1)
print(f"FinBERT done. abnormal_return_1d mean={raw['abnormal_return_1d'].mean():.4f}, "
      f"std={raw['abnormal_return_1d'].std():.4f}")

# ── Per-tweet text features ───────────────────────────────────────────────────
raw['tweet_length'] = raw['tweet_text'].str.len().fillna(0).astype(int)
raw['word_count']   = raw['tweet_text'].str.split().str.len().fillna(0).astype(int)

# ── Aggregate to (date, ticker, ceo) ──────────────────────────────────────────
def _mode_first(s):
    m = s.mode()
    return m.iloc[0] if not m.empty else s.iloc[0]

df = (
    raw.groupby(['date', 'stock_ticker', 'ceo'], sort=False)
    .agg(
        tweet_count        = ('sentiment_score', 'count'),
        sentiment_score    = ('sentiment_score', 'mean'),
        tweet_length       = ('tweet_length', 'mean'),
        word_count         = ('word_count', 'mean'),
        likes              = ('likes', 'sum'),
        retweet_count      = ('retweet_count', 'sum'),
        view_count         = ('view_count', 'sum'),
        reply_count        = ('reply_count', 'sum'),
        tweet_hour         = ('tweet_hour', 'min'),
        is_premarket       = ('is_premarket', 'max'),
        rsi_at_tweet       = ('rsi_at_tweet', 'first'),
        atr_at_tweet       = ('atr_at_tweet', 'first'),
        vix_at_tweet       = ('vix_at_tweet', 'first'),
        days_to_earnings   = ('days_to_earnings', 'first'),
        prev_day_direction = ('prev_day_direction', 'first'),
        news_sentiment_score = ('news_sentiment_score', 'first'),
        return_1d           = ('return_1d', 'first'),
        return_5d           = ('return_5d', 'first'),
        return_20d          = ('return_20d', 'first'),
        volume_ratio_20d    = ('volume_ratio_20d', 'first'),
        dist_from_52w_high  = ('dist_from_52w_high', 'first'),
        dist_from_52w_low   = ('dist_from_52w_low', 'first'),
        spy_return_same_day = ('spy_return_same_day', 'first'),
        finbert_score       = ('finbert_score', 'mean'),
        finbert_positive    = ('finbert_positive', 'mean'),
        finbert_negative    = ('finbert_negative', 'mean'),
        finbert_neutral     = ('finbert_neutral', 'mean'),
        refined_sentiment   = ('refined_sentiment', _mode_first),
        tone_category       = ('tone_category', _mode_first),
        tweet_type          = ('tweet_type', _mode_first),
        abnormal_return_1d  = ('abnormal_return_1d', 'first'),
        next_day_direction  = ('next_day_direction', 'first'),
    )
    .reset_index()
    .sort_values('date')
    .reset_index(drop=True)
)

print(f"After aggregation: {len(df)} rows  ({len(raw) - len(df)} duplicates removed)")

if MIN_SENTIMENT > 0:
    before = len(df)
    signal_mask = (df['sentiment_score'].abs() >= MIN_SENTIMENT) | \
                  (df['finbert_score'].abs() >= MIN_SENTIMENT)
    df = df[signal_mask].reset_index(drop=True)
    print(f"Sentiment filter: kept {len(df)} / {before}")

if len(df) < 50:
    print(f"Only {len(df)} rows — too few to train. Lower --min-sentiment or ingest more data.")
    sys.exit(0)

# ── Winsorize target to tame outliers ─────────────────────────────────────────
# Extreme abnormal returns are often one-off events that would dominate training.
# Cap at ±3 std to keep them without letting them swing the model.
target_std = df['abnormal_return_1d'].std()
target_cap = 3.0 * target_std
df['abnormal_return_1d'] = df['abnormal_return_1d'].clip(-target_cap, target_cap)
print(f"Target winsorized at ±{target_cap:.4f}  "
      f"(range: {df['abnormal_return_1d'].min():.4f} to {df['abnormal_return_1d'].max():.4f})")

# ── Feature engineering ───────────────────────────────────────────────────────
df['sentiment_magnitude'] = df['sentiment_score'].abs()
df['log_likes']     = np.log1p(df['likes'])
df['log_retweets']  = np.log1p(df['retweet_count'])
df['log_views']     = np.log1p(df['view_count'])
df['log_replies']   = np.log1p(df['reply_count'])
df['engagement_rate'] = (
    (df['likes'] + df['retweet_count'] + df['reply_count'])
    / df['view_count'].clip(lower=1)
)
df['rsi_overbought'] = (df['rsi_at_tweet'].fillna(50) > 70).astype(int)
df['rsi_oversold']   = (df['rsi_at_tweet'].fillna(50) < 30).astype(int)

numeric_features = [
    'sentiment_score', 'sentiment_magnitude',
    'tweet_length', 'word_count',
    'tweet_count',
    'log_likes', 'log_retweets', 'log_views', 'log_replies',
    'engagement_rate',
    'tweet_hour', 'is_premarket',
    'rsi_at_tweet', 'atr_at_tweet',
    'rsi_overbought', 'rsi_oversold',
    'vix_at_tweet',
    'days_to_earnings',
    'prev_day_direction',
    'news_sentiment_score',
    'finbert_score', 'finbert_positive', 'finbert_negative', 'finbert_neutral',
    'return_1d', 'return_5d', 'return_20d',
    'volume_ratio_20d',
    'dist_from_52w_high', 'dist_from_52w_low',
    'spy_return_same_day',
]
categorical_features = ['refined_sentiment', 'tone_category', 'tweet_type']

X = df[numeric_features + categorical_features]
y = df['abnormal_return_1d'].values.astype(float)
y_direction = (y > 0).astype(int)   # for direction-accuracy evaluation

# ── Sample weights: recency decay only ────────────────────────────────────────
# No class balancing needed — regression target is already continuous.
HALF_LIFE_DAYS = 180
reference_date = pd.to_datetime(df["date"]).max()
days_ago       = (reference_date - pd.to_datetime(df["date"])).dt.days.values
decay_rate     = np.log(2) / HALF_LIFE_DAYS
sample_weights = np.exp(-decay_rate * days_ago)
sample_weights = sample_weights / sample_weights.mean()

print(f"\nLabel stats — mean={y.mean():.5f}, std={y.std():.5f}, "
      f"pos ratio={y_direction.mean():.3f}")

# ── Preprocessing ─────────────────────────────────────────────────────────────
# XGBoost handles missing values natively, so no imputer needed for numeric.
# But we still need one-hot encoding for categoricals.
preprocessor = ColumnTransformer([
    ('num', 'passthrough', numeric_features),
    ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_features),
])

xgb_reg = xgb.XGBRegressor(
    n_estimators=400,
    learning_rate=0.03,
    max_depth=4,
    subsample=0.8,
    colsample_bytree=0.8,
    min_child_weight=3,
    reg_alpha=0.1,
    reg_lambda=1.0,
    random_state=42,
    objective='reg:squarederror',
    tree_method='hist',
)

pipeline = Pipeline([
    ('prep', preprocessor),
    ('model', xgb_reg),
])

# ── Walk-forward CV ───────────────────────────────────────────────────────────
tscv = TimeSeriesSplit(n_splits=5)

print("\n" + "=" * 60)
print("Walk-Forward CV (5 folds, time-ordered) — XGBoost Regressor")
print("=" * 60)
print(f"  {'Fold':<6} {'Train':>6} {'Test':>6} {'MAE':>8} {'R²':>8} {'IC':>8} {'DirAcc':>8}")

fold_mae, fold_r2, fold_ic, fold_dir_acc = [], [], [], []
for fold_idx, (train_idx, test_idx) in enumerate(tscv.split(X), start=1):
    X_tr, X_te = X.iloc[train_idx], X.iloc[test_idx]
    y_tr, y_te = y[train_idx], y[test_idx]
    w_tr = sample_weights[train_idx]

    pipeline.fit(X_tr, y_tr, model__sample_weight=w_tr)
    y_pred = pipeline.predict(X_te)

    mae = mean_absolute_error(y_te, y_pred)
    r2  = r2_score(y_te, y_pred)
    ic, _ = spearmanr(y_pred, y_te)
    dir_acc = ((y_pred > 0) == (y_te > 0)).mean()

    fold_mae.append(mae); fold_r2.append(r2); fold_ic.append(ic); fold_dir_acc.append(dir_acc)
    print(f"  {fold_idx:<6} {len(train_idx):>6} {len(test_idx):>6} "
          f"{mae:>8.4f} {r2:>8.3f} {ic:>8.3f} {dir_acc:>8.3f}")

print(f"  {'Mean':<6} {'':>6} {'':>6} "
      f"{np.mean(fold_mae):>8.4f} {np.mean(fold_r2):>8.3f} "
      f"{np.mean(fold_ic):>8.3f} {np.mean(fold_dir_acc):>8.3f}")

# ── Final holdout (most recent 20%) for confusion matrix + trading metrics ────
split_idx = int(len(df) * 0.8)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y[:split_idx], y[split_idx:]
w_train = sample_weights[:split_idx]
y_dir_test = y_direction[split_idx:]

pipeline.fit(X_train, y_train, model__sample_weight=w_train)
y_pred = pipeline.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
r2  = r2_score(y_test, y_pred)
ic, ic_p = spearmanr(y_pred, y_test)

print("\n" + "=" * 60)
print(f"Holdout (last {len(y_test)} rows)")
print("=" * 60)
print(f"  MAE:                   {mae:.4f}")
print(f"  R²:                    {r2:.3f}")
print(f"  Spearman IC:           {ic:.3f}  (p={ic_p:.3f})")
print(f"  Direction accuracy:    {((y_pred > 0) == y_dir_test).mean():.3f}")
print(f"  Naive direction accy:  {max(y_dir_test.mean(), 1 - y_dir_test.mean()):.3f}  (majority class)")

# ── Trading-aware metrics ────────────────────────────────────────────────────
# Sort predictions by absolute magnitude — the biggest-conviction bets.
# If the model has signal, those high-magnitude predictions should be right more often.
abs_pred = np.abs(y_pred)
order = np.argsort(-abs_pred)  # descending

print("\n" + "=" * 60)
print("Trading-Aware Evaluation")
print("=" * 60)
print("Hit rate = direction accuracy among top-K most confident predictions")
print(f"  {'Top-K':<10} {'Count':>6} {'Hit Rate':>10} {'Mean |pred|':>14} {'Mean actual':>14}")
for pct in [10, 20, 30, 50, 100]:
    k = max(1, int(len(y_pred) * pct / 100))
    top_idx = order[:k]
    hits = ((y_pred[top_idx] > 0) == y_dir_test[top_idx]).mean()
    mean_abs_pred = abs_pred[top_idx].mean()
    mean_actual = y_test[top_idx].mean()
    print(f"  {'top '+str(pct)+'%':<10} {k:>6} {hits:>10.3f} "
          f"{mean_abs_pred:>14.4f} {mean_actual:>+14.4f}")

# ── Trading simulation: long when pred > 0, short when pred < 0 ───────────────
# Gross strategy (no transaction costs). Sharpe gives risk-adjusted return.
signal = np.sign(y_pred)
strategy_returns = signal * y_test  # aligned returns from the bet
mean_ret = strategy_returns.mean()
std_ret  = strategy_returns.std() + 1e-9
sharpe_annualized = mean_ret / std_ret * np.sqrt(252)

# Buy-and-hold abnormal return benchmark
bh_mean = y_test.mean()
bh_std  = y_test.std() + 1e-9
bh_sharpe = bh_mean / bh_std * np.sqrt(252)

print("\n" + "=" * 60)
print("Strategy Simulation  (gross, no transaction costs)")
print("=" * 60)
print(f"  Long/short on pred sign:  mean={mean_ret:+.5f}  std={std_ret:.5f}  "
      f"Sharpe (ann.)={sharpe_annualized:+.2f}")
print(f"  Buy & hold benchmark:     mean={bh_mean:+.5f}  std={bh_std:.5f}  "
      f"Sharpe (ann.)={bh_sharpe:+.2f}")
print(f"  Strategy win rate:        {(strategy_returns > 0).mean():.3f}")

# ── Feature importance (XGBoost native) ───────────────────────────────────────
# Get feature names after preprocessing
prep = pipeline.named_steps['prep']
all_feature_names = [
    n.replace('num__', '').replace('cat__', '')
    for n in prep.get_feature_names_out()
]
inner = pipeline.named_steps['model']
importance_df = (
    pd.DataFrame({'feature': all_feature_names, 'importance': inner.feature_importances_})
    .sort_values('importance', ascending=False)
    .head(15)
    .reset_index(drop=True)
)
importance_df['importance'] = importance_df['importance'].map('{:.4f}'.format)
print("\n" + "=" * 60)
print("Top 15 Feature Importances (XGBoost gain)")
print("=" * 60)
print(importance_df.to_string(index=False))

# ── Save ──────────────────────────────────────────────────────────────────────
_suffix = f"_{TICKER_FILTER}" if TICKER_FILTER else ""
model_path = os.path.join(os.path.dirname(__file__), f"reg_model{_suffix}.pkl")
joblib.dump(pipeline, model_path)
print(f"\n  Saved XGBoost regressor to {model_path}")
