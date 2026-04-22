"""
Baseline model — predicts next-day stock direction (up=1, down=0) from tweet features.

Usage:
    python3 model/baseline.py                  # train on all CEOs → trained_model.pkl
    python3 model/baseline.py --ticker TSLA    # train on TSLA only → trained_model_TSLA.pkl

The Streamlit Predict tab loads the ticker-specific model if one exists,
falling back to trained_model.pkl for tickers without their own model.

Models trained:
    1. Naive baseline      — always predicts the majority class (sets the floor)
    2. Logistic Regression — linear, interpretable
    3. Random Forest       — non-linear, gives feature importances
    4. Gradient Boosting   — typically strongest on tabular data

Evaluation strategy: walk-forward cross-validation (TimeSeriesSplit, 5 folds).
Each fold trains on all data before its test window — no lookahead.
A single 80/20 split is reported afterward for confusion matrices and feature
importances, but model selection uses the CV mean so no single time window
can bias the choice.
"""
import os
import sys
import argparse
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import joblib

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from classifier import get_finbert_scores_batch

parser = argparse.ArgumentParser()
parser.add_argument("--ticker", type=str, default=None,
                    help="Train on a single ticker only (e.g. TSLA). Saves to trained_model_TSLA.pkl.")
parser.add_argument("--min-sentiment", type=float, default=0.0,
                    help="Drop rows where both |vader_score| and |finbert_score| are below this threshold. "
                         "Filters out neutral tweet-days that add noise. Try 0.15–0.25.")
args = parser.parse_args()
TICKER_FILTER   = args.ticker.upper() if args.ticker else None
MIN_SENTIMENT   = args.min_sentiment
from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.model_selection import TimeSeriesSplit, cross_val_score
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.calibration import CalibratedClassifierCV

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
               next_day_direction,
               LAG(next_day_direction) OVER (
                   PARTITION BY stock_ticker ORDER BY date ASC
               ) AS prev_day_direction
        FROM merged_data
        WHERE next_day_direction IS NOT NULL
        {_ticker_clause}
        ORDER BY date ASC
    """),
    engine,
    params=_params,
)

scope = f"ticker={TICKER_FILTER}" if TICKER_FILTER else "all tickers"
print(f"Loaded {len(raw)} raw tweet rows  ({scope})")

if raw.empty:
    print("\nNo labelled rows found (next_day_direction IS NULL for all records).")
    print("Run POST /process/all via the FastAPI backend to populate the database, then retry.")
    sys.exit(0)

# ── Compute per-tweet text features before aggregating ────────────────────────
raw['tweet_length'] = raw['tweet_text'].str.len().fillna(0).astype(int)
raw['word_count']   = raw['tweet_text'].str.split().str.len().fillna(0).astype(int)

print("Running FinBERT inference on tweet texts (batch)...")
finbert_df = get_finbert_scores_batch(raw['tweet_text'].tolist())
raw = pd.concat([raw.reset_index(drop=True), finbert_df], axis=1)
print(f"FinBERT done  score mean={raw['finbert_score'].mean():.3f}  "
      f"pos mean={raw['finbert_positive'].mean():.3f}  "
      f"neg mean={raw['finbert_negative'].mean():.3f}  "
      f"neu mean={raw['finbert_neutral'].mean():.3f}")
print()

# ── Aggregate to one row per (date, stock_ticker, ceo) ───────────────────────
# Multiple tweets from the same CEO on the same day all predict the same
# next_day_direction, so treating them as separate training examples inflates
# sample count and duplicates labels. Aggregate instead.
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
        tweet_hour         = ('tweet_hour', 'min'),   # earliest tweet of the day
        is_premarket       = ('is_premarket', 'max'), # 1 if any tweet was premarket
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
        finbert_score      = ('finbert_score', 'mean'),
        finbert_positive   = ('finbert_positive', 'mean'),
        finbert_negative   = ('finbert_negative', 'mean'),
        finbert_neutral    = ('finbert_neutral', 'mean'),
        refined_sentiment  = ('refined_sentiment', _mode_first),
        tone_category      = ('tone_category', _mode_first),
        tweet_type         = ('tweet_type', _mode_first),
        next_day_direction = ('next_day_direction', 'first'),
    )
    .reset_index()
    .sort_values('date')
    .reset_index(drop=True)
)

print(f"After daily aggregation: {len(df)} rows  ({len(raw) - len(df)} duplicates removed)")

if MIN_SENTIMENT > 0:
    before = len(df)
    # Keep rows where at least one of VADER or FinBERT shows strong signal
    signal_mask = (df['sentiment_score'].abs() >= MIN_SENTIMENT) | \
                  (df['finbert_score'].abs() >= MIN_SENTIMENT)
    df = df[signal_mask].reset_index(drop=True)
    print(f"Sentiment filter (|score| >= {MIN_SENTIMENT}): "
          f"kept {len(df)} / {before} rows  ({before - len(df)} neutral days dropped)")

print(f"Label balance — Up: {(df.next_day_direction==1).sum()}  Down: {(df.next_day_direction==0).sum()}")
print()

if len(df) < 50:
    print(f"Only {len(df)} rows after filtering — too few to train reliably. "
          "Lower --min-sentiment or fetch more data.")
    sys.exit(0)

# ── Feature engineering ───────────────────────────────────────────────────────

# Sentiment magnitude captures extreme tweets regardless of direction
df['sentiment_magnitude'] = df['sentiment_score'].abs()

# Log-transform skewed engagement counts so one viral tweet doesn't dominate
df['log_likes']     = np.log1p(df['likes'])
df['log_retweets']  = np.log1p(df['retweet_count'])
df['log_views']     = np.log1p(df['view_count'])
df['log_replies']   = np.log1p(df['reply_count'])

# Engagement rate normalises for audience size across CEOs
df['engagement_rate'] = (
    (df['likes'] + df['retweet_count'] + df['reply_count'])
    / df['view_count'].clip(lower=1)
)

# RSI zone flags — overbought/oversold are discrete regime signals
df['rsi_overbought'] = (df['rsi_at_tweet'].fillna(50) > 70).astype(int)
df['rsi_oversold']   = (df['rsi_at_tweet'].fillna(50) < 30).astype(int)

# ── Features ──────────────────────────────────────────────────────────────────

numeric_features = [
    'sentiment_score', 'sentiment_magnitude',
    'tweet_length', 'word_count',             # tweet substance
    'tweet_count',                            # how many times the CEO tweeted that day
    'log_likes', 'log_retweets', 'log_views', 'log_replies',
    'engagement_rate',
    'tweet_hour', 'is_premarket',
    'rsi_at_tweet', 'atr_at_tweet',
    'rsi_overbought', 'rsi_oversold',
    'vix_at_tweet',                            # market fear level on tweet day
    'days_to_earnings',                        # proximity to earnings amplifies tweet impact
    'prev_day_direction',                      # momentum — did stock go up or down yesterday
    'news_sentiment_score',                    # headline sentiment on tweet day
    'finbert_score',                           # finance-domain sentiment (FinBERT)
    'finbert_positive', 'finbert_negative', 'finbert_neutral',
    # Tier 1 — price/volume/market context
    'return_1d', 'return_5d', 'return_20d',    # stock's recent returns before tweet
    'volume_ratio_20d',                        # volume vs 20-day avg (unusual activity)
    'dist_from_52w_high', 'dist_from_52w_low', # position within 52-week range
    'spy_return_same_day',                     # same-day market move (beta separation)
]
categorical_features = ['refined_sentiment', 'tone_category', 'tweet_type']

X = df[numeric_features + categorical_features]
y = df['next_day_direction']

# ── Sample weights — recency decay × class balance ────────────────────────────
# Combines two signals:
#   1. Exponential decay (half-life 180 days): recent tweets matter more.
#   2. Class balance: Up and Down days get equal total weight so the model
#      doesn't learn to always predict the majority class.
# Applied uniformly to all models so GB gets the same balancing as LR/RF.
HALF_LIFE_DAYS = 180
reference_date  = pd.to_datetime(df["date"]).max()
days_ago        = (reference_date - pd.to_datetime(df["date"])).dt.days.values
decay_rate      = np.log(2) / HALF_LIFE_DAYS
recency_weights = np.exp(-decay_rate * days_ago)

class_counts      = y.value_counts()
class_weight_map  = {cls: len(y) / (2.0 * count) for cls, count in class_counts.items()}
balance_weights   = y.map(class_weight_map).values.astype(float)

sample_weights  = recency_weights * balance_weights
sample_weights  = sample_weights / sample_weights.mean()   # normalise: mean weight = 1

print(f"Label balance — Up: {(y==1).sum()}  Down: {(y==0).sum()}  "
      f"(class weight Up={class_weight_map[1]:.3f}  Down={class_weight_map[0]:.3f})")
print(f"Sample weights  min={sample_weights.min():.3f}  max={sample_weights.max():.3f}  "
      f"(half-life={HALF_LIFE_DAYS} days, class-balanced)")
print()

# ── Preprocessing ─────────────────────────────────────────────────────────────

# Two numeric pipelines: scaled (for LR) and unscaled (for tree models)
numeric_pipeline_scaled = Pipeline([
    ('impute', SimpleImputer(strategy='median')),
    ('scale', StandardScaler()),
])

numeric_pipeline_unscaled = Pipeline([
    ('impute', SimpleImputer(strategy='median')),
])

preprocessor_scaled = ColumnTransformer([
    ('num', numeric_pipeline_scaled, numeric_features),
    ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_features),
])

preprocessor_unscaled = ColumnTransformer([
    ('num', numeric_pipeline_unscaled, numeric_features),
    ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_features),
])

# ── Define pipelines (unfitted — CV fits them internally per fold) ────────────

lr = Pipeline([
    ('prep', preprocessor_scaled),
    ('model', LogisticRegression(max_iter=1000, random_state=42)),
])

rf = Pipeline([
    ('prep', preprocessor_unscaled),
    ('model', RandomForestClassifier(n_estimators=200, random_state=42)),
])

gb = Pipeline([
    ('prep', preprocessor_unscaled),
    ('model', GradientBoostingClassifier(
        n_estimators=200, learning_rate=0.05, max_depth=4,
        subsample=0.8, random_state=42,
    )),
])

models = {
    'Logistic Regression': lr,
    'Random Forest':       rf,
    'Gradient Boosting':   gb,
}

# ── Walk-forward cross-validation ────────────────────────────────────────────
# Each fold trains on all data before its test window — no lookahead.
# This is far more stable than a single 80/20 split; one hard market period
# can't swing the reported accuracy by 5+ points.

tscv = TimeSeriesSplit(n_splits=5)

naive_cv  = cross_val_score(DummyClassifier(strategy='most_frequent'),
                            X, y, cv=tscv, scoring='accuracy')

print("=" * 50)
print("Walk-Forward CV (5 folds, time-ordered)")
print("=" * 50)
fold_header = "  ".join([f"F{i+1}" for i in range(5)])
print(f"  {'Model':<25s}  Mean ±Std  {fold_header}")
print(f"  {'Naive baseline':<25s}  {naive_cv.mean():.3f} ±{naive_cv.std():.3f}  " +
      "  ".join([f"{s:.3f}" for s in naive_cv]))

cv_results = {}
for name, pipeline in models.items():
    scores = cross_val_score(pipeline, X, y, cv=tscv, scoring='accuracy')
    cv_results[name] = scores
    delta = scores.mean() - naive_cv.mean()
    fold_str = "  ".join([f"{s:.3f}" for s in scores])
    print(f"  {name:<25s}  {scores.mean():.3f} ±{scores.std():.3f}  {fold_str}  "
          f"({'+' if delta >= 0 else ''}{delta:.3f} vs naive)")

best_cv_name = max(cv_results, key=lambda n: cv_results[n].mean())
best_cv_mean = cv_results[best_cv_name].mean()
print()

# ── Final holdout evaluation (most recent 20%) ───────────────────────────────
# Used for confusion matrices and feature importances only.
# Model selection is based on CV above, not this single window.

split_idx = int(len(df) * 0.8)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
w_train = sample_weights[:split_idx]

majority_class = int(y_train.mode()[0])
naive_holdout  = accuracy_score(y_test, [majority_class] * len(y_test))

print(f"Train: {len(X_train)} rows  |  Holdout test: {len(X_test)} rows")
print()

for name, pipeline in models.items():
    pipeline.fit(X_train, y_train, model__sample_weight=w_train)
    preds = pipeline.predict(X_test)
    acc   = accuracy_score(y_test, preds)

    print("=" * 50)
    print(f"{name} — Holdout")
    print("=" * 50)
    print(f"Accuracy: {acc:.3f}  ({'+' if acc > naive_holdout else ''}{acc - naive_holdout:.3f} vs naive)  "
          f"CV mean: {cv_results[name].mean():.3f} ±{cv_results[name].std():.3f}")
    print()
    print(classification_report(y_test, preds, target_names=['Down', 'Up']))
    print("Confusion matrix (rows=actual, cols=predicted):")
    print(pd.DataFrame(
        confusion_matrix(y_test, preds),
        index=['Actual Down', 'Actual Up'],
        columns=['Pred Down', 'Pred Up']
    ))
    print()

# ── Feature importances (best CV model, if tree-based) ────────────────────────

best_pipeline = models[best_cv_name]
inner_model   = best_pipeline.named_steps['model']

if hasattr(inner_model, 'feature_importances_'):
    all_feature_names = [
        name.replace('num__pipeline__', '').replace('num__', '').replace('cat__', '')
        for name in best_pipeline.named_steps['prep'].get_feature_names_out()
    ]
    importance_df = (
        pd.DataFrame({'feature': all_feature_names,
                      'importance': inner_model.feature_importances_})
        .sort_values('importance', ascending=False)
        .head(10)
        .reset_index(drop=True)
    )
    importance_df['importance'] = importance_df['importance'].map('{:.4f}'.format)

    print("=" * 50)
    print(f"Top 10 Feature Importances ({best_cv_name})")
    print("=" * 50)
    print(importance_df.to_string(index=False))
    print()

# ── Summary ───────────────────────────────────────────────────────────────────

print("=" * 50)
print("Summary  (ranked by CV mean — the reliable number)")
print("=" * 50)
print(f"  {'Naive baseline':<25s}  CV {naive_cv.mean():.3f} ±{naive_cv.std():.3f}")
for name, scores in sorted(cv_results.items(), key=lambda x: -x[1].mean()):
    delta = scores.mean() - naive_cv.mean()
    print(f"  {name:<25s}  CV {scores.mean():.3f} ±{scores.std():.3f}  "
          f"({'+' if delta >= 0 else ''}{delta:.3f} vs naive)")

if best_cv_mean > naive_cv.mean() + 0.05:
    print(f"\n  {best_cv_name} beats naive by {best_cv_mean - naive_cv.mean():.3f} on average — real signal.")
elif best_cv_mean > naive_cv.mean():
    print(f"\n  {best_cv_name} edges out naive by {best_cv_mean - naive_cv.mean():.3f} on average — weak signal.")
else:
    print(f"\n  No model consistently beats naive — features may need more work.")

# ── Calibrate confidence scores ───────────────────────────────────────────────
# Sigmoid (Platt scaling) needs ~150+ training samples to work reliably.
# Below that threshold the calibration folds are too small and it degrades accuracy.
# In that case we skip calibration and use the raw model probabilities.

if len(X_train) >= 150:
    calibrated_model = CalibratedClassifierCV(
        best_pipeline, method='sigmoid', cv=TimeSeriesSplit(n_splits=5)
    )
    calibrated_model.fit(X_train, y_train, sample_weight=w_train)
    calibration_note = "calibrated (sigmoid)"
else:
    best_pipeline.fit(X_train, y_train, model__sample_weight=w_train)
    calibrated_model = best_pipeline
    calibration_note = f"uncalibrated (only {len(X_train)} training rows — calibration skipped)"

# Report accuracy on the holdout.
cal_probs = calibrated_model.predict_proba(X_test)
cal_preds = calibrated_model.predict(X_test)
cal_acc   = accuracy_score(y_test, cal_preds)
print("=" * 50)
print(f"{best_cv_name} — Holdout ({calibration_note})")
print("=" * 50)
print(f"Accuracy: {cal_acc:.3f}  ({'+' if cal_acc > naive_holdout else ''}{cal_acc - naive_holdout:.3f} vs naive)")
print(classification_report(y_test, cal_preds, target_names=['Down', 'Up']))

# ── Confidence bucket analysis ────────────────────────────────────────────────
# Are high-confidence predictions actually more accurate?
# If yes, the confidence score is meaningful and you can filter on it.

confidence = np.max(cal_probs, axis=1)
correct     = (cal_preds == y_test.values).astype(int)

buckets = [(0.50, 0.55), (0.55, 0.60), (0.60, 0.65), (0.65, 0.70), (0.70, 1.01)]
print("=" * 50)
print("Confidence Bucket Analysis")
print("=" * 50)
print(f"  {'Confidence':>15}  {'Count':>6}  {'% of test':>9}  {'Accuracy':>9}")
for lo, hi in buckets:
    mask = (confidence >= lo) & (confidence < hi)
    n = mask.sum()
    if n == 0:
        continue
    acc = correct[mask].mean()
    label = f"{lo*100:.0f}–{min(hi, 1.0)*100:.0f}%"
    print(f"  {label:>15}  {n:>6}  {n/len(y_test)*100:>8.1f}%  {acc:>9.3f}")
print(f"  {'Overall':>15}  {len(y_test):>6}  {'100.0':>9}%  {cal_acc:>9.3f}")
print()

# ── Save calibrated model ─────────────────────────────────────────────────────

_suffix = f"_{TICKER_FILTER}" if TICKER_FILTER else ""
model_path = os.path.join(os.path.dirname(__file__), f"trained_model{_suffix}.pkl")
joblib.dump(calibrated_model, model_path)
print(f"  Saved calibrated {best_cv_name} (CV mean={best_cv_mean:.3f}) to {model_path}")
