"""
Baseline model — predicts next-day stock direction (up=1, down=0) from tweet features.

Usage:
    python3 model/baseline.py

Models trained:
    1. Naive baseline  — always predicts the majority class (sets the floor)
    2. Logistic Regression — linear, interpretable
    3. Random Forest       — non-linear, gives feature importances

Split strategy: time-ordered 80/20 — train on older tweets, test on newer ones.
This is more realistic than a random split for financial data.
"""
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

load_dotenv()

# ── Load data ─────────────────────────────────────────────────────────────────

engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)
df = pd.read_sql(
    text("""
        SELECT date, sentiment_score, likes, retweet_count, view_count, reply_count,
               tweet_hour, is_premarket, refined_sentiment, tone_category, tweet_type,
               rsi_at_tweet, atr_at_tweet, news_sentiment_score, next_day_direction
        FROM merged_data
        WHERE next_day_direction IS NOT NULL
        ORDER BY date ASC
    """),
    engine,
)

print(f"Loaded {len(df)} labeled rows")
print(f"Label balance — Up: {(df.next_day_direction==1).sum()}  Down: {(df.next_day_direction==0).sum()}")
print()

# ── Features ──────────────────────────────────────────────────────────────────

numeric_features = [
    'sentiment_score', 'likes', 'retweet_count', 'view_count',
    'reply_count', 'tweet_hour', 'is_premarket',
    'rsi_at_tweet', 'atr_at_tweet', 'news_sentiment_score',
]
categorical_features = ['refined_sentiment', 'tone_category', 'tweet_type']

X = df[numeric_features + categorical_features]
y = df['next_day_direction']

# Time-ordered split — train on first 80%, test on most recent 20%
split_idx = int(len(df) * 0.8)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

print(f"Train: {len(X_train)} rows  |  Test: {len(X_test)} rows")
print()

# ── Preprocessing ─────────────────────────────────────────────────────────────

numeric_pipeline = Pipeline([
    ('impute', SimpleImputer(strategy='median')),  # fills NaN RSI/ATR with median
])

preprocessor = ColumnTransformer([
    ('num', numeric_pipeline, numeric_features),
    ('cat', OneHotEncoder(handle_unknown='ignore', sparse_output=False), categorical_features),
])

# ── Model 1: Naive baseline ───────────────────────────────────────────────────

majority_class = int(y_train.mode()[0])
majority_label = "Up" if majority_class == 1 else "Down"
naive_preds = [majority_class] * len(y_test)
naive_acc = accuracy_score(y_test, naive_preds)

print("=" * 50)
print(f"Naive Baseline (always predict '{majority_label}')")
print("=" * 50)
print(f"Accuracy: {naive_acc:.3f}  ← this is the floor to beat")
print()

# ── Model 2: Logistic Regression ──────────────────────────────────────────────

lr = Pipeline([
    ('prep', preprocessor),
    ('model', LogisticRegression(max_iter=1000, random_state=42)),
])
lr.fit(X_train, y_train)
lr_preds = lr.predict(X_test)
lr_acc = accuracy_score(y_test, lr_preds)

print("=" * 50)
print("Logistic Regression")
print("=" * 50)
print(f"Accuracy: {lr_acc:.3f}  ({'+' if lr_acc > naive_acc else ''}{lr_acc - naive_acc:.3f} vs baseline)")
print()
print(classification_report(y_test, lr_preds, target_names=['Down', 'Up']))
print("Confusion matrix (rows=actual, cols=predicted):")
print(pd.DataFrame(
    confusion_matrix(y_test, lr_preds),
    index=['Actual Down', 'Actual Up'],
    columns=['Pred Down', 'Pred Up']
))
print()

# ── Model 3: Random Forest ────────────────────────────────────────────────────

rf = Pipeline([
    ('prep', preprocessor),
    ('model', RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')),
])
rf.fit(X_train, y_train)
rf_preds = rf.predict(X_test)
rf_acc = accuracy_score(y_test, rf_preds)

print("=" * 50)
print("Random Forest")
print("=" * 50)
print(f"Accuracy: {rf_acc:.3f}  ({'+' if rf_acc > naive_acc else ''}{rf_acc - naive_acc:.3f} vs baseline)")
print()
print(classification_report(y_test, rf_preds, target_names=['Down', 'Up']))
print("Confusion matrix (rows=actual, cols=predicted):")
print(pd.DataFrame(
    confusion_matrix(y_test, rf_preds),
    index=['Actual Down', 'Actual Up'],
    columns=['Pred Down', 'Pred Up']
))
print()

# ── Feature importances (Random Forest) ──────────────────────────────────────

all_feature_names = [
    name.replace('num__pipeline__', '').replace('num__', '').replace('cat__', '')
    for name in rf.named_steps['prep'].get_feature_names_out()
]
importances = rf.named_steps['model'].feature_importances_

importance_df = (
    pd.DataFrame({'feature': all_feature_names, 'importance': importances})
    .sort_values('importance', ascending=False)
    .head(10)
    .reset_index(drop=True)
)
importance_df['importance'] = importance_df['importance'].map('{:.4f}'.format)

print("=" * 50)
print("Top 10 Feature Importances (Random Forest)")
print("=" * 50)
print(importance_df.to_string(index=False))
print()

# ── Summary ───────────────────────────────────────────────────────────────────

print("=" * 50)
print("Summary")
print("=" * 50)
print(f"  Naive baseline:      {naive_acc:.3f}")
print(f"  Logistic Regression: {lr_acc:.3f}")
print(f"  Random Forest:       {rf_acc:.3f}")

best_acc = max(lr_acc, rf_acc)
if best_acc > naive_acc + 0.05:
    print(f"\n  Best model beats the naive baseline by {best_acc - naive_acc:.3f} — real signal detected.")
elif best_acc > naive_acc:
    print(f"\n  Best model edges out the naive baseline by {best_acc - naive_acc:.3f} — weak signal.")
else:
    print(f"\n  Neither model beats the naive baseline — the current features may not have predictive power yet.")
    print("  Consider adding RSI/ATR state at tweet time and news sentiment (priorities #5 and #6).")
