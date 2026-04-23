"""
Backfill finbert_score for all rows in merged_data that currently have NULL.
Runs FinBERT in batches of 64 to avoid OOM.
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from classifier import get_finbert_scores_batch

load_dotenv()

BATCH_SIZE = 64

engine = create_engine(os.getenv("DATABASE_URL"), pool_pre_ping=True)

with engine.connect() as conn:
    df = pd.read_sql(
        text("SELECT id, tweet_text FROM merged_data WHERE finbert_score IS NULL ORDER BY id"),
        conn,
    )

print(f"Rows to backfill: {len(df)}")
if df.empty:
    print("Nothing to do.")
    exit(0)

# Warm up the model before timing batches
print("Loading FinBERT model...")
_ = get_finbert_scores_batch(["warmup"])
print("Model loaded.")

total_updated = 0
for start in range(0, len(df), BATCH_SIZE):
    batch = df.iloc[start : start + BATCH_SIZE]
    scores = get_finbert_scores_batch(batch["tweet_text"].tolist())

    rows = [{"id": int(row["id"]), "score": scores[i]}
            for i, (_, row) in enumerate(batch.iterrows())
            if scores[i] is not None]

    if rows:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE merged_data SET finbert_score = :score WHERE id = :id"),
                rows,
            )
        total_updated += len(rows)

    done = min(start + BATCH_SIZE, len(df))
    print(f"  {done}/{len(df)} processed, {total_updated} updated so far")

print(f"\nDone — updated {total_updated} rows.")
