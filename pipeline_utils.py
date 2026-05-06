"""
Shared helpers used by main.py, run_pipeline.py, and model/predict.py.
Centralizes logic that was previously duplicated across all three files.
"""
import pandas as pd
import yfinance as yf
from datetime import timedelta, datetime


def build_vix_lookup(start_date, end_date):
    """Returns {date: vix_close} for the given range, or {} on failure."""
    try:
        vix = yf.download("^VIX", start=start_date, end=end_date + timedelta(days=1),
                          auto_adjust=True, progress=False)
        if vix.empty:
            return {}
        if isinstance(vix.columns, pd.MultiIndex):
            vix.columns = vix.columns.get_level_values(0)
        return {d.date(): float(v) for d, v in zip(vix.index, vix["Close"])}
    except Exception:
        return {}


def days_to_nearest_earnings(target_date, earnings_set):
    """Returns calendar days to the nearest earnings date, or None if unknown."""
    if not earnings_set:
        return None
    if isinstance(target_date, datetime):
        target_date = target_date.date()
    diffs = [abs((datetime.strptime(d, "%Y-%m-%d").date() - target_date).days)
             for d in earnings_set]
    return min(diffs)


def shift_weekend_to_monday(dt):
    """If dt falls on Saturday or Sunday, advance it to the following Monday."""
    if dt.weekday() == 5:   # Saturday
        return dt + timedelta(days=2)
    if dt.weekday() == 6:   # Sunday
        return dt + timedelta(days=1)
    return dt


def compute_technicals(stocks_df):
    """
    Adds date_only, atr_14, and rsi_14 columns to stocks_df.
    Returns the modified DataFrame. No-op if stocks_df is empty.
    """
    if stocks_df.empty:
        return stocks_df

    stocks_df = stocks_df.sort_index()

    if isinstance(stocks_df.index, pd.MultiIndex):
        stocks_df["date_only"] = stocks_df.index.get_level_values("timestamp").date
    else:
        stocks_df["date_only"] = stocks_df.index.date

    # ATR (14-period)
    stocks_df["prev_close"] = stocks_df["close"].shift(1)
    stocks_df["tr"] = stocks_df[["high", "low", "prev_close"]].apply(
        lambda r: max(r["high"] - r["low"],
                      abs(r["high"] - r["prev_close"]),
                      abs(r["low"]  - r["prev_close"])), axis=1
    )
    stocks_df["atr_14"] = stocks_df["tr"].rolling(14).mean()

    # RSI (14-period)
    delta = stocks_df["close"].diff()
    gain  = delta.where(delta > 0, 0.0).rolling(14).mean()
    loss  = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
    stocks_df["rsi_14"] = 100 - (100 / (1 + gain / loss))

    return stocks_df
