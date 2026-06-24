#!/usr/bin/env python3
"""
Event-study validation for structured signals (insider trades, congressional
trades, Reddit spikes).

The question this answers: *does a signal actually have edge before we trade it?*
For each event (ticker + date + predicted direction) it measures the forward
return over N trading days, subtracts the market's return over the same window
(SPY) to get the **abnormal return**, then orients it by the predicted direction
to get the **strategy return** (what you'd have made taking the bet). It reports
mean strategy return, hit rate, and a t-statistic per horizon and per group.

A signal type with a positive mean strategy return and a t-stat clearing ~2 is
worth trading; one that doesn't is noise — exactly the call to make before
flipping it live (e.g. whether to set INSIDER_BUYS_ONLY=false).

Usage:
    python3 event_study.py                       # insider_trades, horizons 1/3/5
    python3 event_study.py --source congress     # congressional_trades
    python3 event_study.py --source insider --group-by role
    python3 event_study.py --horizons 1 5 10 --since 2026-01-01

Pure read-only against the DB + yfinance; places no trades, writes nothing.
"""
import os
import argparse
from datetime import date

import numpy as np
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if DATABASE_URL.startswith("postgres://") and not DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
engine = create_engine(DATABASE_URL, pool_pre_ping=True) if DATABASE_URL else None

MARKET_BENCHMARK = "SPY"


# ---------------------------------------------------------------------------
# Event loading — each source maps to (ticker, date, direction, group) rows
# ---------------------------------------------------------------------------

def _load_events(source: str, since: str | None, group_by: str | None) -> pd.DataFrame:
    queries = {
        "insider": ("""
            SELECT ticker, disclosure_date AS event_date, direction,
                   role, value
            FROM insider_trades
            WHERE disclosure_date IS NOT NULL AND direction IN ('Up','Down')
        """),
        "congress": ("""
            SELECT ticker, disclosure_date AS event_date, direction,
                   chamber AS role, NULL AS value
            FROM congress_trades
            WHERE disclosure_date IS NOT NULL AND direction IN ('Up','Down')
        """),
        "reddit": ("""
            SELECT ticker, date AS event_date, direction,
                   NULL AS role, mention_count AS value
            FROM reddit_signals
            WHERE direction IN ('Up','Down')
        """),
    }
    if source not in queries:
        raise SystemExit(f"unknown source '{source}' (choose insider/congress/reddit)")

    sql = queries[source]
    if since:
        sql += " AND event_date >= :since"
    with engine.connect() as c:
        df = pd.read_sql(text(sql), c, params={"since": since} if since else None)

    df = df.dropna(subset=["ticker", "event_date", "direction"])
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.normalize()
    df["ticker"] = df["ticker"].str.upper().str.strip()

    # Default grouping: by direction. Optional: role, or value tercile.
    if group_by == "role":
        df["group"] = df["direction"] + " / " + df["role"].fillna("?").astype(str)
    elif group_by == "value" and df["value"].notna().any():
        df["group"] = df["direction"] + " / " + pd.qcut(
            df["value"].astype(float), 3, labels=["low", "mid", "high"], duplicates="drop"
        ).astype(str)
    else:
        df["group"] = df["direction"]
    return df


# ---------------------------------------------------------------------------
# Price fetch + forward-return computation
# ---------------------------------------------------------------------------

def _price_panel(tickers: list[str], start: date, end: date) -> dict[str, pd.Series]:
    """Download daily closes for tickers + benchmark; return {ticker: close series}."""
    syms = sorted(set(tickers) | {MARKET_BENCHMARK})
    raw = yf.download(syms, start=start.isoformat(), end=end.isoformat(),
                      progress=False, auto_adjust=True)["Close"]
    if isinstance(raw, pd.Series):  # single ticker → Series
        raw = raw.to_frame(syms[0])
    return {s: raw[s].dropna() for s in raw.columns}


def _forward_return(close: pd.Series, event_date: pd.Timestamp, horizon: int):
    """Return over `horizon` trading days from the first session >= event_date."""
    idx = close.index.searchsorted(event_date)
    if idx >= len(close) or idx + horizon >= len(close):
        return None
    entry, exit_ = close.iloc[idx], close.iloc[idx + horizon]
    if entry <= 0:
        return None
    return float(exit_ / entry - 1.0)


def run_event_study(events: pd.DataFrame, horizons: list[int]) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()

    pad = max(horizons) + 6
    start = events["event_date"].min().date() - pd.Timedelta(days=5)
    end   = events["event_date"].max().date() + pd.Timedelta(days=pad)
    panel = _price_panel(events["ticker"].tolist(), start, end)
    spy = panel.get(MARKET_BENCHMARK)

    records = []
    for _, ev in events.iterrows():
        close = panel.get(ev["ticker"])
        if close is None or close.empty:
            continue
        sign = 1.0 if ev["direction"] == "Up" else -1.0
        for h in horizons:
            r = _forward_return(close, ev["event_date"], h)
            if r is None:
                continue
            m = _forward_return(spy, ev["event_date"], h) if spy is not None else 0.0
            abn = r - (m or 0.0)
            records.append({
                "group": ev["group"], "horizon": h,
                "strategy_return": abn * sign,   # oriented by the bet
                "hit": 1 if abn * sign > 0 else 0,
            })

    res = pd.DataFrame(records)
    if res.empty:
        return res

    def _agg(g):
        n = len(g)
        mean = g["strategy_return"].mean()
        std = g["strategy_return"].std(ddof=1) if n > 1 else np.nan
        tstat = mean / (std / np.sqrt(n)) if std and std > 0 else np.nan
        return pd.Series({
            "n": n,
            "mean_ret_%": round(mean * 100, 3),
            "hit_rate_%": round(g["hit"].mean() * 100, 1),
            "t_stat": round(tstat, 2) if pd.notna(tstat) else np.nan,
        })

    return (res.groupby(["group", "horizon"], group_keys=True)
               .apply(_agg, include_groups=False)
               .reset_index())


def main():
    ap = argparse.ArgumentParser(description="Event-study edge validation for signals.")
    ap.add_argument("--source", default="insider", choices=["insider", "congress", "reddit"])
    ap.add_argument("--horizons", type=int, nargs="+", default=[1, 3, 5])
    ap.add_argument("--since", default=None, help="only events on/after YYYY-MM-DD")
    ap.add_argument("--group-by", default=None, choices=["role", "value"],
                    help="break results out by insider role or value tercile")
    args = ap.parse_args()

    if engine is None:
        raise SystemExit("DATABASE_URL not set.")

    events = _load_events(args.source, args.since, args.group_by)
    print(f"Loaded {len(events)} {args.source} event(s) "
          f"across {events['ticker'].nunique()} ticker(s).")
    if events.empty:
        return

    table = run_event_study(events, args.horizons)
    if table.empty:
        print("No events had enough forward price history to evaluate yet.")
        return

    print(f"\nEvent study — strategy return = abnormal return (vs {MARKET_BENCHMARK}), "
          f"oriented by predicted direction.\n"
          f"Positive mean + |t| >~ 2 ⇒ tradeable edge.\n")
    print(table.to_string(index=False))


if __name__ == "__main__":
    main()
