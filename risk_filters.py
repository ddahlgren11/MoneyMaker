"""
Signal-quality risk filters (Part I, Stage 3 of the brief).

Defends the sentiment signal against the failure modes the brief flags:
  - Bots / pump-and-dump rings  → bot-like account heuristics + duplicate-text
    (coordination) detection.
  - Spike-then-reversal pumps    → detect a run-up followed by a sharp reversal.
  - Micro-cap manipulation risk  → avoid thinly-traded / tiny names.

All pure functions (no network) so they're unit-tested directly; reddit_ingest
applies the bot/duplicate checks at ingest time and the watcher/backtest can call
the micro-cap and pump-and-dump guards before acting on a ticker.
"""
from __future__ import annotations

import re
from collections import Counter

import pandas as pd


# ── Bots & coordination ─────────────────────────────────────────────────────

def is_bot_like(account_age_days: float | None,
                posts_last_hour: int | None = None,
                *, min_age_days: int = 14, max_cadence: int = 10) -> bool:
    """Heuristic bot flag: brand-new account or implausibly high posting cadence.

    Unknown inputs (None) are treated as 'not enough evidence' → not flagged,
    so missing metadata never silently drops real users.
    """
    if account_age_days is not None and account_age_days < min_age_days:
        return True
    if posts_last_hour is not None and posts_last_hour > max_cadence:
        return True
    return False


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def duplicate_ratio(texts: list[str]) -> float:
    """Fraction of posts that are copies of an already-seen post (0–1).

    High values indicate copy-paste coordination / botnets. A list of all-unique
    posts returns 0.0; N identical posts returns (N-1)/N.
    """
    if not texts:
        return 0.0
    seen: Counter = Counter(_norm(t) for t in texts)
    duplicates = sum(c - 1 for c in seen.values() if c > 1)
    return round(duplicates / len(texts), 4)


def looks_coordinated(texts: list[str], threshold: float = 0.5) -> bool:
    """True if the duplicate ratio across `texts` exceeds `threshold`."""
    return duplicate_ratio(texts) >= threshold


# ── Pump-and-dump (spike-then-reversal) ─────────────────────────────────────

def detect_pump_dump(closes: pd.Series | list[float],
                     run_up: float = 0.30, reversal: float = 0.15) -> bool:
    """Flag a spike-then-reversal pattern in a recent close series.

    True when the series ran up at least `run_up` from its start to an interior
    peak, then fell at least `reversal` from that peak to the end — the classic
    pump-and-dump shape the brief warns latecomers about.
    """
    s = pd.Series(list(closes), dtype="float64").dropna()
    if len(s) < 3:
        return False
    start = s.iloc[0]
    peak_idx = s.values.argmax()
    peak = s.iloc[peak_idx]
    end = s.iloc[-1]
    if start <= 0 or peak <= 0 or peak_idx == 0 or peak_idx == len(s) - 1:
        return False
    ran_up = (peak / start - 1.0) >= run_up
    reversed_ = (1.0 - end / peak) >= reversal
    return bool(ran_up and reversed_)


# ── Micro-cap / liquidity ───────────────────────────────────────────────────

def is_micro_cap(market_cap: float | None = None,
                 avg_dollar_volume: float | None = None,
                 *, min_cap: float = 3e8, min_dollar_vol: float = 5e6) -> bool:
    """True if a name is too small/illiquid to trade safely (avoid it).

    Uses market cap when available, else average daily dollar volume as a proxy.
    With neither available, returns False (can't tell → don't block).
    """
    if market_cap is not None:
        return market_cap < min_cap
    if avg_dollar_volume is not None:
        return avg_dollar_volume < min_dollar_vol
    return False
