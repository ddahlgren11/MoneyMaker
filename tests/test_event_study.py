"""
Tests for event_study.py — forward-return math and aggregation. The price panel
is built in-memory (no yfinance), so the abnormal-return orientation and t-stat
aggregation are verified deterministically.
"""
import numpy as np
import pandas as pd
import event_study as es


def _series(dates, prices):
    return pd.Series(prices, index=pd.to_datetime(dates))


def test_forward_return_basic():
    close = _series(["2026-06-01", "2026-06-02", "2026-06-03"], [100.0, 110.0, 121.0])
    # 1-day from first session: 100 -> 110 = +10%
    assert abs(es._forward_return(close, pd.Timestamp("2026-06-01"), 1) - 0.10) < 1e-9
    # 2-day: 100 -> 121 = +21%
    assert abs(es._forward_return(close, pd.Timestamp("2026-06-01"), 2) - 0.21) < 1e-9


def test_forward_return_uses_first_session_on_or_after_event():
    close = _series(["2026-06-01", "2026-06-04"], [100.0, 90.0])
    # event on a weekend (06-02) → entry rolls to 06-04
    assert es._forward_return(close, pd.Timestamp("2026-06-02"), 1) is None  # not enough history
    assert abs(es._forward_return(close, pd.Timestamp("2026-06-01"), 1) - (-0.10)) < 1e-9


def test_forward_return_none_when_insufficient_history():
    close = _series(["2026-06-01"], [100.0])
    assert es._forward_return(close, pd.Timestamp("2026-06-01"), 1) is None


def test_strategy_return_orientation(monkeypatch):
    """A Down (short) bet on a stock that falls should show a POSITIVE strategy return."""
    dates = ["2026-06-01", "2026-06-02"]
    panel = {
        "DOWN": _series(dates, [100.0, 90.0]),   # -10%
        "UP":   _series(dates, [100.0, 110.0]),  # +10%
        es.MARKET_BENCHMARK: _series(dates, [100.0, 100.0]),  # flat market
    }
    monkeypatch.setattr(es, "_price_panel", lambda *a, **k: panel)

    events = pd.DataFrame([
        {"ticker": "DOWN", "event_date": pd.Timestamp("2026-06-01"),
         "direction": "Down", "group": "Down"},
        {"ticker": "UP", "event_date": pd.Timestamp("2026-06-01"),
         "direction": "Up", "group": "Up"},
    ])
    out = es.run_event_study(events, [1]).set_index("group")
    # short on a -10% mover → +10% strategy return; long on +10% → +10%
    assert abs(out.loc["Down", "mean_ret_%"] - 10.0) < 1e-6
    assert abs(out.loc["Up", "mean_ret_%"] - 10.0) < 1e-6
    assert out.loc["Down", "hit_rate_%"] == 100.0
    assert out.loc["Up", "hit_rate_%"] == 100.0


def test_abnormal_return_subtracts_market(monkeypatch):
    dates = ["2026-06-01", "2026-06-02"]
    panel = {
        "AAA": _series(dates, [100.0, 105.0]),               # +5%
        es.MARKET_BENCHMARK: _series(dates, [100.0, 103.0]),  # +3%
    }
    monkeypatch.setattr(es, "_price_panel", lambda *a, **k: panel)
    events = pd.DataFrame([{"ticker": "AAA", "event_date": pd.Timestamp("2026-06-01"),
                            "direction": "Up", "group": "Up"}])
    out = es.run_event_study(events, [1]).set_index("group")
    # abnormal = 5% - 3% = 2%
    assert abs(out.loc["Up", "mean_ret_%"] - 2.0) < 1e-6


def test_empty_events_returns_empty():
    assert es.run_event_study(pd.DataFrame(), [1]).empty
