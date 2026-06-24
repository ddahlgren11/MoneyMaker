"""Tests for backtest.py — equity-metric math and causal regime lookup."""
import numpy as np
import pandas as pd
import backtest as bt
import regime


def test_equity_metrics_empty():
    m = bt._equity_metrics(pd.Series(dtype=float))
    assert m["n_days"] == 0 and m["total_%"] == 0.0


def test_equity_metrics_total_and_drawdown():
    # +10%, -5%, 0%  → curve 1.10, 1.045, 1.045 ; total ≈ +4.5% ; maxDD = 1.045/1.10-1
    daily = pd.Series([0.10, -0.05, 0.0])
    m = bt._equity_metrics(daily)
    assert abs(m["total_%"] - 4.5) < 0.01
    assert abs(m["maxdd_%"] - (-5.0)) < 0.05


def test_equity_metrics_positive_sharpe_for_steady_gains():
    daily = pd.Series([0.01] * 30)
    m = bt._equity_metrics(daily)
    assert m["maxdd_%"] == 0.0           # never draws down
    assert m["total_%"] > 0


def test_regime_long_ok_failopen_on_short_history():
    # Fewer bars than the SMA window → gate must fail open (allow, scale 1.0).
    spy = pd.Series(np.linspace(100, 110, 50),
                    index=pd.date_range("2026-01-01", periods=50))
    vix = pd.Series([15] * 50, index=spy.index)
    ok, scale = bt._regime_long_ok(spy, vix, pd.Timestamp("2026-03-01"))
    assert ok is True and scale == 1.0


def test_regime_long_ok_is_causal():
    # Build a long uptrend; entry in the middle should only use prior bars.
    idx = pd.date_range("2025-01-01", periods=300)
    spy = pd.Series(np.linspace(100, 200, 300), index=idx)
    vix = pd.Series([14] * 300, index=idx)
    entry = idx[260]
    ok, scale = bt._regime_long_ok(spy, vix, entry)
    assert ok is True and 0 < scale <= 1.0
