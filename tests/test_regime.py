"""
Tests for regime.py — the market-regime gate's pure computation. No network.
"""
import numpy as np
import pandas as pd
import regime as r


def _rising(n=260, lo=100, hi=200):
    return pd.Series(np.linspace(lo, hi, n))


def _falling(n=260, hi=200, lo=100):
    return pd.Series(np.linspace(hi, lo, n))


def test_trend_up_when_above_sma():
    state, dist = r.trend_state(_rising())
    assert state == "up" and dist > 0


def test_trend_down_when_below_sma():
    state, dist = r.trend_state(_falling())
    assert state == "down" and dist < 0


def test_trend_neutral_on_insufficient_history():
    assert r.trend_state(pd.Series([100, 101, 102]))[0] == "neutral"


def test_trend_confirmation_blocks_single_bar_blip():
    # Long flat series at the SMA, then one bar pops just above the band — not
    # enough to confirm an "up" with CONFIRM_DAYS=2.
    s = pd.Series([100.0] * 259 + [105.0])
    assert r.trend_state(s, band=0.01, confirm_days=2)[0] != "up"


def test_vix_state_thresholds():
    assert r.vix_state(pd.Series([10] * 260))[0] == "low"
    assert r.vix_state(pd.Series([17] * 260))[0] == "normal"
    assert r.vix_state(pd.Series([25] * 260))[0] == "elevated"
    assert r.vix_state(pd.Series([35] * 260))[0] == "crisis"


def test_vix_calm_score_monotonic():
    calm_low = r.vix_state(pd.Series([12] * 260))[1]
    calm_high = r.vix_state(pd.Series([28] * 260))[1]
    assert calm_low > calm_high


def test_gate_calm_uptrend_allows_long():
    g = r.compute_gate(_rising(), pd.Series([13] * 260))
    assert g["long_allowed"] is True
    assert g["short_allowed"] is True
    assert 0 < g["exposure_scale"] <= 1


def test_gate_downtrend_blocks_long_keeps_short():
    g = r.compute_gate(_falling(), pd.Series([18] * 260))
    assert g["long_allowed"] is False
    assert g["short_allowed"] is True
    assert g["exposure_scale"] == 0.0


def test_gate_crisis_vol_blocks_long_even_in_uptrend():
    g = r.compute_gate(_rising(), pd.Series([35] * 260))
    assert g["long_allowed"] is False  # crisis vol overrides an up-trend


def test_gate_for_direction_shorts_always_pass(monkeypatch):
    monkeypatch.setattr(r, "current_regime",
                        lambda asof=None: {"long_allowed": False, "exposure_scale": 0.0,
                                           "trend": "down", "vix": "elevated"})
    ok, scale, _ = r.gate_for_direction("Down")
    assert ok is True and scale == 1.0
    ok2, scale2, _ = r.gate_for_direction("Up")
    assert ok2 is False and scale2 == 0.0
