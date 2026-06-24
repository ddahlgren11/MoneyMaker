"""Tests for risk_filters.py — bot/coordination, pump-and-dump, micro-cap guards."""
import risk_filters as rf


def test_is_bot_like_new_account():
    assert rf.is_bot_like(account_age_days=2) is True
    assert rf.is_bot_like(account_age_days=400) is False


def test_is_bot_like_high_cadence():
    assert rf.is_bot_like(account_age_days=400, posts_last_hour=50) is True


def test_is_bot_like_unknown_inputs_not_flagged():
    assert rf.is_bot_like(account_age_days=None) is False


def test_duplicate_ratio():
    assert rf.duplicate_ratio(["a", "b", "c"]) == 0.0
    # 3 of 4 are copies of "buy gme" → 2 duplicates / 4 = 0.5
    assert rf.duplicate_ratio(["buy GME", "buy gme", " buy  gme ", "hold"]) == 0.5


def test_looks_coordinated():
    assert rf.looks_coordinated(["x", "x", "x", "x"]) is True
    assert rf.looks_coordinated(["a", "b", "c", "d"]) is False


def test_detect_pump_dump_true_on_spike_then_reversal():
    # run up 100→150 (+50%) then collapse to 120 (−20% from peak)
    assert rf.detect_pump_dump([100, 120, 150, 135, 120]) is True


def test_detect_pump_dump_false_on_monotonic_rise():
    assert rf.detect_pump_dump([100, 110, 120, 130, 140]) is False


def test_detect_pump_dump_needs_min_length():
    assert rf.detect_pump_dump([100, 150]) is False


def test_is_micro_cap_by_market_cap():
    assert rf.is_micro_cap(market_cap=1e8) is True       # $100M < $300M floor
    assert rf.is_micro_cap(market_cap=5e9) is False


def test_is_micro_cap_by_dollar_volume_proxy():
    assert rf.is_micro_cap(avg_dollar_volume=1e6) is True
    assert rf.is_micro_cap(avg_dollar_volume=5e7) is False


def test_is_micro_cap_unknown_not_blocked():
    assert rf.is_micro_cap() is False
