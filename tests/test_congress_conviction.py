"""Tests for congress_conviction.py — amount parsing and conviction scoring."""
import congress_conviction as cc


def test_parse_amount_range_midpoint():
    assert cc.parse_amount_range("$1,001 - $15,000") == (1001 + 15000) / 2
    assert cc.parse_amount_range("$1,000,001 - $5,000,000") == (1000001 + 5000000) / 2


def test_parse_amount_single_value():
    assert cc.parse_amount_range("$50,000") == 50000.0


def test_parse_amount_unparseable():
    assert cc.parse_amount_range(None) == 0.0
    assert cc.parse_amount_range("") == 0.0
    assert cc.parse_amount_range("N/A") == 0.0


def test_conviction_in_unit_interval():
    for n in (0, 1, 2, 5, 20):
        for amt in (0, 15000, 250000, 5_000_000):
            s = cc.conviction_score(n, amt)
            assert 0.0 <= s <= 1.0


def test_conviction_rises_with_cluster():
    small = cc.conviction_score(1, 50_000)
    big = cc.conviction_score(4, 50_000)
    assert big > small


def test_conviction_rises_with_amount():
    small = cc.conviction_score(2, 15_000)
    big = cc.conviction_score(2, 5_000_000)
    assert big > small


def test_cluster_component_saturates():
    assert cc._cluster_component(1) == 0.4
    assert cc._cluster_component(4) == 1.0
    assert cc._cluster_component(10) == 1.0


def test_amount_component_bounds():
    assert cc._amount_component(0) == 0.4           # unknown
    assert cc._amount_component(15_000) == 0.3      # floor
    assert cc._amount_component(1_000_000) == 1.0   # ceiling
    mid = cc._amount_component(120_000)
    assert 0.3 < mid < 1.0


def test_strong_signal_scores_high():
    # 5 members + a $5M disclosure should score near the top.
    assert cc.conviction_score(5, 5_000_000) >= 0.9


def test_weak_signal_scores_low():
    # lone member, tiny disclosed amount
    assert cc.conviction_score(1, 5_000) <= 0.45
