"""Tests for sector_map.py — sector lookup, reactivity ordering, signal weighting."""
import sector_map as sm


def test_sector_for_ticker_tech():
    assert sm.sector_for_ticker("AAPL") == "Technology"
    assert sm.sector_for_ticker("NVDA") == "Technology"


def test_sector_for_ticker_comm_and_discretionary():
    assert sm.sector_for_ticker("NFLX") == "Communication Services"
    assert sm.sector_for_ticker("TSLA") == "Consumer Discretionary"


def test_unknown_ticker_falls_back_to_broad_market():
    assert sm.sector_for_ticker("ZZZZ") == "Broad Market"


def test_reactivity_tech_beats_energy_per_brief():
    assert sm._REACTIVITY["Technology"] > sm._REACTIVITY["Financials"]
    assert sm._REACTIVITY["Financials"] > sm._REACTIVITY["Energy"]


def test_reactivity_value_in_unit_interval():
    assert 0.0 < sm.reactivity("AAPL") <= 1.0


def test_signal_weight_structured_topics_unscaled():
    # Direction-explicit signals are not sentiment → no sector discount.
    assert sm.signal_weight("XOM", "congressional_trade") == 1.0
    assert sm.signal_weight("AAPL", "insider_trade") == 1.0


def test_signal_weight_sentiment_uses_reactivity():
    assert sm.signal_weight("AAPL", "company_ops") == sm.reactivity("AAPL")
