"""
Tests for reddit_ingest.py pure functions — ticker extraction and spike
detection. No network/PRAW/DB.
"""
import pandas as pd
from reddit_ingest import extract_tickers, detect_spikes

UNIVERSE = {"TSLA", "AAPL", "NVDA", "GME", "AMD", "MSFT"}


def test_cashtags_always_extracted():
    assert extract_tickers("loading up on $TSLA and $aapl", UNIVERSE) == {"TSLA", "AAPL"}


def test_bare_tokens_require_universe_membership():
    assert extract_tickers("NVDA and GME ripping", UNIVERSE) == {"NVDA", "GME"}
    # FOO isn't a real ticker → ignored
    assert extract_tickers("buy FOO now", UNIVERSE) == set()


def test_common_stopwords_not_treated_as_tickers():
    assert extract_tickers("the CEO did great DD on this IPO, AI is the future", UNIVERSE) == set()


def test_mixed_extraction():
    got = extract_tickers("CEO says $NVDA + AMD > FOMO", UNIVERSE)
    assert got == {"NVDA", "AMD"}


def _today(rows):
    return pd.DataFrame(rows)


def _history(pairs):
    return pd.DataFrame([{"ticker": t, "mention_count": c} for t, c in pairs])


def test_spike_detected_on_volume_and_bullish_sentiment():
    today = _today([{"ticker": "GME", "mention_count": 80, "avg_sentiment": 0.6}])
    hist = _history([("GME", 10), ("GME", 12), ("GME", 8)])
    out = detect_spikes(today, hist)
    assert len(out) == 1
    assert out.iloc[0]["direction"] == "Up"


def test_bearish_spike_is_down():
    today = _today([{"ticker": "AAPL", "mention_count": 40, "avg_sentiment": -0.5}])
    hist = _history([("AAPL", 5), ("AAPL", 6), ("AAPL", 4)])
    assert detect_spikes(today, hist).iloc[0]["direction"] == "Down"


def test_low_mention_count_skipped():
    today = _today([{"ticker": "TSLA", "mention_count": 3, "avg_sentiment": 0.9}])
    assert detect_spikes(today, _history([("TSLA", 1)])).empty


def test_neutral_sentiment_skipped_even_if_spiking():
    today = _today([{"ticker": "NVDA", "mention_count": 100, "avg_sentiment": 0.05}])
    hist = _history([("NVDA", 5), ("NVDA", 5), ("NVDA", 5)])
    assert detect_spikes(today, hist).empty


def test_no_baseline_treats_volume_as_spike():
    # Unknown ticker with no history but mentions >= min and clear sentiment.
    today = _today([{"ticker": "AMD", "mention_count": 20, "avg_sentiment": 0.7}])
    out = detect_spikes(today, _history([]))
    assert len(out) == 1 and out.iloc[0]["direction"] == "Up"


def test_empty_today_returns_empty():
    assert detect_spikes(pd.DataFrame(), _history([("GME", 5)])).empty
