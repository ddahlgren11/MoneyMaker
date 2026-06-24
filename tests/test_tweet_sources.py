"""
Tests for tweet_sources.py — the free syndication backend's parsing. The network
call is monkeypatched so the __NEXT_DATA__ extraction, retweet skipping, and
timestamp parsing are exercised offline.
"""
import json
from datetime import timezone

import tweet_sources as ts


def _html_with(tweets):
    payload = {"props": {"pageProps": {"timeline": {"entries": [
        {"content": {"tweet": t}} for t in tweets
    ]}}}}
    return ('<html><script id="__NEXT_DATA__" type="application/json">'
            + json.dumps(payload)
            + "</script></html>").encode("utf-8")


class _FakeResp:
    def __init__(self, body): self._b = body
    def read(self): return self._b
    def __enter__(self): return self
    def __exit__(self, *a): return False


def _patch_fetch(monkeypatch, body):
    monkeypatch.setattr(ts.urllib.request, "urlopen", lambda *a, **k: _FakeResp(body))


def test_parses_basic_tweet(monkeypatch):
    body = _html_with([{
        "full_text": "Shipping something great today.",
        "created_at": "Mon Jun 15 15:08:54 +0000 2026",
        "favorite_count": 1234, "retweet_count": 56, "reply_count": 7,
    }])
    _patch_fetch(monkeypatch, body)
    rows = ts.fetch_syndication("someceo")
    assert len(rows) == 1
    r = rows[0]
    assert r["text"].startswith("Shipping")
    assert r["likes"] == 1234 and r["retweet_count"] == 56 and r["reply_count"] == 7
    assert r["view_count"] == 0  # not exposed by syndication
    assert r["created"].tzinfo == timezone.utc


def test_retweets_are_skipped(monkeypatch):
    body = _html_with([
        {"full_text": "RT @someone: not my words", "created_at": "Mon Jun 15 15:08:54 +0000 2026"},
        {"retweeted_status": {"id_str": "1"}, "full_text": "echo", "created_at": "Mon Jun 15 15:08:54 +0000 2026"},
        {"full_text": "my own original take", "created_at": "Mon Jun 15 15:08:54 +0000 2026"},
    ])
    _patch_fetch(monkeypatch, body)
    rows = ts.fetch_syndication("someceo")
    assert len(rows) == 1
    assert rows[0]["text"] == "my own original take"


def test_limit_respected(monkeypatch):
    body = _html_with([
        {"full_text": f"tweet {i}", "created_at": "Mon Jun 15 15:08:54 +0000 2026"}
        for i in range(10)
    ])
    _patch_fetch(monkeypatch, body)
    assert len(ts.fetch_syndication("x", limit=4)) == 4


def test_missing_next_data_returns_empty(monkeypatch):
    _patch_fetch(monkeypatch, b"<html>no script here</html>")
    assert ts.fetch_syndication("x") == []


def test_created_at_parsing():
    dt = ts._parse_created("Mon Jun 15 15:08:54 +0000 2026")
    assert dt.year == 2026 and dt.month == 6 and dt.day == 15 and dt.hour == 15
    assert ts._parse_created("garbage") is None
