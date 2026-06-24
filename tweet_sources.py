"""
Free, no-auth tweet sources — a durable replacement for the twikit cookie path.

The twikit backend authenticates with browser cookies that expire (on logout or
X's rotation), which silently froze ingestion for months. This module fetches the
same recent tweets from X's **public syndication endpoint** — the one that powers
embedded timeline widgets. It needs no login, no cookies, and no API key.

Tradeoffs vs. twikit:
  - Only the most recent ~100 tweets per handle (no deep pagination/backfill).
  - No view_count (syndication doesn't expose impressions) → reported as 0.
Everything else (text, timestamp, likes, retweets, replies) is present.

`fetch_syndication()` returns a list of raw tweet dicts; processor.py adds
sentiment, FinBERT, and the hour/premarket fields, so the merged_data shape is
identical regardless of which backend produced the tweet.
"""
import re
import json
import time
import logging
import urllib.request
import urllib.error
from datetime import datetime, timezone

log = logging.getLogger("tweet_sources")

_SYNDICATION_URL = "https://syndication.twitter.com/srv/timeline-profile/screen-name/{handle}"
_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', re.S)
_BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
               "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
_CREATED_FMT = "%a %b %d %H:%M:%S %z %Y"  # "Mon Jun 15 15:08:54 +0000 2026"


def _parse_created(raw: str) -> datetime | None:
    try:
        dt = datetime.strptime(raw, _CREATED_FMT)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None


def fetch_syndication(handle: str, limit: int = 100, timeout: int = 30) -> list[dict]:
    """Fetch recent original tweets for @handle from the public syndication feed.

    Returns a list of dicts: {text, created (tz-aware UTC datetime), likes,
    retweet_count, reply_count, view_count}. Retweets are skipped. Returns []
    (with a logged warning) on any fetch/parse failure — never raises.
    """
    url = _SYNDICATION_URL.format(handle=handle)
    req = urllib.request.Request(url, headers={"User-Agent": _BROWSER_UA,
                                               "Accept-Language": "en-US,en;q=0.9"})
    # The syndication endpoint throttles aggressively; retry 429s with backoff.
    html = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                html = r.read().decode("utf-8", "replace")
            break
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 2:
                time.sleep(2 * (attempt + 1))
                continue
            log.warning("syndication fetch failed for @%s: %s", handle, e)
            return []
        except Exception as e:
            log.warning("syndication fetch failed for @%s: %s", handle, e)
            return []
    if html is None:
        return []

    m = _NEXT_DATA_RE.search(html)
    if not m:
        log.warning("syndication: no __NEXT_DATA__ for @%s (layout changed or "
                    "account private/suspended)", handle)
        return []

    try:
        entries = (json.loads(m.group(1))["props"]["pageProps"]
                   ["timeline"]["entries"])
    except (json.JSONDecodeError, KeyError) as e:
        log.warning("syndication: could not parse timeline for @%s: %s", handle, e)
        return []

    out: list[dict] = []
    for entry in entries:
        tw = (entry.get("content") or {}).get("tweet")
        if not tw:
            continue
        text = tw.get("full_text") or tw.get("text") or ""
        # Skip retweets — they reflect someone else's words, not the author's.
        if tw.get("retweeted_status") or text.startswith("RT @"):
            continue
        out.append({
            "text": text,
            "created": _parse_created(tw.get("created_at", "")),
            "likes": int(tw.get("favorite_count") or 0),
            "retweet_count": int(tw.get("retweet_count") or 0),
            "reply_count": int(tw.get("reply_count") or 0),
            "view_count": 0,  # not exposed by syndication
        })
        if len(out) >= limit:
            break
    log.info("syndication: %d original tweet(s) for @%s", len(out), handle)
    return out
