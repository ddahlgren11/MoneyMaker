import os
import json
import logging
import asyncio
import pandas as pd
from datetime import datetime, timezone, timedelta
from twikit import Client as TwikitClient
from classifier import get_sentiment_score, get_finbert_scores_batch
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("processor")

COOKIES_PATH = os.path.join(os.path.dirname(__file__), "twitter_cookies.json")

# Tweet backend: "syndication" (free, no-auth public endpoint — the default now
# that cookies expire and freeze ingestion) or "twikit" (cookie auth, supports
# deeper pagination/backfill). Override with TWEET_SOURCE in the environment.
TWEET_SOURCE = os.getenv("TWEET_SOURCE", "syndication").lower()

# Market-data feed. Free / paper accounts only get IEX; SIP needs a paid
# subscription (a SIP request on a free key 403s "subscription does not permit").
# Default to IEX so the pipeline works on free keys; override with ALPACA_DATA_FEED=sip.
_DATA_FEED = DataFeed.SIP if os.getenv("ALPACA_DATA_FEED", "iex").lower() == "sip" else DataFeed.IEX


class TwitterAuthError(RuntimeError):
    """Raised when Twitter cookies are missing, malformed, or expired."""


def _load_twitter_cookies(client) -> bool:
    """
    Load and sanity-check twitter_cookies.json. Returns True only if usable
    cookies were loaded. Every failure path logs a clear, actionable message —
    a silent failure here is what froze tweet ingestion for months.
    """
    if not os.path.exists(COOKIES_PATH):
        log.warning(
            "Twitter cookies not found at %s — tweet fetching is DISABLED. "
            "Generate them with `python3 test_twitter_cookies.py <auth_token> <ct0>` "
            "(see README: Twitter cookie setup).", COOKIES_PATH,
        )
        return False
    try:
        with open(COOKIES_PATH) as f:
            cookies = json.load(f)
    except Exception as e:
        log.error("Twitter cookies at %s are not valid JSON (%s) — regenerate them.",
                  COOKIES_PATH, e)
        return False
    missing = [k for k in ("auth_token", "ct0") if not cookies.get(k)]
    if missing:
        log.error("Twitter cookies at %s missing required keys %s — regenerate them.",
                  COOKIES_PATH, missing)
        return False
    try:
        client.load_cookies(COOKIES_PATH)
    except Exception as e:
        log.error("twikit failed to load cookies from %s (%s) — regenerate them.",
                  COOKIES_PATH, e)
        return False
    log.info("Twitter cookies loaded from %s", COOKIES_PATH)
    return True


class DataProcessor:
    def __init__(self):
        self.twitter_client = TwikitClient("en-US")
        self.cookies_loaded = _load_twitter_cookies(self.twitter_client)
        # Market-data client works with either live or paper keys (free IEX feed).
        # Prefer live data keys (set in the retrain workflow); fall back to paper
        # keys (set in the watcher workflow) so both environments work.
        self.stock_client = StockHistoricalDataClient(
            os.getenv("ALPACA_API_KEY") or os.getenv("ALPACA_PAPER_API_KEY"),
            os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_PAPER_SECRET_KEY"),
        )

    @staticmethod
    def _safe_int(value):
        """Convert engagement counts to int, returning 0 for None/'Unavailable'/etc."""
        try:
            return int(value or 0)
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def _build_row(username, text, created, likes, retweets, views, replies):
        """Assemble one merged_data tweet row (shared by both backends)."""
        if created and created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        tweet_hour = created.hour if created else 0
        tweet_minute = created.minute if created else 0
        # NYSE opens 9:30 ET = 14:30 UTC, closes 16:00 ET = 21:00 UTC
        is_premarket = tweet_hour < 14 or (tweet_hour == 14 and tweet_minute < 30)
        return {
            'ceo': username,
            'text': text,
            'sentiment': get_sentiment_score(text),
            'date': created,
            'likes': DataProcessor._safe_int(likes),
            'retweet_count': DataProcessor._safe_int(retweets),
            'view_count': DataProcessor._safe_int(views),
            'reply_count': DataProcessor._safe_int(replies),
            'tweet_hour': tweet_hour,
            'is_premarket': is_premarket,
        }

    def _get_tweets_syndication(self, username, limit=100):
        """Free, no-auth backend: X's public syndication timeline endpoint."""
        from tweet_sources import fetch_syndication
        rows = [
            self._build_row(username, t['text'], t['created'], t['likes'],
                            t['retweet_count'], t['view_count'], t['reply_count'])
            for t in fetch_syndication(username, limit=limit)
            if (t['text'] or '').strip()
        ]
        df = pd.DataFrame(rows)
        if not df.empty:
            df['finbert_score'] = get_finbert_scores_batch(df['text'].tolist())
        return df

    async def get_tweets(self, username, pages=50):
        # Free, no-auth backend (default). Cookies expire and freeze ingestion,
        # so syndication is preferred unless TWEET_SOURCE=twikit is set.
        if TWEET_SOURCE == "syndication":
            return self._get_tweets_syndication(username, limit=20 * max(pages, 1))

        if not self.cookies_loaded:
            raise TwitterAuthError(
                "Twitter cookies missing or invalid — cannot fetch tweets. "
                "Regenerate twitter_cookies.json (see README: Twitter cookie setup), "
                "or use the default free backend (unset TWEET_SOURCE / set it to 'syndication')."
            )
        all_tweets = []
        try:
            user = await self.twitter_client.get_user_by_screen_name(username)
            result = await self.twitter_client.get_user_tweets(user.id, tweet_type="Tweets", count=20)
        except Exception as e:
            msg = str(e).lower()
            if any(s in msg for s in ("401", "unauthorized", "forbidden",
                                      "could not authenticate", "logged out", "denied")):
                raise TwitterAuthError(
                    f"Twitter auth rejected while fetching @{username} ({e}). "
                    "Cookies are likely expired — regenerate twitter_cookies.json."
                ) from e
            raise

        for page_num in range(pages):
            for tweet in result:
                # Skip retweets — they reflect someone else's words, not the CEO's
                if tweet.retweeted_tweet is not None:
                    continue
                all_tweets.append(self._build_row(
                    username, tweet.full_text or tweet.text, tweet.created_at_datetime,
                    tweet.favorite_count, tweet.retweet_count,
                    tweet.view_count, tweet.reply_count,
                ))

            if page_num < pages - 1:
                try:
                    result = await result.next()
                except Exception:
                    break  # no more pages

        df = pd.DataFrame(all_tweets)
        if not df.empty:
            df['finbert_score'] = get_finbert_scores_batch(df['text'].tolist())
        return df

    def get_market_context(self, ticker, start_date, end_date):
        """Fetch SPY and sector ETF bars for the same date range."""
        from context import get_sector_etf
        sector_etf = get_sector_etf(ticker)
        spy_df = self.get_stocks("SPY", start_date, end_date)
        sector_df = self.get_stocks(sector_etf, start_date, end_date)
        return spy_df, sector_df, sector_etf

    def get_stocks(self, symbol, start_date=None, end_date=None):
        if end_date is None:
            end_date = datetime.now(timezone.utc) - timedelta(days=1)
        if start_date is None:
            start_date = end_date - timedelta(days=7)

        request = StockBarsRequest(symbol_or_symbols=symbol, timeframe=TimeFrame.Day,
                                   start=start_date, end=end_date, feed=_DATA_FEED)
        bars = self.stock_client.get_stock_bars(request)
        return bars.df if bars else pd.DataFrame()
