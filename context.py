import os
import logging
import requests
import yfinance as yf
from datetime import timedelta, date
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

_FINNHUB_KEY = os.getenv("FINNHUB_API_KEY")
_AV_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

SECTOR_ETF_MAP = {
    "TSLA": "XLY",
    "AAPL": "XLK",
    "MSFT": "XLK",
    "GOOGL": "XLK",
    "GOOG":  "XLK",
    "DELL":  "XLK",
    "AMZN":  "XLY",
    "META":  "XLK",
    "NVDA":  "XLK",
    "AMD":   "XLK",
    "ABNB":  "XLY",
    "UBER":  "XLY",
    "DIS":   "XLY",
}

def get_sector_etf(ticker):
    return SECTOR_ETF_MAP.get(ticker.upper(), "SPY")

def get_earnings_dates(ticker):
    """Returns a set of earnings date strings (YYYY-MM-DD) for the past 2 years."""
    try:
        t = yf.Ticker(ticker)
        hist = t.earnings_dates
        if hist is None or hist.empty:
            return set()
        return set(hist.index.strftime("%Y-%m-%d").tolist())
    except Exception:
        return set()

def _finnhub_news(ticker, from_date, to_date, limit=5):
    """
    Fetches company news from Finnhub for the given ticker and date range.
    from_date / to_date: date objects.
    Returns a list of dicts with title, source, url, published keys.
    Finnhub supports historical news going back years — no 30-day limit.
    """
    if not _FINNHUB_KEY:
        return []
    try:
        resp = requests.get(
            "https://finnhub.io/api/v1/company-news",
            params={
                "symbol": ticker,
                "from":   from_date.strftime("%Y-%m-%d"),
                "to":     to_date.strftime("%Y-%m-%d"),
                "token":  _FINNHUB_KEY,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning("Finnhub %s [%s→%s]: HTTP %s — %s",
                           ticker, from_date, to_date, resp.status_code, resp.text[:200])
            return []
        articles = resp.json()
        if not isinstance(articles, list):
            logger.warning("Finnhub %s [%s→%s]: unexpected response type %s — %s",
                           ticker, from_date, to_date, type(articles).__name__, str(articles)[:200])
            return []
        logger.debug("Finnhub %s [%s→%s]: %d articles returned", ticker, from_date, to_date, len(articles))
        return [
            {
                "title":     a["headline"],
                "source":    a.get("source", ""),
                "url":       a.get("url", ""),
                "published": from_date.strftime("%Y-%m-%d"),
            }
            for a in articles[:limit]
            if a.get("headline")
        ]
    except Exception as exc:
        logger.error("Finnhub %s [%s→%s]: exception — %s", ticker, from_date, to_date, exc)
        return []


def get_news_for_date(ticker, company_name, target_date):
    """
    Fetches up to 5 news headlines for a ticker on a given date.
    target_date: a date object.
    Returns a list of dicts with title, source, url, published keys.
    """
    return _finnhub_news(
        ticker,
        from_date=target_date - timedelta(days=1),
        to_date=target_date + timedelta(days=1),
        limit=5,
    )


def get_news_for_range(ticker, company_name, start_date, end_date):
    """
    Fetches up to 20 news articles for a ticker over a date range.
    start_date / end_date: date objects.
    """
    return _finnhub_news(ticker, from_date=start_date, to_date=end_date, limit=20)


def _av_news_sentiment_lookup(ticker, start_date, end_date):
    """
    Fetches news sentiment from Alpha Vantage NEWS_SENTIMENT endpoint.
    Returns {date_str: avg_ticker_sentiment_score} or {} on failure.

    AV free tier: 25 req/day, up to 1000 articles per call.
    AV provides pre-computed per-ticker sentiment scores — no TextBlob needed.
    Scores are in [-1, 1]: negative → bearish, positive → bullish.
    """
    if not _AV_KEY:
        return {}
    try:
        resp = requests.get(
            "https://www.alphavantage.co/query",
            params={
                "function":  "NEWS_SENTIMENT",
                "tickers":   ticker,
                "time_from": start_date.strftime("%Y%m%dT0000"),
                "time_to":   end_date.strftime("%Y%m%dT2359"),
                "limit":     1000,
                "apikey":    _AV_KEY,
            },
            timeout=30,
        )
        if resp.status_code != 200:
            logger.warning("AlphaVantage %s: HTTP %s", ticker, resp.status_code)
            return {}
        data = resp.json()
        if "Note" in data or "Information" in data:
            logger.warning("AlphaVantage %s rate-limited: %s", ticker,
                           data.get("Note") or data.get("Information"))
            return {}

        feed = data.get("feed", [])
        logger.info("AlphaVantage %s [%s→%s]: %d articles",
                    ticker, start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), len(feed))

        from collections import defaultdict
        by_date = defaultdict(list)
        for article in feed:
            time_published = article.get("time_published", "")  # YYYYMMDDTHHMMSS
            if not time_published or len(time_published) < 8:
                continue
            day = f"{time_published[:4]}-{time_published[4:6]}-{time_published[6:8]}"

            # Prefer per-ticker sentiment over overall article sentiment
            score = None
            for ts in article.get("ticker_sentiment", []):
                if ts.get("ticker") == ticker:
                    try:
                        score = float(ts["ticker_sentiment_score"])
                    except (KeyError, ValueError, TypeError):
                        pass
                    break
            if score is None:
                try:
                    score = float(article.get("overall_sentiment_score", 0))
                except (ValueError, TypeError):
                    score = 0.0
            by_date[day].append(score)

        return {day: round(sum(scores) / len(scores), 4) for day, scores in by_date.items()}

    except Exception as exc:
        logger.error("AlphaVantage %s: exception — %s", ticker, exc)
        return {}


def build_news_sentiment_lookup(ticker, start_date, end_date, get_sentiment_fn):
    """
    Returns {date_str: avg_sentiment_score} for the given ticker and date range.

    Strategy (in order):
    1. Alpha Vantage NEWS_SENTIMENT — pre-built per-ticker scores, years of history,
       25 free calls/day. Set ALPHA_VANTAGE_API_KEY in .env to enable.
    2. Finnhub bulk — fallback; free tier limited to ~1 year of history.

    Dates not covered by either source will be absent (callers treat as None).
    start_date / end_date: date-like objects with .strftime().
    get_sentiment_fn: callable(text) -> float, used only for the Finnhub fallback.
    """
    # 1. Try Alpha Vantage first — better historical coverage
    if _AV_KEY:
        lookup = _av_news_sentiment_lookup(ticker, start_date, end_date)
        if lookup:
            return lookup
        logger.info("AlphaVantage %s returned no data, falling back to Finnhub", ticker)

    # 2. Finnhub fallback
    if not _FINNHUB_KEY:
        return {}
    try:
        resp = requests.get(
            "https://finnhub.io/api/v1/company-news",
            params={
                "symbol": ticker,
                "from":   start_date.strftime("%Y-%m-%d"),
                "to":     end_date.strftime("%Y-%m-%d"),
                "token":  _FINNHUB_KEY,
            },
            timeout=30,
        )
        if resp.status_code != 200:
            logger.warning("Finnhub bulk %s: HTTP %s — %s",
                           ticker, resp.status_code, resp.text[:200])
            return {}
        articles = resp.json()
        if not isinstance(articles, list):
            logger.warning("Finnhub bulk %s: unexpected response type %s",
                           ticker, type(articles).__name__)
            return {}

        logger.info("Finnhub bulk %s [%s→%s]: %d total articles",
                    ticker, start_date, end_date, len(articles))

        from collections import defaultdict
        from datetime import datetime as _dt
        by_date = defaultdict(list)
        for a in articles:
            headline = a.get("headline", "")
            ts = a.get("datetime")
            if not headline or not ts:
                continue
            day = _dt.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            by_date[day].append(headline)

        lookup = {}
        for day, headlines in by_date.items():
            scores = [get_sentiment_fn(h) for h in headlines]
            lookup[day] = round(sum(scores) / len(scores), 4)

        return lookup

    except Exception as exc:
        logger.error("Finnhub bulk %s: exception — %s", ticker, exc)
        return {}
