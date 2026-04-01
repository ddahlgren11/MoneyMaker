import os
import yfinance as yf
from newsapi import NewsApiClient
from datetime import timedelta, date
from dotenv import load_dotenv

load_dotenv()

_newsapi = NewsApiClient(api_key=os.getenv("NEWSAPI_KEY"))

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

def get_news_for_date(ticker, company_name, target_date):
    """
    Fetches news headlines for a ticker/company on a given date.
    target_date: a date object.
    Returns a list of dicts with title, source, url, publishedAt.
    NewsAPI free tier only supports articles up to 1 month old.
    """
    try:
        from_dt = (target_date - timedelta(days=1)).isoformat()
        to_dt   = (target_date + timedelta(days=1)).isoformat()

        response = _newsapi.get_everything(
            q=f"{ticker} OR {company_name}",
            from_param=from_dt,
            to=to_dt,
            language="en",
            sort_by="relevancy",
            page_size=5,
        )
        articles = response.get("articles", [])
        return [
            {
                "title": a["title"],
                "source": a["source"]["name"],
                "url": a["url"],
                "published": a["publishedAt"][:10],
            }
            for a in articles
            if a.get("title") and "[Removed]" not in a["title"]
        ]
    except Exception:
        return []

def get_news_for_range(ticker, company_name, start_date, end_date):
    """
    Fetches up to 20 news articles for a ticker over a date range.
    start_date / end_date: date objects.
    """
    try:
        response = _newsapi.get_everything(
            q=f"{ticker} OR {company_name}",
            from_param=start_date.isoformat(),
            to=end_date.isoformat(),
            language="en",
            sort_by="publishedAt",
            page_size=20,
        )
        articles = response.get("articles", [])
        return [
            {
                "title": a["title"],
                "source": a["source"]["name"],
                "url": a["url"],
                "published": a["publishedAt"][:10],
            }
            for a in articles
            if a.get("title") and "[Removed]" not in a["title"]
        ]
    except Exception:
        return []
