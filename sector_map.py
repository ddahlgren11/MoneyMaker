"""
Sector matching + sentiment reactivity (Part I, "sector-matched > generic").

The brief's finding: sentiment's predictive power is sector-dependent —
Technology is the most sentiment-reactive, Financials respond to authoritative
voices (moderate), and Energy is fundamentals-driven (weak for direction,
sentiment helps volume/vol more). So a sentiment-derived signal on a tech name
deserves more weight than the same signal on an energy name.

This module reuses the existing ticker→sector-ETF map in context.py, maps the
ETF to a human sector, and attaches a 0–1 `reactivity` weight per the brief.
`signal_weight()` returns a multiplier intended for *sentiment* signals (tweets,
Reddit) only — structured signals (congress/insider) carry explicit direction
and are left unscaled.

Used as an optional conviction multiplier (gated by SECTOR_WEIGHTING_ENABLED in
the watcher) and as a grouping key in the backtest.
"""
import os

from context import get_sector_etf

SECTOR_WEIGHTING_ENABLED = os.getenv("SECTOR_WEIGHTING_ENABLED", "false").lower() == "true"

# Sector-SPDR ETF → human sector name.
_ETF_SECTOR = {
    "XLK": "Technology",
    "XLC": "Communication Services",
    "XLY": "Consumer Discretionary",
    "XLF": "Financials",
    "XLV": "Healthcare",
    "XLE": "Energy",
    "XLI": "Industrials",
    "XLB": "Materials",
    "XLP": "Consumer Staples",
    "XLU": "Utilities",
    "XLRE": "Real Estate",
    "SPY": "Broad Market",
    "TLT": "Rates/Treasuries",
}

# 0–1 sentiment→return reactivity, per the brief's sector ranking. Technology is
# the cleanest sentiment signal; Energy is weak for direction; Financials sit in
# between (authority-driven). Unlisted sectors get a neutral default.
_REACTIVITY = {
    "Technology":              1.00,
    "Communication Services":  0.80,
    "Consumer Discretionary":  0.70,
    "Financials":              0.60,
    "Healthcare":              0.50,
    "Industrials":             0.45,
    "Materials":               0.40,
    "Energy":                  0.30,
    "Consumer Staples":        0.35,
    "Utilities":               0.30,
    "Real Estate":             0.35,
    "Broad Market":            0.50,
    "Rates/Treasuries":        0.40,
}
_DEFAULT_REACTIVITY = 0.50


def sector_for_ticker(ticker: str) -> str:
    """Human sector name for a ticker (via its sector ETF). 'Unknown' if unmapped."""
    if not ticker:
        return "Unknown"
    return _ETF_SECTOR.get(get_sector_etf(ticker), "Unknown")


def reactivity(ticker: str) -> float:
    """0–1 sentiment reactivity weight for a ticker's sector."""
    return _REACTIVITY.get(sector_for_ticker(ticker), _DEFAULT_REACTIVITY)


def signal_weight(ticker: str, topic: str | None = None) -> float:
    """Conviction multiplier for a SENTIMENT signal on `ticker`.

    Structured, direction-explicit topics (congressional/insider trades) are not
    sentiment-driven, so they're returned unscaled (1.0). Sentiment topics are
    scaled by their sector's reactivity — Technology ~1.0 down to Energy ~0.3.
    """
    structured = {"congressional_trade", "insider_trade", "short_report"}
    if topic in structured:
        return 1.0
    return reactivity(ticker)
