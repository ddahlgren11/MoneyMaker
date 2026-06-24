"""
Market-regime gate (Part II of the sentiment/regime research brief).

A top-level GATE that decides whether the sentiment RANKER (the existing signal
pipeline) may go long, short, or flat — sitting *on top of* the per-signal logic,
not replacing it. Per the brief, the gate "buys insurance, not alpha": its job is
drawdown reduction via bear-market avoidance.

Two layers, in the order the brief prescribes:
  1. Trend filter — SPY vs its 200-day SMA, evaluated on the PRIOR close (no
     look-ahead), with a ±band and an N-day confirmation to damp whipsaws.
  2. Volatility filter — VIX regime (absolute level + VIX vs its own 200-day MA).
     In a crisis-vol state, exposure is cut even when the trend says long-allowed
     (targets the Daniel-Moskowitz "momentum crash" panic state).

Output is a gate dict with `long_allowed` / `short_allowed` booleans and a
continuous `exposure_scale` in [0, 1] — the brief's "size by regime confidence,
not binary flips."

Project policy (chosen for this build): the gate constrains LONG entries only;
short signals (congressional/insider sales) are left to fire regardless of
regime. And the whole gate is inert unless REGIME_GATE_ENABLED=true — it is
validated via backtest.py before it touches live trades.

The compute_* functions are pure (take price Series, return states) so they're
unit-tested without any network; only current_regime() hits yfinance.
"""
import os
import logging
from datetime import date

import pandas as pd

log = logging.getLogger("regime")

# ── Tunables (env-overridable) ──────────────────────────────────────────────
SMA_WINDOW       = int(os.getenv("REGIME_SMA_WINDOW", "200"))
TREND_BAND       = float(os.getenv("REGIME_TREND_BAND", "0.01"))   # ±1% dead-band (Siegel)
CONFIRM_DAYS     = int(os.getenv("REGIME_CONFIRM_DAYS", "2"))      # consecutive closes to flip
VIX_CRISIS       = float(os.getenv("REGIME_VIX_CRISIS", "30"))
VIX_ELEVATED     = float(os.getenv("REGIME_VIX_ELEVATED", "20"))
REGIME_GATE_ENABLED = os.getenv("REGIME_GATE_ENABLED", "false").lower() == "true"

_BENCHMARK = "SPY"
_VIX       = "^VIX"


# ── Pure computation ────────────────────────────────────────────────────────

def trend_state(closes: pd.Series, band: float = TREND_BAND,
                confirm_days: int = CONFIRM_DAYS, window: int = SMA_WINDOW):
    """Classify the trend from a close series whose LAST element is the prior
    close (caller is responsible for lagging — no look-ahead here).

    Returns (state, dist_frac) where state ∈ {"up","down","neutral"} and
    dist_frac is (close − SMA)/SMA at the as-of bar (signed distance from the line).
    """
    closes = closes.dropna()
    if len(closes) < window + confirm_days:
        return "neutral", 0.0
    sma = closes.rolling(window).mean()
    dist = (closes - sma) / sma
    asof = dist.iloc[-1]
    # Require `confirm_days` consecutive closes on the same side of the band.
    recent = dist.iloc[-confirm_days:]
    if (recent > band).all():
        return "up", float(asof)
    if (recent < -band).all():
        return "down", float(asof)
    return "neutral", float(asof)


def vix_state(vix_closes: pd.Series, crisis: float = VIX_CRISIS,
              elevated: float = VIX_ELEVATED, window: int = SMA_WINDOW):
    """Classify the volatility regime from a VIX close series (last = as-of bar).

    Returns (label, calm_score) where label ∈ {"low","normal","elevated","crisis"}
    and calm_score ∈ [0,1] (1 = calmest). calm_score blends the absolute level and
    VIX-vs-its-own-MA so it degrades smoothly rather than snapping at thresholds.
    """
    vix_closes = vix_closes.dropna()
    if vix_closes.empty:
        return "normal", 0.5
    level = float(vix_closes.iloc[-1])
    if level >= crisis:
        label = "crisis"
    elif level >= elevated:
        label = "elevated"
    elif level >= 15:
        label = "normal"
    else:
        label = "low"
    # calm_score: 1 at VIX<=12, 0 at VIX>=crisis, linear between; nudged by
    # whether VIX sits above its own long MA (regime-relative stress).
    calm = max(0.0, min(1.0, (crisis - level) / (crisis - 12.0)))
    if len(vix_closes) >= window:
        ma = vix_closes.rolling(window).mean().iloc[-1]
        if pd.notna(ma) and level > ma:
            calm *= 0.7  # above its own MA → discount calmness
    return label, float(calm)


def compute_gate(spy_closes: pd.Series, vix_closes: pd.Series,
                 band: float = TREND_BAND, confirm_days: int = CONFIRM_DAYS,
                 window: int = SMA_WINDOW) -> dict:
    """Combine the trend and volatility layers into a gate decision.

    long_allowed  : trend is up (above the +band, confirmed) and not in crisis vol.
    short_allowed : always True under this project's policy (shorts ungated).
    exposure_scale: 0–1 conviction for position sizing — blends trend distance and
                    VIX calm; halved in a crisis-vol regime; ~0 when flat/neutral.
    """
    tstate, dist = trend_state(spy_closes, band, confirm_days, window)
    vlabel, calm = vix_state(vix_closes, window=window)

    # Trend confidence: saturating function of distance above the band (cap ~10%).
    trend_conf = max(0.0, min((dist - band) / 0.10, 1.0)) if tstate == "up" else 0.0
    exposure = round(0.5 * trend_conf + 0.5 * calm, 3)
    if vlabel == "crisis":
        exposure = round(exposure * 0.5, 3)  # Daniel-Moskowitz panic-state haircut

    long_allowed = (tstate == "up") and (vlabel != "crisis")
    return {
        "trend": tstate,
        "trend_dist": round(dist, 4),
        "vix": vlabel,
        "calm": round(calm, 3),
        "long_allowed": long_allowed,
        "short_allowed": True,            # project policy: shorts ungated
        "exposure_scale": exposure if long_allowed else 0.0,
    }


# ── Fetch layer (yfinance) + per-day cache ──────────────────────────────────

_cache: dict[str, dict] = {}


def _fetch_closes(symbol: str, lookback_days: int = 420) -> pd.Series:
    import yfinance as yf
    end = pd.Timestamp.utcnow().normalize()
    start = end - pd.Timedelta(days=lookback_days)
    df = yf.download(symbol, start=start.date().isoformat(), end=end.date().isoformat(),
                     progress=False, auto_adjust=True)
    if df.empty:
        return pd.Series(dtype=float)
    s = df["Close"]
    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]
    return s.dropna()


def current_regime(asof: date | None = None) -> dict:
    """Fetch SPY + VIX and compute today's gate, lagging to the PRIOR close.

    Cached per UTC date so repeated watcher poll cycles don't refetch. Fails
    safe: on any data error, returns an all-permissive gate (so a yfinance
    outage never silently halts trading).
    """
    key = (asof or date.today()).isoformat()
    if key in _cache:
        return _cache[key]
    try:
        spy = _fetch_closes(_BENCHMARK)
        vix = _fetch_closes(_VIX)
        if spy.empty:
            raise RuntimeError("no SPY data")
        # Lag one bar: decisions use the prior completed close, never today's.
        gate = compute_gate(spy.iloc[:-1] if len(spy) > 1 else spy,
                             vix.iloc[:-1] if len(vix) > 1 else vix)
    except Exception as e:
        log.warning("regime fetch failed (%s) — defaulting to permissive gate", e)
        gate = {"trend": "unknown", "trend_dist": 0.0, "vix": "unknown", "calm": 0.5,
                "long_allowed": True, "short_allowed": True, "exposure_scale": 1.0,
                "error": str(e)}
    _cache[key] = gate
    return gate


def gate_for_direction(direction: str, asof: date | None = None) -> tuple[bool, float, str]:
    """Apply the gate to a single signal direction.

    Returns (allowed, exposure_scale, reason). Per project policy only LONG
    ('Up') entries are gated; shorts always pass. exposure_scale multiplies the
    conviction-based notional (1.0 for ungated shorts).
    """
    g = current_regime(asof)
    if direction == "Up":
        if g["long_allowed"]:
            return True, max(g["exposure_scale"], 0.0) or 1.0, f"regime ok (trend={g['trend']}, vix={g['vix']})"
        return False, 0.0, f"regime block: long suppressed (trend={g['trend']}, vix={g['vix']})"
    return True, 1.0, "short ungated"
