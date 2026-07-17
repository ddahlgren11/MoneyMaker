"""
Conviction scoring for congressional-trade signals.

The bake-off showed copying every disclosure equally is breadth without
discrimination. The documented edge (such as it is) concentrates in stronger
signals, so this scores each disclosure 0–1 on two *robust* dimensions and lets
the watcher size (and optionally filter) by it:

  1. Cluster — how many DISTINCT members disclosed the same ticker+direction in a
     recent window. One member is noise; several is a real signal.
  2. Amount — the disclosed dollar range. A $5M trade carries more information
     than a $15k one.

Deliberately NOT included: a per-member "track record" score. Each member has
only a handful of disclosures, so a track-record weight would be fit to tiny,
noisy samples — exactly the overfitting this project keeps warning about. Add it
only if/when there's enough history to measure it out-of-sample.

The scoring functions are pure (unit-tested); the cluster COUNT is supplied by
the caller (watch.py queries congress_trades for it).
"""
import re
import math

# Cluster window: members trading the same name within this many days count together.
CLUSTER_WINDOW_DAYS = 14

# Amount scale: disclosed ranges below LO are "small", at/above HI are "large".
_AMOUNT_LO = 15_000.0
_AMOUNT_HI = 1_000_000.0


def parse_amount_range(amount: str | None) -> float:
    """Parse a disclosed amount range (e.g. '$1,001 - $15,000') to a USD midpoint.

    Returns the midpoint of a range, the single value if not a range, or 0.0 if
    unparseable.
    """
    if not amount:
        return 0.0
    nums = re.findall(r"[\d,]+(?:\.\d+)?", str(amount).replace("$", ""))
    vals = []
    for n in nums:
        n = n.replace(",", "")
        try:
            v = float(n)
        except ValueError:
            continue
        if v > 0:
            vals.append(v)
    if not vals:
        return 0.0
    if len(vals) >= 2:
        return (vals[0] + vals[1]) / 2.0   # midpoint of the disclosed range
    return vals[0]


def _cluster_component(cluster_n: int) -> float:
    """0–1 from the number of distinct members: 1→0.4, 2→0.6, 3→0.8, 4+→1.0."""
    return max(0.0, min(1.0, 0.4 + (cluster_n - 1) * 0.2))


def _amount_component(amount_usd: float) -> float:
    """0–1 from disclosed dollars, log-scaled between LO and HI. Unknown → 0.4."""
    if amount_usd <= 0:
        return 0.4
    if amount_usd <= _AMOUNT_LO:
        return 0.3
    if amount_usd >= _AMOUNT_HI:
        return 1.0
    return 0.3 + 0.7 * (math.log(amount_usd / _AMOUNT_LO) / math.log(_AMOUNT_HI / _AMOUNT_LO))


def conviction_score(cluster_n: int, amount_usd: float) -> float:
    """Blend cluster (60%) and amount (40%) into a 0–1 conviction score.

    Weighted toward cluster because multi-member agreement is the better-documented
    signal. Used as the `tightness` in position sizing (higher → bigger bet).
    """
    score = 0.6 * _cluster_component(cluster_n) + 0.4 * _amount_component(amount_usd)
    return round(max(0.0, min(1.0, score)), 3)
