#!/usr/bin/env python3
"""
Walk-forward, cost-aware backtest of the market-regime GATE (Part II validation).

The brief's hard rule: don't let the regime gate touch live trades until it shows
a meaningful drawdown reduction out-of-sample, benchmarked against BOTH the
un-gated strategy AND a passive baseline. This harness does exactly that.

It replays historical signals (congress / insider / reddit) as next-day-horizon
trades and builds two equity curves:
  - UNGATED  — every signal taken (long Up / short Down), the current behaviour.
  - GATED    — long entries taken only when the regime gate's long_allowed is true
               on the prior close (no look-ahead), and sized by exposure_scale;
               shorts always taken (project policy). The gate is rules-based, so
               "walk-forward" = evaluating it causally bar-by-bar with a 1-day lag.

Benchmarks: SPY buy-and-hold and an equal-weight basket of the traded names.
Reports total/annualized return, Sharpe, and max drawdown per strategy, plus the
gate's drawdown reduction — the number that decides whether to set
REGIME_GATE_ENABLED=true.

Usage:
    python3 backtest.py                       # congress signals, 1-day hold, 10bps
    python3 backtest.py --source insider --hold 3 --cost-bps 5
    python3 backtest.py --source congress --since 2026-01-01

Read-only (DB + yfinance); places no trades.
"""
import argparse

import numpy as np
import pandas as pd

from event_study import _load_events, _price_panel, _forward_return, MARKET_BENCHMARK
import regime


def _equity_metrics(daily: pd.Series) -> dict:
    """Total/annualized return, Sharpe, and max drawdown from a daily-return series."""
    if daily.empty:
        return {"n_days": 0, "total_%": 0.0, "ann_%": 0.0, "sharpe": float("nan"), "maxdd_%": 0.0}
    curve = (1 + daily).cumprod()
    total = curve.iloc[-1] - 1
    ann = (1 + total) ** (252 / len(daily)) - 1 if len(daily) > 0 else 0.0
    vol = daily.std(ddof=1)
    sharpe = (daily.mean() / vol * np.sqrt(252)) if vol and vol > 0 else float("nan")
    maxdd = (curve / curve.cummax() - 1).min()
    return {"n_days": len(daily), "total_%": round(total * 100, 2),
            "ann_%": round(ann * 100, 2),
            "sharpe": round(sharpe, 2) if pd.notna(sharpe) else float("nan"),
            "maxdd_%": round(maxdd * 100, 2)}


def _regime_long_ok(spy: pd.Series, vix: pd.Series, entry: pd.Timestamp):
    """Causal regime decision for a long entry: use only closes strictly before
    `entry` (lag ≥1 bar), returning (long_allowed, exposure_scale)."""
    spy_hist = spy.loc[spy.index < entry]
    vix_hist = vix.loc[vix.index < entry]
    if len(spy_hist) < regime.SMA_WINDOW + regime.CONFIRM_DAYS:
        return True, 1.0  # not enough history → don't gate (fail-open, like live)
    g = regime.compute_gate(spy_hist, vix_hist)
    return g["long_allowed"], (g["exposure_scale"] or 1.0)


def run(source: str, hold: int, cost_bps: float, since: str | None) -> None:
    events = _load_events(source, since, group_by=None)
    if events.empty:
        print(f"No {source} events to backtest.")
        return
    print(f"Backtesting {len(events)} {source} signal(s), {hold}-day hold, "
          f"{cost_bps:.0f}bps/side cost.\n")

    pad = hold + 6
    start = events["event_date"].min().date() - pd.Timedelta(days=420)  # 200d MA warmup
    end   = events["event_date"].max().date() + pd.Timedelta(days=pad)
    panel = _price_panel(events["ticker"].tolist(), start, end)
    spy = panel.get(MARKET_BENCHMARK)
    vix = regime._fetch_closes("^VIX", lookback_days=(end - start).days + 5)

    cost = cost_bps / 1e4
    rows_ungated, rows_gated = [], []

    for _, ev in events.iterrows():
        close = panel.get(ev["ticker"])
        if close is None or close.empty:
            continue
        idx = close.index.searchsorted(ev["event_date"])
        if idx >= len(close) or idx + hold >= len(close):
            continue
        entry = close.index[idx]
        raw = _forward_return(close, ev["event_date"], hold)
        if raw is None:
            continue
        sign = 1.0 if ev["direction"] == "Up" else -1.0
        trade_ret = raw * sign - 2 * cost  # entry + exit cost

        rows_ungated.append({"date": entry, "ret": trade_ret})

        # Gated: longs only when regime permits, scaled by exposure; shorts always.
        if ev["direction"] == "Up":
            ok, scale = _regime_long_ok(spy, vix, entry) if spy is not None else (True, 1.0)
            if ok:
                rows_gated.append({"date": entry, "ret": trade_ret * scale})
        else:
            rows_gated.append({"date": entry, "ret": trade_ret})

    def _daily(rows):
        if not rows:
            return pd.Series(dtype=float)
        df = pd.DataFrame(rows)
        return df.groupby("date")["ret"].mean().sort_index()

    ung = _daily(rows_ungated)
    gat = _daily(rows_gated)

    # Benchmarks over the traded window
    bench = {}
    if spy is not None and not ung.empty:
        win = spy.loc[(spy.index >= ung.index.min()) & (spy.index <= end.isoformat())]
        if len(win) > 1:
            bench["SPY buy&hold total_%"] = round((win.iloc[-1] / win.iloc[0] - 1) * 100, 2)
    ew = [(panel[t].iloc[-1] / panel[t].iloc[0] - 1)
          for t in events["ticker"].unique()
          if t in panel and len(panel[t]) > 1 and panel[t].iloc[0] > 0]
    if ew:
        bench["equal-weight basket total_%"] = round(float(np.mean(ew)) * 100, 2)

    m_ung, m_gat = _equity_metrics(ung), _equity_metrics(gat)
    table = pd.DataFrame({"UNGATED": m_ung, "GATED": m_gat}).T
    print(table.to_string())
    print()
    for k, v in bench.items():
        print(f"  benchmark — {k}: {v}")

    if pd.notna(m_ung["maxdd_%"]) and m_ung["maxdd_%"] < 0:
        dd_cut = (1 - m_gat["maxdd_%"] / m_ung["maxdd_%"]) * 100
        print(f"\n  Regime gate drawdown reduction: {dd_cut:+.1f}%  "
              f"(brief's bar to enable: ~25–30% with no Sharpe deterioration)")
        print(f"  Sharpe: ungated {m_ung['sharpe']} → gated {m_gat['sharpe']}")
        verdict = ("ENABLE-worthy" if dd_cut >= 25 and
                   (pd.isna(m_ung["sharpe"]) or m_gat["sharpe"] >= m_ung["sharpe"] - 0.05)
                   else "NOT yet — keep REGIME_GATE_ENABLED=false")
        print(f"  Verdict: {verdict}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Backtest the regime gate vs ungated + benchmarks.")
    ap.add_argument("--source", default="congress", choices=["congress", "insider", "reddit"])
    ap.add_argument("--hold", type=int, default=1, help="holding horizon in trading days")
    ap.add_argument("--cost-bps", type=float, default=10.0, help="transaction cost per side, bps")
    ap.add_argument("--since", default=None, help="only events on/after YYYY-MM-DD")
    args = ap.parse_args()
    run(args.source, args.hold, args.cost_bps, args.since)
