# MoneyMaker — The "Explain It Like I'm Five" Edition

> This is the plain-English companion to the real [README.md](README.md). Same
> facts, zero jargon, lots of analogies. It covers everything we built on the
> `feature/regime-gate` branch (which also includes the `feature/insider-reddit-signals`
> work underneath it). Nothing here is on `main` yet — this is the workshop, not
> the showroom.

---

## The 30-second version

Imagine a robot that reads the news and the gossip, notices when important people
are buying or selling stocks, and then makes pretend bets with pretend money to
see if it can guess which stocks go up or down tomorrow.

We spent this branch doing three things:
1. **Giving the robot better ears** — new free places to listen (government
   filings, company-insider filings, Reddit, and a no-password way to read tweets).
2. **Giving the robot a sense of when to keep its mouth shut** — a "is the overall
   market healthy right now?" filter (the big new thing).
3. **Building a taste-tester** — a way to check whether any of this actually makes
   money *before* we trust it, instead of finding out the hard way.

Almost everything new is **turned OFF by default**. Think training wheels: built,
bolted on, but not load-bearing until we've proven it's safe.

---

## Part 1 — The robot's ears (where signals come from)

A "signal" is just a hint that a stock might move. We collect hints from a few
crowds. Here's each one, and how much to trust it.

### 🏛️ Congress trades — *"follow the people who write the rules"*
When members of Congress buy or sell stocks, the law makes them post it publicly.
We grab those filings automatically. If a senator **buys** something, that's a
thumbs-up; if they **sell**, thumbs-down.

**Analogy:** it's like noticing the referees of a game quietly placing their own
bets. Might mean nothing… but you'd want to know. This is our most reliable hint
so far.

### 🏢 Insider trades (SEC Form 4) — *"the chef ordering off their own menu"* — NEW
When a company's CEO, CFO, or directors buy or sell **their own company's stock**,
they legally have to report it within two days. We now read those filings straight
from the government's free database (EDGAR).

**Analogy:** if the head chef suddenly orders a huge plate of their own cooking,
that's a good sign about the food. If they won't touch it, raise an eyebrow.
We act on the **buys** (the clean signal) and ignore most sells, because insiders
sell for boring reasons (taxes, new house, divorce) but usually only buy when they
genuinely think the stock is cheap.

### 📣 Reddit (WallStreetBets etc.) — *"the stadium crowd"* — NEW, EXPERIMENTAL
We read finance subreddits, count how often each stock gets mentioned, and measure
the mood (happy/angry). When a stock suddenly gets talked about **way** more than
usual *and* the mood is strongly one direction, that's a "spike."

**Analogy:** a stadium has a constant background hum that tells you nothing. But a
sudden *roar* usually means something just happened. We ignore the hum and only
care about the roar. **Important:** the crowd is often loud and *wrong*, so this is
marked experimental and isn't allowed to place real bets yet.

### 🐦 Tweets — *"we found a side door"* — FIXED
The robot used to read tweets by logging in with a saved password cookie. That
cookie kept expiring, and ingestion silently died for months (that's why your
tweet data was stale back to last fall). We switched to a **free public side door**
(the same feed websites use to embed tweets) that needs no login at all.

**Analogy:** we were sneaking in with a keycard that kept getting deactivated.
Now we just use the public front lobby. (Downside: the lobby occasionally says
"too many people, come back in a minute" — that's the rate-limiting — but it
doesn't need a key that dies.)

---

## Part 2 — The big new idea: the "Market Weather" gate 🌦️

This is the headline feature of the branch. It comes from a research brief that
basically said: *"Your stock-picking robot is fine, but it has no idea whether
it's sunny or storming outside. Teach it to check the weather first."*

### What it does
Before the robot makes a **buy** (a bet that a stock goes UP), it first asks two
questions about the whole market:

1. **Is the market in an uptrend?** We check the S&P 500 against its **200-day
   average** — basically, "is the market above its year-long trend line?" If yes,
   the coast is reasonably clear. If it's below, we're in a downtrend and buying is
   risky.
2. **Is everyone panicking?** We check the "fear gauge" (the VIX). If fear is in
   crisis mode, we don't buy even if the trend looks okay.

**Analogy:** It's an umbrella rule. You can still go outside (the robot still
works), but if the sky is dark and stormy, you *don't* go for a long walk. The
gate doesn't pick stocks — it just decides whether it's a good day to be out at all.

### Three rules you specifically chose
- **It only blocks BUYS.** Your "sell/short" bets (from Congress and insiders
  selling) keep firing no matter the weather. You wanted shorts left alone — done.
- **It's turned OFF by default.** It does nothing until you flip a switch
  (`REGIME_GATE_ENABLED=true`). We don't trust it with real (pretend) money until
  it's proven.
- **It sizes bets by confidence, not on/off.** Beautiful sunny day = bet bigger.
  Iffy, cloudy day = bet smaller. It's a dimmer switch, not a light switch.

### Why this matters (the honest truth)
The brief was blunt about this, and so am I: **the weather gate doesn't make you
richer — it stops you from getting wrecked.** Its whole job is to keep the robot
out of bear-market disasters. In a long, sunny bull market, it'll actually make
slightly *less* than just always betting, because it sometimes sits out. You're
buying insurance, not a money printer.

---

## Part 3 — Two more tweaks from the brief

### 🎯 Sector weighting — *"trust the right crowds"* — OFF by default
The research found that crowd chatter predicts **tech stocks** well, but is
basically useless for **energy stocks** (oil prices are driven by OPEC and wars,
not vibes). So we let the robot weight a gossip-based signal more heavily for tech
and lightly for energy.

**Analogy:** rumors at a tech conference are worth listening to; rumors at a gas
station about oil prices, less so. (Note: this only applies to *gossip* signals —
a congressman literally buying a stock is a fact, not a rumor, so it's never
discounted.)

### 🚧 The bouncers (risk filters) — *"keep the scammers out"*
We added filters to spot:
- **Bots** — brand-new accounts spamming the same message (account age + posting
  speed checks).
- **Pump-and-dumps** — a stock that rockets up then crashes (the classic scam shape).
- **Tiny stocks** — penny stocks too small and illiquid to trade safely.

**Analogy:** bouncers at the door. Most of the crowd is fine, but you don't want
the obvious scammers and troublemakers influencing your decisions.

---

## Part 4 — The taste-tester (why we're not just guessing) 🧪

Here's the part I'm proudest of, because it's the grown-up part.

Two tools that **check whether a signal actually works before we trade it**:

- **`event_study.py`** — for any signal type, it asks: "Historically, when this
  hint fired, did the stock actually move the way we predicted?" It spits out a
  score and a confidence number.
- **`backtest.py`** — replays history *as if* the robot had been trading, then
  compares: robot-with-weather-gate vs. robot-without vs. just-buying-the-S&P-500.
  It even subtracts trading fees so we don't fool ourselves.

**Analogy:** before serving a new dish to customers, you taste it yourself. If it's
bad, it never leaves the kitchen.

**What the taste-test already told us:** when we ran the weather gate against recent
history, it only reduced losses by ~7%. The research says we shouldn't bother
turning it on unless it cuts losses by ~25–30%. So the tool did its job and said
**"not good enough yet — leave it off."** And we listened. (The reason: the gate
shines in market *crashes*, and there hasn't been one in the recent data, so there
was nothing for it to protect against.)

---

## Part 5 — The big reality check 💵

I'm going to keep being straight with you, because that's more useful than hype:

- This is all **pretend money** (a paper-trading account). Real money behaves worse
  (you don't always get the price you want).
- The track record so far is **tiny and barely positive** — roughly **+1% a year**,
  which is *worse* than a boring savings account (~3.5–4%) that has zero risk.
- So none of this is "ready to fund with real dollars." It's a **science lab**. The
  whole point of the event-study and backtest tools is to find out *if* and *when*
  any of these signals can beat that savings-account bar. Until then, the lab stays
  a lab.

**Analogy:** we've built a really nice race car and a really nice safety system,
and we're testing it on a closed track with traffic cones, not on the highway with
your actual savings in the trunk.

---

## What's a switch vs. what's always on

| Thing we built | On by default? | Why |
|---|---|---|
| Congress trade signals | ✅ On | Proven-ish; already your best signal |
| Insider **buy** signals | ✅ On (buys only) | Well-studied; sells are noise |
| Free tweet feed | ✅ On | Replaces the broken cookie method |
| Reddit signals | ❌ Off | Unproven; crowd is often wrong |
| Weather (regime) gate | ❌ Off | Must pass the backtest first |
| Sector weighting | ❌ Off | Nice-to-have; validate first |

The rule we followed all branch: **build it, test it, but don't let it touch
(pretend) money until the numbers earn it.**

---

## The cheat sheet (commands)

```bash
# See if a signal type actually has an edge (read-only, safe):
python3 event_study.py --source insider
python3 event_study.py --source congress

# Test whether the weather gate would've helped (read-only, safe):
python3 backtest.py --source congress --hold 3

# Safe dry-run of the whole robot (no real orders):
python3 watch.py --once --db-only --dry-run
```

---

## In one sentence

We taught the robot to listen in more (and freer) places, to check the market's
weather before buying, to ignore obvious scams, and — most importantly — to
**prove an idea makes money before risking any on it.** Most of it is sitting
safely switched off, waiting for the data to say "go."

*This is a research project, not investment advice. The robot is playing with
Monopoly money on purpose.*
