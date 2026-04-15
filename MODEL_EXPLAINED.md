# How the Prediction Model Works

---

## The Big Question

Every time a CEO posts a tweet, the model tries to answer one question:

> **Will this CEO's stock go up or down tomorrow?**

That's it. Up = 1, Down = 0. The model makes a guess and tells you how confident it is.

---

## Step 1 — Collecting Clues

Think of the model like a detective. Before it makes a guess, it looks at a bunch of clues about the tweet. These clues are called **features**.

Here's what it looks at:

### About the Tweet Itself
- **How positive or negative was it?** A tweet like "Best quarter ever!" scores high. "Deeply disappointed" scores low.
- **How extreme was the emotion?** A tweet that's strongly negative and a tweet that's strongly positive both have high *magnitude* — extreme emotion in either direction tends to matter.
- **How long was it?** Short tweets like "🚀" are different from a long, detailed business update.
- **How many words did it have?** More words usually means a more deliberate, thought-out message.

### About the Reaction to the Tweet
- **Likes, retweets, views, replies** — a tweet that goes viral carries more weight than one nobody saw.
- **Engagement rate** — this is like "of everyone who saw it, how many actually reacted?" A tweet with 10,000 likes but 10 million views is less impactful than one with 10,000 likes and 50,000 views.
- The counts are run through a **log transform**, which is just a way of saying "going from 0 to 1,000 likes matters a lot more than going from 1,000,000 to 1,001,000." It levels the playing field so one viral tweet doesn't completely dominate.

### About When It Was Posted
- **What hour was it posted?** A tweet at 6am before the market opens hits differently than one at 2pm.
- **Was it before the market opened?** Pre-market tweets can move a stock before most people even wake up.

### About the Stock at That Moment
- **RSI (Relative Strength Index)** — think of this like a thermometer for the stock. Above 70 means the stock is "overheated" (overbought) and might be due for a drop. Below 30 means it's "ice cold" (oversold) and might be due for a bounce.
- **ATR (Average True Range)** — this measures how wildly the stock has been swinging lately. A stock that moves $20 a day is way more volatile than one that moves $1.
- **What did the stock do yesterday?** If it went up yesterday, does it tend to keep going up? Or reverse?

### About the Outside World
- **VIX (the "Fear Index")** — this measures how nervous the entire market is. When VIX is high, everyone is scared and stocks are more likely to move dramatically in either direction.
- **Days until earnings** — CEO tweets right before an earnings report are a big deal. The model knows how close earnings day is.
- **News sentiment that day** — were the headlines about this company positive or negative on the same day as the tweet?

### About the Tweet's Style
- **Tone** — was it emotional (excited/angry) or informational (announcing something)?
- **Type** — was it a company milestone, a discussion starter, a poll, or just a personal comment?

---

## Step 2 — Three Students Take the Same Test

The model doesn't just use one algorithm — it trains three different ones and picks the best:

### Student 1: Logistic Regression
The simple student. Imagine drawing a straight line on a graph and saying "everything on this side = Up, everything on that side = Down." It's fast and easy to understand but can miss complex patterns.

### Student 2: Random Forest
Imagine 200 different people each writing their own flowchart of yes/no questions about the tweet. Each person votes Up or Down, and the majority wins. Because 200 people are all making different flowcharts independently, one weird outlier can't throw off the whole group.

### Student 3: Gradient Boosting
This one is more like a study group. One student takes the test and gets some answers wrong. The next student specifically focuses on fixing those wrong answers. Then the next student fixes *their* mistakes. Each round gets a little bit better by learning from the previous round's failures.

The best-performing student (measured fairly, as explained below) gets saved and used for real predictions.

---

## Step 3 — Grading the Test Fairly

Here's a tricky problem with stock prediction: **you can't test on data from the same time period you trained on.**

Imagine studying for a history test by memorizing the exact answers, then taking that same test. You'd score 100% but learn nothing — you just memorized. That's called **data leakage** and it makes your model look great on paper but fail in real life.

To avoid this, the model uses **walk-forward testing** — like how a real trader would operate:

1. Train on January through August. Test on September.
2. Train on January through September. Test on October.
3. Train on January through October. Test on November.
4. ...and so on, 5 rounds total.

Each time, the model only uses information that would have existed *at that point in time*. No peeking at the future.

This gives a much more honest picture of whether the model actually learned something real.

---

## Step 4 — Making the Confidence Score Honest

After picking the best model, the raw confidence scores aren't always accurate. The model might say "I'm 70% sure this goes up" when it's actually only right 55% of the time at that confidence level. That's like a weather app saying "70% chance of rain" but it's really more like a coin flip.

**Calibration** fixes this. It's like grading on a curve, but in reverse — adjusting the scores so that when the model says "65% confident," it actually means something.

After calibration, the model groups predictions into buckets and checks:

| Model Says... | Is It Actually Right...? |
|---|---|
| 50–55% confident | ~53% of the time |
| 55–60% confident | ~57% of the time |
| 60–65% confident | ~62% of the time |
| 65–70% confident | ~65% of the time |

If accuracy climbs as confidence climbs, the score is trustworthy and you can filter out low-confidence predictions.

---

## Step 5 — The Model Gets Smarter Every Day

Every weekday morning at 8am, an automated job runs that:

1. **Pulls fresh tweets** from all 10 CEOs
2. **Fetches the latest stock prices** to see what happened
3. **Writes everything to the database** — now there's new labeled data
4. **Retrains the model** on all the data including yesterday's
5. **Saves the updated model** so the app uses the newest version

Think of it like a student who reviews yesterday's mistakes every single morning before school. The more days that pass, the more data the model has trained on, and the better it gets.

---

## What the Model Cannot Do

- It cannot predict the future with certainty — no model can
- It does not know about breaking news, earnings surprises, or macro events that haven't happened yet
- It works best as **one signal among many**, not as the sole reason to make a decision
- With ~1,000 rows of training data it is still learning — accuracy will improve as the daily pipeline adds more labeled examples over time
