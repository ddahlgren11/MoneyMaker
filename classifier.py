from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_analyzer = SentimentIntensityAnalyzer()

_finbert_pipeline = None

def _get_finbert():
    global _finbert_pipeline
    if _finbert_pipeline is None:
        from transformers import pipeline
        _finbert_pipeline = pipeline(
            "text-classification",
            model="ProsusAI/finbert",
            truncation=True,
            max_length=512,
        )
    return _finbert_pipeline

def get_finbert_score(text):
    """Returns a score in [-1, 1]: positive prob minus negative prob."""
    if not text or not str(text).strip():
        return 0.0
    pipe = _get_finbert()
    result = pipe(str(text)[:512])[0]
    label, score = result["label"], result["score"]
    if label == "positive":
        return score
    elif label == "negative":
        return -score
    return 0.0  # neutral

def get_finbert_scores_batch(texts, batch_size=64):
    """
    Batch FinBERT inference. Returns a DataFrame with columns:
        finbert_score    — positive_prob minus negative_prob  [-1, 1]
        finbert_positive — raw positive probability
        finbert_negative — raw negative probability
        finbert_neutral  — raw neutral probability
    """
    import pandas as pd
    if not texts:
        return pd.DataFrame(columns=["finbert_score", "finbert_positive", "finbert_negative", "finbert_neutral"])
    pipe = _get_finbert()
    cleaned = [str(t)[:512] if t and str(t).strip() else "" for t in texts]
    results = pipe(cleaned, batch_size=batch_size, truncation=True, top_k=None)

    rows = []
    for probs in results:
        d = {r["label"]: r["score"] for r in probs}
        pos = d.get("positive", 0.0)
        neg = d.get("negative", 0.0)
        neu = d.get("neutral",  0.0)
        rows.append({
            "finbert_score":    pos - neg,
            "finbert_positive": pos,
            "finbert_negative": neg,
            "finbert_neutral":  neu,
        })
    import pandas as pd
    return pd.DataFrame(rows)

def get_sentiment_score(text):
    return _analyzer.polarity_scores(text)['compound']

def get_refined_sentiment(score):
    if score <= -0.6: return 'Very Negative'
    elif score <= -0.2: return 'Negative'
    elif score < 0.2: return 'Neutral'
    elif score < 0.6: return 'Positive'
    else: return 'Very Positive'

def get_tone_category(text, score):
    text_lower = text.lower()
    emo_kws = [
        "excited", "exciting", "happy", "joyful", "amazing", "awesome",
        "incredible", "fantastic", "thrilled", "proud", "love", "great",
        "angry", "angry", "sad", "disappointed", "terrible", "horrible",
        "outraged", "furious", "disgusted", "frustrated", "upset",
    ]
    info_kws = [
        "announcement", "announce", "announcing", "launch", "launching",
        "update", "issue", "statement", "report", "release", "introducing",
        "partnership", "deal", "acquisition", "earnings", "quarterly",
        "revenue", "profit", "guidance", "forecast", "milestone",
    ]

    is_emotional = any(kw in text_lower for kw in emo_kws)
    is_informational = any(kw in text_lower for kw in info_kws)

    if is_emotional and is_informational:
        return 'Emotional (Joyful/Excited)' if score > 0.2 else 'Informational (Mixed)'
    elif is_emotional:
        return 'Emotional (Joyful/Excited)' if score > 0 else 'Emotional (Angry/Frustrated)'
    elif is_informational:
        return 'Informational (Promotional/Update)'
    return 'General Commentary'

def get_tweet_type(text):
    text_lower = text.lower()
    if any(kw in text_lower for kw in ["poll", "vote", "voting"]): return 'Poll/Vote'
    if any(kw in text_lower for kw in ["thoughts", "discuss", "opinion", "what do you", "agree", "disagree"]): return 'Discussion Starter'
    if any(kw in text_lower for kw in [
        "launch", "event", "update", "announce", "partnership", "deal",
        "acquisition", "earnings", "milestone", "record", "release", "new product",
    ]): return 'Company Milestone'
    if any(kw in text_lower for kw in ["thanks", "thank you", "congrats", "congratulations", "welcome"]): return 'Acknowledgment'
    return 'Personal/General Commentary'