from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_analyzer = SentimentIntensityAnalyzer()

def get_sentiment_score(text):
    return _analyzer.polarity_scores(text)['compound']

# ── FinBERT ───────────────────────────────────────────────────────────────────
# Lazy-loaded on first use so startup stays fast.
# Score is positive_prob - negative_prob → [-1, 1], same sign convention as VADER.

_finbert_pipeline = None

def _get_finbert_pipeline():
    global _finbert_pipeline
    if _finbert_pipeline is None:
        from transformers import pipeline as hf_pipeline
        _finbert_pipeline = hf_pipeline(
            'text-classification',
            model='ProsusAI/finbert',
            top_k=None,
            truncation=True,
            max_length=512,
        )
    return _finbert_pipeline

def get_finbert_score(text):
    """Single-text FinBERT score in [-1, 1]. Returns None on failure."""
    try:
        result = _get_finbert_pipeline()(str(text)[:512])[0]
        s = {r['label']: r['score'] for r in result}
        return round(s.get('positive', 0) - s.get('negative', 0), 4)
    except Exception:
        return None

def get_finbert_scores_batch(texts):
    """Batch FinBERT scoring — much faster than calling one-by-one during ingestion."""
    try:
        pipe = _get_finbert_pipeline()
        results = pipe([str(t)[:512] for t in texts])
        out = []
        for result in results:
            s = {r['label']: r['score'] for r in result}
            out.append(round(s.get('positive', 0) - s.get('negative', 0), 4))
        return out
    except Exception:
        return [None] * len(texts)

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