from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

_analyzer = SentimentIntensityAnalyzer()

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