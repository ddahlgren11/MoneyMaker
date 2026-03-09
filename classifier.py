from textblob import TextBlob

def get_refined_sentiment(score):
    if score <= -0.6: return 'Very Negative'
    elif score <= -0.2: return 'Negative'
    elif score < 0.2: return 'Neutral'
    elif score < 0.6: return 'Positive'
    else: return 'Very Positive'

def get_tone_category(text, score):
    text_lower = text.lower()
    emo_kws = ["excited", "happy", "joyful", "amazing", "angry", "sad"]
    info_kws = ["announcement", "launch", "update", "issue", "statement"]

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
    if any(kw in text_lower for kw in ["poll", "vote"]): return 'Poll/Vote'
    if any(kw in text_lower for kw in ["thoughts", "discuss"]): return 'Discussion Starter'
    if any(kw in text_lower for kw in ["launch", "event", "update"]): return 'Company Milestone'
    return 'Personal/General Commentary'