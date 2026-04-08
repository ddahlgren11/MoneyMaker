"""
Unit tests for classifier.py — no external dependencies, always fast.
"""
import pytest
from classifier import get_refined_sentiment, get_tone_category, get_tweet_type

VALID_REFINED = {"Very Positive", "Positive", "Neutral", "Negative", "Very Negative"}
VALID_TONE = {
    "Emotional (Joyful/Excited)",
    "Emotional (Angry/Frustrated)",
    "Informational (Promotional/Update)",
    "Informational (Mixed)",
    "General Commentary",
}
VALID_TWEET_TYPE = {
    "Poll/Vote",
    "Discussion Starter",
    "Company Milestone",
    "Personal/General Commentary",
}


class TestGetRefinedSentiment:
    def test_very_positive_high_score(self):
        assert get_refined_sentiment(0.9) == "Very Positive"

    def test_very_positive_boundary(self):
        assert get_refined_sentiment(0.6) == "Very Positive"

    def test_positive(self):
        assert get_refined_sentiment(0.4) == "Positive"

    def test_positive_boundary(self):
        assert get_refined_sentiment(0.2) == "Positive"

    def test_neutral_zero(self):
        assert get_refined_sentiment(0.0) == "Neutral"

    def test_neutral_slightly_negative(self):
        assert get_refined_sentiment(-0.1) == "Neutral"

    def test_negative(self):
        assert get_refined_sentiment(-0.4) == "Negative"

    def test_very_negative(self):
        assert get_refined_sentiment(-0.8) == "Very Negative"

    def test_very_negative_boundary(self):
        assert get_refined_sentiment(-0.6) == "Very Negative"

    def test_extreme_positive(self):
        assert get_refined_sentiment(1.0) == "Very Positive"

    def test_extreme_negative(self):
        assert get_refined_sentiment(-1.0) == "Very Negative"

    def test_always_returns_valid_label(self):
        """Sweep across the sentiment range — every score maps to a known label."""
        scores = [-1.0, -0.7, -0.5, -0.3, -0.1, 0.0, 0.1, 0.3, 0.5, 0.7, 1.0]
        for score in scores:
            result = get_refined_sentiment(score)
            assert result in VALID_REFINED, f"score={score} returned unexpected label: {result!r}"


class TestGetToneCategory:
    def test_returns_string(self):
        result = get_tone_category("Big product launch today!", 0.8)
        assert isinstance(result, str) and len(result) > 0

    def test_returns_valid_category(self):
        result = get_tone_category("Big product launch today!", 0.8)
        assert result in VALID_TONE

    def test_emotional_positive_keyword(self):
        result = get_tone_category("I am so excited about this!", 0.9)
        assert result == "Emotional (Joyful/Excited)"

    def test_emotional_negative_keyword(self):
        result = get_tone_category("I am angry about this situation.", -0.5)
        assert result == "Emotional (Angry/Frustrated)"

    def test_informational_keyword(self):
        result = get_tone_category("Product launch announcement.", 0.0)
        assert result == "Informational (Promotional/Update)"

    def test_general_commentary_fallback(self):
        result = get_tone_category("Just a random thought.", 0.0)
        assert result == "General Commentary"

    def test_emotional_and_informational_positive(self):
        result = get_tone_category("Excited about our big announcement!", 0.8)
        assert result == "Emotional (Joyful/Excited)"

    def test_emotional_and_informational_negative(self):
        result = get_tone_category("Angry about this announcement.", -0.5)
        assert result == "Informational (Mixed)"


class TestGetTweetType:
    def test_returns_string(self):
        result = get_tweet_type("Some tweet text")
        assert isinstance(result, str) and len(result) > 0

    def test_returns_valid_type(self):
        result = get_tweet_type("Some tweet text")
        assert result in VALID_TWEET_TYPE

    def test_poll_keyword(self):
        assert get_tweet_type("Vote for your favorite option!") == "Poll/Vote"

    def test_discussion_keyword(self):
        assert get_tweet_type("Thoughts on AI?") == "Discussion Starter"

    def test_milestone_keyword(self):
        assert get_tweet_type("Big product launch today!") == "Company Milestone"

    def test_general_fallback(self):
        assert get_tweet_type("Had a great morning.") == "Personal/General Commentary"

    def test_case_insensitive(self):
        assert get_tweet_type("VOTE NOW!") == "Poll/Vote"
        assert get_tweet_type("LAUNCH EVENT") == "Company Milestone"
