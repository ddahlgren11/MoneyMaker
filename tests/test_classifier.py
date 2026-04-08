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
        # A clearly high score should map to the top label
        assert get_refined_sentiment(0.9) == "Very Positive"

    def test_very_positive_boundary(self):
        # 0.6 is the exact lower boundary for "Very Positive" — boundary values are easy to get wrong
        assert get_refined_sentiment(0.6) == "Very Positive"

    def test_positive(self):
        # Mid-range positive score should land in "Positive"
        assert get_refined_sentiment(0.4) == "Positive"

    def test_positive_boundary(self):
        # 0.2 is the exact lower boundary for "Positive"
        assert get_refined_sentiment(0.2) == "Positive"

    def test_neutral_zero(self):
        # Zero sentiment should be Neutral
        assert get_refined_sentiment(0.0) == "Neutral"

    def test_neutral_slightly_negative(self):
        # Slightly negative scores still fall in the Neutral band
        assert get_refined_sentiment(-0.1) == "Neutral"

    def test_negative(self):
        # Mid-range negative score should land in "Negative"
        assert get_refined_sentiment(-0.4) == "Negative"

    def test_very_negative(self):
        # Clearly negative score should map to the bottom label
        assert get_refined_sentiment(-0.8) == "Very Negative"

    def test_very_negative_boundary(self):
        # -0.6 is the exact upper boundary for "Very Negative"
        assert get_refined_sentiment(-0.6) == "Very Negative"

    def test_extreme_positive(self):
        # Maximum possible score should not crash or return an unexpected label
        assert get_refined_sentiment(1.0) == "Very Positive"

    def test_extreme_negative(self):
        # Minimum possible score should not crash or return an unexpected label
        assert get_refined_sentiment(-1.0) == "Very Negative"

    def test_always_returns_valid_label(self):
        # Sweeps 11 evenly-spaced scores across the full range to confirm
        # every value maps to one of the 5 known labels — catches any gaps in the logic
        scores = [-1.0, -0.7, -0.5, -0.3, -0.1, 0.0, 0.1, 0.3, 0.5, 0.7, 1.0]
        for score in scores:
            result = get_refined_sentiment(score)
            assert result in VALID_REFINED, f"score={score} returned unexpected label: {result!r}"


class TestGetToneCategory:
    def test_returns_string(self):
        # Basic sanity check — function should always return a non-empty string
        result = get_tone_category("Big product launch today!", 0.8)
        assert isinstance(result, str) and len(result) > 0

    def test_returns_valid_category(self):
        # Result must be one of the known tone categories, not something unexpected
        result = get_tone_category("Big product launch today!", 0.8)
        assert result in VALID_TONE

    def test_emotional_positive_keyword(self):
        # "excited" is an emotional keyword + positive score → Joyful/Excited
        result = get_tone_category("I am so excited about this!", 0.9)
        assert result == "Emotional (Joyful/Excited)"

    def test_emotional_negative_keyword(self):
        # "angry" is an emotional keyword + negative score → Angry/Frustrated
        result = get_tone_category("I am angry about this situation.", -0.5)
        assert result == "Emotional (Angry/Frustrated)"

    def test_informational_keyword(self):
        # "announcement" and "launch" are informational keywords → Promotional/Update
        result = get_tone_category("Product launch announcement.", 0.0)
        assert result == "Informational (Promotional/Update)"

    def test_general_commentary_fallback(self):
        # No keywords matched → should fall back to General Commentary
        result = get_tone_category("Just a random thought.", 0.0)
        assert result == "General Commentary"

    def test_emotional_and_informational_positive(self):
        # Tweet has both emotional AND informational keywords, positive score
        # → emotional wins and maps to Joyful/Excited
        result = get_tone_category("Excited about our big announcement!", 0.8)
        assert result == "Emotional (Joyful/Excited)"

    def test_emotional_and_informational_negative(self):
        # Tweet has both emotional AND informational keywords, negative score
        # → maps to Informational (Mixed)
        result = get_tone_category("Angry about this announcement.", -0.5)
        assert result == "Informational (Mixed)"


class TestGetTweetType:
    def test_returns_string(self):
        # Basic sanity check — function should always return a non-empty string
        result = get_tweet_type("Some tweet text")
        assert isinstance(result, str) and len(result) > 0

    def test_returns_valid_type(self):
        # Result must be one of the 4 known tweet types
        result = get_tweet_type("Some tweet text")
        assert result in VALID_TWEET_TYPE

    def test_poll_keyword(self):
        # "vote" keyword → Poll/Vote category
        assert get_tweet_type("Vote for your favorite option!") == "Poll/Vote"

    def test_discussion_keyword(self):
        # "thoughts" keyword → Discussion Starter category
        assert get_tweet_type("Thoughts on AI?") == "Discussion Starter"

    def test_milestone_keyword(self):
        # "launch" keyword → Company Milestone category
        assert get_tweet_type("Big product launch today!") == "Company Milestone"

    def test_general_fallback(self):
        # No keywords matched → falls back to Personal/General Commentary
        assert get_tweet_type("Had a great morning.") == "Personal/General Commentary"

    def test_case_insensitive(self):
        # Keywords should match regardless of capitalization
        assert get_tweet_type("VOTE NOW!") == "Poll/Vote"
        assert get_tweet_type("LAUNCH EVENT") == "Company Milestone"
