"""
Tests for get_sentiment_score() in classifier.py.

This function is called on every single tweet in the pipeline but had
zero tests — a regression here would silently corrupt every sentiment
value written to the database.
"""
import pytest
from classifier import get_sentiment_score


class TestGetSentimentScore:
    def test_returns_float(self):
        result = get_sentiment_score("The company had a great quarter.")
        assert isinstance(result, float)

    def test_range_minus_one_to_one(self):
        # VADER's compound score is always in [-1, 1] — anything outside
        # means the analyzer is broken or returning a different field
        texts = [
            "Absolutely amazing incredible fantastic news!",
            "Terrible horrible disaster and complete failure.",
            "The meeting is at 3pm on Tuesday.",
            "",
        ]
        for text in texts:
            score = get_sentiment_score(text)
            assert -1.0 <= score <= 1.0, f"Score {score} out of [-1, 1] for: {text!r}"

    def test_positive_text_returns_positive_score(self):
        score = get_sentiment_score("We are thrilled to announce record profits and incredible growth!")
        assert score > 0

    def test_negative_text_returns_negative_score(self):
        score = get_sentiment_score("Terrible results. Massive losses. Huge disappointment.")
        assert score < 0

    def test_neutral_text_near_zero(self):
        # A purely factual statement should score close to zero
        score = get_sentiment_score("The meeting is scheduled for Tuesday at the office.")
        assert -0.3 <= score <= 0.3

    def test_empty_string_does_not_crash(self):
        # Empty strings come in from the pipeline when a tweet has no text
        score = get_sentiment_score("")
        assert isinstance(score, float)
        assert -1.0 <= score <= 1.0

    def test_stronger_positive_scores_higher(self):
        # A very enthusiastic tweet should score higher than a mildly positive one
        mild = get_sentiment_score("Good news today.")
        strong = get_sentiment_score("Absolutely incredible, amazing, fantastic, wonderful news!")
        assert strong > mild

    def test_stronger_negative_scores_lower(self):
        # A strongly negative tweet should score lower than a mildly negative one
        mild = get_sentiment_score("Not great results this quarter.")
        strong = get_sentiment_score("Catastrophic failure. Terrible, horrible, awful disaster.")
        assert strong < mild

    def test_whitespace_only_does_not_crash(self):
        score = get_sentiment_score("   ")
        assert isinstance(score, float)

    def test_special_characters_do_not_crash(self):
        score = get_sentiment_score("🚀🚀🚀 To the moon!!! #TSLA $$$")
        assert isinstance(score, float)
        assert -1.0 <= score <= 1.0
