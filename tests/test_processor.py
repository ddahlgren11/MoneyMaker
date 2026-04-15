"""
Unit tests for DataProcessor._safe_int in processor.py.

This static method is called on every tweet's engagement counts (likes,
retweets, views, replies).  Broken handling here silently zeros out
engagement features for every record in the database.
"""
from processor import DataProcessor

_safe_int = DataProcessor._safe_int


class TestSafeInt:
    def test_normal_integer(self):
        assert _safe_int(42) == 42

    def test_integer_string(self):
        assert _safe_int("1500") == 1500

    def test_none_returns_zero(self):
        # Twitter API returns None for unavailable counts
        assert _safe_int(None) == 0

    def test_empty_string_returns_zero(self):
        assert _safe_int("") == 0

    def test_unavailable_string_returns_zero(self):
        # tweety-ns returns the literal string "Unavailable" for some fields
        assert _safe_int("Unavailable") == 0

    def test_zero_returns_zero(self):
        assert _safe_int(0) == 0

    def test_zero_string_returns_zero(self):
        assert _safe_int("0") == 0

    def test_float_truncates_to_int(self):
        assert _safe_int(3.9) == 3

    def test_float_string_returns_zero(self):
        # "1.5" can't be passed to int() directly — should return 0, not crash
        assert _safe_int("1.5") == 0

    def test_arbitrary_non_numeric_string_returns_zero(self):
        assert _safe_int("N/A") == 0
        assert _safe_int("--") == 0

    def test_large_number(self):
        # Viral tweets can have tens of millions of views
        assert _safe_int(50_000_000) == 50_000_000

    def test_returns_int_type(self):
        assert type(_safe_int(10)) is int
        assert type(_safe_int(None)) is int
