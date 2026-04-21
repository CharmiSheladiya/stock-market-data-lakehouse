"""Tests for the data quality validation framework."""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from src.quality.validators import CheckStatus, DataQualityValidator


# ------------------------------------------------------------------ #
#  Fixtures
# ------------------------------------------------------------------ #
@pytest.fixture
def valid_df() -> pl.DataFrame:
    """A clean DataFrame that passes all checks."""
    return pl.DataFrame(
        {
            "symbol": ["AAPL", "AAPL", "MSFT", "MSFT"],
            "date": [
                date.today() - timedelta(days=1),
                date.today() - timedelta(days=2),
                date.today() - timedelta(days=1),
                date.today() - timedelta(days=2),
            ],
            "open": [150.0, 148.0, 300.0, 298.0],
            "high": [155.0, 152.0, 305.0, 302.0],
            "low": [149.0, 147.5, 299.0, 297.0],
            "close": [153.5, 150.0, 303.0, 300.0],
            "volume": [1000000, 950000, 800000, 750000],
        }
    )


@pytest.fixture
def dirty_df() -> pl.DataFrame:
    """DataFrame with quality issues."""
    return pl.DataFrame(
        {
            "symbol": ["AAPL", None, "MSFT", "MSFT"],
            "date": [date(2025, 1, 10), date(2025, 1, 9), date(2025, 1, 10), date(2025, 1, 10)],
            "open": [150.0, -1.0, 300.0, 300.0],
            "high": [155.0, 152.0, 290.0, 305.0],  # high < low for row 3
            "low": [149.0, 147.5, 299.0, 297.0],
            "close": [153.5, 150.0, 303.0, 303.0],
            "volume": [1000000, 950000, 800000, 800000],
        }
    )


# ------------------------------------------------------------------ #
#  Not null checks
# ------------------------------------------------------------------ #
class TestNotNullCheck:
    def test_no_nulls_passes(self, valid_df):
        validator = DataQualityValidator("test_table")
        results = validator.check_not_null(valid_df, ["symbol", "close"])

        assert all(r.status == CheckStatus.PASSED for r in results)

    def test_nulls_fail(self, dirty_df):
        validator = DataQualityValidator("test_table")
        results = validator.check_not_null(dirty_df, ["symbol"])

        assert results[0].status == CheckStatus.FAILED
        assert results[0].records_failed == 1


# ------------------------------------------------------------------ #
#  Positive values checks
# ------------------------------------------------------------------ #
class TestPositiveValuesCheck:
    def test_all_positive_passes(self, valid_df):
        validator = DataQualityValidator("test_table")
        results = validator.check_positive_values(valid_df, ["open", "close"])

        assert all(r.status == CheckStatus.PASSED for r in results)

    def test_negative_values_fail(self, dirty_df):
        validator = DataQualityValidator("test_table")
        results = validator.check_positive_values(dirty_df, ["open"])

        assert results[0].status == CheckStatus.FAILED
        assert results[0].records_failed == 1


# ------------------------------------------------------------------ #
#  Uniqueness check
# ------------------------------------------------------------------ #
class TestUniqueCheck:
    def test_unique_passes(self, valid_df):
        validator = DataQualityValidator("test_table")
        result = validator.check_unique(valid_df, ["symbol", "date"])

        assert result.status == CheckStatus.PASSED

    def test_duplicates_fail(self, dirty_df):
        validator = DataQualityValidator("test_table")
        result = validator.check_unique(dirty_df, ["symbol", "date"])

        assert result.status == CheckStatus.FAILED
        assert result.records_failed > 0


# ------------------------------------------------------------------ #
#  Freshness check
# ------------------------------------------------------------------ #
class TestFreshnessCheck:
    def test_fresh_data_passes(self, valid_df):
        validator = DataQualityValidator("test_table")
        result = validator.check_freshness(valid_df, "date", max_age_days=3)

        assert result.status == CheckStatus.PASSED

    def test_stale_data_warns(self, dirty_df):
        validator = DataQualityValidator("test_table")
        result = validator.check_freshness(dirty_df, "date", max_age_days=1)

        assert result.status == CheckStatus.WARNING


# ------------------------------------------------------------------ #
#  High >= Low domain check
# ------------------------------------------------------------------ #
class TestHighLowCheck:
    def test_valid_relationship_passes(self, valid_df):
        validator = DataQualityValidator("test_table")
        result = validator.check_high_low_relationship(valid_df)

        assert result.status == CheckStatus.PASSED

    def test_invalid_relationship_fails(self, dirty_df):
        validator = DataQualityValidator("test_table")
        result = validator.check_high_low_relationship(dirty_df)

        assert result.status == CheckStatus.FAILED
        assert result.records_failed == 1


# ------------------------------------------------------------------ #
#  Summary
# ------------------------------------------------------------------ #
class TestSummary:
    def test_summary_counts(self, valid_df):
        validator = DataQualityValidator("test_table")
        validator.check_not_null(valid_df, ["symbol"])
        validator.check_unique(valid_df, ["symbol", "date"])

        summary = validator.get_summary()
        assert summary["total_checks"] == 2
        assert summary["all_passed"] is True

    def test_pass_rate(self, valid_df):
        validator = DataQualityValidator("test_table")
        results = validator.check_not_null(valid_df, ["symbol"])
        assert results[0].pass_rate == 1.0
