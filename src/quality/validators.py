"""Data quality validation framework.

Provides composable check functions that return structured results.
Designed to run after each layer write to ensure data integrity.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum

import polars as pl

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


class CheckStatus(str, Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"


@dataclass
class CheckResult:
    """Result of a single data quality check."""

    check_name: str
    status: CheckStatus
    table_name: str
    records_checked: int
    records_failed: int = 0
    details: str = ""
    executed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def pass_rate(self) -> float:
        if self.records_checked == 0:
            return 0.0
        return (self.records_checked - self.records_failed) / self.records_checked


class DataQualityValidator:
    """Composable data quality checks for stock market data."""

    def __init__(self, table_name: str) -> None:
        self.table_name = table_name
        self.results: list[CheckResult] = []

    def check_not_null(self, df: pl.DataFrame, columns: list[str]) -> list[CheckResult]:
        """Verify specified columns have no null values."""
        results = []
        for col in columns:
            null_count = df.filter(pl.col(col).is_null()).height
            status = CheckStatus.PASSED if null_count == 0 else CheckStatus.FAILED
            result = CheckResult(
                check_name=f"not_null_{col}",
                status=status,
                table_name=self.table_name,
                records_checked=df.height,
                records_failed=null_count,
                details=f"Column '{col}': {null_count} nulls found",
            )
            results.append(result)
            logger.info("DQ check %s: %s (%d/%d)", result.check_name, status, null_count, df.height)
        self.results.extend(results)
        return results

    def check_positive_values(self, df: pl.DataFrame, columns: list[str]) -> list[CheckResult]:
        """Verify numerical columns have strictly positive values."""
        results = []
        for col in columns:
            neg_count = df.filter(pl.col(col) <= 0).height
            status = CheckStatus.PASSED if neg_count == 0 else CheckStatus.FAILED
            result = CheckResult(
                check_name=f"positive_{col}",
                status=status,
                table_name=self.table_name,
                records_checked=df.height,
                records_failed=neg_count,
                details=f"Column '{col}': {neg_count} non-positive values",
            )
            results.append(result)
        self.results.extend(results)
        return results

    def check_unique(self, df: pl.DataFrame, columns: list[str]) -> CheckResult:
        """Verify a combination of columns is unique (composite key)."""
        total = df.height
        unique = df.unique(subset=columns).height
        duplicates = total - unique
        status = CheckStatus.PASSED if duplicates == 0 else CheckStatus.FAILED
        result = CheckResult(
            check_name=f"unique_{'_'.join(columns)}",
            status=status,
            table_name=self.table_name,
            records_checked=total,
            records_failed=duplicates,
            details=f"Composite key {columns}: {duplicates} duplicate rows",
        )
        self.results.append(result)
        return result

    def check_freshness(self, df: pl.DataFrame, date_column: str, max_age_days: int = 3) -> CheckResult:
        """Verify data is fresh (most recent record within N days)."""
        max_date = df.select(pl.col(date_column).max()).item()
        if isinstance(max_date, date):
            age_days = (date.today() - max_date).days
        else:
            age_days = max_age_days + 1  # force failure for invalid data

        status = CheckStatus.PASSED if age_days <= max_age_days else CheckStatus.WARNING
        result = CheckResult(
            check_name=f"freshness_{date_column}",
            status=status,
            table_name=self.table_name,
            records_checked=df.height,
            records_failed=0 if status == CheckStatus.PASSED else 1,
            details=f"Most recent date: {max_date}, age: {age_days} days (max {max_age_days})",
        )
        self.results.append(result)
        return result

    def check_value_range(
        self, df: pl.DataFrame, column: str, min_val: float, max_val: float
    ) -> CheckResult:
        """Verify column values fall within an expected range."""
        out_of_range = df.filter(
            (pl.col(column) < min_val) | (pl.col(column) > max_val)
        ).height
        status = CheckStatus.PASSED if out_of_range == 0 else CheckStatus.WARNING
        result = CheckResult(
            check_name=f"range_{column}",
            status=status,
            table_name=self.table_name,
            records_checked=df.height,
            records_failed=out_of_range,
            details=f"Column '{column}': {out_of_range} values outside [{min_val}, {max_val}]",
        )
        self.results.append(result)
        return result

    def check_high_low_relationship(self, df: pl.DataFrame) -> CheckResult:
        """Verify high >= low for all records (domain rule)."""
        violations = df.filter(pl.col("high") < pl.col("low")).height
        status = CheckStatus.PASSED if violations == 0 else CheckStatus.FAILED
        result = CheckResult(
            check_name="high_gte_low",
            status=status,
            table_name=self.table_name,
            records_checked=df.height,
            records_failed=violations,
            details=f"{violations} records where high < low",
        )
        self.results.append(result)
        return result

    def get_summary(self) -> dict:
        """Return a summary of all checks run."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == CheckStatus.PASSED)
        failed = sum(1 for r in self.results if r.status == CheckStatus.FAILED)
        warnings = sum(1 for r in self.results if r.status == CheckStatus.WARNING)
        return {
            "table": self.table_name,
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "warnings": warnings,
            "all_passed": failed == 0,
        }
