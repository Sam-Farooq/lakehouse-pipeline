"""Data quality gates, run by Airflow between silver and gold.

Each check returns a row count that violates it. Zero is a pass. Returning the
count rather than a boolean means the alert says how bad it is, which decides
whether anyone is woken up.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F


@dataclass
class CheckResult:
    name: str
    violations: int
    total: int
    blocking: bool

    @property
    def passed(self) -> bool:
        return self.violations == 0

    @property
    def rate(self) -> float:
        return self.violations / self.total if self.total else 0.0


def run_checks(df: DataFrame) -> list[CheckResult]:
    total = df.count()

    def count(condition) -> int:
        return df.filter(condition).count()

    return [
        CheckResult("transaction_id_not_null",
                    count(F.col("transaction_id").isNull()), total, blocking=True),
        CheckResult("transaction_id_unique",
                    total - df.select("transaction_id").distinct().count(), total, True),
        CheckResult("amount_positive",
                    count(F.col("amount") <= 0), total, blocking=True),
        CheckResult("currency_known",
                    count(~F.col("currency").isin("EUR", "USD", "GBP", "CHF", "SEK")),
                    total, blocking=True),
        # Not blocking: a clock-skewed client should not stop the daily load.
        CheckResult("occurred_at_not_future",
                    count(F.col("occurred_at") > F.current_timestamp()), total, False),
        CheckResult("country_populated",
                    count(F.col("country") == "XX"), total, blocking=False),
    ]


def assert_blocking(results: list[CheckResult]) -> None:
    failed = [r for r in results if r.blocking and not r.passed]
    if failed:
        detail = ", ".join(f"{r.name}={r.violations}/{r.total}" for r in failed)
        raise ValueError(f"blocking data quality failures: {detail}")
