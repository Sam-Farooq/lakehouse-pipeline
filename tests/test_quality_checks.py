from datetime import UTC, datetime

from pyspark.sql import Row

from lakehouse.quality.expectations import assert_blocking, run_checks
from tests.conftest import QUALITY_IN

T0 = datetime(2026, 3, 1, 12, 0, tzinfo=UTC)


def _rows(spark, rows):
    return spark.createDataFrame(rows, schema=QUALITY_IN)


def _row(**kw):
    base = {"transaction_id": "a", "account_id": "1", "amount": 10.0,
            "currency": "EUR", "country": "DE", "occurred_at": T0}
    return Row(**{**base, **kw})


def test_clean_frame_passes_every_check(spark):
    df = _rows(spark, [_row(transaction_id="a"), _row(transaction_id="b")])
    assert all(r.passed for r in run_checks(df))


def test_duplicate_ids_are_counted_and_block(spark):
    df = _rows(spark, [_row(transaction_id="a"), _row(transaction_id="a")])
    results = {r.name: r for r in run_checks(df)}
    assert results["transaction_id_unique"].violations == 1
    try:
        assert_blocking(list(results.values()))
    except ValueError as exc:
        assert "transaction_id_unique" in str(exc)
    else:
        raise AssertionError("expected a blocking failure")


def test_placeholder_country_warns_but_does_not_block(spark):
    df = _rows(spark, [_row(transaction_id="a", country="XX")])
    results = {r.name: r for r in run_checks(df)}
    assert results["country_populated"].violations == 1
    assert not results["country_populated"].blocking
    assert_blocking(list(results.values()))  # must not raise
