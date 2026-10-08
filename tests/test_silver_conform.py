"""Conformance rules. These are the ones that corrupt money if they regress."""
from datetime import UTC, datetime

import pytest
from pyspark.sql import Row

from lakehouse.streaming.silver import conform

T0 = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)


def _df(spark, rows):
    return spark.createDataFrame(rows)


def test_duplicate_transaction_ids_collapse(spark):
    rows = [
        Row(transaction_id="a", account_id="1", counterparty_id="c", amount=10.0,
            currency="EUR", country="de", channel="CARD", mcc=1, occurred_at=T0, _raw="{}"),
        Row(transaction_id="a", account_id="1", counterparty_id="c", amount=10.0,
            currency="EUR", country="de", channel="CARD", mcc=1, occurred_at=T0, _raw="{}"),
    ]
    out = conform(_df(spark, rows), "2 hours")
    assert out.count() == 1


def test_zero_and_null_amounts_are_dropped(spark):
    rows = [
        Row(transaction_id="a", account_id="1", counterparty_id=None, amount=0.0,
            currency="EUR", country="DE", channel="card", mcc=None, occurred_at=T0, _raw=None),
        Row(transaction_id="b", account_id="1", counterparty_id=None, amount=None,
            currency="EUR", country="DE", channel="card", mcc=None, occurred_at=T0, _raw=None),
        Row(transaction_id="c", account_id="1", counterparty_id=None, amount=5.0,
            currency="EUR", country="DE", channel="card", mcc=None, occurred_at=T0, _raw=None),
    ]
    out = conform(_df(spark, rows), "2 hours")
    assert [r.transaction_id for r in out.collect()] == ["c"]


def test_unknown_currency_is_rejected_not_defaulted(spark):
    rows = [
        Row(transaction_id="a", account_id="1", counterparty_id=None, amount=5.0,
            currency="XBT", country="DE", channel="card", mcc=None, occurred_at=T0, _raw=None),
    ]
    assert conform(_df(spark, rows), "2 hours").count() == 0


def test_country_and_channel_are_normalised_with_a_placeholder(spark):
    rows = [
        Row(transaction_id="a", account_id="1", counterparty_id=None, amount=5.0,
            currency="EUR", country=None, channel="SEPA", mcc=None, occurred_at=T0, _raw=None),
    ]
    row = conform(_df(spark, rows), "2 hours").collect()[0]
    assert row.country == "XX" and row.channel == "sepa"


@pytest.mark.parametrize("amount,expected", [(9999.99, False), (10000.0, False), (10000.01, True)])
def test_high_value_boundary(spark, amount, expected):
    rows = [Row(transaction_id="a", account_id="1", counterparty_id=None, amount=amount,
                currency="EUR", country="DE", channel="card", mcc=None, occurred_at=T0, _raw=None)]
    assert conform(_df(spark, rows), "2 hours").collect()[0].is_high_value is expected


def test_raw_payload_is_dropped_from_silver(spark):
    rows = [Row(transaction_id="a", account_id="1", counterparty_id=None, amount=5.0,
                currency="EUR", country="DE", channel="card", mcc=None, occurred_at=T0,
                _raw='{"big": "blob"}')]
    assert "_raw" not in conform(_df(spark, rows), "2 hours").columns
