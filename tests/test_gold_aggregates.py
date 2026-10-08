from datetime import UTC, date, datetime

from pyspark.sql import Row

from lakehouse.batch.gold import corridor_volume, daily_account_activity
from tests.conftest import SILVER_OUT

D = date(2026, 3, 1)


def _silver(spark):
    return spark.createDataFrame([
        Row(event_date=D, account_id="A", currency="EUR", amount=100.0, counterparty_id="x",
            country="DE", channel="card", is_high_value=False, occurred_at=datetime(2026, 3, 1, 9, tzinfo=UTC)),
        Row(event_date=D, account_id="A", currency="EUR", amount=20000.0, counterparty_id="y",
            country="FR", channel="swift", is_high_value=True, occurred_at=datetime(2026, 3, 1, 10, tzinfo=UTC)),
        Row(event_date=date(2026, 2, 28), account_id="A", currency="EUR", amount=50.0,
            counterparty_id="z", country="DE", channel="card", is_high_value=False,
            occurred_at=datetime(2026, 2, 28, 9, tzinfo=UTC)),
    ], schema=SILVER_OUT)
def test_aggregate_covers_only_the_run_date(spark):
    out = daily_account_activity(_silver(spark), D).collect()
    assert len(out) == 1 and out[0].txn_count == 2


def test_totals_and_distinct_counts(spark):
    row = daily_account_activity(_silver(spark), D).collect()[0]
    assert row.total_amount == 20100.0
    assert row.high_value_count == 1
    assert row.distinct_counterparties == 2
    assert row.distinct_countries == 2


def test_corridor_splits_by_country_and_channel(spark):
    rows = {(r.country, r.channel): r for r in corridor_volume(_silver(spark), D).collect()}
    assert set(rows) == {("DE", "card"), ("FR", "swift")}
    assert rows[("FR", "swift")].total_amount == 20000.0
