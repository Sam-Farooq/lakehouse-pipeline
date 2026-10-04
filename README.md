# lakehouse-pipeline

Transaction events from Kafka into a Delta Lake medallion, aggregated nightly
by Airflow, modelled in dbt, published to BigQuery.

```
 producer ──► Kafka ──► bronze ──► silver ──► gold ──► BigQuery
              (6 part)  (append)   (merge)    (daily)      │
                                                           ▼
                                                     dbt marts
```

Two always-on Structured Streaming jobs, one scheduled DAG. The split is not
cosmetic: the streams have no date and Airflow should not own them, while the
daily aggregate does and should not be a stream.

## The three layers, and why each exists

**Bronze is append-only and never cleaned.** Schema cast, Kafka coordinates,
ingest timestamp, and the original payload in `_raw`. No deduplication, no
validation, nothing dropped.

That last part is the whole point. When silver has a bug, the fix is to delete
silver and rebuild it, which is only possible if bronze still holds everything
that ever arrived, including the malformed rows. Keeping `_raw` has settled
more than one argument about whether a field changed upstream or the parser
broke.

**Silver deduplicates, validates and conforms.** This is where correctness
lives:

- `dropDuplicates(["transaction_id"])` inside a 2 hour watermark. Kafka is
  at-least-once, so the same transaction arrives twice whenever a consumer
  rebalances. A duplicated payment in a daily total is noticed by someone
  outside engineering.
- Unknown currencies are **rejected, not defaulted**. Defaulting an unexpected
  currency to EUR turns a visible failure into a silent one.
- Merge on `transaction_id`, updating only when `s.occurred_at > t.occurred_at`.
  Without that condition a replay overwrites good rows with older copies.

**Gold is daily aggregates**, written with `replaceWhere` on the run date. A
plain `mode("overwrite")` drops the whole table every morning, which is how a
backfill quietly loses a year of history.

## The watermark is a real tradeoff

2 hours, set in `LH_LATE_ARRIVAL_WATERMARK`.

Spark holds the deduplication key set in state for the watermark span, so the
window is memory. Too short and a mobile client that buffered offline for
ninety minutes gets counted twice. Too long and the state store grows until
the driver dies at 3am.

2 hours came from the arrival distribution in staging: the 99.9th percentile
delay was 71 minutes. The producer in `src/lakehouse/producer/events.py`
reproduces that shape, with a 5% late rate and a 3% duplicate rate, because a
pipeline only tested on clean data has not been tested.

## Quality gates run before aggregation, not after

`src/lakehouse/quality/expectations.py`. Six checks, each returning a violation
count rather than a boolean, so the alert says how bad it is.

Four are blocking: null ids, duplicate ids, non-positive amounts, unknown
currencies. Two warn only: future timestamps and placeholder countries. A
clock-skewed client should not stop the nightly load.

The DAG also refuses to aggregate a day the stream never finished writing.
Running anyway produces a gold partition that looks complete, gets cached by
the dashboard, and is wrong until someone notices the totals are low.

## File sizing

`optimizeWrite` and `autoCompact` are on. A 30 second trigger writes roughly
2,900 files per partition per day otherwise, and read performance falls off a
cliff within a week. The trigger interval itself is the other half of that
tradeoff: 30 seconds was what the fraud team's latency requirement actually
needed, and anything under it buys nothing and costs small files.

## dbt

`account_risk_daily` flags an account-day as `volume_spike`, `multi_country`
or `repeated_high_value` against a 30 day trailing baseline.

The rules are deliberately boring arithmetic rather than a model. An analyst
has to explain a flag to a regulator, and "the gradient boosting said so" is
not an explanation. Tested with `accepted_values` on the flag and
`unique_combination_of_columns` on the grain.

## Running it

```bash
cp .env.example .env
make up                      # kafka (KRaft), minio, postgres, airflow
make produce                 # 500 events/sec with duplicates and late arrivals
make bronze                  # in another shell
make silver                  # in a third
make gold DATE=2026-03-01
```

Airflow is at `localhost:8080`, MinIO at `localhost:9001`.

## Deployment

`helm/` runs the two streams on EKS. `replicas: 1` and `strategy: Recreate`
are both deliberate: two drivers sharing a checkpoint corrupt it, and Delta
does not fail politely when that happens.

## Tests

`pytest -q` against a local Delta session. The silver tests are the ones worth
reading. Each asserts a rule that costs money if it regresses, including the
high-value boundary at exactly 10,000.00, which is the kind of off-by-one that
only shows up in a quarterly report.
