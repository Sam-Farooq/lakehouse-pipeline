-- Thin rename-and-cast layer over the gold table Spark writes.
-- No business logic here on purpose: when a mart is wrong, this file should
-- never be a suspect.
select
    event_date,
    account_id,
    currency,
    txn_count,
    total_amount,
    avg_amount,
    max_amount,
    high_value_count,
    distinct_counterparties,
    distinct_countries
from {{ source('gold', 'daily_account_activity') }}
