{{ config(materialized='incremental', unique_key=['event_date', 'account_id']) }}

-- Flags an account-day for review. Deliberately boring arithmetic rather than
-- a model: an analyst has to be able to explain a flag to a regulator, and
-- "the gradient boosting said so" is not an explanation.

with daily as (
    select * from {{ ref('stg_transactions') }}
    {% if is_incremental() %}
    where event_date >= date_sub(current_date(), interval 3 day)
    {% endif %}
),

baseline as (
    select
        account_id,
        avg(total_amount) over (
            partition by account_id
            order by event_date
            rows between 30 preceding and 1 preceding
        ) as trailing_avg_amount,
        event_date
    from daily
)

select
    d.event_date,
    d.account_id,
    d.currency,
    d.txn_count,
    d.total_amount,
    d.high_value_count,
    d.distinct_countries,
    b.trailing_avg_amount,
    safe_divide(d.total_amount, nullif(b.trailing_avg_amount, 0)) as amount_vs_baseline,
    case
        when d.distinct_countries >= 4 then 'multi_country'
        when safe_divide(d.total_amount, nullif(b.trailing_avg_amount, 0)) > 5 then 'volume_spike'
        when d.high_value_count >= 3 then 'repeated_high_value'
        else 'normal'
    end as risk_flag
from daily d
left join baseline b
    on d.account_id = b.account_id and d.event_date = b.event_date
