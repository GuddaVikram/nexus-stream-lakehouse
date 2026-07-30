{{ config(materialized='table') }}

with hourly_metrics as (
    select
        date_trunc('hour', extracted_at) as trend_hour,
        bitcoin_usd,
        ethereum_usd,
        solana_usd
    -- ✅ Ensure this uses the explicit macro. Do NOT type the raw table name string.
    from {{ ref('stg_market_metrics') }}
)

select
    trend_hour,
    round(avg(bitcoin_usd), 2) as avg_bitcoin_price,
    round(avg(ethereum_usd), 2) as avg_ethereum_price,
    round(avg(solana_usd), 2) as avg_solana_price,
    count(*) as total_heartbeat_ticks_received
from hourly_metrics
group by 1
order by trend_hour desc