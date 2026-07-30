{{ config(materialized='view') }}

with raw_lake_data as (
    -- DuckDB natively queries parquet files straight from your running MinIO S3 bucket path!
    select * from read_parquet('s3://telemetry-data-lake/bronze/market_metrics/*.parquet')
)

select
    -- Convert unix integer timestamp to a standard readable timestamp format
    epoch_ms(extraction_timestamp * 1000) as extracted_at,
    
    -- Clean and structure currency numbers smoothly
    round(bitcoin_price, 2) as bitcoin_usd,
    round(ethereum_price, 2) as ethereum_usd,
    round(solana_price, 2) as solana_usd
from raw_lake_data
where extraction_timestamp is not null