/*
  Staging model: Clean and standardize raw daily prices.
  - Cast data types
  - Remove duplicates
  - Filter out invalid records
*/

with raw_prices as (
    select * from {{ source('raw', 'daily_prices') }}
),

cleaned as (
    select
        symbol,
        cast(date as date)                          as trade_date,
        cast(open as decimal(18, 4))                as open_price,
        cast(high as decimal(18, 4))                as high_price,
        cast(low as decimal(18, 4))                 as low_price,
        cast(close as decimal(18, 4))               as close_price,
        cast(volume as bigint)                      as volume,
        cast(ingested_at as timestamp)              as ingested_at,
        row_number() over (
            partition by symbol, date
            order by ingested_at desc
        ) as rn
    from raw_prices
    where
        open > 0
        and high > 0
        and low > 0
        and close > 0
        and volume >= 0
)

select
    symbol,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    ingested_at
from cleaned
where rn = 1
