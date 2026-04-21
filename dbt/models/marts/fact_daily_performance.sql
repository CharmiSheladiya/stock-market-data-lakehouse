/*
  Mart: Fact table for daily stock performance with all technical indicators.
  Serves as the primary analytics table for dashboards and APIs.
*/

select
    symbol,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    daily_return_pct,
    daily_price_change,
    ma_7d,
    ma_21d,
    ma_50d,
    avg_volume_21d,
    volatility_21d,
    trend_signal,
    high_volume_flag,
    ingested_at,
    current_timestamp as updated_at
from {{ ref('int_price_with_indicators') }}
