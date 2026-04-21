/*
  Intermediate model: Compute technical indicators & price metrics.
  - Daily returns
  - Moving averages (7, 21, 50 days)
  - Volatility (21-day rolling std dev)
  - VWAP-style price-volume metric
*/

with daily as (
    select * from {{ ref('stg_daily_prices') }}
),

with_lag as (
    select
        *,
        lag(close_price) over (
            partition by symbol order by trade_date
        ) as prev_close
    from daily
),

with_returns as (
    select
        *,
        case
            when prev_close is not null and prev_close > 0
            then round((close_price - prev_close) / prev_close * 100, 4)
            else null
        end as daily_return_pct,

        close_price - prev_close as daily_price_change
    from with_lag
),

with_moving_averages as (
    select
        *,
        round(avg(close_price) over (
            partition by symbol order by trade_date
            rows between 6 preceding and current row
        ), 4) as ma_7d,

        round(avg(close_price) over (
            partition by symbol order by trade_date
            rows between 20 preceding and current row
        ), 4) as ma_21d,

        round(avg(close_price) over (
            partition by symbol order by trade_date
            rows between 49 preceding and current row
        ), 4) as ma_50d,

        round(avg(volume) over (
            partition by symbol order by trade_date
            rows between 20 preceding and current row
        ), 0) as avg_volume_21d,

        round(stddev(daily_return_pct) over (
            partition by symbol order by trade_date
            rows between 20 preceding and current row
        ), 4) as volatility_21d
    from with_returns
)

select
    symbol,
    trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    prev_close,
    daily_return_pct,
    daily_price_change,
    ma_7d,
    ma_21d,
    ma_50d,
    avg_volume_21d,
    volatility_21d,
    case
        when ma_7d > ma_21d then 'BULLISH'
        when ma_7d < ma_21d then 'BEARISH'
        else 'NEUTRAL'
    end as trend_signal,
    case
        when volume > avg_volume_21d * 1.5 then true
        else false
    end as high_volume_flag,
    ingested_at
from with_moving_averages
