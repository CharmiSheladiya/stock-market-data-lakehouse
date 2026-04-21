/*
  Mart: Aggregated weekly performance summary per symbol.
*/

with daily as (
    select * from {{ ref('fact_daily_performance') }}
),

weekly as (
    select
        symbol,
        date_trunc('week', trade_date)              as week_start,
        min(trade_date)                              as first_trade_date,
        max(trade_date)                              as last_trade_date,
        count(*)                                     as trading_days,

        -- OHLCV at weekly level
        first_value(open_price) over (
            partition by symbol, date_trunc('week', trade_date)
            order by trade_date
        )                                            as week_open,
        max(high_price)                              as week_high,
        min(low_price)                               as week_low,
        last_value(close_price) over (
            partition by symbol, date_trunc('week', trade_date)
            order by trade_date
            rows between unbounded preceding and unbounded following
        )                                            as week_close,
        sum(volume)                                  as total_volume,
        round(avg(volume), 0)                        as avg_daily_volume,

        -- Performance
        round(avg(daily_return_pct), 4)              as avg_daily_return,
        round(avg(volatility_21d), 4)                as avg_volatility,
        sum(case when daily_return_pct > 0 then 1 else 0 end) as up_days,
        sum(case when daily_return_pct < 0 then 1 else 0 end) as down_days
    from daily
    group by
        symbol,
        date_trunc('week', trade_date),
        open_price,
        close_price,
        trade_date
)

select distinct
    symbol,
    week_start,
    first_trade_date,
    last_trade_date,
    trading_days,
    week_open,
    week_high,
    week_low,
    week_close,
    round((week_close - week_open) / nullif(week_open, 0) * 100, 4) as weekly_return_pct,
    total_volume,
    avg_daily_volume,
    avg_daily_return,
    avg_volatility,
    up_days,
    down_days,
    current_timestamp as updated_at
from weekly
