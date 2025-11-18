/*
  Ad-hoc Analysis: Revenue Trend Analysis

  Purpose: Example analysis query for exploring revenue trends
  This is NOT a model - it's stored in analyses/ for documentation
  and version control of common analytical queries.

  Run with: dbt compile --select revenue_trend_analysis
  Then execute the compiled SQL in Snowflake
*/

with daily_sales as (
    select
        date_day,
        net_revenue,
        total_orders,
        completed_orders,
        avg_order_value
    from {{ ref('mart_sales_daily') }}
    where date_day >= dateadd(day, -30, current_date())
),

with_moving_averages as (
    select
        date_day,
        net_revenue,
        total_orders,
        avg_order_value,

        -- 7-day moving average
        avg(net_revenue) over (
            order by date_day
            rows between 6 preceding and current row
        ) as revenue_7day_ma,

        -- Week-over-week growth
        (net_revenue - lag(net_revenue, 7) over (order by date_day)) /
            nullif(lag(net_revenue, 7) over (order by date_day), 0) * 100 as wow_growth_pct

    from daily_sales
)

select
    date_day,
    net_revenue,
    revenue_7day_ma,
    round(wow_growth_pct, 2) as wow_growth_pct,
    total_orders,
    avg_order_value
from with_moving_averages
order by date_day desc
