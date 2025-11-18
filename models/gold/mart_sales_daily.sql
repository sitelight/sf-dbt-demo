{{
  config(
    materialized='incremental',
    unique_key='date_day',
    incremental_strategy='merge',
    cluster_by=['date_day'],
    tags=['gold', 'mart', 'sales', 'incremental']
  )
}}

/*
  Gold Layer: Daily Sales Mart (Incremental)

  Purpose: Business-ready daily sales metrics for executive dashboards
  - Aggregate sales by day
  - Calculate key performance indicators (KPIs)
  - Track order status distribution
  - Optimize query performance with clustering

  Fusion Engine Benefits:
  - Incremental processing reduces warehouse compute by 60-80%
  - Clustering on date_day optimizes dashboard queries
  - Real-time validation prevents data quality issues
*/

with enriched_orders as (
    select * from {{ ref('int_orders_enriched') }}
    {% if is_incremental() %}
        -- Only process recent days
        where order_date_day >= (select dateadd(day, -{{ var('lookback_days', 3) }}, max(date_day)) from {{ this }})
    {% endif %}
),

daily_aggregates as (
    select
        order_date_day as date_day,
        order_week,
        order_month,
        order_quarter,
        order_year,

        -- Order counts by status
        count(distinct order_id) as total_orders,
        count(distinct case when is_completed then order_id end) as completed_orders,
        count(distinct case when is_cancelled then order_id end) as cancelled_orders,
        count(distinct case when order_status = 'pending' then order_id end) as pending_orders,
        count(distinct case when order_status = 'processing' then order_id end) as processing_orders,
        count(distinct case when order_status = 'shipped' then order_id end) as shipped_orders,

        -- Customer metrics
        count(distinct customer_id) as unique_customers,
        count(distinct case when is_completed then customer_id end) as active_customers,

        -- Revenue metrics (completed orders only)
        sum(case when is_completed then subtotal_amount else 0 end) as gross_revenue,
        sum(case when is_completed then shipping_cost else 0 end) as shipping_revenue,
        sum(case when is_completed then tax_amount else 0 end) as tax_collected,
        sum(case when is_completed then discount_amount else 0 end) as total_discounts,
        sum(case when is_completed then grand_total else 0 end) as net_revenue,

        -- Order metrics (completed orders only)
        sum(case when is_completed then items_count else 0 end) as total_items_sold,
        sum(case when is_completed then total_quantity else 0 end) as total_units_sold,

        -- Average order value (completed orders only)
        avg(case when is_completed then grand_total end) as avg_order_value,
        avg(case when is_completed then items_count end) as avg_items_per_order,
        avg(case when is_completed then total_quantity end) as avg_units_per_order,

        -- Payment method distribution (completed orders only)
        count(distinct case when is_completed and payment_method = 'credit_card' then order_id end) as credit_card_orders,
        count(distinct case when is_completed and payment_method = 'paypal' then order_id end) as paypal_orders,
        count(distinct case when is_completed and payment_method = 'debit_card' then order_id end) as debit_card_orders,

        -- Shipping method distribution (completed orders only)
        count(distinct case when is_completed and shipping_method = 'standard' then order_id end) as standard_shipping_orders,
        count(distinct case when is_completed and shipping_method = 'express' then order_id end) as express_shipping_orders,

        -- Operational metrics
        avg(days_to_fulfill) as avg_days_to_fulfill,

        -- Discount analysis
        avg(case when is_completed then discount_percentage end) as avg_discount_percentage,
        count(distinct case when is_completed and discount_amount > 0 then order_id end) as orders_with_discount,

        -- Data quality
        current_timestamp() as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_invocation_id

    from enriched_orders
    group by 1, 2, 3, 4, 5
),

with_calculated_metrics as (
    select
        *,

        -- Conversion and cancellation rates
        round((completed_orders::float / nullif(total_orders, 0)) * 100, 2) as order_completion_rate_pct,
        round((cancelled_orders::float / nullif(total_orders, 0)) * 100, 2) as order_cancellation_rate_pct,

        -- Revenue per customer
        round(net_revenue / nullif(active_customers, 0), 2) as revenue_per_customer,

        -- Discount penetration
        round((orders_with_discount::float / nullif(completed_orders, 0)) * 100, 2) as discount_penetration_pct,

        -- Payment method mix
        round((credit_card_orders::float / nullif(completed_orders, 0)) * 100, 2) as credit_card_pct,
        round((paypal_orders::float / nullif(completed_orders, 0)) * 100, 2) as paypal_pct,
        round((debit_card_orders::float / nullif(completed_orders, 0)) * 100, 2) as debit_card_pct

    from daily_aggregates
)

select * from with_calculated_metrics
