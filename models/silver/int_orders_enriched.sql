{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='fail',
    tags=['silver', 'intermediate', 'incremental']
  )
}}

/*
  Silver Layer: Enriched Orders (Incremental Model)

  Purpose: Combine order header with line items to create complete order view
  - Calculate order-level totals from line items
  - Join with customer information
  - Include all financial calculations
  - Process only new/updated orders (incremental)

  Fusion Engine Highlight: This incremental model benefits from state-aware
  orchestration, processing only changed data to reduce compute costs.
*/

with {% if is_incremental() %}
max_updated_at as (
    select max(updated_at) as max_updated_at
    from {{ this }}
),
{% endif %}

orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
        -- Incremental filter: Process only recent orders
        where updated_at > (select max_updated_at from max_updated_at)
           or updated_at > dateadd(day, -{{ var('lookback_days', 3) }}, current_timestamp())
    {% endif %}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

-- Aggregate order items to order level
order_aggregates as (
    select
        order_id,
        count(distinct order_item_id) as items_count,
        count(distinct product_id) as unique_products_count,
        sum(quantity) as total_quantity,
        sum(line_total) as subtotal_amount,
        min(created_at) as first_item_at,
        max(created_at) as last_item_at
    from order_items
    group by order_id
),

-- Join everything together
enriched as (
    select
        -- Order identifiers
        o.order_id,
        o.order_key,
        o.customer_id,
        c.customer_key,

        -- Customer information (denormalized for performance)
        c.full_name as customer_name,
        c.email as customer_email,
        c.city as customer_city,
        c.state as customer_state,
        c.country as customer_country,

        -- Order dates
        o.order_date,
        o.order_date_day,
        o.order_week,
        o.order_month,
        o.order_quarter,
        o.order_year,

        -- Order status
        o.order_status,
        o.is_completed,
        o.is_cancelled,

        -- Order details
        o.shipping_method,
        o.payment_method,

        -- Order metrics from line items
        coalesce(oa.items_count, 0) as items_count,
        coalesce(oa.unique_products_count, 0) as unique_products_count,
        coalesce(oa.total_quantity, 0) as total_quantity,

        -- Financial calculations
        coalesce(oa.subtotal_amount, 0) as subtotal_amount,
        o.shipping_cost,
        o.tax_amount,
        o.discount_amount,

        -- Grand total calculation
        coalesce(oa.subtotal_amount, 0)
            + o.shipping_cost
            + o.tax_amount
            - o.discount_amount as grand_total,

        -- Average order value metrics
        case
            when coalesce(oa.items_count, 0) > 0
            then coalesce(oa.subtotal_amount, 0) / oa.items_count
            else 0
        end as avg_item_value,

        -- Discount percentage
        case
            when coalesce(oa.subtotal_amount, 0) > 0
            then round((o.discount_amount / oa.subtotal_amount) * 100, 2)
            else 0
        end as discount_percentage,

        -- Order timing
        o.created_at as order_created_at,
        o.updated_at as order_updated_at,
        datediff(day, o.created_at, o.updated_at) as days_to_fulfill,

        -- Audit columns
        current_timestamp() as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_invocation_id

    from orders o
    left join order_aggregates oa on o.order_id = oa.order_id
    left join customers c on o.customer_id = c.customer_id
)

select * from enriched
