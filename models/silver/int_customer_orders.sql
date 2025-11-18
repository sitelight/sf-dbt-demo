{{
  config(
    materialized='table',
    tags=['silver', 'intermediate', 'customers']
  )
}}

/*
  Silver Layer: Customer Order History

  Purpose: Create a complete view of customer transaction history
  - Join order items with products for detailed line-item view
  - Include customer and order information
  - Calculate profit margins at line level
  - Foundation for customer analytics in gold layer

  This model demonstrates Fusion Engine's column-level lineage tracking,
  crucial for PII compliance and data governance.
*/

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

joined as (
    select
        -- Customer information
        c.customer_id,
        c.customer_key,
        c.full_name as customer_name,
        c.email as customer_email,
        c.city,
        c.state,
        c.country,

        -- Order information
        o.order_id,
        o.order_key,
        o.order_date,
        o.order_date_day,
        o.order_status,
        o.is_completed,
        o.is_cancelled,
        o.payment_method,
        o.shipping_method,

        -- Order line item
        oi.order_item_id,
        oi.order_item_key,

        -- Product information
        p.product_id,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,

        -- Quantities and financials
        oi.quantity,
        oi.unit_price,
        oi.line_total,
        p.unit_cost,

        -- Profit calculations at line level
        oi.line_total - (p.unit_cost * oi.quantity) as line_profit,
        round(((oi.line_total - (p.unit_cost * oi.quantity)) / nullif(oi.line_total, 0)) * 100, 2) as line_profit_margin_pct,

        -- Order-level amounts (for aggregation)
        o.shipping_cost,
        o.tax_amount,
        o.discount_amount,

        -- Data quality flag
        oi.is_line_total_valid,

        -- Timestamps
        o.order_date as transaction_date,
        oi.created_at as line_item_created_at,

        -- Audit
        current_timestamp() as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_invocation_id

    from order_items oi
    inner join products p on oi.product_id = p.product_id
    inner join orders o on oi.order_id = o.order_id
    inner join customers c on o.customer_id = c.customer_id
)

select * from joined
