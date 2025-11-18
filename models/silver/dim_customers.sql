{{
  config(
    materialized='table',
    tags=['silver', 'dimension', 'customers']
  )
}}

/*
  Silver Layer: Customer Dimension

  Purpose: Create a slowly changing dimension (Type 1) for customers
  - Enrich customer data with first/last order dates
  - Calculate customer tenure
  - Add customer segmentation flags
  - Prepare for gold layer customer analytics

  This is a Type 1 SCD - always showing current state of customer.
  For Type 2 SCD with history tracking, use snapshots.
*/

with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(distinct order_id) as total_orders,
        count(distinct case when is_completed then order_id end) as completed_orders,
        count(distinct case when is_cancelled then order_id end) as cancelled_orders
    from {{ ref('stg_orders') }}
    group by customer_id
),

customer_dimension as (
    select
        -- Primary key
        c.customer_id,
        c.customer_key,

        -- Customer attributes
        c.first_name,
        c.last_name,
        c.full_name,
        c.email,
        c.phone,

        -- Location
        c.city,
        c.state,
        c.country,
        c.postal_code,

        -- Order history
        co.first_order_date,
        co.last_order_date,
        coalesce(co.total_orders, 0) as total_orders,
        coalesce(co.completed_orders, 0) as completed_orders,
        coalesce(co.cancelled_orders, 0) as cancelled_orders,

        -- Customer tenure
        datediff(day, co.first_order_date, current_date()) as days_since_first_order,
        datediff(day, co.last_order_date, current_date()) as days_since_last_order,

        -- Customer segmentation
        case
            when co.first_order_date is null then 'No Orders'
            when datediff(day, co.first_order_date, current_date()) <= 30 then 'New'
            when datediff(day, co.last_order_date, current_date()) <= 90 then 'Active'
            when datediff(day, co.last_order_date, current_date()) <= 365 then 'At Risk'
            else 'Churned'
        end as customer_segment,

        case
            when coalesce(co.completed_orders, 0) = 0 then 'Never Purchased'
            when coalesce(co.completed_orders, 0) = 1 then 'One-Time Buyer'
            when coalesce(co.completed_orders, 0) <= 5 then 'Occasional Buyer'
            when coalesce(co.completed_orders, 0) <= 10 then 'Regular Buyer'
            else 'VIP Buyer'
        end as purchase_frequency_segment,

        -- Timestamps
        c.created_at as customer_created_at,
        c.updated_at as customer_updated_at,

        -- Audit
        current_timestamp() as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_invocation_id

    from customers c
    left join customer_orders co on c.customer_id = co.customer_id
)

select * from customer_dimension
