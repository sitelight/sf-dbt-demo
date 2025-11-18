{{
  config(
    materialized='view',
    tags=['bronze', 'staging', 'orders']
  )
}}

/*
  Bronze Layer: Orders Staging Model

  Purpose: Clean and standardize order header data
  - Normalize status values
  - Extract date components for partitioning
  - Calculate order metrics
  - Prepare for incremental processing in silver layer
*/

with source as (
    select * from {{ source('raw', 'raw_orders') }}
),

transformed as (
    select
        -- Primary Key
        order_id,
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_key,

        -- Foreign Keys
        customer_id,

        -- Order Dates
        order_date,
        date(order_date) as order_date_day,
        date_trunc('week', order_date) as order_week,
        date_trunc('month', order_date) as order_month,
        date_trunc('quarter', order_date) as order_quarter,
        date_trunc('year', order_date) as order_year,

        -- Order Status
        lower(trim(order_status)) as order_status,
        case
            when lower(order_status) in ('completed', 'shipped') then true
            else false
        end as is_completed,
        case
            when lower(order_status) = 'cancelled' then true
            else false
        end as is_cancelled,

        -- Order Details
        lower(trim(shipping_method)) as shipping_method,
        lower(trim(payment_method)) as payment_method,

        -- Financial amounts
        shipping_cost,
        tax_amount,
        discount_amount,

        -- Metadata
        created_at,
        updated_at,

        -- Audit columns
        current_timestamp() as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_invocation_id

    from source
)

select * from transformed
