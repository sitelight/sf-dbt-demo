{{
  config(
    materialized='view',
    tags=['bronze', 'staging', 'order_items']
  )
}}

/*
  Bronze Layer: Order Items Staging Model

  Purpose: Clean and standardize order line item data
  - Validate quantity and pricing logic
  - Add calculated fields for analysis
  - Prepare for join with products and orders

  Fusion Engine Benefit: Detects calculation errors and column mismatches
  before warehouse execution, saving compute costs.
*/

with source as (
    select * from {{ source('raw', 'raw_order_items') }}
),

transformed as (
    select
        -- Primary Key
        order_item_id,
        {{ dbt_utils.generate_surrogate_key(['order_item_id']) }} as order_item_key,

        -- Foreign Keys
        order_id,
        product_id,

        -- Quantities and Pricing
        quantity,
        unit_price,
        line_total,

        -- Validation: Check if line_total matches calculation
        round(quantity * unit_price, 2) as calculated_line_total,
        abs(line_total - round(quantity * unit_price, 2)) as line_total_variance,
        case
            when abs(line_total - round(quantity * unit_price, 2)) < 0.01 then true
            else false
        end as is_line_total_valid,

        -- Metadata
        created_at,

        -- Audit columns
        current_timestamp() as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_invocation_id

    from source
)

select * from transformed
