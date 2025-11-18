{{
  config(
    materialized='view',
    tags=['bronze', 'staging', 'products']
  )
}}

/*
  Bronze Layer: Product Staging Model

  Purpose: Standardize product catalog data
  - Clean and standardize product names
  - Calculate profit margins
  - Ensure data type consistency
  - Add business-friendly fields
*/

with source as (
    select * from {{ source('raw', 'raw_products') }}
),

transformed as (
    select
        -- Primary Key
        product_id,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,

        -- Product Information
        trim(product_name) as product_name,
        trim(category) as category,
        trim(subcategory) as subcategory,
        trim(brand) as brand,

        -- Pricing
        price as unit_price,
        cost as unit_cost,
        price - cost as unit_profit,
        round(((price - cost) / nullif(price, 0)) * 100, 2) as profit_margin_pct,

        -- Inventory
        stock_quantity,
        case
            when stock_quantity = 0 then 'Out of Stock'
            when stock_quantity <= 50 then 'Low Stock'
            when stock_quantity <= 150 then 'Medium Stock'
            else 'High Stock'
        end as inventory_status,

        -- Metadata
        created_at,
        updated_at,

        -- Audit columns
        current_timestamp() as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_invocation_id

    from source
)

select * from transformed
