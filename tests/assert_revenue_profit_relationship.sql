/*
  Custom Singular Test: Assert Revenue-Profit Relationship

  Purpose: Validate that profit never exceeds revenue for any product
  This business rule ensures data integrity in financial calculations.
*/

with product_performance as (
    select
        product_id,
        product_name,
        total_revenue,
        total_profit
    from {{ ref('mart_product_performance') }}
    where total_revenue > 0  -- Only check products with sales
),

invalid_products as (
    select
        product_id,
        product_name,
        total_revenue,
        total_profit,
        'Profit cannot exceed revenue' as validation_error
    from product_performance
    where total_profit > total_revenue
)

-- This test passes if no rows are returned
select * from invalid_products
