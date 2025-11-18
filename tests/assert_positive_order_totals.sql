/*
  Custom Singular Test: Assert Positive Order Totals

  Purpose: Validate that all completed orders have positive grand totals
  This test will fail if any completed orders have zero or negative amounts,
  which would indicate a data quality issue.

  Fusion Engine Benefit: This test validates locally before warehouse execution,
  catching data quality issues early in the development cycle.
*/

with orders as (
    select
        order_id,
        order_status,
        grand_total,
        customer_id,
        order_date
    from {{ ref('int_orders_enriched') }}
    where is_completed = true
),

invalid_orders as (
    select
        order_id,
        customer_id,
        order_date,
        grand_total,
        'Grand total must be positive for completed orders' as validation_error
    from orders
    where grand_total <= 0
)

-- This test passes if no rows are returned
select * from invalid_orders
