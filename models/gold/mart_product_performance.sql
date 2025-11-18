{{
  config(
    materialized='table',
    cluster_by=['category'],
    tags=['gold', 'mart', 'products', 'inventory']
  )
}}

/*
  Gold Layer: Product Performance Mart

  Purpose: Analyze product sales performance and profitability
  - Sales volume and revenue by product
  - Profit margins and contribution
  - Product ranking and best sellers
  - Inventory performance insights

  Business Use Cases:
  - Inventory optimization
  - Pricing strategy
  - Product recommendations
  - Category performance analysis
*/

with products as (
    select
        product_id,
        product_key,
        product_name,
        category,
        subcategory,
        brand,
        unit_price,
        unit_cost,
        unit_profit,
        profit_margin_pct,
        stock_quantity,
        inventory_status
    from {{ ref('stg_products') }}
),

product_sales as (
    select
        product_id,
        order_id,
        order_date,
        is_completed,
        quantity,
        line_total,
        line_profit
    from {{ ref('int_customer_orders') }}
    where is_completed = true  -- Only completed orders
),

-- Aggregate product metrics
product_metrics as (
    select
        product_id,

        -- Sales volume
        count(distinct order_id) as orders_count,
        sum(quantity) as total_units_sold,

        -- Revenue metrics
        sum(line_total) as total_revenue,
        avg(line_total) as avg_line_revenue,

        -- Profit metrics
        sum(line_profit) as total_profit,
        avg(line_profit) as avg_line_profit,

        -- Date range
        min(order_date) as first_sale_date,
        max(order_date) as last_sale_date,
        count(distinct date_trunc('day', order_date)) as days_with_sales

    from product_sales
    group by product_id
),

-- Calculate product rankings
product_rankings as (
    select
        product_id,
        row_number() over (order by total_revenue desc) as revenue_rank,
        row_number() over (order by total_profit desc) as profit_rank,
        row_number() over (order by total_units_sold desc) as volume_rank
    from product_metrics
),

-- Join everything together
product_performance as (
    select
        -- Product information
        p.product_id,
        p.product_key,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,

        -- Pricing and cost
        p.unit_price,
        p.unit_cost,
        p.unit_profit,
        p.profit_margin_pct,

        -- Inventory
        p.stock_quantity,
        p.inventory_status,

        -- Sales metrics
        coalesce(pm.orders_count, 0) as orders_count,
        coalesce(pm.total_units_sold, 0) as total_units_sold,
        coalesce(pm.total_revenue, 0) as total_revenue,
        coalesce(pm.avg_line_revenue, 0) as avg_line_revenue,

        -- Profit metrics
        coalesce(pm.total_profit, 0) as total_profit,
        coalesce(pm.avg_line_profit, 0) as avg_line_profit,
        round((pm.total_profit / nullif(pm.total_revenue, 0)) * 100, 2) as actual_profit_margin_pct,

        -- Performance indicators
        case
            when pm.total_units_sold is null then 'Never Sold'
            when pm.total_units_sold >= 10 then 'Best Seller'
            when pm.total_units_sold >= 5 then 'Good Seller'
            when pm.total_units_sold >= 2 then 'Moderate Seller'
            else 'Slow Mover'
        end as sales_velocity,

        -- Inventory health
        case
            when p.stock_quantity = 0 then 'Out of Stock'
            when p.stock_quantity > 0 and pm.total_units_sold is null then 'Overstock Risk'
            when p.stock_quantity <= 50 and pm.total_units_sold >= 10 then 'Reorder Soon'
            when p.stock_quantity <= 100 and pm.total_units_sold >= 5 then 'Monitor Stock'
            else 'Healthy'
        end as inventory_health,

        -- Date information
        pm.first_sale_date,
        pm.last_sale_date,
        coalesce(pm.days_with_sales, 0) as days_with_sales,
        datediff(day, pm.first_sale_date, pm.last_sale_date) + 1 as days_on_market,

        -- Rankings
        pr.revenue_rank,
        pr.profit_rank,
        pr.volume_rank,

        -- Performance score (weighted average of normalized ranks)
        case
            when pr.revenue_rank is not null then
                round(
                    (
                        (1.0 / pr.revenue_rank) * 0.4 +  -- 40% weight on revenue
                        (1.0 / pr.profit_rank) * 0.4 +    -- 40% weight on profit
                        (1.0 / pr.volume_rank) * 0.2      -- 20% weight on volume
                    ) * 1000,
                    2
                )
            else 0
        end as performance_score,

        -- Best seller flags
        case when pr.revenue_rank <= 5 then true else false end as is_top_revenue_product,
        case when pr.profit_rank <= 5 then true else false end as is_top_profit_product,
        case when pr.volume_rank <= 5 then true else false end as is_top_volume_product,

        -- Audit
        current_timestamp() as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_invocation_id

    from products p
    left join product_metrics pm on p.product_id = pm.product_id
    left join product_rankings pr on p.product_id = pr.product_id
)

select * from product_performance
order by total_revenue desc
