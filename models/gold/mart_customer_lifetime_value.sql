{{
  config(
    materialized='table',
    cluster_by=['customer_segment'],
    tags=['gold', 'mart', 'customers', 'clv']
  )
}}

/*
  Gold Layer: Customer Lifetime Value (CLV) Mart

  Purpose: Calculate customer value metrics for marketing and retention
  - Total revenue and profit per customer
  - Customer lifetime value calculation
  - RFM (Recency, Frequency, Monetary) segmentation
  - Customer health scoring

  Business Use Cases:
  - Marketing campaign targeting
  - Customer retention strategies
  - VIP customer identification
  - Churn prediction
*/

with customer_dimension as (
    select * from {{ ref('dim_customers') }}
),

customer_transactions as (
    -- Aggregate line-item data to order level
    select
        customer_id,
        order_id,
        order_date,
        max(is_completed) as is_completed,
        max(is_cancelled) as is_cancelled,

        -- Aggregate line items to calculate order totals
        sum(line_total) as subtotal_amount,
        max(shipping_cost) as shipping_cost,
        max(tax_amount) as tax_amount,
        max(discount_amount) as discount_amount,
        sum(line_total) + max(shipping_cost) + max(tax_amount) - max(discount_amount) as grand_total,
        sum(line_profit) as line_profit

    from {{ ref('int_customer_orders') }}
    group by customer_id, order_id, order_date
),

-- Aggregate customer metrics
customer_metrics as (
    select
        customer_id,

        -- Order counts
        count(distinct order_id) as total_orders,
        count(distinct case when is_completed then order_id end) as completed_orders,
        count(distinct case when is_cancelled then order_id end) as cancelled_orders,

        -- Revenue metrics (completed orders only)
        sum(case when is_completed then grand_total else 0 end) as lifetime_revenue,
        sum(case when is_completed then subtotal_amount else 0 end) as lifetime_gross_revenue,
        sum(case when is_completed then line_profit else 0 end) as lifetime_profit,
        sum(case when is_completed then discount_amount else 0 end) as total_discounts_received,

        -- Order value metrics
        avg(case when is_completed then grand_total end) as avg_order_value,
        min(case when is_completed then grand_total end) as min_order_value,
        max(case when is_completed then grand_total end) as max_order_value,

        -- Recency, Frequency, Monetary (RFM)
        max(case when is_completed then order_date end) as last_order_date,
        min(case when is_completed then order_date end) as first_order_date,
        count(distinct case when is_completed then order_id end) as purchase_frequency,
        sum(case when is_completed then grand_total else 0 end) as monetary_value,

        -- Cancellation rate
        round((count(distinct case when is_cancelled then order_id end)::float /
               nullif(count(distinct order_id), 0)) * 100, 2) as cancellation_rate_pct

    from customer_transactions
    group by customer_id
),

-- Calculate RFM scores
rfm_scores as (
    select
        *,
        datediff(day, last_order_date, current_date()) as recency_days,

        -- RFM scoring (1-5 scale, 5 being best)
        ntile(5) over (order by datediff(day, last_order_date, current_date()) asc) as recency_score,
        ntile(5) over (order by purchase_frequency) as frequency_score,
        ntile(5) over (order by monetary_value) as monetary_score

    from customer_metrics
),

-- Combine with customer dimension
customer_lifetime_value as (
    select
        -- Customer information
        cd.customer_id,
        cd.customer_key,
        cd.full_name,
        cd.email,
        cd.city,
        cd.state,
        cd.country,
        cd.customer_segment,
        cd.purchase_frequency_segment,

        -- Order history from dimension
        cd.first_order_date,
        cd.last_order_date,
        cd.days_since_first_order,
        cd.days_since_last_order,

        -- Comprehensive metrics from transactions
        cm.total_orders,
        cm.completed_orders,
        cm.cancelled_orders,
        cm.cancellation_rate_pct,

        -- Revenue and profit
        cm.lifetime_revenue,
        cm.lifetime_gross_revenue,
        cm.lifetime_profit,
        round((cm.lifetime_profit / nullif(cm.lifetime_revenue, 0)) * 100, 2) as profit_margin_pct,
        cm.total_discounts_received,

        -- Order value metrics
        cm.avg_order_value,
        cm.min_order_value,
        cm.max_order_value,

        -- RFM Analysis
        rfm.recency_days,
        rfm.recency_score,
        rfm.frequency_score,
        rfm.monetary_score,
        rfm.recency_score + rfm.frequency_score + rfm.monetary_score as rfm_total_score,

        -- CLV calculation (simple: historical revenue)
        -- For predictive CLV, integrate with ML models
        cm.lifetime_revenue as customer_lifetime_value,

        -- Customer value tiers based on revenue
        case
            when cm.lifetime_revenue >= 1000 then 'Platinum'
            when cm.lifetime_revenue >= 500 then 'Gold'
            when cm.lifetime_revenue >= 250 then 'Silver'
            when cm.lifetime_revenue > 0 then 'Bronze'
            else 'No Purchase'
        end as customer_value_tier,

        -- RFM segments for marketing
        case
            when rfm.recency_score >= 4 and rfm.frequency_score >= 4 and rfm.monetary_score >= 4 then 'Champions'
            when rfm.recency_score >= 3 and rfm.frequency_score >= 3 and rfm.monetary_score >= 3 then 'Loyal Customers'
            when rfm.recency_score >= 4 and rfm.frequency_score <= 2 then 'Promising'
            when rfm.recency_score >= 3 and rfm.frequency_score <= 2 then 'Potential Loyalists'
            when rfm.recency_score <= 2 and rfm.frequency_score >= 3 then 'At Risk'
            when rfm.recency_score <= 2 and rfm.frequency_score <= 2 then 'Lost'
            else 'Need Attention'
        end as rfm_segment,

        -- Audit
        current_timestamp() as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_invocation_id

    from customer_dimension cd
    left join customer_metrics cm on cd.customer_id = cm.customer_id
    left join rfm_scores rfm on cd.customer_id = rfm.customer_id
)

select * from customer_lifetime_value
