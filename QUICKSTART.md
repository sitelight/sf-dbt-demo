# 🚀 Quick Start Guide

Get your demo up and running in 5 minutes!

## Prerequisites Check

```bash
# Check Python version (need 3.8+)
python --version

# Check if dbt is installed
dbt --version
```

## Installation

### 1. Install dbt (if not already installed)

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dbt with Snowflake adapter
pip install dbt-core dbt-snowflake
```

### 2. Configure Snowflake Connection

**Option A: Use included profiles.yml**

Edit `profiles.yml` with your Snowflake credentials:
```yaml
account: abc12345.us-east-1  # Your Snowflake account
user: YOUR_USERNAME
password: YOUR_PASSWORD
role: YOUR_ROLE
warehouse: YOUR_WAREHOUSE
database: YOUR_DATABASE
```

**Option B: Move to ~/.dbt/ (Recommended)**

```bash
# Create dbt directory
mkdir -p ~/.dbt

# Copy and edit profiles.yml
cp profiles.yml ~/.dbt/profiles.yml
nano ~/.dbt/profiles.yml  # Edit with your credentials
```

### 3. Test Connection

```bash
dbt debug
```

Look for: `All checks passed!`

## Run the Demo

### Quick Run (All-in-One)

```bash
# Install packages, load data, build models, run tests
dbt deps && dbt seed && dbt run && dbt test
```

### Step-by-Step

```bash
# 1. Install dbt packages
dbt deps

# 2. Load sample data into Snowflake
dbt seed

# 3. Build all models (Bronze → Silver → Gold)
dbt run

# 4. Run data quality tests
dbt test

# 5. Generate and view documentation
dbt docs generate
dbt docs serve
```

## What You Should See

### dbt seed
```
Completed successfully
4/4 seeds loaded
```

### dbt run
```
Completed successfully
10/10 models built
- 4 views (Bronze)
- 3 tables (Silver)
- 3 tables (Gold)
```

### dbt test
```
Completed successfully
40+ tests passed
```

## View Results in Snowflake

```sql
-- Show all schemas created
SHOW SCHEMAS LIKE '%DBT%';

-- Query bronze layer
SELECT * FROM dbt_dev_bronze.stg_orders LIMIT 10;

-- Query silver layer
SELECT * FROM dbt_dev_silver.int_orders_enriched LIMIT 10;

-- Query gold layer - Daily Sales
SELECT * FROM dbt_dev_gold.mart_sales_daily
ORDER BY date_day DESC;

-- Query gold layer - Customer Lifetime Value
SELECT
    customer_value_tier,
    rfm_segment,
    COUNT(*) as customer_count,
    ROUND(SUM(lifetime_revenue), 2) as total_revenue
FROM dbt_dev_gold.mart_customer_lifetime_value
GROUP BY 1, 2
ORDER BY total_revenue DESC;

-- Query gold layer - Top Products
SELECT
    product_name,
    category,
    total_revenue,
    total_profit,
    sales_velocity,
    inventory_health
FROM dbt_dev_gold.mart_product_performance
WHERE is_top_revenue_product = true
ORDER BY revenue_rank;
```

## Troubleshooting

### "Connection refused" or "Invalid credentials"
- Check your Snowflake account identifier format
- Verify username/password in profiles.yml
- Ensure your Snowflake account is active

### "Database does not exist"
- Create the database in Snowflake first:
  ```sql
  CREATE DATABASE YOUR_DATABASE;
  ```

### "Insufficient privileges"
- Ensure your role has CREATE SCHEMA privileges
- Ask your Snowflake admin for appropriate permissions

### "Package not found"
Run `dbt deps` to install required packages

## Next Steps

1. ✅ Explore the [README.md](README.md) for complete demo flow
2. ✅ Check out the lineage graph at `http://localhost:8080`
3. ✅ Review model SQL in the `models/` directory
4. ✅ Customize models for your own use case

## Demo Highlights to Showcase

🎯 **Fusion Engine Benefits:**
- 30x faster parsing
- Real-time SQL validation
- Column-level lineage
- State-aware orchestration
- Cost optimization

🏗️ **Architecture:**
- Complete medallion implementation
- 10 models across 3 layers
- Incremental processing
- 40+ data quality tests

💼 **Business Value:**
- Daily sales analytics
- Customer lifetime value
- Product performance insights
- RFM segmentation

---

**Ready to demo? You got this! 🚀**
