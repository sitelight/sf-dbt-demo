{{
  config(
    materialized='view',
    tags=['bronze', 'staging', 'customers']
  )
}}

/*
  Bronze Layer: Customer Staging Model

  Purpose: Light transformation of raw customer data
  - Standardize column names
  - Cast data types appropriately
  - Add surrogate keys if needed
  - Preserve raw data integrity

  This view showcases Snowflake Fusion Engine's real-time validation
  without consuming warehouse resources.
*/

with source as (
    select * from {{ ref('raw_customers') }}
),

renamed as (
    select
        -- Primary Key
        customer_id,

        -- Customer Information
        first_name,
        last_name,
        {{ dbt_utils.generate_surrogate_key(['first_name', 'last_name', 'email']) }} as customer_key,
        first_name || ' ' || last_name as full_name,
        lower(trim(email)) as email,
        phone,

        -- Location
        city,
        state,
        country,
        postal_code,

        -- Metadata
        created_at,
        updated_at,

        -- Audit columns for tracking
        current_timestamp() as dbt_loaded_at,
        '{{ invocation_id }}' as dbt_invocation_id

    from source
)

select * from renamed
