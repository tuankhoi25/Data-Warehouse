-- SCD2: using delete+insert with is_incremental()

{{
  config(
    materialized='incremental',
    unique_key='customer_location_key',
    incremental_strategy='delete+insert',
  )
}}

WITH delta_rows AS (
    SELECT
        *
    FROM {{ ref('stg_postgres__customer_locations') }}
),

{% if is_incremental() %}
active_rows AS (
    SELECT
        *
    FROM {{ this }}
    WHERE is_current = TRUE
),
expired_rows AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['er.customer_location_id', 'er.valid_from']) }} AS customer_location_key,
        er.customer_location_id,
        er.customer_key,
        er.location_key,
        er.valid_from,
        dr.source_updated_at AS valid_to,
        FALSE AS is_current
    FROM active_rows AS er
    INNER JOIN delta_rows AS dr
        ON er.customer_location_id = dr.customer_location_id
    WHERE er.valid_from < dr.source_updated_at
),
{% endif %}

dim_customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),
dim_locations AS (
    SELECT * FROM {{ ref('dim_locations') }}
),
inserted_rows AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_location_id', 'source_updated_at']) }} AS customer_location_key,
        customer_location_id,
        dc.customer_key,
        dl.location_key,
        source_updated_at AS valid_from,
        CAST(NULL AS Nullable(DateTime)) AS valid_to,
        TRUE AS is_current
    FROM delta_rows AS dr
    JOIN dim_customers AS dc
        ON dc.customer_id = dr.customer_id
    JOIN dim_locations AS dl
        ON dl.location_id = dr.location_id
)

SELECT
    *
FROM inserted_rows
{% if is_incremental() %}
UNION ALL
SELECT * FROM expired_rows
{% endif %}