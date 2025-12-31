-- SCD1: using delete+insert without is_incremental()

{{
  config(
    order_by="(product_category_key)",
    primary_key="(product_category_key)",
    materialized="incremental",
    unique_key="product_category_key",
    incremental_strategy="delete+insert",
  )
}}

WITH delta_rows AS (
    SELECT
        *
    FROM {{ ref('stg_postgres__product_categories') }}
),
dim_categories AS (
    SELECT 
        * 
    FROM {{ ref('dim_categories') }}
),
dim_products AS (
    SELECT 
        * 
    FROM {{ ref('dim_products') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['dr.product_category_id']) }} AS product_category_key,
    dr.product_category_id,
    dp.product_key,
    dc.category_key
FROM delta_rows AS dr
JOIN dim_categories AS dc
    ON dc.category_id = dr.category_id
JOIN dim_products AS dp
    ON dp.product_id = dr.product_id