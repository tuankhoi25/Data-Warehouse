-- SCD1: using delete+insert without is_incremental()

{{
  config(
    order_by="(category_key)",
    primary_key="(category_key)",
    materialized="incremental",
    unique_key="category_key",
    incremental_strategy="delete+insert",
  )
}}

WITH delta_rows AS (
    SELECT
        *
    FROM {{ ref('stg_postgres__categories') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['category_id']) }} AS category_key,
    category_id,
    category_name
FROM delta_rows