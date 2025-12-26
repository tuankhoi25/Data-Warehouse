-- SCD1: using delete+insert without is_incremental()

{{
  config(
    materialized='incremental',
    unique_key='customer_key',
    incremental_strategy='delete+insert',
  )
}}

WITH delta_rows AS (
    SELECT
        *
    FROM {{ ref('stg_postgres__customers') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_key,
    customer_id,
    sex,
    customer_name,
    mail,
    birth_date,
    signup_date
FROM delta_rows