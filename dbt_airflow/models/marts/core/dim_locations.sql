-- SCD1: using delete+insert without is_incremental()

{{
  config(
    materialized='incremental',
    unique_key='location_key',
    incremental_strategy='delete+insert',
  )
}}

WITH delta_rows AS (
    SELECT
        *
    FROM {{ ref('stg_postgres__locations') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['location_id']) }} AS location_key,
    location_id,
    street_address,
    city,
    state,
    zipcode,
    country
FROM delta_rows