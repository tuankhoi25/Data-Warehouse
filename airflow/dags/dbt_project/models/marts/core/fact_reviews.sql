-- SCD2: using delete+insert with is_incremental()

{{
  config(
    order_by="(is_current, valid_from, review_id, review_key)",
    primary_key="(is_current, valid_from, review_id, review_key)",
    materialized="incremental",
    unique_key="review_key",
    incremental_strategy="delete+insert",
  )
}}

WITH delta_rows AS (
    SELECT
        *
    FROM {{ ref('stg_postgres__reviews') }}
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
        {{ dbt_utils.generate_surrogate_key(['er.review_id', 'er.valid_from']) }} AS review_key,
        er.review_id,
        er.customer_key,
        er.product_key,
        er.star_rating,
        er.helpful_votes,
        er.total_votes,
        er.marketplace,
        er.verified_purchase,
        er.review_headline,
        er.review_body,
        er.valid_from,
        er.valid_from_date_key,
        toNullable(dr.source_updated_at) AS valid_to,
        FALSE AS is_current
    FROM active_rows AS er
    INNER JOIN delta_rows AS dr
        ON er.review_id = dr.review_id
    WHERE er.valid_from < dr.source_updated_at
),
{% endif %}

dim_customers AS (
    SELECT 
        customer_key,
        customer_id
    FROM {{ ref('dim_customers') }}
),
dim_products AS (
    SELECT 
        product_key,
        product_id
    FROM {{ ref('dim_products') }}
    WHERE is_current = TRUE
),
inserted_rows AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['dr.review_id', 'dr.source_updated_at']) }} AS review_key,
        dr.review_id,
        dc.customer_key,
        dp.product_key,
        dr.star_rating,
        dr.helpful_votes,
        dr.total_votes,
        dr.marketplace,
        dr.verified_purchase,
        dr.review_headline,
        dr.review_body,
        dr.source_updated_at AS valid_from,
        toInt64(formatDateTime(dr.source_updated_at, '%Y%m%d')) AS valid_from_date_key,
        (lead(toNullable(dr.source_updated_at), 1) OVER (PARTITION BY dr.review_id ORDER BY dr.source_updated_at ASC)) AS valid_to,
        CASE
            WHEN valid_to IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current
    FROM delta_rows AS dr
    JOIN dim_customers AS dc
        ON dc.customer_id = dr.customer_id
    JOIN dim_products AS dp
        ON dp.product_id = dr.product_id
)

SELECT
    *
FROM inserted_rows
{% if is_incremental() %}
UNION ALL
SELECT 
    *
FROM expired_rows
{% endif %}