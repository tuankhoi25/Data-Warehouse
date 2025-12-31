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
        dr.source_updated_at AS valid_to,
        FALSE AS is_current
    FROM active_rows AS er
    INNER JOIN delta_rows AS dr
        ON er.review_id = dr.review_id
    WHERE er.valid_from < dr.source_updated_at
),
{% endif %}

dim_customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),
dim_products AS (
    SELECT * FROM {{ ref('dim_products') }}
    WHERE is_current = TRUE
),
inserted_rows AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['review_id', 'source_updated_at']) }} AS review_key,
        review_id,
        customer_key,
        product_key,
        star_rating,
        helpful_votes,
        total_votes,
        marketplace,
        verified_purchase,
        review_headline,
        review_body,
        source_updated_at AS valid_from,
        toInt64(formatDateTime(source_updated_at, '%Y%m%d')) AS valid_from_date_key,
        CAST(NULL AS Nullable(DateTime)) AS valid_to,
        TRUE AS is_current
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
SELECT * FROM expired_rows
{% endif %}