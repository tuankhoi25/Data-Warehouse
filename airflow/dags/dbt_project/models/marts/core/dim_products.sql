-- SCD2: using delete+insert with is_incremental()

{{
    config(
        order_by="(is_current, valid_from, product_id, product_key)",
        primary_key="(is_current, valid_from, product_id, product_key)",
        materialized="incremental",
        unique_key="product_key",
        incremental_strategy="delete+insert",
    )
}}

WITH delta_rows AS (
    SELECT
        *
    FROM {{ ref('stg_postgres__products') }}
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
        {{ dbt_utils.generate_surrogate_key(['er.product_id', 'er.valid_from']) }} AS product_key,
        er.product_id,
        er.product_title,
        er.currency,
        er.price,
        er.valid_from,
        dr.source_updated_at AS valid_to,
        FALSE AS is_current
    FROM active_rows AS er
    INNER JOIN delta_rows AS dr
        ON er.product_id = dr.product_id
    WHERE er.valid_from < dr.source_updated_at

),
{% endif %}

inserted_rows AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['product_id', 'source_updated_at']) }} AS product_key,
        product_id,
        product_title,
        currency,
        price,
        source_updated_at AS valid_from,
        CAST(NULL AS Nullable(DateTime)) AS valid_to,
        TRUE AS is_current
    FROM delta_rows
)

SELECT
    *
FROM inserted_rows
{% if is_incremental() %}
UNION ALL
SELECT * FROM expired_rows
{% endif %}