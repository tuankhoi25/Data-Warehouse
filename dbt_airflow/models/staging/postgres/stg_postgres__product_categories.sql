{% set current_ingest_ts = get_current_ingestion_ts() %}

WITH latest_raw_product_category AS (
    SELECT 
        id,
        product_id,
        category_id
    FROM {{ ref('raw_postgres__product_categories') }}
    WHERE _ingested_at = {{ current_ingest_ts }}
)

SELECT 
    id AS product_category_id,
    product_id,
    category_id
FROM latest_raw_product_category