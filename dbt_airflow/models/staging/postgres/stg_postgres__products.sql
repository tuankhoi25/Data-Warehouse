{% set current_ingest_ts = get_current_ingestion_ts() %}

WITH latest_raw_product AS (
    SELECT 
        product_id,
        product_title,
        currency,
        price,
        created_at,
        updated_at
    FROM {{ ref('raw_postgres__products') }}
    WHERE _ingested_at = {{ current_ingest_ts }}
)

SELECT 
    product_id,
    coalesce(product_title, 'Unknown') AS product_title,
    coalesce(currency, 'Unknown') AS currency,
    coalesce(price, -1) AS price,
    created_at AS source_created_at,
    updated_at AS source_updated_at
FROM latest_raw_product