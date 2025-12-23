WITH latest_ingestion_timestamp AS (
    SELECT 
        max(_ingested_at) AS max_ingested_at
    FROM {{ ref('raw_product') }}
),
latest_raw_product AS (
    SELECT 
        rp.product_id,
        rp.product_title,
        rp.currency,
        rp.price,
        rp.created_at,
        rp.updated_at,
    FROM {{ ref('raw_product') }} AS rp
    INNER JOIN latest_ingestion_timestamp AS lit
        ON rp._ingested_at = lit.max_ingested_at
)

SELECT 
    product_id,
    product_title,
    currency,
    price,
    created_at AS source_created_at,
    updated_at AS source_updated_at,
FROM latest_raw_product