WITH latest_ingestion_timestamp AS (
    SELECT 
        max(_ingested_at) AS max_ingested_at
    FROM {{ ref('raw_product_category') }}
),
latest_raw_product_category AS (
    SELECT 
        rpc.id,
        rpc.product_id,
        rpc.category_id
    FROM {{ ref('raw_product_category') }} AS rpc
    INNER JOIN latest_ingestion_timestamp AS lit
        ON rpc._ingested_at = lit.max_ingested_at
)

SELECT 
    id AS product_category_id,
    product_id,
    category_id
FROM latest_raw_product_category