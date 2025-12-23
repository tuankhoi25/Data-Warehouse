WITH latest_ingestion_timestamp AS (
    SELECT 
        max(_ingested_at) AS max_ingested_at
    FROM {{ ref('customer_location') }}
),
last_raw_customer_location AS (
    SELECT
        cl.id,
        cl.customer_id,
        cl.location_id,
        cl.created_at,
        cl.updated_at
    FROM {{ ref('customer_location') }} AS cl
    INNER JOIN latest_ingestion_timestamp AS lit
        ON cl._ingested_at = lit.max_ingested_at
)
SELECT 
    id AS customer_location_id,
    customer_id,
    location_id,
    created_at AS source_created_at,
    updated_at AS source_updated_at
FROM last_raw_customer_location