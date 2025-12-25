{% set current_ingest_ts = get_current_ingestion_ts() %}

WITH last_raw_customer_location AS (
    SELECT 
        id,
        customer_id,
        location_id,
        created_at,
        updated_at
    FROM {{ ref('raw_postgres__customer_locations') }}
    WHERE _ingested_at = {{ current_ingest_ts }}
)

SELECT 
    id AS customer_location_id,
    customer_id,
    location_id,
    created_at AS source_created_at,
    updated_at AS source_updated_at
FROM last_raw_customer_location