{% set current_ingest_ts = get_current_ingestion_ts() %}

WITH latest_raw_location AS (
    SELECT 
        id,
        street_address,
        city,
        state,
        zipcode,
        country
    FROM {{ ref('raw_postgres__locations') }}
    WHERE _ingested_at = {{ current_ingest_ts }}
)

SELECT 
    id AS location_id,
    coalesce(street_address, 'Unknown') AS street_address,
    coalesce(city, 'Unknown') AS city,
    coalesce(state, 'Unknown') AS state,
    leftPad(toString(zipcode), 6, '0') AS zipcode,
    coalesce(country, 'Unknown') AS country
FROM latest_raw_location