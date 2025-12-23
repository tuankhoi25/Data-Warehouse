WITH latest_ingestion_timestamp AS (
    SELECT 
        max(_ingested_at) AS max_ingested_at
    FROM {{ ref('raw_location') }}
),
latest_raw_location AS (
    SELECT 
        rl.id,
        rl.street_address,
        rl.city,
        rl.state,
        rl.zipcode,
        rl.country,
    FROM {{ ref('raw_location') }} AS rl
    INNER JOIN latest_ingestion_timestamp AS lit
        ON rl._ingested_at = lit.max_ingested_at
)

SELECT 
    id AS location_id,
    street_address,
    city,
    state,
    leftPad(toString(zipcode), 6, '0') AS zipcode,
    country
FROM latest_raw_location