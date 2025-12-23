WITH pg_location AS (
    SELECT * FROM {{ source('postgres', 'location') }}
)

SELECT 
    id,
    street_address,
    city,
    state,
    zipcode,
    country,
    created_at,
    updated_at,
    now64(3) AS _ingested_at,
    '{{ invocation_id }}' AS _batch_id
FROM pg_location

{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}