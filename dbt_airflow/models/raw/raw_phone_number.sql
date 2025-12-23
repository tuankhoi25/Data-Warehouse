WITH pg_phone_number AS (
    SELECT * FROM {{ source('postgres', 'phone_number') }}
)

SELECT 
    id,
    phone_number,
    created_at,
    updated_at,
    now64(3) AS _ingested_at,
    '{{ invocation_id }}' AS _batch_id
FROM pg_phone_number

{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}