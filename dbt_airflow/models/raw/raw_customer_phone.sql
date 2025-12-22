WITH pg_customer_phone AS (
    SELECT * FROM {{ source('postgres', 'customer_phone') }}
)

SELECT 
    *,
    now64(3) AS _ingested_at,
    '{{ invocation_id }}' AS _batch_id
FROM pg_customer_phone

{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }}) AND updated_at < toDateTime64('2009-12-14 01:01:01', 3)
{% endif %}