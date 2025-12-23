WITH pg_shadow_product AS (
    SELECT * FROM {{ source('postgres', 'shadow_product') }}
)

SELECT 
    id,
    product_id,
    product_title,
    currency,
    price,
    created_at,
    updated_at,
    now64(3) AS _ingested_at,
    '{{ invocation_id }}' AS _batch_id
FROM pg_shadow_product

{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}