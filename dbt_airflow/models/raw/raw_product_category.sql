WITH pg_product_category AS (
    SELECT * FROM {{ source('postgres', 'product_category') }}
)

SELECT 
    id,
    product_id,
    category_id,
    created_at,
    updated_at,
    now64(3) AS _ingested_at,
    '{{ invocation_id }}' AS _batch_id
FROM pg_product_category

{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}