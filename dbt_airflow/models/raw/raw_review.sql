WITH pg_review AS (
    SELECT * FROM {{ source('postgres', 'review') }}
)

SELECT 
    id,
    customer_id,
    product_id,
    star_rating,
    helpful_votes,
    total_votes,
    marketplace,
    verified_purchase,
    review_headline,
    review_body,
    created_at,
    updated_at,
    now64(3) AS _ingested_at,
    '{{ invocation_id }}' AS _batch_id
FROM pg_review

{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}