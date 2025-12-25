{% set current_ingest_ts = get_current_ingestion_ts() %}

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
    {{ current_ingest_ts }} AS _ingested_at,
    cast('{{ invocation_id }}', 'String') AS _batch_id
FROM pg_review
WHERE 1=1

{% if is_incremental() %}
    AND updated_at > (SELECT max(updated_at) FROM {{ this }})
    AND updated_at <= {{ current_ingest_ts }}
{% endif %}