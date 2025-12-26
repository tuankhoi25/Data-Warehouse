{% set current_ingest_ts = get_current_ingestion_ts() %}

WITH latest_raw_review AS (
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
        updated_at
    FROM {{ ref('raw_postgres__reviews') }}
    WHERE _ingested_at = {{ current_ingest_ts }}
)

SELECT 
    id AS review_id,
    customer_id,
    product_id,
    cast(
        CASE
            WHEN star_rating IS NULL THEN -1
            WHEN lower(star_rating) IN ('one', '1', '1 star', '1*', '*') THEN 1
            WHEN lower(star_rating) IN ('two', '2', '2 stars', '2*', '**') THEN 2
            WHEN lower(star_rating) IN ('three', '3', '3 stars', '3*', '***') THEN 3
            WHEN lower(star_rating) IN ('four', '4', '4 stars', '4*', '****') THEN 4
            WHEN lower(star_rating) IN ('five', '5', '5 stars', '5*', '*****') THEN 5
            ELSE -1
        END,
        'Int8'
    ) AS star_rating,
    coalesce(helpful_votes, 0) AS helpful_votes,
    coalesce(total_votes, 0) AS total_votes,
    coalesce(marketplace, 'Unknown') AS marketplace,
    CASE
        WHEN verified_purchase IS NULL THEN FALSE
        WHEN lower(verified_purchase) IN (
            'yes', 'y', 'true', 't', '1',
            'verified', 'purchased', 'confirmed'
        )
        THEN TRUE
        ELSE FALSE
    END AS verified_purchase,
    coalesce(review_headline, 'Unknown') AS review_headline,
    coalesce(review_body, 'Unknown') AS review_body,
    created_at AS source_created_at,
    updated_at AS source_updated_at
FROM latest_raw_review