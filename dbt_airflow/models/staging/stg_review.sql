WITH latest_ingestion_timestamp AS (
    SELECT 
        max(_ingested_at) AS max_ingested_at
    FROM {{ ref('raw_review') }}
),
latest_raw_review AS (
    SELECT 
        rv.id,
        rv.customer_id,
        rv.product_id,
        rv.star_rating,
        rv.helpful_votes,
        rv.total_votes,
        rv.marketplace,
        rv.verified_purchase,
        rv.review_headline,
        rv.review_body,
        rv.updated_at,
    FROM {{ ref('raw_review') }} AS rv
    INNER JOIN latest_ingestion_timestamp AS lit
        ON rv._ingested_at = lit.max_ingested_at
)

SELECT 
    id AS review_id,
    customer_id,
    product_id,
    CASE
        WHEN lower(star_rating) IN ('one', '1', '1 star', '1*', '*') THEN '1'
        WHEN lower(star_rating) IN ('two', '2', '2 stars', '2*', '**') THEN '2'
        WHEN lower(star_rating) IN ('three', '3', '3 stars', '3*', '***') THEN '3'
        WHEN lower(star_rating) IN ('four', '4', '4 stars', '4*', '****') THEN '4'
        WHEN lower(star_rating) IN ('five', '5', '5 stars', '5*', '*****') THEN '5'
        ELSE '9'
    END AS star_rating,
    helpful_votes,
    total_votes,
    marketplace,
    toBool(
        CASE
            WHEN lower(verified_purchase) IN (
                'yes', 'y', 'true', 't', '1',
                'verified', 'purchased', 'confirmed'
            )
            THEN 'true'
            ELSE 'false'
        END
    ) AS verified_purchase,
    review_headline,
    review_body,
    updated_at AS modified_date
FROM latest_raw_review