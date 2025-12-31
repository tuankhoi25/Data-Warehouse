{{
    config(
        order_by="(source_updated_at, product_id, customer_id, review_id)",
        primary_key="(source_updated_at, product_id, customer_id, review_id)",
        post_hook=[
            "DELETE FROM {{ ref('incremental_load_log').render() }} WHERE model_name='{{ this.name }}'",
            "INSERT INTO {{ ref('incremental_load_log').render() }} (model_name, last_ingested_ts) VALUES ( '{{ this.name }}', {{ get_current_ingestion_ts() }})"
        ]
    )
}}

WITH watermark AS (
    SELECT 
        toDate(last_ingested_ts) AS last_ingested_ts,
        toDate({{ get_current_ingestion_ts() }}) AS current_ingest_ts
    FROM {{ ref('incremental_load_log').render() }}
    WHERE model_name = '{{ this.name }}'
    LIMIT 1
),
new_record AS (
    SELECT 
        r.id,
        r.customer_id,
        r.product_id,
        r.star_rating,
        r.helpful_votes,
        r.total_votes,
        r.marketplace,
        r.verified_purchase,
        r.review_headline,
        r.review_body,
        r.created_at,
        r.updated_at
    FROM {{ source('postgres', 'review') }} AS r
    {%- if is_exists(this.database, this.schema, this.identifier) %}
    CROSS JOIN watermark AS w
    WHERE 1 = 1
        AND r.updated_at > w.last_ingested_ts
        AND r.updated_at <= w.current_ingest_ts
    {%- endif -%}
),
handle_null AS (
    SELECT * REPLACE (
        coalesce(helpful_votes, 0) AS helpful_votes,
        coalesce(total_votes, 0) AS total_votes,
        coalesce(marketplace, 'Un') AS marketplace,
        coalesce(verified_purchase, '0') AS verified_purchase,
        coalesce(review_headline, 'Unknown') AS review_headline,
        coalesce(review_body, 'Unknown') AS review_body,
        coalesce(updated_at, created_at) AS updated_at
    )
    FROM new_record
    WHERE 1 = 1
        AND star_rating IS NOT NULL
),
type_casting AS (
    SELECT * REPLACE (
        cast(star_rating, 'Int8') AS star_rating,
        cast(helpful_votes, 'Int32') AS helpful_votes,
        cast(total_votes, 'Int32') AS total_votes,
        cast(marketplace, 'LowCardinality(FixedString(2))') AS marketplace,
        cast(
            CASE 
                WHEN verified_purchase IN ('y', 'Y', 't', 'T', '1') THEN 1 
                ELSE 0
            END, 
            'Bool'
        ) AS verified_purchase,
        cast(review_headline, 'String') AS review_headline,
        cast(review_body, 'String') AS review_body,
        cast(updated_at, 'Date') AS updated_at
    )
    FROM handle_null
)

SELECT 
    id AS review_id,
    customer_id,
    product_id,
    star_rating,
    helpful_votes,
    total_votes,
    marketplace,
    verified_purchase,
    review_headline,
    review_body,
    created_at AS source_created_at,
    updated_at AS source_updated_at
FROM type_casting