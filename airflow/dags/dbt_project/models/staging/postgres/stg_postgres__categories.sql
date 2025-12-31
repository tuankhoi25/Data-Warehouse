{{
    config(
        order_by="(category_id)",
        primary_key="(category_id)",
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
        c.id,
        c.category_name,
        c.updated_at
    FROM {{ source('postgres', 'category') }} AS c
    {%- if is_exists(this.database, this.schema, this.identifier) %}
    CROSS JOIN watermark AS w
    WHERE 1 = 1
        AND c.updated_at > w.last_ingested_ts
        AND c.updated_at <= w.current_ingest_ts
    {%- endif -%}
),
type_casting AS (
    SELECT * REPLACE (
        cast(updated_at, 'Date') AS updated_at
    )
    FROM new_record
)

SELECT 
    id AS category_id,
    CASE
        WHEN category_name IN (
            'Personal_Care_Appliances',
            'Toys',
            'Beauty',
            'Video Games',
            'Digital_Ebook_Purchase',
            'Watches',
            'Pet Products',
            'Grocery',
            'Other',
            'Mobile_Apps',
            'Office Products',
            'Camera',
            'Wireless',
            'Apparel',
            'Automotive',
            'Outdoors',
            'Major Appliances',
            'Furniture',
            'Tools',
            'Books',
            'Musical Instruments',
            'Baby',
            'Health & Personal Care',
            'Sports',
            'Electronics',
            'Mobile_Electronics',
            'Shoes'
        )
        THEN category_name
        ELSE 'Other'
    END AS category_name
FROM type_casting