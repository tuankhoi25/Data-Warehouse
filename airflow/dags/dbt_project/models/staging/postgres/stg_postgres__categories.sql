{% set current_ingest_ts = get_current_ingestion_ts() %}

WITH latest_raw_category AS (
    SELECT 
        id,
        category_name
    FROM {{ ref('raw_postgres__categories') }}
    WHERE _ingested_at = {{ current_ingest_ts }}
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
FROM latest_raw_category