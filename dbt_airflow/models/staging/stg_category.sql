WITH latest_ingestion_timestamp AS (
    SELECT 
        max(_ingested_at) AS max_ingested_at
    FROM {{ ref('raw_category') }}
),
latest_raw_category AS (
    SELECT 
        rc.id,
        rc.category_name
    FROM {{ ref('raw_category') }} AS rc
    INNER JOIN latest_ingestion_timestamp AS lit
        ON rc._ingested_at = lit.max_ingested_at
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