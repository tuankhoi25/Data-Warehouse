SELECT * FROM {{ source('clickhouse', 'staging_category') }}
WHERE id < 5