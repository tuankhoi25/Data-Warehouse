{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT *
FROM postgresql(
    'postgres:5432',
    'oltp',
    'category',
    'clickhouse_user',
    'clickhouse_password'
)
{% if is_incremental() %}
WHERE id > (SELECT max(id) FROM {{ this }})
{% endif %}