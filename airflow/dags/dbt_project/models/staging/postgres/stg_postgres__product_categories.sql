{{
    config(
        order_by="(product_id)",
        primary_key="(product_id)",
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
        pc.id,
        pc.product_id,
        pc.category_id,
        pc.created_at,
        pc.updated_at
    FROM {{ source('postgres', 'product_category') }} AS pc
    {%- if is_exists(this.database, this.schema, this.identifier) %}
    CROSS JOIN watermark AS w
    WHERE 1 = 1
        AND pc.updated_at > w.last_ingested_ts
        AND pc.updated_at <= w.current_ingest_ts
    {%- endif -%}
),
handle_null AS (
    SELECT * REPLACE (
        coalesce(updated_at, created_at) AS updated_at
    )
    FROM new_record
),
type_casting AS (
    SELECT * REPLACE (
        cast(updated_at, 'Date') AS updated_at
    )
    FROM handle_null
)

SELECT 
    id AS product_category_id,
    product_id,
    category_id,
FROM type_casting