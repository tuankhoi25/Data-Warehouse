{{
    config(
        order_by="(source_updated_at, customer_id, location_id, customer_location_id)",
        primary_key="(source_updated_at, customer_id, location_id, customer_location_id)",
        post_hook=[
            "DELETE FROM {{ ref('incremental_load_log') }} WHERE model_name='{{ this.name }}'",
            "INSERT INTO {{ ref('incremental_load_log') }} (model_name, last_ingested_ts) VALUES ('{{ this.name }}', {{ get_current_ingestion_ts() }})"
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
        cl.id,
        cl.customer_id,
        cl.location_id,
        cl.created_at,
        cl.updated_at
    FROM {{ source('postgres', 'customer_location') }} AS cl
    {%- if is_exists(this.database, this.schema, this.identifier) %}
    CROSS JOIN watermark AS w
    WHERE 1 = 1
        AND cl.updated_at > w.last_ingested_ts
        AND cl.updated_at <= w.current_ingest_ts
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
    id AS customer_location_id,
    customer_id,
    location_id,
    created_at AS source_created_at,
    updated_at AS source_updated_at
FROM type_casting