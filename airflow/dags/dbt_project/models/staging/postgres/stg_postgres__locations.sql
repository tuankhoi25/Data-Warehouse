{{
    config(
        order_by="(location_id)",
        primary_key="(location_id)",
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
        l.id,
        l.street_address,
        l.city,
        l.state,
        l.zipcode,
        l.country,
        l.created_at,
        l.updated_at
    FROM {{ source('postgres', 'location') }} AS l
    {%- if is_exists(this.database, this.schema, this.identifier) %}
    CROSS JOIN watermark AS w
    WHERE 1 = 1
        AND l.updated_at > w.last_ingested_ts
        AND l.updated_at <= w.current_ingest_ts
    {%- endif -%}
),
handle_null AS (
    SELECT * REPLACE(
        coalesce(zipcode, -1) AS zipcode,
        coalesce(city, 'Unknown') AS city,
        coalesce(state, 'Unknown') AS state,
        coalesce(country, 'Unknown') AS country,
        coalesce(updated_at, created_at) AS updated_at
    )
    FROM new_record
),
type_casting AS (
    SELECT * REPLACE (
        cast(city, 'LowCardinality(String)') AS city,
        cast(state, 'LowCardinality(String)') AS state,
        cast(leftPad(toString(zipcode), 6, '0'), 'FixedString(6)') AS zipcode,
        cast(updated_at, 'String') AS updated_at
    )
    FROM handle_null
)

SELECT 
    id AS location_id,
    street_address,
    city,
    state,
    zipcode,
    country,
    updated_at AS source_updated_at
FROM type_casting