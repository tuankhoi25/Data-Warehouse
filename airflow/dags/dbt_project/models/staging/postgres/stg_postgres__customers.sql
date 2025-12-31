{{
    config(
        order_by="(customer_id)",
        primary_key="(customer_id)",
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
        c.name,
        c.sex,
        c.mail,
        c.birthdate,
        c.created_at,
        c.updated_at
    FROM {{ source('postgres', 'customer') }} AS c
    {%- if is_exists(this.database, this.schema, this.identifier) %}
    CROSS JOIN watermark AS w
    WHERE 1 = 1
        AND c.updated_at > w.last_ingested_ts
        AND c.updated_at <= w.current_ingest_ts
    {%- endif -%}
),
handle_null AS
(
    SELECT * REPLACE (
        coalesce(name, 'Unknown') AS name,
        coalesce(sex, 'O') AS sex,
        coalesce(mail, 'Unknown@gmail.com') AS mail,
        coalesce(birthdate, toDateTime('1000-01-01')) AS birthdate
    )
    FROM new_record
),
type_casting AS (
    SELECT * REPLACE (
        cast(name, 'String') AS name,
        cast(
            CASE
                WHEN sex IN ('1', 'm', 'M') THEN 1
                WHEN sex IN ('2', 'f', 'F') THEN 2
                ELSE 3
            END,
            'Int8'
        ) AS sex,
        cast(mail, 'String') AS mail,
        cast(birthdate, 'Date') AS birthdate,
        cast(updated_at, 'Date') AS updated_at
    )
    FROM handle_null
)

SELECT
    id AS customer_id,
    name AS customer_name,
    sex,
    mail,
    birthdate AS birth_date,
    created_at AS signup_date
FROM type_casting