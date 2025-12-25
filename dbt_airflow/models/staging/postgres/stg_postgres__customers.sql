{% set current_ingest_ts = get_current_ingestion_ts() %}

WITH latest_raw_customer AS (
    SELECT 
        id,
        name,
        sex,
        mail,
        birthdate,
        created_at
    FROM {{ ref('raw_postgres__customers') }}
    WHERE _ingested_at = {{ current_ingest_ts }}
)

SELECT
    id AS customer_id,
    coalesce(name, 'Unknown') AS customer_name,
    cast(
        CASE
            WHEN sex IS NULL THEN 3
            WHEN lower(sex) IN ('1', 'm', 'male') THEN 1
            WHEN lower(sex) IN ('2', 'f', 'female') THEN 2
            WHEN lower(sex) IN ('3', 'o', 'other') THEN 3
            ELSE 3
        END,
        'Int8'
    ) AS sex,
    coalesce(mail, 'Unknown@gmail.com') AS mail,
    coalesce(birthdate, toDateTime('1900-01-01')) AS birth_date,
    created_at AS signup_date
FROM latest_raw_customer