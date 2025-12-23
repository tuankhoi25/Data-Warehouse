WITH latest_ingestion_timestamp AS (
    SELECT 
        max(_ingested_at) AS max_ingested_at
    FROM {{ ref('raw_customer') }}
),
latest_raw_customer AS (
    SELECT 
        rc.id,
        rc.name,
        rc.sex,
        rc.mail,
        rc.birthdate,
        rc.created_at,
    FROM {{ ref('raw_customer') }} AS rc
    INNER JOIN latest_ingestion_timestamp AS lit
        ON rc._ingested_at = lit.max_ingested_at
)
SELECT
    id AS customer_id,
    name AS customer_name,
    CASE
        WHEN lower(sex) IN ('1', 'm', 'male') THEN '1'
        WHEN lower(sex) IN ('2', 'f', 'female') THEN '2'
        WHEN lower(sex) IN ('3', 'o', 'other') THEN '3'
        ELSE '3'
    END AS sex,
    mail,
    birthdate,
    created_at AS signup_date
FROM latest_raw_customer