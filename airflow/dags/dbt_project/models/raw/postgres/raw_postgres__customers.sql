{% set current_ingest_ts = get_current_ingestion_ts() %}

WITH pg_customer AS (
    SELECT * FROM {{ source('postgres', 'customer') }}
)

SELECT 
    id,
    name,
    sex,
    mail,
    birthdate,
    login_username,
    login_password,
    created_at,
    updated_at,
    {{ current_ingest_ts }} AS _ingested_at,
    cast('{{ invocation_id }}', 'String') AS _batch_id
FROM pg_customer
WHERE 1=1

{% if is_incremental() %}
    AND updated_at > (SELECT max(updated_at) FROM {{ this }})
    AND updated_at <= {{ current_ingest_ts }}
{% endif %}