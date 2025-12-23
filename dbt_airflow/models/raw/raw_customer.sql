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
    now64(3) AS _ingested_at,
    '{{ invocation_id }}' AS _batch_id
FROM pg_customer

{% if is_incremental() %}
WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
{% endif %}