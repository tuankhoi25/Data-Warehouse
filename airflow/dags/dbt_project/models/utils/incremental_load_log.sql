{{
    config(
        order_by='model_name',
        primary_key='model_name'
    )
}}

SELECT
    'Data Build Tool'::String AS model_name,
    toDateTime('1999-01-01 00:00:00') AS last_ingested_ts