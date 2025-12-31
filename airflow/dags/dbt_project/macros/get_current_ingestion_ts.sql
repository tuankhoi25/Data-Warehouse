{%- macro get_current_ingestion_ts() -%}
parseDateTimeBestEffort('{{ var("logical_date") }}')
{%- endmacro -%}