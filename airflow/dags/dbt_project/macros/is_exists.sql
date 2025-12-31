{% macro is_exists(database, schema, identifier) %}
    {% if not execute %}
        {{ return(false) }}
    {% endif %}

    {% set relation = adapter.get_relation(
        database=database,
        schema=schema,
        identifier=identifier
    ) %}

    {% if relation is none %}
        {{ return(false) }}
    {% endif %}

    {{ return(true) }}
{% endmacro %}