{{
    config(
        order_by="(date_key)",
        primary_key="(date_key)",
        materialized = "table"
    )
}}

{% set start_date = '2005-01-01' %}
{% set end_date = '2015-01-01' %}

WITH dates AS (

    SELECT
        addDays(toDate('{{ start_date }}'), number) AS full_date
    FROM numbers(
        dateDiff(
            'day',
            toDate('{{ start_date }}'),
            toDate('{{ end_date }}')
        ) + 1
    )

)

SELECT
    full_date,
    toYYYYMMDD(full_date) AS date_key,
    toDayOfMonth(full_date) AS day,
    toMonth(full_date) AS month,
    monthName(full_date) AS month_name,
    toQuarter(full_date) AS quarter,
    concat('Q', toString(toQuarter(full_date))) AS quarter_name,
    toYear(full_date) AS year,
    toDayOfWeek(full_date) AS day_of_week,
    dateName('weekday', full_date) AS day_name,
    toISOWeek(full_date) AS week_of_year,
    toDayOfWeek(full_date) in (6, 7) AS is_weekend,
    false AS is_holiday
FROM dates