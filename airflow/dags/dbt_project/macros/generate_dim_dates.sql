{% macro generate_dim_dates(start_date, end_date) %}

{# 
  start_date, end_date truyền vào dạng 'YYYY-MM-DD'
#}

with dates as (

    select
        addDays(toDate('{{ start_date }}'), number) as full_date
    from numbers(
        dateDiff(
            'day',
            toDate('{{ start_date }}'),
            toDate('{{ end_date }}')
        ) + 1
    )

)

select
    full_date,

    -- surrogate key
    toYYYYMMDD(full_date) as date_key,

    -- day
    toDayOfMonth(full_date) as day,

    -- month
    toMonth(full_date) as month,
    monthName(full_date) as month_name,

    -- quarter
    toQuarter(full_date) as quarter,
    concat('Q', toString(toQuarter(full_date))) as quarter_name,

    -- year
    toYear(full_date) as year,

    -- day of week (Monday = 1, Sunday = 7)
    toDayOfWeek(full_date) as day_of_week,
    dateName('weekday', full_date) as day_name,

    -- week of year (ISO)
    toISOWeek(full_date) as week_of_year,

    -- flags
    toDayOfWeek(full_date) in (6, 7) as is_weekend,
    false as is_holiday

from dates
order by full_date

{% endmacro %}
