{{
    config(
        materialized = "table"
    )
}}

{{ generate_dim_dates(
        start_date='2005-01-01', 
        end_date='2015-01-01'
    ) 
}}