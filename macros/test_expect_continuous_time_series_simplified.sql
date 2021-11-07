{% test expect_continuous_time_series_simplified(model,partition_by_columns, date_column) %}
        {{ config(store_failures = true) }}
        {%- set partition_by_columns=partition_by_columns | join(', ') -%}
        with windowed as (
            
                select {{partition_by_columns}},{{date_column}} as curr_date,
                lag({{date_column}}) over (
                    PARTITION BY {{partition_by_columns}} ORDER BY {{date_column}}
                ) as previous_date
            
            from {{model}}
        ),

        validation_errors as (
            SELECT {{partition_by_columns}},previous_date, curr_date, DATEDIFF(MONTH, previous_date, curr_date) AS MONTH_DIFF FROM windowed
            WHERE MONTH_DIFF <> 1
        
        )

        select * from validation_errors
{% endtest %}


