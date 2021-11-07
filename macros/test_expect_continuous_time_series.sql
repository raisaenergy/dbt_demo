{% test expect_continuous_time_series(model,partition_by_columns, day_column_or_value, month_column_or_value, year_column_or_value, date_column, date_from_parts = false) %}
  {{ config(store_failures = true) }}
  {{ return(adapter.dispatch('expect_continuous_time_series')(model,partition_by_columns,day_column_or_value, month_column_or_value, year_column_or_value, date_column, date_from_parts)) }}

{% endtest %}

{% macro default__expect_continuous_time_series(model,partition_by_columns, day_column_or_value, month_column_or_value, year_column_or_value,date_column, date_from_parts = false) %}
{%- set partition_by_columns=partition_by_columns | join(', ') -%}
{% if date_from_parts and (not day_column_or_value and not month_column_or_value and not year_column_or_value ) %}
    {{ exceptions.raise_compiler_error(
        "`date_from_parts` argument is set to True, so you have to pass arguments `day_column_or_value` , `month_column_or_value`, `year_column_or_value` ."
    ) }}
{% endif %}


with windowed as (
    {% if date_from_parts %}
        select {{partition_by_columns}},DATE_FROM_PARTS({{year_column_or_value}},{{month_column_or_value}},{{day_column_or_value}}) as curr_date,
        {% if (year_column_or_value|int == 0) and (month_column_or_value|int == 0) and (day_column_or_value|int == 0) %}
            lag(DATE_FROM_PARTS({{year_column_or_value}},{{month_column_or_value}},{{day_column_or_value}})) over (
                PARTITION BY {{partition_by_columns}} ORDER BY {{year_column_or_value}},{{month_column_or_value}},{{day_column_or_value}}
            ) as previous_date
        {% endif %}
        {% if (year_column_or_value|int == 0) and (month_column_or_value|int != 0) and (day_column_or_value|int == 0) %}
            lag(DATE_FROM_PARTS({{year_column_or_value}},{{month_column_or_value}},{{day_column_or_value}})) over (
                PARTITION BY {{partition_by_columns}} ORDER BY {{year_column_or_value}},{{day_column_or_value}}
            ) as previous_date
        {% endif %}
        {% if (year_column_or_value|int == 0) and (month_column_or_value|int == 0) and (day_column_or_value|int != 0) %}
            lag(DATE_FROM_PARTS({{year_column_or_value}},{{month_column_or_value}},{{day_column_or_value}})) over (
                PARTITION BY {{partition_by_columns}} ORDER BY {{year_column_or_value}},{{month_column_or_value}}
            ) as previous_date
        {% endif %}
        {% if (year_column_or_value|int != 0) and (month_column_or_value|int == 0) and (day_column_or_value|int == 0) %}
            lag(DATE_FROM_PARTS({{year_column_or_value}},{{month_column_or_value}},{{day_column_or_value}})) over (
                PARTITION BY {{partition_by_columns}} ORDER BY {{month_column_or_value}},{{day_column_or_value}}
            ) as previous_date
        {% endif %}
        {% if (year_column_or_value|int == 0) and (month_column_or_value|int != 0) and (day_column_or_value|int != 0) %}
            lag(DATE_FROM_PARTS({{year_column_or_value}},{{month_column_or_value}},{{day_column_or_value}})) over (
                PARTITION BY {{partition_by_columns}} ORDER BY {{year_column_or_value}}
            ) as previous_date
        {% endif %}
        {% if (year_column_or_value|int != 0) and (month_column_or_value|int != 0) and (day_column_or_value|int == 0) %}
            lag(DATE_FROM_PARTS({{year_column_or_value}},{{month_column_or_value}},{{day_column_or_value}})) over (
                PARTITION BY {{partition_by_columns}} ORDER BY {{day_column_or_value}}
            ) as previous_date
        {% endif %}
        {% if (year_column_or_value|int != 0) and (month_column_or_value|int == 0) and (day_column_or_value|int != 0) %}
            lag(DATE_FROM_PARTS({{year_column_or_value}},{{month_column_or_value}},{{day_column_or_value}})) over (
                PARTITION BY {{partition_by_columns}} ORDER BY {{month_column_or_value}}
            ) as previous_date
        {% endif %}
    {% else %}
        select {{partition_by_columns}},{{date_column}} as curr_date,
        lag({{date_column}}) over (
            PARTITION BY {{partition_by_columns}} ORDER BY {{date_column}}
        ) as previous_date
    {% endif %}

    from {{ model }}
),

validation_errors as (
    SELECT {{partition_by_columns}},previous_date, curr_date, DATEDIFF(MONTH, previous_date, curr_date) AS MONTH_DIFF FROM windowed
    WHERE MONTH_DIFF <> 1
   
)

select *
from validation_errors

{% endmacro %}