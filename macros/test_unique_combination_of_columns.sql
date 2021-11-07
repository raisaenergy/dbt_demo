{% test unique_combination_of_columns(model, combination_of_columns,mostly) %}
  {{ return(adapter.dispatch('test_unique_combination_of_columns')(model, combination_of_columns, mostly)) }}
{% endtest %}

{% macro snowflake__test_unique_combination_of_columns(model, combination_of_columns, mostly) %}


{% if not mostly %}
    {%- set mostly=1 %}
{% elif mostly<0 or mostly>1 %}
    {{ exceptions.raise_compiler_error(
        "`mostly` argument must be between 0 and 1."
    ) }}

{% endif %}

{%- set columns_csv=combination_of_columns | join(', ') %}


with validation_errors as (

    select
        {{ columns_csv }}
    from {{ model }}
    group by {{ columns_csv }}
    having count(*) > 1
),
counter_total as (
    select 
        (select count(*) from validation_errors) as total_failed,
        (select count(*) from {{ model }}) as total_rows
),
percentage_calc as (
  select (total_failed / total_rows) perc from counter_total
),
final as (
  select {{ columns_csv }} from validation_errors, percentage_calc
  where (1-perc)< {{mostly}}
)
select * from final

{% endmacro %}