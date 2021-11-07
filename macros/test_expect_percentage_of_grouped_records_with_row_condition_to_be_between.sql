{% test expect_percentage_of_grouped_records_with_row_condition_to_be_between(model,group_by_columns, row_condition, percentage_row_condition,min_percentage=0, max_percentage=100, strict_min= false, strict_max=false ) %}
  {{ config(severity = 'warn') }}
  {{ return(adapter.dispatch('expect_percentage_of_grouped_records_with_row_condition_to_be_between')(model,group_by_columns, row_condition, percentage_row_condition, min_percentage, max_percentage,strict_min, strict_max)) }}
{% endtest %}

{% macro snowflake__expect_percentage_of_grouped_records_with_row_condition_to_be_between(model,group_by_columns, row_condition, percentage_row_condition, min_percentage=0, max_percentage=100, strict_min= false, strict_max= false ) %}
{%- set ns1 = namespace(select_string='') -%}
{%- set ns2 = namespace(join_string='') -%}


{% if (min_percentage<0 or min_percentage>100)  %}
    {{ exceptions.raise_compiler_error(
        "`min_percentage` argument must be between 0 and 100."
    ) }}
{% elif (max_percentage<0 or max_percentage>100)  %}
    {{ exceptions.raise_compiler_error(
        "`max_percentage` argument must be between 0 and 100."
    ) }}

{% endif %}

{%- for column in group_by_columns -%}
    {%- set ns1.select_string = ns1.select_string ~ "counter_total." ~ column -%}
    {%- if not loop.last -%}
         {%- set ns1.select_string= ns1.select_string ~ ", "  -%}
    {%- endif -%}
    {%- set ns2.join_string = ns2.join_string ~ "validation_errors." ~ column ~ "= " ~ "counter_total." ~ column -%}
    {%- if not loop.last %}
         {%- set ns2.join_string= ns2.join_string ~ " AND " -%}
    {%- endif -%}
{%- endfor -%}

{%- set columns=group_by_columns | join(', ') -%}

with validation_errors as(
  select {{columns}},count(*) count_of_records_with_row_condition
  from {{model}} 
  where {{row_condition}}
  group by {{columns}}
  
),
counter_total as (
    select {{columns}}, count(*) total_count_of_records from {{model}}  group by {{columns}}
),
inner_join as(
  select {{ns1.select_string}}, counter_total.total_count_of_records, validation_errors.count_of_records_with_row_condition 
  from counter_total 
  inner join validation_errors
  on ({{ns2.join_string}})

),
total_perc as (
  select {{columns}}, round((count_of_records_with_row_condition/total_count_of_records)*100,2) as perc
  from inner_join
  {% if percentage_row_condition %}
        {%- if strict_min and strict_max -%}
                where (perc > {{min_percentage}} and perc < {{max_percentage}}) and {{percentage_row_condition}}
        {%- elif  strict_min and not strict_max -%}
                where (perc > {{min_percentage}} and perc <= {{max_percentage}}) and {{percentage_row_condition}}
        {%- elif  not strict_min and strict_max -%}
                where (perc >= {{min_percentage}} and perc < {{max_percentage}}) and {{percentage_row_condition}}
        {%- else -%}
                where (perc >= {{min_percentage}} and perc <= {{max_percentage}}) and {{percentage_row_condition}}
        {%- endif -%}
  {%- else -%}
        {%- if strict_min and strict_max -%}
                where (perc > {{min_percentage}} and perc < {{max_percentage}}) 
        {%- elif  strict_min and not strict_max -%}
                where (perc > {{min_percentage}}  and perc <= {{max_percentage}}) 
        {%- elif  not strict_min and strict_max -%}
                where (perc >= {{min_percentage}} and perc < {{max_percentage}}) 
        {%- else -%}
                where (perc >= {{min_percentage}} and perc <= {{max_percentage}})
        {%- endif -%}  
  {%- endif -%}
)
select * from total_perc

{% endmacro %}