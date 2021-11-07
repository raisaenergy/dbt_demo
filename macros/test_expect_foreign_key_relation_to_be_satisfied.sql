{% test expect_foreign_key_relation_to_be_satisfied(model,column_name, table2, column2) %}
  {{ return(adapter.dispatch('expect_foreign_key_relation_to_be_satisfied')(model,column_name, table2, column2)) }}
{% endtest %}

{% macro snowflake__expect_foreign_key_relation_to_be_satisfied(model,column_name, table2, column2) %}

with table_a as (
select {{column2}},count(*) as num_rows
from {{table2}}
group by {{column2}}

),

table_b as (
select {{column_name}},count(*) as num_rows
from {{model}}
group by {{column_name}}
),


except_b as (
  select {{column_name}}
  from table_b
  except
  select {{column2}}
  from table_a
)


select *
from except_b



{% endmacro %}