{#
  Small helpers so the SAME model SQL runs on DuckDB (dev) and Snowflake (prod).
  dbt's "dispatch" picks the right version for the current warehouse.
#}

{# value of `expr` on the row where `order_by` is highest #}
{% macro arg_max(expr, order_by) %}
  {{ return(adapter.dispatch('arg_max', 'aqi')(expr, order_by)) }}
{% endmacro %}

{% macro default__arg_max(expr, order_by) %}max_by({{ expr }}, {{ order_by }}){% endmacro %}
{% macro duckdb__arg_max(expr, order_by) %}arg_max({{ expr }}, {{ order_by }}){% endmacro %}
