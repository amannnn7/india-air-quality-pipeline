{#
  By default dbt names custom schemas "<target>_<custom>" (e.g. main_ref).
  We want clean names: main, ref.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}{{ target.schema }}{%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}
