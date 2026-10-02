{#
  DuckDB only: expose the local Parquet lake as raw.air_quality_hourly, so the dbt source
  has the same name on both warehouses. On Snowflake we return a harmless no-op query.
#}
{% macro create_duckdb_raw_view() %}
  {% if target.type == 'duckdb' %}
    create schema if not exists raw;
    create or replace view raw.air_quality_hourly as
    select * exclude (date)
    from read_parquet(
        '{{ env_var("AQI_RAW_DIR", "../data/raw/air_quality_hourly") }}/*/*.parquet',
        hive_partitioning = true
    );
  {% else %}
    select 1
  {% endif %}
{% endmacro %}
