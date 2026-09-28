{#
    Use the custom schema (BigQuery dataset) exactly as configured, instead of dbt's default of
    prefixing it with the target schema. This is what lets the staging view live in marineflow_silver
    and the gold tables in marineflow_gold (see dbt_project.yml).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
