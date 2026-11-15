{# Models go to the schema they name (gold), not "<target>_gold". #}
{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ custom_schema_name or target.schema }}
{%- endmacro %}
