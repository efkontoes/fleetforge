{# The Silver build Gold is computed from: var silver_build_id, else the newest one. #}
{% macro silver_build() %}
  {% if not execute %}
    {{ return({"build_id": "", "events_snapshot_id": 0, "outcomes_snapshot_id": 0,
               "bronze_snapshot_id": 0}) }}
  {% endif %}
  {% set wanted = var("silver_build_id") %}
  {% set sql %}
    select build_id, events_snapshot_id, outcomes_snapshot_id, bronze_snapshot_id
    from {{ source("silver", "build_log") }}
    {% if wanted %} where build_id = '{{ wanted }}' {% endif %}
    order by committed_at desc
    limit 1
  {% endset %}
  {% set rows = run_query(sql).rows %}
  {% if rows | length == 0 %}
    {{ exceptions.raise_compiler_error("no published Silver build " ~ wanted) }}
  {% endif %}
  {% set r = rows[0] %}
  {{ return({"build_id": r[0], "events_snapshot_id": r[1], "outcomes_snapshot_id": r[2],
             "bronze_snapshot_id": r[3]}) }}
{% endmacro %}

{% macro at_snapshot(source_name, table_name, snapshot_id) -%}
  {{ source(source_name, table_name) }} for version as of {{ snapshot_id }}
{%- endmacro %}

{# SQL predicate: is this observed_date reopened for recomputation? #}
{% macro reopened(column) -%}
  {%- if var("reopen") -%} true
  {%- elif var("reopen_from") -%}
    {{ column }} between date '{{ var("reopen_from") }}' and date '{{ var("reopen_to") or var("reopen_from") }}'
  {%- else -%} false
  {%- endif -%}
{%- endmacro %}
