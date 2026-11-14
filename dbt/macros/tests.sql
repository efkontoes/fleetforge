{# Rows whose key columns repeat. #}
{% test unique_combination(model, columns) %}
  select {{ columns | join(", ") }}, count(*) as copies
  from {{ model }}
  group by {{ columns | join(", ") }}
  having count(*) > 1
{% endtest %}

{# Rows outside [min_value, max_value]; NULLs pass (not_null tests them). #}
{% test between(model, column_name, min_value, max_value) %}
  select {{ column_name }}
  from {{ model }}
  where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}

{# Rows for which a boolean SQL expression is false. #}
{% test holds(model, expression) %}
  select *
  from {{ model }}
  where not ({{ expression }})
{% endtest %}
