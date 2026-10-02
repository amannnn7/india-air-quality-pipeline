{#
  Fails for every non-null value outside [min_value, max_value].
  Catches unit mistakes (e.g. mg vs ug) and broken sensors/models early.
#}
{% test value_in_range(model, column_name, min_value, max_value) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})
{% endtest %}
