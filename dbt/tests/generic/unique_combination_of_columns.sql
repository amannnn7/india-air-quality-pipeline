{#
  Fails if any combination of the given columns appears more than once.
  (Same idea as dbt_utils.unique_combination_of_columns, written here to avoid a package dependency.)
#}
{% test unique_combination_of_columns(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}
