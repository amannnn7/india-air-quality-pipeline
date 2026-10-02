-- DIMENSION: one row per city we track. Descriptive attributes used to slice the facts.

select
    city_id,
    city_name,
    state,
    region,
    latitude,
    longitude
from {{ ref('cities') }}
