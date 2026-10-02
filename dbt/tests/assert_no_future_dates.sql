-- Singular test: we only store observed (past) data, never forecasts.
select daily_aqi_id, observed_date
from {{ ref('fct_city_daily_aqi') }}
where observed_date > current_date
