-- Singular test: the category label must always agree with the number.
-- Returns the bad rows; zero rows = pass.
select daily_aqi_id, aqi, aqi_category
from {{ ref('fct_city_daily_aqi') }}
where aqi is not null
  and aqi_category <> case
        when aqi <= 50  then 'Good'
        when aqi <= 100 then 'Satisfactory'
        when aqi <= 200 then 'Moderate'
        when aqi <= 300 then 'Poor'
        when aqi <= 400 then 'Very Poor'
        else 'Severe'
      end
