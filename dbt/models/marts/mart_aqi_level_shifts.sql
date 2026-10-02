-- DATA-QUALITY MART: sudden, lasting jumps in a city's AQI ("level shifts").
--
-- Real air quality moves with weather and seasons; it rarely jumps to a new level overnight and stays
-- there. When it does in the data, the usual cause is upstream (a model upgrade at the provider, a
-- changed grid cell, a unit change), not the air. This model finds those dates so a person can check them.
--
-- Method, per city, for every valid day D:
--   before = average AQI of the 14 valid days before D
--   after  = average AQI of D and the 13 valid days after it
--   shift  = after - before
-- A day is flagged when ALL of these hold:
--   * |shift| >= var('level_shift_threshold')  (default 50 AQI points, half a CPCB band): big enough to matter
--   * |shift| >= 2 x the usual day-to-day spread (pooled standard deviation of the two windows):
--     big compared with normal noise, so a naturally jumpy city like Delhi is not flagged every fortnight
--   * it is the biggest shift within +/- 7 rows, so one change is reported once, not 10 times
-- Windows count rows, so a missing day widens the window slightly; both windows need >= 10 valid days.

{% set threshold = var('level_shift_threshold', 50) %}

with daily as (

    select city_id, observed_date, aqi
    from {{ ref('fct_city_daily_aqi') }}
    where is_aqi_valid

),

windows as (

    select
        city_id,
        observed_date,
        avg(aqi)   over (partition by city_id order by observed_date rows between 14 preceding and 1 preceding) as before_avg,
        count(aqi) over (partition by city_id order by observed_date rows between 14 preceding and 1 preceding) as before_days,
        avg(aqi)   over (partition by city_id order by observed_date rows between current row and 13 following) as after_avg,
        count(aqi) over (partition by city_id order by observed_date rows between current row and 13 following) as after_days,
        stddev_samp(aqi) over (partition by city_id order by observed_date rows between 14 preceding and 1 preceding) as before_sd,
        stddev_samp(aqi) over (partition by city_id order by observed_date rows between current row and 13 following) as after_sd
    from daily

),

shifts as (

    select
        city_id,
        observed_date,
        before_avg,
        after_avg,
        after_avg - before_avg as shift,
        sqrt((before_sd * before_sd + after_sd * after_sd) / 2) as pooled_sd
    from windows
    where before_days >= 10 and after_days >= 10

),

peaks as (

    select
        *,
        max(abs(shift)) over (partition by city_id order by observed_date rows between 7 preceding and 7 following) as local_max
    from shifts

)

select
    city_id || '|' || cast(observed_date as varchar)   as level_shift_id,
    city_id,
    observed_date                                       as shift_date,
    cast(round(before_avg, 0) as integer)               as before_avg_aqi,
    cast(round(after_avg, 0) as integer)                as after_avg_aqi,
    cast(round(shift, 0) as integer)                    as shift_aqi,
    case when shift > 0 then 'up' else 'down' end       as direction
from peaks
where abs(shift) >= {{ threshold }}
  and abs(shift) >= 2 * pooled_sd
  and abs(shift) = local_max
