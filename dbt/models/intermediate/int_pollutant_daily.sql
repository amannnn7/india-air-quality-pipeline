-- INTERMEDIATE: turn hourly readings into the daily numbers the CPCB AQI method needs.
--   * PM2.5, PM10, NO2, SO2 -> 24-hour average
--   * O3 and CO             -> highest 8-hour rolling average of the day
-- We also count how many hours each value is based on, so the fact model can reject thin data.

with hourly as (

    select * from {{ ref('stg_air_quality__hourly') }}

),

daily_averages as (

    select
        city_id,
        observed_date,
        count(*)       as hours_reported,
        avg(pm2_5)     as pm2_5_24h,
        count(pm2_5)   as pm2_5_hours,
        avg(pm10)      as pm10_24h,
        count(pm10)    as pm10_hours,
        avg(no2)       as no2_24h,
        count(no2)     as no2_hours,
        avg(so2)       as so2_24h,
        count(so2)     as so2_hours
    from hourly
    group by city_id, observed_date

),

-- For every hour, average the 8 hours ending at that hour (this hour + the 7 before it).
-- A time-based self-join is exact even when some hours are missing, and runs the same on
-- DuckDB and Snowflake.
rolling_8h as (

    select
        h.city_id,
        h.observed_date,
        h.observed_at,
        avg(w.o3)          as o3_8h_avg,
        count(w.o3)        as o3_8h_hours,
        avg(w.co_mg_m3)    as co_8h_avg,
        count(w.co_mg_m3)  as co_8h_hours
    from hourly as h
    inner join hourly as w
        on  w.city_id = h.city_id
        and w.observed_at >  {{ dbt.dateadd('hour', -8, 'h.observed_at') }}
        and w.observed_at <= h.observed_at
    group by h.city_id, h.observed_date, h.observed_at

),

daily_rolling_max as (

    select
        city_id,
        observed_date,
        -- an 8-hour window only counts if at least 6 of its 8 hours have data
        max(case when o3_8h_hours >= 6 then o3_8h_avg end) as o3_8h_max,
        max(case when co_8h_hours >= 6 then co_8h_avg end) as co_8h_max
    from rolling_8h
    group by city_id, observed_date

)

select
    a.*,
    r.o3_8h_max,
    r.co_8h_max
from daily_averages as a
left join daily_rolling_max as r
    on r.city_id = a.city_id and r.observed_date = a.observed_date
