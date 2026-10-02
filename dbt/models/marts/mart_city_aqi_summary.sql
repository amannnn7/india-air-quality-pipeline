-- MART: one row per city summarising the last 30 days. This is what a dashboard or a manager reads.

with facts as (

    select * from {{ ref('fct_city_daily_aqi') }}
    where is_aqi_valid

),

window_end as (

    select max(observed_date) as last_date from facts

),

last_30 as (

    select f.*
    from facts as f
    cross join window_end as w
    where f.observed_date > {{ dbt.dateadd('day', -30, 'w.last_date') }}

),

latest as (

    select city_id, aqi as latest_aqi, aqi_category as latest_category, observed_date as latest_date
    from facts
    qualify row_number() over (partition by city_id order by observed_date desc) = 1

),

summary as (

    select
        city_id,
        count(*)                                        as days_measured,
        cast(round(avg(aqi), 0) as integer)             as avg_aqi_30d,
        max(aqi)                                        as worst_aqi_30d,
        min(aqi)                                        as best_aqi_30d,
        sum(case when aqi <= 100 then 1 else 0 end)     as good_or_satisfactory_days,
        sum(case when aqi > 200 then 1 else 0 end)      as poor_or_worse_days,
        mode(dominant_pollutant)                        as usual_dominant_pollutant
    from last_30
    group by city_id

)

select
    c.city_id,
    c.city_name,
    c.state,
    c.region,
    l.latest_date,
    l.latest_aqi,
    l.latest_category,
    s.days_measured,
    s.avg_aqi_30d,
    s.worst_aqi_30d,
    s.best_aqi_30d,
    s.good_or_satisfactory_days,
    s.poor_or_worse_days,
    s.usual_dominant_pollutant,
    rank() over (order by s.avg_aqi_30d desc nulls last)  as pollution_rank_30d
from {{ ref('dim_city') }} as c
left join summary as s on s.city_id = c.city_id
left join latest  as l on l.city_id = c.city_id
