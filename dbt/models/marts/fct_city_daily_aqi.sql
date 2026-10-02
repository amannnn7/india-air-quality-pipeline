-- FACT: one row per city per day with the Indian National AQI (CPCB method).
--
-- How the AQI is built:
--   1. Each pollutant's daily value is placed in its CPCB breakpoint band.
--   2. Inside the band we interpolate linearly to get a sub-index (0-500).
--   3. The day's AQI is the HIGHEST sub-index; that pollutant is the "dominant" one.
--   4. The AQI is only published if >= 3 pollutants are valid and one is PM2.5 or PM10.
--
-- INCREMENTAL: after the first build, each run only re-processes the last `lookback_days`
-- days and MERGEs them in (Snowflake) / delete+inserts them (DuckDB), instead of rebuilding
-- the whole history. 7 extra days are read so the trend columns (lag, 7-day average) stay correct.
-- On Snowflake dbt stages the changed rows in a temporary view and MERGEs them on daily_aqi_id.

{{
    config(
        materialized = 'incremental',
        unique_key = 'daily_aqi_id',
        incremental_strategy = 'merge' if target.type == 'snowflake' else 'delete+insert',
        on_schema_change = 'append_new_columns',
    )
}}

{% set lookback = var('lookback_days') %}
{% set min_hours = var('min_hours_for_valid_day') %}
{% set min_pollutants = var('min_pollutants_for_aqi') %}

with daily as (

    select * from {{ ref('int_pollutant_daily') }}
    {% if is_incremental() %}
    where observed_date >= (
        select {{ dbt.dateadd('day', -(lookback + 7), 'max(observed_date)') }} from {{ this }}
    )
    {% endif %}

),

breakpoints as (

    select * from {{ ref('aqi_breakpoints') }}

),

-- one row per (city, day, pollutant); a value is only usable if it has enough hours behind it
pollutant_values as (

    select city_id, observed_date, 'pm2_5' as pollutant, pm2_5_24h as value, pm2_5_hours >= {{ min_hours }} as is_valid from daily
    union all
    select city_id, observed_date, 'pm10',  pm10_24h,  pm10_hours  >= {{ min_hours }} from daily
    union all
    select city_id, observed_date, 'no2',   no2_24h,   no2_hours   >= {{ min_hours }} from daily
    union all
    select city_id, observed_date, 'so2',   so2_24h,   so2_hours   >= {{ min_hours }} from daily
    union all
    select city_id, observed_date, 'o3',    o3_8h_max, o3_8h_max is not null from daily
    union all
    select city_id, observed_date, 'co',    co_8h_max, co_8h_max is not null from daily

),

sub_indices as (

    select
        v.city_id,
        v.observed_date,
        v.pollutant,
        case v.pollutant
            when 'pm2_5' then 6 when 'pm10' then 5 when 'no2' then 4
            when 'o3' then 3 when 'so2' then 2 else 1
        end as tie_break_priority,
        cast(round(
            b.index_low
            + (least(v.value, b.conc_high) - b.conc_low)
            * (b.index_high - b.index_low) / (b.conc_high - b.conc_low)
        ) as integer) as sub_index
    from pollutant_values as v
    inner join breakpoints as b
        on  b.pollutant = v.pollutant
        and v.value >= b.conc_low
        and (v.value < b.conc_high or b.category = 'Severe')   -- Severe band is open-ended; capped at 500
    where v.is_valid
      and v.value is not null

),

per_day as (

    select
        city_id,
        observed_date,
        max(sub_index)                                                    as max_sub_index,
        -- ties are broken by a fixed pollutant priority, so re-runs always pick the same pollutant
        {{ arg_max('pollutant', 'sub_index * 10 + tie_break_priority') }} as top_pollutant,
        count(*)                                                          as pollutants_used,
        max(case when pollutant in ('pm2_5', 'pm10') then 1 else 0 end)   as has_particulates,
        max(case when pollutant = 'pm2_5' then sub_index end)             as pm2_5_sub_index,
        max(case when pollutant = 'pm10'  then sub_index end)             as pm10_sub_index,
        max(case when pollutant = 'no2'   then sub_index end)             as no2_sub_index,
        max(case when pollutant = 'so2'   then sub_index end)             as so2_sub_index,
        max(case when pollutant = 'o3'    then sub_index end)             as o3_sub_index,
        max(case when pollutant = 'co'    then sub_index end)             as co_sub_index
    from sub_indices
    group by city_id, observed_date

),

joined as (

    select
        d.*,
        p.max_sub_index,
        p.top_pollutant,
        coalesce(p.pollutants_used, 0) as pollutants_used,
        (coalesce(p.pollutants_used, 0) >= {{ min_pollutants }} and p.has_particulates = 1) as is_aqi_valid,
        p.pm2_5_sub_index, p.pm10_sub_index, p.no2_sub_index,
        p.so2_sub_index,   p.o3_sub_index,   p.co_sub_index
    from daily as d
    left join per_day as p
        on p.city_id = d.city_id and p.observed_date = d.observed_date

),

final as (

    select
        city_id || '|' || cast(observed_date as varchar)                    as daily_aqi_id,
        city_id,
        observed_date,
        hours_reported,
        pollutants_used,
        is_aqi_valid,
        case when is_aqi_valid then max_sub_index end                       as aqi,
        case
            when not is_aqi_valid     then null
            when max_sub_index <= 50  then 'Good'
            when max_sub_index <= 100 then 'Satisfactory'
            when max_sub_index <= 200 then 'Moderate'
            when max_sub_index <= 300 then 'Poor'
            when max_sub_index <= 400 then 'Very Poor'
            else 'Severe'
        end                                                                 as aqi_category,
        case when is_aqi_valid then top_pollutant end                       as dominant_pollutant,
        pm2_5_sub_index, pm10_sub_index, no2_sub_index,
        so2_sub_index,   o3_sub_index,   co_sub_index,
        round(pm2_5_24h, 1)  as pm2_5_24h_ug_m3,
        round(pm10_24h, 1)   as pm10_24h_ug_m3,
        round(no2_24h, 1)    as no2_24h_ug_m3,
        round(so2_24h, 1)    as so2_24h_ug_m3,
        round(o3_8h_max, 1)  as o3_8h_max_ug_m3,
        round(co_8h_max, 2)  as co_8h_max_mg_m3
    from joined

),

with_trends as (

    select
        *,
        -- trend helpers: change vs. the previous day and the 7-day rolling average
        aqi - lag(aqi) over (partition by city_id order by observed_date)          as aqi_change_vs_prev_day,
        round(avg(aqi) over (
            partition by city_id order by observed_date rows between 6 preceding and current row
        ), 1)                                                                      as aqi_7d_avg,
        {{ dbt.current_timestamp() }}                                              as dbt_updated_at
    from final

)

select * from with_trends
{% if is_incremental() %}
where observed_date >= (select {{ dbt.dateadd('day', -lookback, 'max(observed_date)') }} from {{ this }})
{% endif %}
