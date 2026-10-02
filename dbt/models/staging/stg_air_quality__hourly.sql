-- STAGING: clean and standardise the raw hourly rows. One row per city per hour.
-- Rules for this layer: rename, cast, fix units, remove duplicates. No business logic.

with source as (

    select * from {{ source('raw', 'air_quality_hourly') }}

),

renamed as (

    select
        lower(trim(city_id))                    as city_id,
        cast(observed_at as timestamp)          as observed_at,
        cast(observed_at as date)               as observed_date,
        pm2_5,
        pm10,
        no2,
        so2,
        o3,
        co / 1000.0                             as co_mg_m3,   -- API sends ug/m3, CPCB CO limits are in mg/m3
        source,
        ingested_at,
        run_id
    from source

),

deduplicated as (

    -- If the same hour was ever landed twice, keep the most recently ingested copy.
    select *
    from renamed
    qualify row_number() over (partition by city_id, observed_at order by ingested_at desc) = 1

)

select * from deduplicated
