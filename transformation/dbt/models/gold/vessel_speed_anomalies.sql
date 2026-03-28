{{ config(
    materialized='incremental',
    unique_key='anomaly_id',
    incremental_strategy='merge',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    cluster_by=['mmsi', 'vessel_type_normalized']
) }}

with speed_calc as (
    select
        mmsi,
        vessel_name,
        vessel_type_normalized,
        flag_country,
        ocean_region,
        eez_country,
        event_timestamp,
        date_day                                                         as event_date,
        latitude,
        longitude,
        speed_over_ground                                                as reported_sog,
        speed_change_rate,                                               -- ya calculado en silver
        lag(latitude)      over (partition by mmsi order by event_timestamp) as prev_lat,
        lag(longitude)     over (partition by mmsi order by event_timestamp) as prev_lon,
        lag(event_timestamp) over (partition by mmsi order by event_timestamp) as prev_timestamp
    from {{ ref('stg_vessel_positions') }}
    {% if is_incremental() %}
        where event_timestamp >= (
            select timestamp_sub(max(event_timestamp), interval 2 hour)
            from {{ this }}
        )
    {% endif %}
),

with_calculated_speed as (
    select
        *,
        safe_divide(
            st_distance(
                st_geogpoint(prev_lon, prev_lat),
                st_geogpoint(longitude, latitude)
            ) / 1852.0,
            timestamp_diff(event_timestamp, prev_timestamp, second) / 3600.0
        ) as calculated_speed_knots
    from speed_calc
    where prev_lat is not null
      and timestamp_diff(event_timestamp, prev_timestamp, second) > 0
),

type_limits as (
    select vessel_type_normalized, max_speed_knots
    from unnest([
        struct('cargo'               as vessel_type_normalized, 25.0 as max_speed_knots),
        struct('tanker',             18.0),
        struct('fishing',            15.0),
        struct('passenger',          30.0),
        struct('tug',                14.0),
        struct('special_craft',      20.0), 
        struct('sailing_or_pleasure', 20.0), 
        struct('other',              35.0),
        struct('unknown',            35.0)
    ])
)

select
    to_hex(md5(concat(s.mmsi, cast(s.event_timestamp as string)))) as anomaly_id,
    s.mmsi,
    s.vessel_name,
    s.vessel_type_normalized,
    s.flag_country,
    s.ocean_region,
    s.eez_country,
    s.event_timestamp,
    s.event_date,
    s.reported_sog,
    s.calculated_speed_knots,
    s.speed_change_rate,
    coalesce(t.max_speed_knots, 35.0) as type_max_speed,
    round(s.calculated_speed_knots - coalesce(t.max_speed_knots, 35.0), 2) as speed_delta_knots,
    greatest(s.calculated_speed_knots - coalesce(t.max_speed_knots, 35.0), 0) as speed_excess_knots,
    s.latitude,
    s.longitude,
    case
        when s.calculated_speed_knots > coalesce(t.max_speed_knots, 35)
         and abs(s.speed_change_rate) > 5   then 'GPS_SPOOFING'     -- imposible + aceleración brusca
        when s.calculated_speed_knots > coalesce(t.max_speed_knots, 35)
                                            then 'IMPOSSIBLE_SPEED'  -- imposible pero gradual
        when abs(s.speed_change_rate) > 10  then 'SUDDEN_ACCELERATION' -- físicamente posible pero anómalo
    end as anomaly_type
from with_calculated_speed s
left join type_limits t using (vessel_type_normalized)
where s.calculated_speed_knots > coalesce(t.max_speed_knots, 35)
   or abs(s.speed_change_rate) > 10