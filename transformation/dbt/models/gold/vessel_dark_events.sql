{{ config(
    materialized='incremental',
    unique_key='dark_event_id',
    incremental_strategy='merge',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    cluster_by=['mmsi', 'flag_country']
) }}

with ordered_positions as (
    select
        mmsi,
        vessel_name,
        flag_country,
        vessel_type_normalized,
        latitude,
        longitude,
        ocean_region,
        nearest_port,
        eez_country,
        event_timestamp,
        lag(event_timestamp) over (partition by mmsi order by event_timestamp) as prev_timestamp,
        lag(latitude)        over (partition by mmsi order by event_timestamp) as prev_lat,
        lag(longitude)       over (partition by mmsi order by event_timestamp) as prev_lon,
        lag(nearest_port)    over (partition by mmsi order by event_timestamp) as prev_nearest_port
    from {{ ref('stg_vessel_positions') }}
    {% if is_incremental() %}
        where event_timestamp >= (
            select timestamp_sub(max(signal_recovered_at), interval 6 hour)
            from {{ this }}
        )
    {% endif %}
),

dark_events as (
    select
        to_hex(md5(concat(mmsi, cast(event_timestamp as string))))  as dark_event_id,
        mmsi,
        vessel_name,
        flag_country,
        vessel_type_normalized,
        prev_timestamp                                               as signal_lost_at,
        event_timestamp                                              as signal_recovered_at,
        timestamp_diff(event_timestamp, prev_timestamp, minute)      as gap_minutes,
        prev_lat                                                     as last_known_lat,
        prev_lon                                                     as last_known_lon,
        latitude                                                     as reappearance_lat,
        longitude                                                    as reappearance_lon,
        ocean_region,
        eez_country,
        prev_nearest_port                                            as last_known_port,
        nearest_port                                                 as reappearance_port,
        st_distance(
            st_geogpoint(prev_lon, prev_lat),
            st_geogpoint(longitude, latitude)
        ) / 1000.0                                                   as displacement_km,
        date(event_timestamp)                                        as event_date
    from ordered_positions
    where timestamp_diff(event_timestamp, prev_timestamp, minute) > 120
      and prev_lat is not null
)

select
    *,
    case
        when gap_minutes > 720 and displacement_km > 100 then 'CRITICAL'
        when gap_minutes > 360 and displacement_km > 50  then 'HIGH'
        when gap_minutes > 120                           then 'MEDIUM'
    end as severity,
    -- Contexto extra: desapareció en EEZ de otro país = más sospechoso
    case
        when eez_country is not null
         and last_known_port is not null
         and reappearance_port != last_known_port then true
        else false
    end as crossed_eez_during_gap
from dark_events