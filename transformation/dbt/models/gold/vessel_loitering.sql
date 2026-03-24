{{ config(
    materialized='incremental',
    unique_key='loiter_event_id',
    incremental_strategy='merge',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    cluster_by=['mmsi', 'ocean_region']
) }}

with candidate_positions as (
    select
        mmsi,
        vessel_name,
        vessel_type_normalized,
        flag_country,
        ocean_region,
        eez_country,
        nearest_port,
        is_in_port_zone,
        distance_to_port_km,
        navigational_status,
        event_timestamp,
        date_day                                as event_date,
        latitude,
        longitude,
        speed_over_ground,
        heading_change_degrees,                 -- cambio de rumbo entre pings, ya calculado
        round(latitude,  1)                     as lat_cell,
        round(longitude, 1)                     as lon_cell
    from {{ ref('stg_vessel_positions') }}
    where speed_over_ground between 0.1 and 4.0
      -- Excluir fondeo/amarre legítimo reportado por el propio buque
      and navigational_status not in ('at anchor', 'moored')
      -- Solo interesa fuera de zona portuaria
      and (is_in_port_zone = false or is_in_port_zone is null)
    {% if is_incremental() %}
        and event_timestamp >= (
            select timestamp_sub(max(window_start), interval 4 hour)
            from {{ this }}
        )
    {% endif %}
),

loiter_sessions as (
    select
        mmsi,
        vessel_name,
        vessel_type_normalized,
        flag_country,
        ocean_region,
        eez_country,
        nearest_port,
        lat_cell,
        lon_cell,
        concat(cast(lat_cell as string), '_', cast(lon_cell as string))  as loiter_zone,
        min(event_timestamp)                                              as window_start,
        max(event_timestamp)                                              as window_end,
        count(*)                                                          as position_count,
        timestamp_diff(max(event_timestamp), min(event_timestamp), minute) as duration_minutes,
        avg(speed_over_ground)                                            as avg_speed_knots,
        -- Alta varianza de rumbo = círculos = señal fuerte de espera activa
        avg(abs(heading_change_degrees))                                  as avg_heading_change,
        avg(distance_to_port_km)                                          as avg_distance_to_port_km,
        min(event_date)                                                   as event_date
    from candidate_positions
    group by 1,2,3,4,5,6,7,8,9,10
    having timestamp_diff(max(event_timestamp), min(event_timestamp), minute) > 180
)

select
    to_hex(md5(concat(mmsi, cast(window_start as string))))  as loiter_event_id,
    *,
    case
        when duration_minutes > 720 and avg_heading_change > 20 then 'HIGH'
        when duration_minutes > 360                             then 'MEDIUM'
        else                                                         'LOW'
    end as risk_level,
    -- Flag específico: potencial STS (lejos de puerto, mucho tiempo, dando vueltas)
    case
        when avg_distance_to_port_km > 50
         and duration_minutes > 240
         and avg_heading_change > 15   then true
        else                                false
    end as potential_sts_transfer
from loiter_sessions