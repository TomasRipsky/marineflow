{{ config(
    materialized='incremental',
    unique_key='loiter_event_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    cluster_by=['mmsi', 'ocean_region']
) }}

-- One row per loitering session: a vessel moving slowly (0.1-4 kn) outside port
-- zones for more than 180 minutes inside the same 0.1-degree cell.
--
-- port_name, port_country and distance_to_port_km are deliberately not carried
-- over: Silver only fills them inside the port boxes and this model only looks
-- outside them, so they would always be null.

with candidate_positions as (
    select
        mmsi,
        vessel_name,
        vessel_type_normalized,
        flag_country,
        ocean_region,
        event_timestamp,
        date_day                                as event_date,
        speed_over_ground,
        heading_change_degrees,                 -- heading change between consecutive pings, computed in Silver
        round(latitude,  1)                     as lat_cell,
        round(longitude, 1)                     as lon_cell
    from {{ ref('stg_vessel_positions') }}
    where speed_over_ground between 0.1 and 4.0
      -- Exclude anchoring and mooring declared by the vessel itself. These are the exact
      -- labels Silver writes (at_anchor, moored). A null status is kept: the vessel did not
      -- declare itself anchored, and `NOT IN` on a null would drop the row.
      and (navigational_status is null or navigational_status not in ('at_anchor', 'moored'))
      -- Only positions outside port zones
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
        lat_cell,
        lon_cell,
        concat(cast(lat_cell as string), '_', cast(lon_cell as string))  as loiter_zone,
        min(event_timestamp)                                              as window_start,
        max(event_timestamp)                                              as window_end,
        count(*)                                                          as position_count,
        timestamp_diff(max(event_timestamp), min(event_timestamp), minute) as duration_minutes,
        avg(speed_over_ground)                                            as avg_speed_knots,
        -- A high average heading change means the vessel is circling, a sign of active waiting
        avg(abs(heading_change_degrees))                                  as avg_heading_change,
        min(event_date)                                                   as event_date
    from candidate_positions
    group by 1,2,3,4,5,6,7,8
    having timestamp_diff(max(event_timestamp), min(event_timestamp), minute) > 180
)

select
    to_hex(md5(concat(mmsi, cast(window_start as string))))  as loiter_event_id,
    *,
    case
        when duration_minutes > 720 and avg_heading_change > 20 then 'HIGH'
        when duration_minutes > 360                             then 'MEDIUM'
        else                                                         'LOW'
    end as risk_level
from loiter_sessions
