-- =============================================================================
-- vessel_activity_summary.sql
-- Gold model — daily activity summary per vessel.
--
-- Answers questions like:
--   - How far did vessel X travel today?
--   - How long was it in port vs at sea?
--   - What was its average speed?
--   - Did it show anomalous behaviour?
--
-- Grain: one row per (mmsi, date_day)
-- Partitioned by: date_day
-- Clustered by: mmsi
-- =============================================================================

{{
    config(
        materialized='table',
        dataset='marineflow_gold',
        partition_by={
            "field": "date_day",
            "data_type": "date"
        },
        cluster_by=["mmsi", "flag_country"],
        description="Daily vessel activity summary — grain: (mmsi, date_day)"
    )
}}

with positions as (
    select * from {{ ref('stg_vessel_positions') }}
),

-- Calculate time delta between consecutive positions per vessel per day
with_time_delta as (
    select
        *,
        lag(event_timestamp) over (
            partition by mmsi, date_day
            order by event_timestamp
        ) as prev_timestamp
    from positions
),

with_segment as (
    select
        *,
        -- Time gap in minutes between consecutive positions
        timestamp_diff(event_timestamp, prev_timestamp, minute) as minutes_since_prev,

        -- Estimated distance using equirectangular approximation (km)
        -- Accurate enough for short segments between AIS pings
        case
            when prev_timestamp is not null
            then sqrt(
                pow((latitude  - lag(latitude)  over (partition by mmsi, date_day order by event_timestamp)) * 111.0, 2) +
                pow((longitude - lag(longitude) over (partition by mmsi, date_day order by event_timestamp)) * 111.0, 2)
            )
            else 0
        end as segment_distance_km
    from with_time_delta
),

daily_summary as (
    select
        mmsi,
        date_day,

        -- Vessel identity (take most recent non-null value)
        max(vessel_name)           as vessel_name,
        max(flag_country)          as flag_country,
        max(vessel_type_normalized) as vessel_type,
        max(ocean_region)          as primary_ocean_region,

        -- Activity counts
        count(*)                                                    as position_count,
        count(distinct nearest_port)                                as distinct_ports_visited,
        countif(is_in_port_zone = true)                             as positions_in_port,
        countif(is_in_port_zone = false or is_in_port_zone is null) as positions_at_sea,

        -- Time analysis
        min(event_timestamp)   as first_seen,
        max(event_timestamp)   as last_seen,
        timestamp_diff(
            max(event_timestamp),
            min(event_timestamp),
            minute
        )                          as active_minutes,

        -- Speed analysis
        round(avg(speed_over_ground), 2)                           as avg_speed_knots,
        round(max(speed_over_ground), 2)                           as max_speed_knots,
        round(avg(case when speed_over_ground > 0.5
                       then speed_over_ground end), 2)             as avg_underway_speed_knots,
        countif(speed_over_ground < 0.5)                           as positions_stationary,

        -- Distance
        round(sum(segment_distance_km), 2)                         as estimated_distance_km,

        -- Anomaly signals (inputs for ML model in Phase 4)
        round(avg(speed_change_rate), 3)                           as avg_speed_change_rate,
        round(max(speed_change_rate), 3)                           as max_speed_change_rate,
        round(avg(heading_change_degrees), 3)                      as avg_heading_change,
        round(max(heading_change_degrees), 3)                      as max_heading_change,
        countif(speed_change_rate > 5)                             as sudden_speed_changes,
        countif(heading_change_degrees > 45)                       as sharp_turns,

        -- AIS gap detection — gaps > 30 min suggest dark vessel behaviour
        countif(minutes_since_prev > 30)                           as ais_gaps_30min,
        countif(minutes_since_prev > 120)                          as ais_gaps_2hr,
        max(minutes_since_prev)                                    as max_ais_gap_minutes,

        -- Port info
        max(nearest_port)                                          as last_known_port,
        max(destination_clean)                                     as declared_destination,

        -- Lineage
        max(_source_system)                                        as source_system,
        max(_pipeline_version)                                     as pipeline_version,
        current_timestamp()                                        as gold_processed_at

    from with_segment
    group by mmsi, date_day
)

select * from daily_summary