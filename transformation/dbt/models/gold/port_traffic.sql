-- =============================================================================
-- port_traffic.sql
-- Gold model — daily traffic analysis per port.
--
-- Answers questions like:
--   - How many vessels passed through Rotterdam today?
--   - What vessel types dominate Singapore?
--   - Which ports have the highest anomaly rates?
--
-- Grain: one row per (nearest_port, date_day)
-- Partitioned by: date_day
-- Clustered by: nearest_port
-- =============================================================================

{{
    config(
        materialized='table',
        dataset='marineflow_gold',
        partition_by={
            "field": "date_day",
            "data_type": "date"
        },
        cluster_by=["nearest_port", "eez_country"],
        description="Daily port traffic analysis — grain: (nearest_port, date_day)"
    )
}}

with positions as (
    select * from {{ ref('stg_vessel_positions') }}
    where nearest_port is not null
),

-- Detect port entries: vessel transitions from outside to inside port zone
port_events as (
    select
        mmsi,
        nearest_port,
        eez_country,
        date_day,
        event_timestamp,
        is_in_port_zone,
        vessel_type_normalized,
        flag_country,
        speed_over_ground,
        speed_change_rate,

        -- Previous state for entry/exit detection
        lag(is_in_port_zone) over (
            partition by mmsi
            order by event_timestamp
        ) as prev_in_port
    from positions
),

port_entries as (
    select
        nearest_port,
        eez_country,
        date_day,
        mmsi,
        vessel_type_normalized,
        flag_country,
        event_timestamp as entry_time,
        speed_over_ground,
        speed_change_rate
    from port_events
    where
        is_in_port_zone = true
        and (prev_in_port = false or prev_in_port is null)
),

daily_port_traffic as (
    select
        p.nearest_port,
        p.eez_country,
        p.date_day,

        -- Volume
        count(distinct p.mmsi)                      as unique_vessels,
        count(*)                                    as total_position_reports,

        -- Vessel type breakdown
        countif(p.vessel_type_normalized = 'cargo')      as cargo_vessels,
        countif(p.vessel_type_normalized = 'tanker')     as tanker_vessels,
        countif(p.vessel_type_normalized = 'passenger')  as passenger_vessels,
        countif(p.vessel_type_normalized = 'fishing')    as fishing_vessels,
        countif(p.vessel_type_normalized = 'tug')        as tug_vessels,
        countif(p.vessel_type_normalized is null
             or p.vessel_type_normalized = 'other')      as other_vessels,

        -- Flag diversity
        count(distinct p.flag_country)              as distinct_flag_countries,

        -- Speed profile in port
        round(avg(p.speed_over_ground), 2)          as avg_speed_in_port,
        round(max(p.speed_over_ground), 2)          as max_speed_in_port,

        -- Anomaly signals
        countif(p.speed_change_rate > 3)            as sudden_manoeuvres,
        round(avg(p.speed_change_rate), 3)          as avg_speed_change_rate,

        -- Entry events (from sub-query)
        count(distinct e.mmsi)                      as vessel_entries,

        -- Activity window
        min(p.event_timestamp)                      as first_activity,
        max(p.event_timestamp)                      as last_activity,

        current_timestamp()                         as gold_processed_at

    from positions p
    left join port_entries e
        on  p.nearest_port = e.nearest_port
        and p.date_day     = e.date_day
        and p.mmsi         = e.mmsi

    group by p.nearest_port, p.eez_country, p.date_day
)

select * from daily_port_traffic