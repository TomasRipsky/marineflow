-- =============================================================================
-- port_traffic.sql
-- Gold model — daily traffic analysis per port.
--
-- Answers questions like:
--   - How many vessels passed through Rotterdam today?
--   - What vessel types dominate Singapore?
--   - Which ports see the most sudden manoeuvres?
--
-- Grain: one row per (port_name, date_day)
-- Partitioned by: date_day
-- Clustered by: port_name
-- =============================================================================

{{
    config(
        materialized='table',
        dataset='marineflow_gold',
        partition_by={
            "field": "date_day",
            "data_type": "date"
        },
        cluster_by=["port_name", "port_country"],
        description="Daily port traffic analysis — grain: (port_name, date_day)"
    )
}}

with positions as (
    select * from {{ ref('stg_vessel_positions') }}
    where port_name is not null
),

-- Detect port entries: vessel transitions from outside to inside port zone
port_events as (
    select
        mmsi,
        port_name,
        port_country,
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
        port_name,
        port_country,
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
        p.port_name,
        p.port_country,
        p.date_day,

        -- Volume
        count(distinct p.mmsi)                      as unique_vessels,
        count(*)                                    as total_position_reports,

        -- Vessel type breakdown: distinct vessels per Silver category. A vessel
        -- has a single type (latest metadata), so these columns sum to
        -- unique_vessels. other_vessels is the catch-all (other, unknown,
        -- wing_in_ground). Keep the categories in sync with silver_metadata.py.
        count(distinct if(p.vessel_type_normalized = 'cargo',               p.mmsi, null)) as cargo_vessels,
        count(distinct if(p.vessel_type_normalized = 'tanker',              p.mmsi, null)) as tanker_vessels,
        count(distinct if(p.vessel_type_normalized = 'passenger',           p.mmsi, null)) as passenger_vessels,
        count(distinct if(p.vessel_type_normalized = 'fishing',             p.mmsi, null)) as fishing_vessels,
        count(distinct if(p.vessel_type_normalized = 'tug',                 p.mmsi, null)) as tug_vessels,
        count(distinct if(p.vessel_type_normalized = 'special_craft',       p.mmsi, null)) as special_craft_vessels,
        count(distinct if(p.vessel_type_normalized = 'sailing_or_pleasure', p.mmsi, null)) as sailing_or_pleasure_vessels,
        count(distinct if(p.vessel_type_normalized = 'high_speed_craft',    p.mmsi, null)) as high_speed_craft_vessels,
        count(distinct if(
            coalesce(p.vessel_type_normalized, 'unknown') not in (
                'cargo', 'tanker', 'passenger', 'fishing', 'tug',
                'special_craft', 'sailing_or_pleasure', 'high_speed_craft'
            ), p.mmsi, null))                                                            as other_vessels,

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
        on  p.port_name = e.port_name
        and p.date_day     = e.date_day
        and p.mmsi         = e.mmsi

    group by p.port_name, p.port_country, p.date_day
)

select * from daily_port_traffic