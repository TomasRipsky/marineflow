-- =============================================================================
-- vessel_erratic_course.sql
-- Gold model — vessels with repeated sharp turns inconsistent with normal
-- navigation.
--
-- Grain: one row per (mmsi, date_day)
-- =============================================================================

{{
    config(
        materialized='table',
        dataset='marineflow_gold',
        partition_by={
            "field": "date_day",
            "data_type": "date"
        },
        cluster_by=["mmsi"],
        description="Vessels with repeated sharp turns (>10/day) — rule-based, no ML"
    )
}}

with activity as (
    select * from {{ ref('vessel_activity_summary') }}
),

positions as (
    select * from {{ ref('stg_vessel_positions') }}
),

erratic as (
    select
        mmsi,
        date_day,
        vessel_name,
        flag_country,
        vessel_type,
        primary_ocean_region          as ocean_region,
        last_known_port,
        avg_speed_knots,
        sharp_turns,
        avg_heading_change,
        case
            when sharp_turns > 20 then 'high'
            when sharp_turns > 10 then 'medium'
            else 'low'
        end                            as severity,
        concat(
            cast(sharp_turns as string),
            ' sharp turns (>45°) detected — avg heading change: ',
            cast(round(avg_heading_change, 1) as string),
            '°'
        )                              as description
    from activity
    where
        sharp_turns > 10
        and avg_speed_knots > 1        -- exclude vessels rotating at anchor
),

with_position as (
    select
        e.*,
        p.latitude,
        p.longitude,
        p.event_timestamp             as last_position_timestamp,
        row_number() over (
            partition by e.mmsi, e.date_day
            order by p.event_timestamp desc
        ) as rn
    from erratic e
    left join positions p
        on e.mmsi      = p.mmsi
        and e.date_day = p.date_day
)

select
    {{ dbt_utils.generate_surrogate_key(['mmsi', 'date_day']) }} as alert_id,
    mmsi,
    date_day,
    vessel_name,
    flag_country,
    vessel_type,
    ocean_region,
    last_known_port,
    severity,
    sharp_turns,
    avg_heading_change,
    description,
    avg_speed_knots,
    latitude,
    longitude,
    last_position_timestamp,
    current_timestamp()               as gold_processed_at
from with_position
where rn = 1
