-- =============================================================================
-- anomaly_candidates.sql
-- Gold model — vessels showing suspicious behaviour patterns.
--
-- This model is the primary input for the Isolation Forest anomaly detector
-- in Phase 4. It flags vessels based on rule-based heuristics that serve
-- as labeled signals for the ML model.
--
-- Alert types:
--   - ais_gap:         vessel disappears from AIS for > 2 hours
--   - speed_anomaly:   sudden extreme speed change
--   - dark_vessel:     high speed with AIS gaps (possible transponder tampering)
--   - erratic_course:  repeated sharp turns inconsistent with normal navigation
--
-- Grain: one row per (mmsi, date_day, alert_type)
-- Partitioned by: date_day
-- Clustered by: alert_type, mmsi
-- =============================================================================

{{
    config(
        materialized='table',
        dataset='marineflow_gold',
        partition_by={
            "field": "date_day",
            "data_type": "date"
        },
        cluster_by=["alert_type", "mmsi"],
        description="Rule-based anomaly candidates — input for Isolation Forest (Phase 4)"
    )
}}

with activity as (
    select * from {{ ref('vessel_activity_summary') }}
),

positions as (
    select * from {{ ref('stg_vessel_positions') }}
),

-- ── AIS Gap alerts ──────────────────────────────────────────────────────────
ais_gap_alerts as (
    select
        mmsi,
        date_day,
        vessel_name,
        flag_country,
        vessel_type,
        primary_ocean_region         as ocean_region,
        last_known_port,
        avg_speed_knots,
        max_ais_gap_minutes          as anomaly_value,
        'ais_gap'                    as alert_type,
        case
            when max_ais_gap_minutes > 360 then 'high'
            when max_ais_gap_minutes > 120 then 'medium'
            else 'low'
        end                          as severity,
        concat(
            'Vessel disappeared from AIS for ',
            cast(max_ais_gap_minutes as string),
            ' minutes'
        )                            as description
    from activity
    where max_ais_gap_minutes > 120
),

-- ── Speed anomaly alerts ─────────────────────────────────────────────────────
speed_anomaly_alerts as (
    select
        mmsi,
        date_day,
        vessel_name,
        flag_country,
        vessel_type,
        primary_ocean_region         as ocean_region,
        last_known_port,
        avg_speed_knots,
        max_speed_change_rate        as anomaly_value,
        'speed_anomaly'              as alert_type,
        case
            when max_speed_change_rate > 15 then 'high'
            when max_speed_change_rate > 8  then 'medium'
            else 'low'
        end                          as severity,
        concat(
            'Sudden speed change of ',
            cast(round(max_speed_change_rate, 1) as string),
            ' knots detected'
        )                            as description
    from activity
    where max_speed_change_rate > 8
),

-- ── Dark vessel alerts (high speed + AIS gaps) ────────────────────────────────
dark_vessel_alerts as (
    select
        mmsi,
        date_day,
        vessel_name,
        flag_country,
        vessel_type,
        primary_ocean_region         as ocean_region,
        last_known_port,
        avg_speed_knots,
        cast(ais_gaps_2hr as float64) as anomaly_value,
        'dark_vessel'                as alert_type,
        'high'                       as severity,
        concat(
            'Vessel had ',
            cast(ais_gaps_2hr as string),
            ' AIS gaps > 2hr while travelling at avg ',
            cast(round(avg_speed_knots, 1) as string),
            ' knots'
        )                            as description
    from activity
    where
        ais_gaps_2hr > 0
        and avg_speed_knots > 3      -- moving, not anchored
),

-- ── Erratic course alerts ────────────────────────────────────────────────────
erratic_course_alerts as (
    select
        mmsi,
        date_day,
        vessel_name,
        flag_country,
        vessel_type,
        primary_ocean_region         as ocean_region,
        last_known_port,
        avg_speed_knots,
        cast(sharp_turns as float64) as anomaly_value,
        'erratic_course'             as alert_type,
        case
            when sharp_turns > 20 then 'high'
            when sharp_turns > 10 then 'medium'
            else 'low'
        end                          as severity,
        concat(
            cast(sharp_turns as string),
            ' sharp turns (>45°) detected — avg heading change: ',
            cast(round(avg_heading_change, 1) as string),
            '°'
        )                            as description
    from activity
    where
        sharp_turns > 10
        and avg_speed_knots > 1      -- exclude vessels rotating at anchor
),

-- ── Union all alert types ────────────────────────────────────────────────────
all_alerts as (
    select * from ais_gap_alerts
    union all
    select * from speed_anomaly_alerts
    union all
    select * from dark_vessel_alerts
    union all
    select * from erratic_course_alerts
),

-- ── Add position context for map rendering ────────────────────────────────────
with_position as (
    select
        a.*,
        p.latitude,
        p.longitude,
        p.event_timestamp            as last_position_timestamp,
        row_number() over (
            partition by a.mmsi, a.date_day, a.alert_type
            order by p.event_timestamp desc
        ) as rn
    from all_alerts a
    left join positions p
        on a.mmsi     = p.mmsi
        and a.date_day = p.date_day
)

select
    -- Generate deterministic alert ID
    {{ dbt_utils.generate_surrogate_key(['mmsi', 'date_day', 'alert_type']) }} as alert_id,
    mmsi,
    date_day,
    vessel_name,
    flag_country,
    vessel_type,
    ocean_region,
    last_known_port,
    alert_type,
    severity,
    anomaly_value,
    description,
    avg_speed_knots,
    latitude,
    longitude,
    last_position_timestamp,
    current_timestamp()              as gold_processed_at
from with_position
where rn = 1