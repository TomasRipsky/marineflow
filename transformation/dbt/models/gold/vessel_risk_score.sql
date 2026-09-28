{{ config(
    materialized='incremental',
    unique_key='score_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    partition_by={'field': 'score_date', 'data_type': 'date'},
    cluster_by=['flag_country']
) }}

with dark as (
    select
        mmsi,
        count(*)                                                              as dark_events,
        sum(case when severity = 'CRITICAL' then 3
                 when severity = 'HIGH'     then 2
                 else                            1 end)                       as dark_score,
        countif(reappeared_in_other_port = true)                               as port_change_gaps
    from {{ ref('vessel_dark_events') }}
    where event_date >= date_sub(current_date(), interval 30 day)
    group by 1
),

speed as (
    select
        mmsi,
        count(*)                                                              as speed_anomalies,
        countif(anomaly_type = 'GPS_SPOOFING')                               as spoofing_signals,
        sum(speed_excess_knots)                                               as total_excess_knots
    from {{ ref('vessel_speed_anomalies') }}
    where event_date >= date_sub(current_date(), interval 30 day)
    group by 1
),

loiter as (
    select
        mmsi,
        count(*)                                                              as loiter_events,
        sum(duration_minutes)                                                 as total_loiter_minutes
    from {{ ref('vessel_loitering') }}
    where event_date >= date_sub(current_date(), interval 30 day)
    group by 1
),

vessels as (
    select distinct
        mmsi,
        vessel_name,
        vessel_type_normalized,
        flag_country,
        -- Last port zone the vessel was seen in (null if it never entered one of the 15 port boxes)
        last_value(port_name ignore nulls) over (
            partition by mmsi order by event_timestamp
            rows between unbounded preceding and unbounded following
        ) as last_known_port
    from {{ ref('stg_vessel_positions') }}
    qualify row_number() over (partition by mmsi order by event_timestamp desc) = 1
)

select
    to_hex(md5(concat(v.mmsi, cast(current_date() as string))))  as score_id,
    v.mmsi,
    v.vessel_name,
    v.vessel_type_normalized,
    v.flag_country,
    v.last_known_port,
    coalesce(d.dark_events,    0)                                as dark_events_30d,
    coalesce(d.port_change_gaps, 0)                             as port_change_gaps_30d,
    coalesce(s.speed_anomalies, 0)                               as speed_anomalies_30d,
    coalesce(s.spoofing_signals, 0)                              as spoofing_signals_30d,
    coalesce(l.loiter_events,  0)                                as loiter_events_30d,
    -- Weights: spoofing, and dark events that end in a different port zone, weigh more (clearer intent)
    coalesce(d.dark_score, 0)          * 10
    + coalesce(d.port_change_gaps, 0) * 8
    + coalesce(s.spoofing_signals, 0)  * 15
    + coalesce(s.speed_anomalies, 0)   * 5
    + coalesce(l.loiter_events, 0)     * 3                       as risk_score,
    current_date()                                               as score_date
from vessels v
left join dark   d using (mmsi)
left join speed  s using (mmsi)
left join loiter l using (mmsi)
where coalesce(d.dark_score, 0)
    + coalesce(s.speed_anomalies, 0)
    + coalesce(l.loiter_events, 0) > 0