-- =============================================================================
-- stg_vessel_positions.sql
-- Staging layer — joins vessel_positions_clean with vessel_metadata to
-- resolve vessel_type_normalized and destination_clean.
--
-- vessel_positions_clean contains only what Silver can derive from
-- PositionReport messages. Fields from ShipStaticData (vessel_type,
-- destination) live in vessel_metadata and are joined here so all
-- Gold models get a complete view without needing to know the join logic.
-- =============================================================================

with positions as (
    select * from {{ source('silver', 'vessel_positions_clean') }}
),

-- Latest metadata per MMSI — ShipStaticData is infrequent so we take
-- the most recent known values
latest_metadata as (
    select
        mmsi,
        vessel_type_normalized,
        destination_clean,
        imo_number,
        callsign,
        draught,
        row_number() over (
            partition by mmsi
            order by processing_timestamp desc
        ) as rn
    from {{ source('silver', 'vessel_metadata') }}
    where mmsi is not null
),

metadata as (
    select * from latest_metadata where rn = 1
),

staged as (
    select
        -- Identity
        p.mmsi,
        p.vessel_name,
        p.flag_country,
        -- From vessel_metadata join — not available in vessel_positions_clean
        coalesce(m.vessel_type_normalized, 'unknown') as vessel_type_normalized,

        -- Position
        p.latitude,
        p.longitude,
        p.ocean_region,
        p.nearest_port,
        p.eez_country,
        p.is_in_port_zone,
        p.distance_to_port_km,

        -- Movement
        p.speed_over_ground,
        p.course_over_ground,
        p.heading,
        p.navigational_status,
        p.speed_change_rate,
        p.heading_change_degrees,

        -- From vessel_metadata join — not available in vessel_positions_clean
        m.destination_clean,
        m.imo_number,
        m.callsign,
        m.draught,

        -- Timestamps
        p.event_timestamp,
        p.processing_timestamp,
        date(p.event_timestamp)              as date_day,
        extract(hour from p.event_timestamp) as hour_of_day,

        -- Lineage
        p._source_system,
        p._source_file,
        p._bronze_batch_id,
        p._silver_batch_id,
        p._pipeline_version

    from positions p
    left join metadata m on p.mmsi = m.mmsi
    where
        p.mmsi is not null
        and p.latitude  between -90  and 90
        and p.longitude between -180 and 180
        and p.event_timestamp is not null
        and p.mmsi not like '0%'   -- filter AIS test transmissions
)

select * from staged