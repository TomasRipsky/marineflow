# =============================================================================
# MARINEFLOW — AIS Message Parser
# ingestion/ais_producer/parser.py
#
# Minimal ingestion layer — transforms raw aisstream.io WebSocket messages
# into structured Pydantic models and publishes them to Pub/Sub.
#
# Responsibilities (intentionally limited):
#   - Extract and validate required fields (coordinates, MMSI)
#   - Normalize the aisstream.io timestamp to ISO 8601
#   - Preserve all raw field names and values unchanged
#   - Route messages to the correct Pub/Sub topic
#
# NOT responsible for:
#   - Business transformations (nav status translation, vessel type mapping)
#   - Derived fields (flag_country from MMSI, ocean region, port proximity)
#   - Data enrichment of any kind
#   All of the above happen in the Silver layer.
# =============================================================================

import json
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, field_validator


# =============================================================================
# Pydantic models
# =============================================================================

class VesselPosition(BaseModel):
    """
    Bronze record — exact mirror of the raw aisstream.io WebSocket message.

    Two sections as in the raw message:
      - Message.PositionReport: data from the vessel transponder (AIS standard)
      - MetaData: convenience fields added by aisstream.io (not from transponder)

    Bronze philosophy: zero transformations, zero derivations.
    All field names match the original JSON keys exactly.
    """
    # --- From Message.PositionReport (vessel transponder) ---
    Cog: Optional[float] = None
    CommunicationState: Optional[int] = None
    Latitude: float
    Longitude: float
    MessageID: Optional[int] = None
    NavigationalStatus: Optional[int] = None   # raw integer — translated in Silver
    PositionAccuracy: Optional[bool] = None
    Raim: Optional[bool] = None
    RateOfTurn: Optional[int] = None
    RepeatIndicator: Optional[int] = None
    Sog: Optional[float] = None
    Spare: Optional[int] = None
    SpecialManoeuvreIndicator: Optional[int] = None
    Timestamp: Optional[int] = None
    TrueHeading: Optional[int] = None          # 511 = unavailable per AIS spec
    UserID: int                                 # MMSI from transponder
    Valid: Optional[bool] = None
    # --- From MetaData (added by aisstream.io) ---
    MMSI: str
    MMSI_String: Optional[str] = None
    ShipName: Optional[str] = None             # raw, untrimmed
    time_utc: str                               # normalized to ISO 8601
    # --- Pipeline metadata ---
    ingestion_timestamp: str
    source: str = "aisstream_live"
    raw_message: Optional[str] = None

    @field_validator("Latitude")
    @classmethod
    def validate_latitude(cls, v: float) -> float:
        if not -90 <= v <= 90:
            raise ValueError(f"Invalid latitude: {v}")
        return v

    @field_validator("Longitude")
    @classmethod
    def validate_longitude(cls, v: float) -> float:
        if not -180 <= v <= 180:
            raise ValueError(f"Invalid longitude: {v}")
        return v


class VesselMetadata(BaseModel):
    """
    Raw ShipStaticData record — published to the vessel-metadata Pub/Sub topic.
    Transformations (vessel_type normalization, flag_country) happen in Silver.
    """
    mmsi: str
    vessel_name: Optional[str] = None
    vessel_type: Optional[int] = None          # raw AIS type integer
    imo_number: Optional[str] = None
    callsign: Optional[str] = None
    destination: Optional[str] = None
    draught: Optional[float] = None
    ingestion_timestamp: str
    source: str = "aisstream_live"


# =============================================================================
# Timestamp normalization
# =============================================================================

def _now_utc() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _parse_ais_timestamp(raw_ts: Optional[str]) -> str:
    """
    Normalize aisstream.io timestamp to ISO 8601.
    Input:  "2026-03-12 13:09:41.810776108 +0000 UTC"
    Output: "2026-03-12T13:09:41.810776+00:00"
    Falls back to current UTC time if parsing fails.
    """
    if not raw_ts:
        return _now_utc()
    try:
        clean = raw_ts.replace(" UTC", "").strip()
        parts = clean.split(".")
        if len(parts) == 2:
            tz_idx = parts[1].find(" ")
            if tz_idx > 0:
                decimal = parts[1][:tz_idx][:6]   # truncate nanoseconds to microseconds
                tz = parts[1][tz_idx:]
                clean = f"{parts[0]}.{decimal}{tz}"
        return datetime.fromisoformat(clean).isoformat()
    except Exception:
        return _now_utc()


# =============================================================================
# Parsers
# =============================================================================

def parse_position_report(raw: dict) -> Optional[VesselPosition]:
    """
    Parse a PositionReport AIS message from aisstream.io.
    Returns None if required fields are missing or coordinates are invalid.
    """
    try:
        metadata = raw.get("MetaData", {})
        message  = raw.get("Message", {}).get("PositionReport", {})

        mmsi = str(metadata.get("MMSI") or message.get("UserID", ""))
        if not mmsi:
            return None

        lat = message.get("Latitude") or metadata.get("latitude")
        lon = message.get("Longitude") or metadata.get("longitude")

        # AIS uses 91/181 as "not available" sentinel values
        if lat is None or lon is None or abs(lat) > 90 or abs(lon) > 180:
            return None

        return VesselPosition(
            Cog=message.get("Cog"),
            CommunicationState=message.get("CommunicationState"),
            Latitude=float(lat),
            Longitude=float(lon),
            MessageID=message.get("MessageID"),
            NavigationalStatus=message.get("NavigationalStatus"),
            PositionAccuracy=message.get("PositionAccuracy"),
            Raim=message.get("Raim"),
            RateOfTurn=message.get("RateOfTurn"),
            RepeatIndicator=message.get("RepeatIndicator"),
            Sog=message.get("Sog"),
            Spare=message.get("Spare"),
            SpecialManoeuvreIndicator=message.get("SpecialManoeuvreIndicator"),
            Timestamp=message.get("Timestamp"),
            TrueHeading=message.get("TrueHeading"),
            UserID=int(message.get("UserID", mmsi)),
            Valid=message.get("Valid"),
            MMSI=mmsi,
            MMSI_String=str(metadata.get("MMSI_String")) if metadata.get("MMSI_String") else None,
            ShipName=metadata.get("ShipName") or None,  # "" → None
            time_utc=_parse_ais_timestamp(
                metadata.get("TimeUtc") or metadata.get("time_utc")
            ),
            ingestion_timestamp=_now_utc(),
            raw_message=json.dumps(raw),
        )

    except Exception:
        return None


def parse_ship_static_data(raw: dict) -> Optional[VesselMetadata]:
    """
    Parse a ShipStaticData AIS message.
    Returns None if MMSI is missing.
    """
    try:
        metadata = raw.get("MetaData", {})
        message  = raw.get("Message", {}).get("ShipStaticData", {})

        mmsi = str(metadata.get("MMSI") or message.get("UserID", ""))
        if not mmsi:
            return None

        return VesselMetadata(
            mmsi=mmsi,
            vessel_name=metadata.get("ShipName") or message.get("Name") or None,
            vessel_type=message.get("Type"),                    # raw integer
            imo_number=str(message.get("ImoNumber")) if message.get("ImoNumber") else None,
            callsign=message.get("Callsign") or None,
            destination=message.get("Destination") or None,
            draught=message.get("MaximumStaticDraught"),
            ingestion_timestamp=_now_utc(),
        )

    except Exception:
        return None


# =============================================================================
# Message router
# =============================================================================

def parse_message(raw: dict) -> tuple[Optional[object], Optional[str]]:
    """
    Route an AIS message to the correct parser based on MessageType.
    Returns (parsed_object, topic_key) or (None, None) if not handled.
    """
    message_type = raw.get("MessageType", "")

    if message_type == "PositionReport":
        return parse_position_report(raw), "positions"

    if message_type == "ShipStaticData":
        return parse_ship_static_data(raw), "metadata"

    return None, None