# =============================================================================
# MARINEFLOW — AIS Message Parser
# ingestion/ais_producer/parser.py
#
# Transforms raw AIS WebSocket messages into normalized dicts
# that match the BigQuery schema defined in Terraform.
# =============================================================================

import json
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, field_validator


# =============================================================================
# Pydantic models — one per message type we process
# =============================================================================

class VesselPosition(BaseModel):
    """Normalized position record — maps to BigQuery vessel_positions_raw."""

    mmsi: str
    vessel_name: Optional[str] = None
    vessel_type: Optional[str] = None
    latitude: float
    longitude: float
    speed_over_ground: Optional[float] = None
    course_over_ground: Optional[float] = None
    heading: Optional[int] = None
    navigational_status: Optional[str] = None
    destination: Optional[str] = None
    eta: Optional[str] = None
    draught: Optional[float] = None
    flag_country: Optional[str] = None
    imo_number: Optional[str] = None
    callsign: Optional[str] = None
    event_timestamp: str
    ingestion_timestamp: str
    source: str = "aisstream_live"
    raw_message: Optional[str] = None

    @field_validator("latitude")
    @classmethod
    def validate_latitude(cls, v: float) -> float:
        if not -90 <= v <= 90:
            raise ValueError(f"Invalid latitude: {v}")
        return v

    @field_validator("longitude")
    @classmethod
    def validate_longitude(cls, v: float) -> float:
        if not -180 <= v <= 180:
            raise ValueError(f"Invalid longitude: {v}")
        return v


class VesselMetadata(BaseModel):
    """Static vessel data — maps to the vessel-metadata Pub/Sub topic."""

    mmsi: str
    vessel_name: Optional[str] = None
    vessel_type: Optional[str] = None
    imo_number: Optional[str] = None
    callsign: Optional[str] = None
    destination: Optional[str] = None
    draught: Optional[float] = None
    flag_country: Optional[str] = None
    ingestion_timestamp: str
    source: str = "aisstream_live"


# =============================================================================
# Navigational status mapping (AIS integer codes → human-readable strings)
# Source: ITU-R M.1371-5 standard
# =============================================================================
NAV_STATUS_MAP = {
    0: "under_way_engine",
    1: "at_anchor",
    2: "not_under_command",
    3: "restricted_manoeuvrability",
    4: "constrained_by_draught",
    5: "moored",
    6: "aground",
    7: "engaged_in_fishing",
    8: "under_way_sailing",
    15: "undefined",
}

# Vessel type mapping (AIS integer codes → categories)
# Source: ITU-R M.1371-5 standard, Table 20
VESSEL_TYPE_MAP = {
    **dict.fromkeys(range(70, 80), "cargo"),
    **dict.fromkeys(range(80, 90), "tanker"),
    **dict.fromkeys(range(60, 70), "passenger"),
    **dict.fromkeys(range(30, 36), "fishing"),
    **dict.fromkeys([1, 2, 3, 4, 5, 6, 7, 8, 9], "reserved"),
    **dict.fromkeys(range(20, 30), "wing_in_ground"),
    **dict.fromkeys(range(36, 40), "sailing_or_pleasure"),
    **dict.fromkeys(range(50, 60), "special_craft"),
    **dict.fromkeys(range(90, 100), "other"),
}


def _get_vessel_type(type_code: Optional[int]) -> Optional[str]:
    """Map AIS vessel type integer to a readable category string."""
    if type_code is None:
        return None
    return VESSEL_TYPE_MAP.get(type_code, f"unknown_{type_code}")


def _get_nav_status(status_code: Optional[int]) -> Optional[str]:
    """Map AIS navigational status integer to a readable string."""
    if status_code is None:
        return None
    return NAV_STATUS_MAP.get(status_code, f"unknown_{status_code}")


def _mmsi_to_flag(mmsi: str) -> Optional[str]:
    """
    Derive the flag country from the first 3 digits of the MMSI.
    MMSI MID (Maritime Identification Digits) are standardized by ITU.
    This covers the most common MIDs — not exhaustive.
    """
    mid_map = {
        "211": "DE", "219": "DK", "224": "ES", "225": "ES",
        "226": "FR", "228": "FR", "232": "GB", "233": "GB",
        "244": "NL", "245": "NL", "247": "IT", "248": "MT",
        "255": "PT", "257": "NO", "265": "SE", "266": "SE",
        "269": "CH", "271": "TR", "273": "RU", "276": "EE",
        "277": "LV", "278": "LT", "303": "US", "338": "US",
        "366": "US", "367": "US", "368": "US", "369": "US",
        "412": "CN", "413": "CN", "414": "CN", "416": "TW",
        "431": "JP", "432": "JP", "440": "KR", "441": "KR",
        "477": "HK", "518": "NZ", "503": "AU", "636": "LR",
        "657": "TZ", "667": "GN", "710": "BR", "720": "AR",
    }
    if len(mmsi) >= 3:
        return mid_map.get(mmsi[:3])
    return None


def _now_utc() -> str:
    """Return current UTC timestamp as ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


# =============================================================================
# Main parsing functions
# =============================================================================

def parse_position_report(raw: dict) -> Optional[VesselPosition]:
    """
    Parse a PositionReport AIS message from aisstream.io into a VesselPosition.
    Returns None if the message is invalid or missing required fields.
    """
    try:
        metadata = raw.get("MetaData", {})
        message = raw.get("Message", {}).get("PositionReport", {})

        mmsi = str(metadata.get("MMSI") or message.get("UserID", ""))
        if not mmsi:
            return None

        lat = message.get("Latitude") or metadata.get("latitude")
        lon = message.get("Longitude") or metadata.get("longitude")

        # Skip invalid coordinates (AIS uses 91/181 as "not available")
        if lat is None or lon is None or abs(lat) > 90 or abs(lon) > 180:
            return None

        return VesselPosition(
            mmsi=mmsi,
            vessel_name=metadata.get("ShipName", "").strip() or None,
            vessel_type=_get_vessel_type(None),  # not in PositionReport
            latitude=float(lat),
            longitude=float(lon),
            speed_over_ground=message.get("Sog"),
            course_over_ground=message.get("Cog"),
            heading=message.get("TrueHeading"),
            navigational_status=_get_nav_status(message.get("NavigationalStatus")),
            flag_country=_mmsi_to_flag(mmsi),
            event_timestamp=metadata.get("time_utc", _now_utc()),
            ingestion_timestamp=_now_utc(),
            raw_message=json.dumps(raw),
        )

    except Exception:
        return None


def parse_ship_static_data(raw: dict) -> Optional[VesselMetadata]:
    """
    Parse a ShipStaticData AIS message into a VesselMetadata record.
    Returns None if the message is invalid or missing required fields.
    """
    try:
        metadata = raw.get("MetaData", {})
        message = raw.get("Message", {}).get("ShipStaticData", {})

        mmsi = str(metadata.get("MMSI") or message.get("UserID", ""))
        if not mmsi:
            return None

        # ETA is a nested object in ShipStaticData
        eta_obj = message.get("Eta", {}) or {}
        eta_str = None
        if eta_obj:
            eta_str = (
                f"{eta_obj.get('Month', 0):02d}"
                f"{eta_obj.get('Day', 0):02d}"
                f"{eta_obj.get('Hour', 0):02d}"
                f"{eta_obj.get('Minute', 0):02d}"
            )

        return VesselMetadata(
            mmsi=mmsi,
            vessel_name=(message.get("Name") or metadata.get("ShipName", "")).strip() or None,
            vessel_type=_get_vessel_type(message.get("Type")),
            imo_number=str(message.get("ImoNumber")) if message.get("ImoNumber") else None,
            callsign=message.get("Callsign", "").strip() or None,
            destination=message.get("Destination", "").strip() or None,
            draught=message.get("MaximumStaticDraught"),
            flag_country=_mmsi_to_flag(mmsi),
            ingestion_timestamp=_now_utc(),
        )

    except Exception:
        return None


def parse_message(raw: dict) -> tuple[Optional[object], Optional[str]]:
    """
    Route an AIS message to the correct parser based on MessageType.

    Returns:
        (parsed_object, topic_key) where topic_key is 'positions' or 'metadata'
        Returns (None, None) if the message type is not handled.
    """
    message_type = raw.get("MessageType", "")

    if message_type == "PositionReport":
        parsed = parse_position_report(raw)
        return parsed, "positions"

    elif message_type == "ShipStaticData":
        parsed = parse_ship_static_data(raw)
        return parsed, "metadata"

    return None, None