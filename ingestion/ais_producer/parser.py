# =============================================================================
# MARINEFLOW — AIS Message Parser
# ingestion/ais_producer/parser.py
#
# Minimal ingestion layer — validates and routes raw aisstream.io messages.
#
# Responsibilities:
#   - Validate required fields (coordinates, MMSI)
#   - Normalize the aisstream.io timestamp to ISO 8601
#   - Route messages to the correct Pub/Sub topic
#
# NOT responsible for:
#   - Field renaming or business transformations
#   - Derived fields (flag_country, vessel type mapping, etc.)
#   - Any enrichment — all of that happens in Silver
#
# Message published to Pub/Sub: the raw aisstream.io JSON as-is.
# Bronze reads and decodes the Pub/Sub envelope written by the GCS subscription.
# =============================================================================

import json
from datetime import datetime, timezone
from typing import Optional


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
                decimal = parts[1][:tz_idx][:6]  # truncate nanoseconds to microseconds
                tz      = parts[1][tz_idx:]
                clean   = f"{parts[0]}.{decimal}{tz}"
        return datetime.fromisoformat(clean).isoformat()
    except Exception:
        return _now_utc()


# =============================================================================
# Validation and routing
# =============================================================================

def parse_message(raw: dict) -> tuple[Optional[dict], Optional[str]]:
    """
    Validate and route an AIS message based on MessageType.

    Validation:
      - PositionReport: requires valid lat/lon and MMSI
      - ShipStaticData: requires MMSI

    Returns (payload_dict, topic_key) or (None, None) if invalid/unhandled.
    The payload is the original raw dict with time_utc normalized to ISO 8601.
    """
    message_type = raw.get("MessageType", "")

    if message_type == "PositionReport":
        metadata = raw.get("MetaData", {})
        message  = raw.get("Message", {}).get("PositionReport", {})

        mmsi = str(metadata.get("MMSI") or message.get("UserID", ""))
        if not mmsi:
            return None, None

        lat = message.get("Latitude") or metadata.get("latitude")
        lon = message.get("Longitude") or metadata.get("longitude")

        # AIS uses 91/181 as "not available" sentinel values
        if lat is None or lon is None or abs(lat) > 90 or abs(lon) > 180:
            return None, None

        # Normalize timestamp in-place — only field we touch
        if metadata.get("time_utc"):
            raw["MetaData"]["time_utc"] = _parse_ais_timestamp(metadata["time_utc"])

        return raw, "positions"

    if message_type == "ShipStaticData":
        metadata = raw.get("MetaData", {})
        message  = raw.get("Message", {}).get("ShipStaticData", {})

        mmsi = str(metadata.get("MMSI") or message.get("UserID", ""))
        if not mmsi:
            return None, None

        if metadata.get("time_utc"):
            raw["MetaData"]["time_utc"] = _parse_ais_timestamp(metadata["time_utc"])

        return raw, "metadata"

    return None, None