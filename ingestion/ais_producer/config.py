# =============================================================================
# MARINEFLOW — AIS Producer Configuration
# ingestion/ais_producer/config.py
# =============================================================================

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # --- AIS Stream ---
    AIS_API_KEY: str = os.getenv("AIS_API_KEY", "")
    AIS_WS_URL: str = "wss://stream.aisstream.io/v0/stream"

    # Bounding boxes to subscribe to — global coverage by default
    # Format: [[[lat1, lon1], [lat2, lon2]], ...]
    # Default covers the entire world
    AIS_BOUNDING_BOXES: list = [[[-90, -180], [90, 180]]]

    # Filter to the two most data-rich message types
    AIS_MESSAGE_TYPES: list = ["PositionReport", "ShipStaticData"]

    # --- GCP ---
    GCP_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "")

    # --- Pub/Sub Topics ---
    TOPIC_POSITIONS: str = os.getenv("PUBSUB_TOPIC_POSITIONS", "vessel-positions")
    TOPIC_METADATA: str = os.getenv("PUBSUB_TOPIC_METADATA", "vessel-metadata")
    TOPIC_DLQ: str = os.getenv("PUBSUB_TOPIC_DLQ", "dead-letter-queue")

    # --- Producer settings ---
    # Max messages to batch before flushing to Pub/Sub
    PUBSUB_BATCH_MAX_MESSAGES: int = int(os.getenv("PUBSUB_BATCH_MAX_MESSAGES", "100"))
    # Max latency in seconds before flushing a batch
    PUBSUB_BATCH_MAX_LATENCY: float = float(os.getenv("PUBSUB_BATCH_MAX_LATENCY", "0.5"))

    # --- Logging ---
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = os.getenv("LOG_FORMAT", "json")  # json | console

    # --- Source tag ---
    MESSAGE_SOURCE: str = "aisstream_live"

    @classmethod
    def validate(cls) -> None:
        """Validate that all required config values are present."""
        missing = []
        if not cls.AIS_API_KEY:
            missing.append("AIS_API_KEY")
        if not cls.GCP_PROJECT_ID:
            missing.append("GCP_PROJECT_ID")
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )