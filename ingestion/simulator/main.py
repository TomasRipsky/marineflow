# =============================================================================
# MARINEFLOW — AIS Simulator
# ingestion/simulator/main.py
#
# Generates realistic synthetic AIS data and publishes it to Pub/Sub.
# Used for local development and testing without consuming AIS API quota.
#
# Simulates a fleet of vessels moving along realistic maritime routes.
#
# Usage:
#   python main.py
#   python main.py --vessels 50 --interval 2.0
# =============================================================================

import argparse
import asyncio
import json
import math
import os
import random
import signal
import sys
from datetime import datetime, timezone
from typing import Optional

import structlog
from dotenv import load_dotenv
from faker import Faker
from google.cloud import pubsub_v1

load_dotenv()

logger = structlog.get_logger(__name__)
fake = Faker()

# =============================================================================
# Configuration
# =============================================================================

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
TOPIC_POSITIONS = os.getenv("PUBSUB_TOPIC_POSITIONS", "vessel-positions")
TOPIC_METADATA = os.getenv("PUBSUB_TOPIC_METADATA", "vessel-metadata")

# =============================================================================
# Realistic maritime routes (start_lat, start_lon, end_lat, end_lon, name)
# These are real major shipping lanes
# =============================================================================
SHIPPING_ROUTES = [
    # Trans-Atlantic: New York → Rotterdam
    (40.7, -74.0, 51.9, 4.1, "transatlantic_nyc_rotterdam"),
    # Asia-Europe: Shanghai → Rotterdam via Suez
    (31.2, 121.5, 51.9, 4.1, "asia_europe_suez"),
    # Trans-Pacific: Shanghai → Los Angeles
    (31.2, 121.5, 33.7, -118.2, "transpacific_sha_lax"),
    # Europe-Americas: Rotterdam → Santos
    (51.9, 4.1, -23.9, -46.3, "europe_brazil"),
    # Middle East-Asia: Strait of Hormuz → Singapore
    (26.5, 56.3, 1.2, 103.8, "hormuz_singapore"),
    # North Sea: Rotterdam → Hamburg
    (51.9, 4.1, 53.5, 9.9, "north_sea_rotterdam_hamburg"),
    # Mediterranean: Barcelona → Port Said
    (41.3, 2.1, 31.2, 32.3, "mediterranean_barcelona_portsaid"),
    # Indian Ocean: Mumbai → Singapore
    (18.9, 72.8, 1.2, 103.8, "indian_ocean_mumbai_singapore"),
]

VESSEL_TYPES = [
    (70, "cargo"), (71, "cargo"), (72, "cargo"),
    (80, "tanker"), (81, "tanker"), (82, "tanker"),
    (60, "passenger"), (69, "passenger"),
    (30, "fishing"), (35, "fishing"),
    (52, "tug"), (55, "law_enforcement"),
]

NAV_STATUSES = [0, 0, 0, 1, 5, 8]  # Weighted towards under_way


# =============================================================================
# Vessel simulation
# =============================================================================

class SimulatedVessel:
    """
    Represents a single vessel moving along a shipping route.
    Position is updated on each tick using linear interpolation.
    """

    def __init__(self, vessel_id: int) -> None:
        route = random.choice(SHIPPING_ROUTES)
        self.route_name = route[4]
        self.start_lat, self.start_lon = route[0], route[1]
        self.end_lat, self.end_lon = route[2], route[3]

        # Start at a random point along the route
        self.progress = random.uniform(0, 1)
        self.direction = random.choice([1, -1])  # 1=forward, -1=backward

        # Vessel identity
        self.mmsi = str(random.randint(200000000, 799999999))
        self.imo = str(random.randint(1000000, 9999999))
        self.name = f"{fake.last_name().upper()} {random.choice(['STAR', 'QUEEN', 'KING', 'TRADER', 'EXPRESS', 'PIONEER'])}"
        self.callsign = f"{random.choice(['V', 'C', 'D', 'F', 'G'])}{fake.lexify('???').upper()}"
        type_code, _ = random.choice(VESSEL_TYPES)
        self.type_code = type_code
        self.draught = round(random.uniform(4.0, 18.0), 1)
        self.destination = random.choice(["ROTTERDAM", "SHANGHAI", "SINGAPORE", "HAMBURG", "NEW YORK", "SANTOS", "DUBAI"])
        self.speed = round(random.uniform(8.0, 22.0), 1)  # knots
        self.nav_status = random.choice(NAV_STATUSES)

        # Occasional anomaly flag — 5% of vessels behave anomalously
        self.is_anomalous = random.random() < 0.05

    def _interpolate_position(self) -> tuple[float, float]:
        """Calculate current lat/lon based on progress along the route."""
        lat = self.start_lat + (self.end_lat - self.start_lat) * self.progress
        lon = self.start_lon + (self.end_lon - self.start_lon) * self.progress
        # Add small random noise to simulate GPS imprecision
        lat += random.gauss(0, 0.01)
        lon += random.gauss(0, 0.01)
        return round(lat, 6), round(lon, 6)

    def _calculate_heading(self) -> int:
        """Calculate approximate heading based on route direction."""
        dlat = (self.end_lat - self.start_lat) * self.direction
        dlon = (self.end_lon - self.start_lon) * self.direction
        heading = math.degrees(math.atan2(dlon, dlat)) % 360
        # Add small variation
        heading = (heading + random.gauss(0, 5)) % 360
        return int(heading)

    def tick(self, interval_seconds: float) -> None:
        """Advance vessel position by one time step."""
        # Anomalous vessels occasionally stop transmitting (simulate AIS gap)
        if self.is_anomalous and random.random() < 0.1:
            return

        speed_progress = (self.speed * interval_seconds) / (60 * 60 * 111)  # rough deg/s
        self.progress += speed_progress * self.direction

        # Reverse direction at route endpoints
        if self.progress >= 1.0:
            self.progress = 1.0
            self.direction = -1
        elif self.progress <= 0.0:
            self.progress = 0.0
            self.direction = 1

    def to_position_message(self) -> dict:
        """Generate a PositionReport-style message dict."""
        lat, lon = self._interpolate_position()

        # Anomalous vessels may report impossible speeds
        speed = self.speed
        if self.is_anomalous and random.random() < 0.2:
            speed = random.uniform(35.0, 60.0)

        return {
            "mmsi": self.mmsi,
            "vessel_name": self.name,
            "vessel_type": str(self.type_code),
            "latitude": lat,
            "longitude": lon,
            "speed_over_ground": round(speed, 1),
            "course_over_ground": round(self._calculate_heading(), 1),
            "heading": self._calculate_heading(),
            "navigational_status": str(self.nav_status),
            "flag_country": None,
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "simulator",
            "raw_message": None,
        }

    def to_metadata_message(self) -> dict:
        """Generate a ShipStaticData-style message dict."""
        return {
            "mmsi": self.mmsi,
            "vessel_name": self.name,
            "vessel_type": str(self.type_code),
            "imo_number": self.imo,
            "callsign": self.callsign,
            "destination": self.destination,
            "draught": self.draught,
            "flag_country": None,
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "simulator",
        }


# =============================================================================
# Main simulation loop
# =============================================================================

async def run_simulator(num_vessels: int, interval: float) -> None:
    """
    Main simulation loop — creates a fleet of vessels and publishes
    their positions to Pub/Sub at the specified interval.
    """
    if not GCP_PROJECT_ID:
        logger.error("missing_gcp_project_id")
        sys.exit(1)

    # Initialize Pub/Sub publisher
    batch_settings = pubsub_v1.types.BatchSettings(
        max_messages=500,
        max_latency=1.0,
    )
    client = pubsub_v1.PublisherClient(batch_settings=batch_settings)
    positions_topic = client.topic_path(GCP_PROJECT_ID, TOPIC_POSITIONS)
    metadata_topic = client.topic_path(GCP_PROJECT_ID, TOPIC_METADATA)

    # Create the fleet
    fleet = [SimulatedVessel(i) for i in range(num_vessels)]
    logger.info("simulator_started", vessels=num_vessels, interval_seconds=interval)

    # Publish initial metadata for all vessels
    for vessel in fleet:
        payload = json.dumps(vessel.to_metadata_message()).encode("utf-8")
        client.publish(metadata_topic, payload)
    logger.info("initial_metadata_published", vessels=num_vessels)

    tick_count = 0
    try:
        while True:
            for vessel in fleet:
                vessel.tick(interval)
                payload = json.dumps(vessel.to_position_message()).encode("utf-8")
                client.publish(positions_topic, payload)

            tick_count += 1
            if tick_count % 10 == 0:
                logger.info(
                    "simulator_heartbeat",
                    tick=tick_count,
                    vessels=num_vessels,
                    messages_sent=tick_count * num_vessels,
                )

            await asyncio.sleep(interval)

    except asyncio.CancelledError:
        client.stop()
        logger.info("simulator_stopped", total_ticks=tick_count)


# =============================================================================
# Entry point
# =============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MarineFlow AIS Simulator")
    parser.add_argument("--vessels", type=int, default=100, help="Number of vessels to simulate (default: 100)")
    parser.add_argument("--interval", type=float, default=5.0, help="Position update interval in seconds (default: 5.0)")
    return parser.parse_args()


async def main() -> None:
    args = parse_args()

    loop = asyncio.get_event_loop()
    task = asyncio.create_task(run_simulator(args.vessels, args.interval))

    def shutdown(sig, frame):
        logger.info("shutdown_signal_received")
        task.cancel()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        await task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())