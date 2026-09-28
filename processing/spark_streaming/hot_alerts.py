# =============================================================================
# MARINEFLOW — Hot Path: Real-Time Vessel Alerts
# processing/spark_streaming/hot_alerts.py
#
# Speed layer (Lambda architecture) — complements, does not replace, the
# batch/cold path in dbt (vessel_dark_events.sql, vessel_speed_anomalies.sql).
# Those remain the authoritative, richer analytics (geospatial displacement,
# port-zone context, graduated severity). This job exists purely for early warning
# with sub-minute latency on the two signals where that actually matters.
#
# Source:  Kafka topic vessel-positions-bronze (flattened, Bronze output)
# Sink:    Kafka topic vessel-alerts (JSON) — no GCS write, this is
#          deliberately ephemeral speed-layer output, not an archive.
# Checkpoint: local Docker volume (not GCS) — this job has zero GCP
#          dependency, it's pure Kafka-to-Kafka.
#
# Stateful processing via applyInPandasWithState, keyed by MMSI:
#   - SPEED ANOMALY: compares each new position to the previous one for the
#     same vessel (calculated great-circle speed vs. reported SOG delta).
#     Mirrors vessel_speed_anomalies.sql's thresholds, but uses a single
#     global max-speed limit (35 kn) — no vessel-type join in the hot path,
#     unlike dbt which has per-type limits. Simplification, not a bug.
#   - AIS GAP: ProcessingTimeTimeout of 120 min per vessel (same threshold
#     as vessel_dark_events.sql's MEDIUM tier). Fires when that timeout
#     expires, without waiting for the vessel to reappear — dbt only detects gaps retroactively when the vessel
#     reappears, so this is a genuine capability the batch layer doesn't
#     have, not just a faster version of the same thing. No displacement/
#     severity grading here (that needs the reappearance point) — dbt still
#     owns the full graduated CRITICAL/HIGH/MEDIUM classification.
#
# Run:
#   spark-submit \
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
#     hot_alerts.py
#   (no GCS jars needed — this job never touches GCS or BigQuery; it does need
#   pandas and pyarrow, which the Spark image installs from requirements.txt)
# =============================================================================

import math
import os
import sys
import time
from collections import namedtuple
from datetime import timedelta

import structlog
from dotenv import load_dotenv

load_dotenv()

import logging as _stdlib_logging
_stdlib_logging.getLogger("py4j").setLevel(_stdlib_logging.WARNING)

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=True),
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(_stdlib_logging.INFO),
)
logger = structlog.get_logger("hot_alerts")

# =============================================================================
# Configuration
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC_POSITIONS_BRONZE = os.getenv("KAFKA_TOPIC_POSITIONS_BRONZE", "vessel-positions-bronze")
KAFKA_TOPIC_ALERTS      = os.getenv("KAFKA_TOPIC_ALERTS", "vessel-alerts")
KAFKA_STARTING_OFFSETS  = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

CHECKPOINT_DIR = os.getenv("HOT_ALERTS_CHECKPOINT_DIR", "/opt/spark/checkpoints/hot_alerts")

# Same thresholds as vessel_speed_anomalies.sql, collapsed to one global limit
MAX_SPEED_KNOTS = 35.0
SPEED_CHANGE_SPOOFING_THRESHOLD = 5.0     # combined with over-limit speed -> GPS_SPOOFING
SPEED_CHANGE_ACCEL_THRESHOLD = 10.0       # alone -> SUDDEN_ACCELERATION

# Same threshold as vessel_dark_events.sql's MEDIUM tier
GAP_TIMEOUT_MINUTES = 120

# AIS reports 102.3 kn when speed over ground is not available
SOG_NOT_AVAILABLE = 102.3

# Spark hands the per-vessel state back as a plain tuple in get_state_schema()
# order, not as an object with attributes, so it is wrapped for readable access.
VesselState = namedtuple("VesselState", ["last_lat", "last_lon", "last_sog", "last_time_utc"])


def haversine_nm(lat1, lon1, lat2, lon2) -> float:
    """Great-circle distance in nautical miles."""
    R_km = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    dist_km = 2 * R_km * math.asin(math.sqrt(a))
    return dist_km / 1.852


def clean_sog(value):
    """Reported SOG in knots, or None if it is missing (NaN in pandas) or the AIS 'not available' value."""
    if value is None or value != value or value >= SOG_NOT_AVAILABLE:
        return None
    return float(value)


def get_source_schema():
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    return StructType([
        StructField("MMSI",      StringType(), True),
        StructField("Latitude",  DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField("Sog",       DoubleType(), True),
        StructField("time_utc",  StringType(), True),
    ])


def get_state_schema():
    from pyspark.sql.types import StructType, StructField, DoubleType, StringType
    return StructType([
        StructField("last_lat",   DoubleType(), True),
        StructField("last_lon",  DoubleType(), True),
        StructField("last_sog",  DoubleType(), True),
        StructField("last_time_utc", StringType(), True),  # ISO string, parsed with pandas
    ])


def get_output_schema():
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    return StructType([
        StructField("mmsi",         StringType(), True),
        StructField("alert_type",   StringType(), True),
        StructField("severity",     StringType(), True),
        StructField("description",  StringType(), True),
        StructField("latitude",     DoubleType(), True),
        StructField("longitude",    DoubleType(), True),
        StructField("detected_at",  StringType(), True),
    ])


def process_vessel(key, pdf_iter, state):
    """
    applyInPandasWithState callback — one invocation per MMSI per micro-batch,
    plus one invocation per timed-out MMSI (with an empty iterator) each cycle.
    """
    import pandas as pd

    (mmsi,) = key
    alerts = []
    now_iso = pd.Timestamp.now("UTC").isoformat()

    if state.hasTimedOut:
        if state.exists:
            prev = VesselState(*state.get)
            alerts.append({
                "mmsi": mmsi,
                "alert_type": "AIS_GAP",
                "severity": "medium",
                "description": f"No messages for >{GAP_TIMEOUT_MINUTES} min — last seen at {prev.last_time_utc}",
                "latitude": prev.last_lat,
                "longitude": prev.last_lon,
                "detected_at": now_iso,
            })
        state.remove()
        return iter([pd.DataFrame(alerts, columns=[f.name for f in get_output_schema().fields])])

    for pdf in pdf_iter:
        for _, row in pdf.iterrows():
            curr_lat, curr_lon, curr_sog = row["Latitude"], row["Longitude"], clean_sog(row["Sog"])
            curr_ts_str = row["time_utc"]
            if curr_lat is None or curr_lon is None or curr_ts_str is None:
                continue
            curr_ts = pd.Timestamp(curr_ts_str)

            if state.exists:
                prev = VesselState(*state.get)
                try:
                    prev_ts = pd.Timestamp(prev.last_time_utc)
                    dt_hours = (curr_ts - prev_ts).total_seconds() / 3600.0
                    if dt_hours > 0 and prev.last_lat is not None:
                        dist_nm = haversine_nm(prev.last_lat, prev.last_lon, curr_lat, curr_lon)
                        calc_speed = dist_nm / dt_hours
                        if curr_sog is not None and prev.last_sog is not None:
                            speed_change = abs(curr_sog - prev.last_sog)
                        else:
                            speed_change = 0.0  # unknown SOG is no evidence of a speed change

                        alert_type = None
                        if calc_speed > MAX_SPEED_KNOTS and speed_change > SPEED_CHANGE_SPOOFING_THRESHOLD:
                            alert_type = "GPS_SPOOFING"
                        elif calc_speed > MAX_SPEED_KNOTS:
                            alert_type = "IMPOSSIBLE_SPEED"
                        elif speed_change > SPEED_CHANGE_ACCEL_THRESHOLD:
                            alert_type = "SUDDEN_ACCELERATION"

                        if alert_type:
                            alerts.append({
                                "mmsi": mmsi,
                                "alert_type": alert_type,
                                "severity": "high" if alert_type != "SUDDEN_ACCELERATION" else "medium",
                                "description": (
                                    f"calculated_speed={calc_speed:.1f}kn "
                                    f"reported_sog={curr_sog}kn speed_change={speed_change:.1f}kn"
                                ),
                                "latitude": curr_lat,
                                "longitude": curr_lon,
                                "detected_at": now_iso,
                            })
                except (ValueError, TypeError):
                    pass  # malformed timestamp — skip anomaly check, still update state below

            state.update(VesselState(
                last_lat=curr_lat, last_lon=curr_lon,
                last_sog=curr_sog, last_time_utc=curr_ts_str,
            ))

    if state.exists:
        state.setTimeoutDuration(GAP_TIMEOUT_MINUTES * 60 * 1000)

    return iter([pd.DataFrame(alerts, columns=[f.name for f in get_output_schema().fields])])


def create_spark_session():
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("MarineFlow-Hot-Alerts")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.ivy", "/opt/spark/.ivy2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_config() -> None:
    pass  # no required env vars — this job has no GCP dependency


def main() -> None:
    validate_config()
    logger.info(
        "🚀 starting MarineFlow Hot Alerts job",
        kafka_source=KAFKA_TOPIC_POSITIONS_BRONZE,
        kafka_sink=KAFKA_TOPIC_ALERTS,
        checkpoint=CHECKPOINT_DIR,
        gap_timeout_minutes=GAP_TIMEOUT_MINUTES,
        max_speed_knots=MAX_SPEED_KNOTS,
    )

    spark = create_spark_session()
    from pyspark.sql import functions as F
    from pyspark.sql.streaming.state import GroupStateTimeout

    kafka_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_POSITIONS_BRONZE)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    positions = (
        kafka_raw
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(F.from_json(F.col("json_value"), get_source_schema()).alias("data"))
        .select("data.*")
    )

    alerts = positions.groupBy("MMSI").applyInPandasWithState(
        process_vessel,
        outputStructType=get_output_schema(),
        stateStructType=get_state_schema(),
        outputMode="append",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
    )

    query = (
        alerts
        .select(
            F.col("mmsi").alias("key"),
            F.to_json(F.struct(*[c for c in alerts.columns])).alias("value"),
        )
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", KAFKA_TOPIC_ALERTS)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .queryName("hot_alerts")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("👂 listening on Kafka — one cycle every 30s")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Shutdown signal received")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
