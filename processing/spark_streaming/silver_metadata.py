# =============================================================================
# MARINEFLOW — Spark Silver Metadata Job
# processing/spark_streaming/silver_metadata.py
#
# Reads vessel-metadata messages from Pub/Sub (ShipStaticData),
# transforms them, and upserts into marineflow_silver.vessel_metadata
# for joining with vessel_positions_clean in Gold.
#
# This job runs independently from silver_positions.py.
# Gold models join both Silver tables on mmsi to get the full picture.
#
# Fields populated here (null in positions Silver):
#   - vessel_type_normalized  (from raw integer or string)
#   - destination_clean       (normalized free text)
#   - imo_number
#   - callsign
#   - draught
#
# Run:
#   spark-submit \
#     --jars jars/gcs-connector-hadoop3-latest.jar,jars/spark-bigquery-0.40.0.jar \
#     silver_metadata.py
# =============================================================================

import logging
import os
import sys
import time

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

GCP_PROJECT_ID    = os.getenv("GCP_PROJECT_ID", "")
GCS_BUCKET        = os.getenv("GCS_BUCKET", "")
BQ_DATASET_SILVER = os.getenv("BQ_DATASET_SILVER", "marineflow_silver")
PUBSUB_SUB        = os.getenv("PUBSUB_SUB_METADATA", "vessel-metadata-spark-sub")

METADATA_OUTPUT_DIR = f"gs://{GCS_BUCKET}/silver/vessel_metadata"
BQ_TABLE            = f"{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.vessel_metadata"

BATCH_SIZE     = int(os.getenv("METADATA_BATCH_SIZE", "200"))
BATCH_INTERVAL = int(os.getenv("METADATA_BATCH_INTERVAL", "60"))
MAX_BATCHES    = int(os.getenv("METADATA_MAX_BATCHES", "0"))

# =============================================================================
# Reference data
# =============================================================================

# AIS vessel type codes → normalized category
# Source: ITU-R M.1371-5, Table 20
VESSEL_TYPE_MAP = {
    **dict.fromkeys(range(70, 80), "cargo"),
    **dict.fromkeys(range(80, 90), "tanker"),
    **dict.fromkeys(range(60, 70), "passenger"),
    **dict.fromkeys(range(30, 36), "fishing"),
    **dict.fromkeys(range(50, 60), "special_craft"),
    **dict.fromkeys(range(36, 40), "sailing_or_pleasure"),
    **dict.fromkeys(range(20, 30), "wing_in_ground"),
    **dict.fromkeys(range(90, 100), "other"),
}

# Destination strings that indicate no real destination
DESTINATION_JUNK = {
    "", "NULL", "NONE", "N/A", "NA", "NIL", "UNKNOWN", "?", ".",
    "0", "00", "000", "TBD", "TBA",
}


# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("MarineFlow-Silver-Metadata")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =============================================================================
# Pub/Sub pull
# =============================================================================

def pull_metadata_messages(max_messages: int):
    """Pull vessel-metadata messages from Pub/Sub synchronously."""
    from google.cloud import pubsub_v1

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(GCP_PROJECT_ID, PUBSUB_SUB)

    messages = []
    try:
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": max_messages},
            timeout=30,
        )

        ack_ids = []
        for msg in response.received_messages:
            try:
                import json
                data = json.loads(msg.message.data.decode("utf-8"))
                messages.append(data)
                ack_ids.append(msg.ack_id)
            except Exception as e:
                logger.warning(f"Failed to decode metadata message: {e}")
                ack_ids.append(msg.ack_id)  # ack anyway to avoid redelivery

        if ack_ids:
            subscriber.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
            )
            logger.info(f"Pulled and acked {len(messages)} metadata messages")

    except Exception as e:
        logger.error(f"Pub/Sub pull failed: {e}")
    finally:
        subscriber.close()

    return messages


# =============================================================================
# Transform
# =============================================================================

def normalize_vessel_type(raw_type):
    """
    Normalize vessel type to a standard category string.
    Handles both integer codes (new parser) and legacy string values.
    """
    if raw_type is None:
        return None

    # New parser sends raw integer
    if isinstance(raw_type, int):
        return VESSEL_TYPE_MAP.get(raw_type, f"other")

    # Legacy parser sent strings — pass through known values, normalize unknown
    raw_str = str(raw_type).strip().lower()
    known = {
        "cargo", "tanker", "passenger", "fishing",
        "special_craft", "sailing_or_pleasure", "other",
        "wing_in_ground", "tug",
    }
    return raw_str if raw_str in known else "other"


def normalize_destination(raw_dest):
    """
    Clean AIS destination free text.
    AIS destinations are 20-char free text — quality varies wildly.
    """
    if not raw_dest:
        return None
    cleaned = raw_dest.strip().upper()
    if cleaned in DESTINATION_JUNK:
        return None
    # Remove obvious garbage patterns (non-printable, control chars)
    cleaned = "".join(c for c in cleaned if c.isprintable())
    return cleaned if cleaned else None


def transform_metadata(messages):
    """Apply Silver transformations to raw metadata dicts."""
    import uuid
    from datetime import datetime, timezone

    batch_id = str(uuid.uuid4())[:8]
    now = datetime.now(timezone.utc).isoformat()
    transformed = []

    for msg in messages:
        mmsi = str(msg.get("mmsi", "")).strip()
        if not mmsi:
            continue

        transformed.append({
            "mmsi":                   mmsi,
            "vessel_name":            (msg.get("vessel_name") or "").strip() or None,
            "vessel_type_normalized": normalize_vessel_type(msg.get("vessel_type")),
            "imo_number":             msg.get("imo_number"),
            "callsign":               (msg.get("callsign") or "").strip() or None,
            "destination_clean":      normalize_destination(msg.get("destination")),
            "draught":                msg.get("draught"),
            "flag_country":           msg.get("flag_country"),
            "source_system":          msg.get("source", "aisstream_live"),
            "ingestion_timestamp":    msg.get("ingestion_timestamp", now),
            "processing_timestamp":   now,
            "_silver_batch_id":       batch_id,
        })

    return transformed


# =============================================================================
# Schema
# =============================================================================

def get_metadata_schema():
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, FloatType, TimestampType,
    )
    return StructType([
        StructField("mmsi",                   StringType(),    False),
        StructField("vessel_name",            StringType(),    True),
        StructField("vessel_type_normalized", StringType(),    True),
        StructField("imo_number",             StringType(),    True),
        StructField("callsign",               StringType(),    True),
        StructField("destination_clean",      StringType(),    True),
        StructField("draught",                FloatType(),     True),
        StructField("flag_country",           StringType(),    True),
        StructField("source_system",          StringType(),    True),
        StructField("ingestion_timestamp",    StringType(),    True),  # cast to timestamp after createDataFrame
        StructField("processing_timestamp",   StringType(),    True),  # cast to timestamp after createDataFrame
        StructField("_silver_batch_id",       StringType(),    True),
    ])


# =============================================================================
# Write
# =============================================================================

def write_metadata_gcs(df, batch_number: int) -> int:
    from pyspark.sql import functions as F

    enriched = df.withColumn(
        "partition_date",
        F.to_date(F.col("processing_timestamp").cast("timestamp"))
    )
    count = enriched.count()
    if count == 0:
        return 0

    # coalesce(1) — metadata messages are sparse, one file per batch is enough.
    enriched.coalesce(1).write.mode("append").partitionBy("partition_date").parquet(METADATA_OUTPUT_DIR)
    logger.info(f"Batch {batch_number}: wrote {count} metadata records to GCS")
    return count




# =============================================================================
# Entry point
# =============================================================================

def validate_config() -> None:
    missing = [v for v in ["GCP_PROJECT_ID", "GCS_BUCKET"] if not os.getenv(v)]
    if missing:
        logger.error(f"Missing required env vars: {missing}")
        sys.exit(1)


def main() -> None:
    validate_config()

    logger.info("Starting MarineFlow Silver Metadata job")
    logger.info(f"Subscription: {PUBSUB_SUB}")
    logger.info(f"Output GCS:   {METADATA_OUTPUT_DIR}")
    logger.info(f"Output BQ:    {BQ_TABLE}")

    spark = create_spark_session()
    schema = get_metadata_schema()
    total_records = 0
    batch_number = 0

    try:
        while True:
            batch_number += 1
            logger.info(f"Starting metadata batch {batch_number}")

            try:
                messages = pull_metadata_messages(BATCH_SIZE)

                if not messages:
                    logger.info(f"Batch {batch_number}: no metadata messages available")
                else:
                    transformed = transform_metadata(messages)

                    if transformed:
                        from pyspark.sql import functions as F
                        df = spark.createDataFrame(transformed, schema=schema)

                        # Cast ISO 8601 strings to TimestampType for BQ Parquet compatibility
                        df = df.withColumns({
                            "ingestion_timestamp":  F.to_timestamp(F.col("ingestion_timestamp")),
                            "processing_timestamp": F.to_timestamp(F.col("processing_timestamp")),
                        })

                        if os.getenv("METADATA_DEBUG"):
                            logger.info("DEBUG mode — showing sample:")
                            df.select(
                                "mmsi", "vessel_name",
                                "vessel_type_normalized", "destination_clean"
                            ).show(5, truncate=False)

                        count = write_metadata_gcs(df, batch_number)
                        total_records += count

                        # BigQuery reads directly from GCS via external table —
                        # no explicit write needed here.

            except Exception as e:
                logger.error(f"Batch {batch_number} failed: {e}", exc_info=True)

            logger.info(f"Total metadata records written: {total_records}")

            if MAX_BATCHES > 0 and batch_number >= MAX_BATCHES:
                logger.info(f"Reached max batches ({MAX_BATCHES}), stopping")
                break

            logger.info(f"Waiting {BATCH_INTERVAL}s until next batch...")
            time.sleep(BATCH_INTERVAL)

    except KeyboardInterrupt:
        logger.info(f"Shutdown signal: total metadata records: {total_records}")
    except Exception as e:
        logger.error(f"Metadata job failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()