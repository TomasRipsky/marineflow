# =============================================================================
# MARINEFLOW — Spark Bronze Layer Job
# processing/spark_streaming/bronze_positions.py
#
# Reads vessel position messages from Pub/Sub using the Python client,
# processes them in micro-batches with Spark, and writes to GCS as Parquet.
#
# Architecture note:
# We use the Python Pub/Sub client for ingestion instead of a native Spark
# connector. This is the standard pattern for GCP + Spark pipelines because:
# 1. The native Pub/Sub Spark connector has limited community support
# 2. The Python client gives us full control over ack/nack logic
# 3. Micro-batch processing is idiomatic for exactly-once semantics
#    with external sources
#
# Run:
#   spark-submit \
#     --jars jars/gcs-connector-hadoop3-latest.jar \
#     bronze_positions.py
# =============================================================================

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import List

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

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
GCS_BUCKET = os.getenv("GCS_BUCKET", "")
SUBSCRIPTION_ID = os.getenv("PUBSUB_SUB_POSITIONS", "vessel-positions-spark-sub")
SUBSCRIPTION_PATH = f"projects/{GCP_PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"
BRONZE_OUTPUT_DIR = f"gs://{GCS_BUCKET}/bronze/vessel_positions"

# How many messages to pull per micro-batch
BATCH_SIZE = int(os.getenv("BRONZE_BATCH_SIZE", "500"))
# How long to wait between batches (seconds)
BATCH_INTERVAL = int(os.getenv("BRONZE_BATCH_INTERVAL", "30"))
# How many batches to run (0 = infinite)
MAX_BATCHES = int(os.getenv("BRONZE_MAX_BATCHES", "0"))


# =============================================================================
# Pub/Sub Pull
# =============================================================================

def pull_messages_from_pubsub(max_messages: int) -> List[dict]:
    """
    Pull a batch of messages from Pub/Sub and acknowledge them.
    Returns a list of parsed JSON dicts.
    """
    from google.cloud import pubsub_v1

    subscriber = pubsub_v1.SubscriberClient()
    messages = []
    ack_ids = []

    try:
        response = subscriber.pull(
            request={
                "subscription": SUBSCRIPTION_PATH,
                "max_messages": max_messages,
            },
            timeout=10,
        )

        for received_message in response.received_messages:
            try:
                data = json.loads(received_message.message.data.decode("utf-8"))
                data["pipeline_ingest_time"] = datetime.now(timezone.utc).isoformat()
                data["pubsub_message_id"] = received_message.message.message_id
                messages.append(data)
                ack_ids.append(received_message.ack_id)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse message: {e}")
                ack_ids.append(received_message.ack_id)

        if ack_ids:
            subscriber.acknowledge(
                request={
                    "subscription": SUBSCRIPTION_PATH,
                    "ack_ids": ack_ids,
                }
            )

        logger.info(f"Pulled and acknowledged {len(messages)} messages")

    except Exception as e:
        logger.error(f"Pub/Sub pull failed: {e}")

    finally:
        subscriber.close()

    return messages


# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("MarineFlow-Bronze-Positions")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g"))
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        .config(
            "spark.hadoop.google.cloud.auth.type",
            "APPLICATION_DEFAULT",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# =============================================================================
# Bronze processing
# =============================================================================

def process_batch(spark, messages: List[dict], batch_number: int) -> int:
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
    )

    if not messages:
        logger.info(f"Batch {batch_number}: no messages to process")
        return 0

    SCHEMA = StructType([
        StructField("mmsi",                StringType(),  True),
        StructField("vessel_name",         StringType(),  True),
        StructField("vessel_type",         StringType(),  True),
        StructField("latitude",            DoubleType(),  True),
        StructField("longitude",           DoubleType(),  True),
        StructField("speed_over_ground",   DoubleType(),  True),
        StructField("course_over_ground",  DoubleType(),  True),
        StructField("heading",             IntegerType(), True),
        StructField("navigational_status", StringType(),  True),
        StructField("destination",         StringType(),  True),
        StructField("flag_country",        StringType(),  True),
        StructField("imo_number",          StringType(),  True),
        StructField("callsign",            StringType(),  True),
        StructField("event_timestamp",     StringType(),  True),
        StructField("ingestion_timestamp", StringType(),  True),
        StructField("source",              StringType(),  True),
        StructField("pipeline_ingest_time",StringType(),  True),
        StructField("pubsub_message_id",   StringType(),  True),
    ])

    # Normalize types and ensure all schema fields exist in every message
    for msg in messages:
        for field in ["latitude", "longitude", "speed_over_ground",
                      "course_over_ground"]:
            if field in msg and msg[field] is not None:
                msg[field] = float(msg[field])
        if "heading" in msg and msg["heading"] is not None:
            msg["heading"] = int(msg["heading"])
        # Ensure all schema fields exist
        for sf in SCHEMA.fields:
            if sf.name not in msg:
                msg[sf.name] = None

    # Keep only schema fields to avoid unknown columns
    messages = [{sf.name: msg.get(sf.name) for sf in SCHEMA.fields} for msg in messages]

    df = spark.createDataFrame(messages, schema=SCHEMA)

    validated = df.filter(
        F.col("mmsi").isNotNull()
        & F.col("latitude").isNotNull()
        & F.col("longitude").isNotNull()
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
        & (F.col("latitude") != 91.0)
        & (F.col("longitude") != 181.0)
    )

    enriched = validated.withColumns({
        "event_timestamp": F.to_timestamp(F.col("event_timestamp")),
        "partition_date": F.to_date(F.col("event_timestamp")),
        "partition_hour": F.hour(F.col("event_timestamp")),
    })

    record_count = enriched.count()

    enriched.write.mode("append").partitionBy(
        "partition_date", "partition_hour"
    ).parquet(BRONZE_OUTPUT_DIR)

    logger.info(f"Batch {batch_number}: wrote {record_count} records to {BRONZE_OUTPUT_DIR}")
    return record_count

# =============================================================================
# Entry point
# =============================================================================

def validate_config() -> None:
    missing = [v for v in ["GCP_PROJECT_ID", "GCS_BUCKET"] if not os.getenv(v)]
    if missing:
        logger.error(f"Missing required environment variables: {missing}")
        sys.exit(1)


def main() -> None:
    validate_config()

    logger.info("Starting MarineFlow Bronze Positions job")
    logger.info(f"Subscription: {SUBSCRIPTION_PATH}")
    logger.info(f"Output: {BRONZE_OUTPUT_DIR}")
    logger.info(f"Batch size: {BATCH_SIZE} messages every {BATCH_INTERVAL}s")

    spark = create_spark_session()
    total_records = 0
    batch_number = 0

    try:
        while True:
            batch_number += 1
            logger.info(f"Starting batch {batch_number}")

            messages = pull_messages_from_pubsub(BATCH_SIZE)
            records_written = process_batch(spark, messages, batch_number)
            total_records += records_written

            logger.info(f"Total records written so far: {total_records}")

            if MAX_BATCHES > 0 and batch_number >= MAX_BATCHES:
                logger.info(f"Reached max batches ({MAX_BATCHES}), stopping")
                break

            logger.info(f"Waiting {BATCH_INTERVAL}s until next batch...")
            time.sleep(BATCH_INTERVAL)

    except KeyboardInterrupt:
        logger.info(f"Shutdown signal: total records written: {total_records}")
    except Exception as e:
        logger.error(f"Bronze job failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()