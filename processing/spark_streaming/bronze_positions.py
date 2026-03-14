# =============================================================================
# MARINEFLOW — Spark Bronze Layer Job
# processing/spark_streaming/bronze_positions.py
#
# Reads vessel position messages from Pub/Sub and writes to:
#   1. GCS Parquet  — partitioned by date/hour, raw field names preserved
#   2. BigQuery     — vessel_positions_raw with full lineage metadata
#
# Bronze philosophy:
#   - Zero business transformations — data arrives exactly as from source
#   - Minimal validation — only filter physically impossible coordinates
#   - Full lineage — every record knows its origin, batch, file and version
#
# Partition design:
#   GCS: bronze/vessel_positions/partition_date=YYYY-MM-DD/partition_hour=HH/
#   Silver excludes the current hour while Bronze is writing to it (delay <= 1h)
#
# Run:
#   spark-submit \
#     --jars jars/gcs-connector-hadoop3-latest.jar,jars/spark-bigquery.jar \
#     --driver-class-path jars/gcs-connector-hadoop3-latest.jar:jars/spark-bigquery.jar \
#     bronze_positions.py
# =============================================================================

import json
import logging
import os
import sys
import time
import uuid
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

GCP_PROJECT_ID    = os.getenv("GCP_PROJECT_ID", "")
GCS_BUCKET        = os.getenv("GCS_BUCKET", "")
SUBSCRIPTION_ID   = os.getenv("PUBSUB_SUB_POSITIONS", "vessel-positions-spark-sub")
SUBSCRIPTION_PATH = f"projects/{GCP_PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"

BRONZE_OUTPUT_DIR = f"gs://{GCS_BUCKET}/bronze/vessel_positions"
BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE", "marineflow_bronze")
BQ_TABLE          = f"{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.vessel_positions_raw"

BATCH_SIZE       = int(os.getenv("BRONZE_BATCH_SIZE", "500"))
BATCH_INTERVAL   = int(os.getenv("BRONZE_BATCH_INTERVAL", "30"))
MAX_BATCHES      = int(os.getenv("BRONZE_MAX_BATCHES", "0"))
PIPELINE_VERSION = os.getenv("PIPELINE_VERSION", "dev")


# =============================================================================
# Schema
# =============================================================================

def get_bronze_schema():
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, DoubleType, IntegerType, BooleanType,
    )
    # Exact mirror of the aisstream.io WebSocket message structure.
    # Field names match the original JSON keys — no renaming, no transformation.
    # All business enrichments (flag_country, nav status, vessel_type) happen in Silver.
    return StructType([
        # Message.PositionReport — from vessel transponder
        StructField("Cog",                      DoubleType(),  True),
        StructField("CommunicationState",        IntegerType(), True),
        StructField("Latitude",                  DoubleType(),  True),
        StructField("Longitude",                 DoubleType(),  True),
        StructField("MessageID",                 IntegerType(), True),
        StructField("NavigationalStatus",        IntegerType(), True),  # raw int — translated in Silver
        StructField("PositionAccuracy",          BooleanType(), True),
        StructField("Raim",                      BooleanType(), True),
        StructField("RateOfTurn",                IntegerType(), True),
        StructField("RepeatIndicator",           IntegerType(), True),
        StructField("Sog",                       DoubleType(),  True),
        StructField("Spare",                     IntegerType(), True),
        StructField("SpecialManoeuvreIndicator", IntegerType(), True),
        StructField("Timestamp",                 IntegerType(), True),
        StructField("TrueHeading",               IntegerType(), True),  # 511 = unavailable
        StructField("UserID",                    IntegerType(), True),
        StructField("Valid",                     BooleanType(), True),
        # MetaData — added by aisstream.io, not from transponder
        StructField("MMSI",                      StringType(),  True),
        StructField("MMSI_String",               StringType(),  True),
        StructField("ShipName",                  StringType(),  True),  # raw, untrimmed
        StructField("time_utc",                  StringType(),  True),  # ISO 8601 string
        # Pipeline metadata — added by our ingestion layer
        StructField("ingestion_timestamp",       StringType(),  True),
        StructField("source",                    StringType(),  True),
        StructField("raw_message",               StringType(),  True),
        StructField("_pubsub_message_id",        StringType(),  True),
    ])


# =============================================================================
# Pub/Sub
# =============================================================================

def pull_messages_from_pubsub(max_messages: int) -> List[dict]:
    """Pull a batch of messages from Pub/Sub and acknowledge them."""
    from google.cloud import pubsub_v1

    subscriber = pubsub_v1.SubscriberClient()
    messages = []
    ack_ids  = []

    try:
        response = subscriber.pull(
            request={"subscription": SUBSCRIPTION_PATH, "max_messages": max_messages},
            timeout=10,
        )

        for received in response.received_messages:
            try:
                data = json.loads(received.message.data.decode("utf-8"))
                data["_pubsub_message_id"] = received.message.message_id
                messages.append(data)
                ack_ids.append(received.ack_id)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to decode message: {e}")
                ack_ids.append(received.ack_id)  # ack anyway to avoid redelivery

        if ack_ids:
            subscriber.acknowledge(
                request={"subscription": SUBSCRIPTION_PATH, "ack_ids": ack_ids}
            )

        logger.info(f"Pulled and acknowledged {len(messages)} messages")

    except Exception as e:
        logger.error(f"Pub/Sub pull failed: {e}")
    finally:
        subscriber.close()

    return messages


# =============================================================================
# DataFrame construction
# =============================================================================

def build_dataframe(spark, messages: List[dict]):
    """
    Build a Spark DataFrame from raw Pub/Sub message dicts.

    Aligns each dict to the schema:
      - Fields missing from the message are filled with None
      - Extra fields not in the schema are dropped
    This ensures createDataFrame never fails due to schema mismatches.
    """
    schema = get_bronze_schema()
    schema_fields = {sf.name for sf in schema.fields}

    aligned = [
        {field: msg.get(field) for field in schema_fields}
        for msg in messages
    ]

    return spark.createDataFrame(aligned, schema=schema)


# =============================================================================
# Validation
# =============================================================================

def validate(df):
    """
    Filter out physically impossible records.
    Bronze rejects only what cannot possibly be real data —
    no defaults, no corrections, no business logic.
    """
    from pyspark.sql import functions as F

    return df.filter(
        F.col("MMSI").isNotNull()
        & F.col("Latitude").isNotNull()
        & F.col("Longitude").isNotNull()
        & F.col("Latitude").between(-90, 90)
        & F.col("Longitude").between(-180, 180)
        & (F.col("Latitude") != 91.0)    # AIS "not available" sentinel
        & (F.col("Longitude") != 181.0)  # AIS "not available" sentinel
        & F.col("time_utc").isNotNull()
        & (F.col("time_utc") != "")
    )


# =============================================================================
# Partitioning
# =============================================================================

def add_partition_columns(df):
    """
    Parse ISO 8601 timestamp strings to TimestampType and derive partition columns.
    Casting strings to timestamps is a storage requirement, not a business transformation.
    Records where timestamp parsing fails are dropped — they cannot be partitioned.
    """
    from pyspark.sql import functions as F

    df = df.withColumns({
        "time_utc":            F.to_timestamp(F.col("time_utc")),
        "ingestion_timestamp": F.to_timestamp(F.col("ingestion_timestamp")),
        "partition_date":      F.to_date(F.col("time_utc")),
        "partition_hour":      F.hour(F.col("time_utc")),
    })

    return df.filter(
        F.col("partition_date").isNotNull()
        & F.col("partition_hour").isNotNull()
    )


# =============================================================================
# Lineage
# =============================================================================

def add_lineage(df, batch_id: str, partition_path: str):
    """
    Attach lineage columns to every record.
    Enables full traceability from any Gold record back to the raw Pub/Sub message.
    """
    from pyspark.sql import functions as F

    return df.withColumns({
        "_source_system":    F.coalesce(F.col("source"), F.lit("unknown")),
        "_source_file":      F.lit(partition_path),
        "_batch_id":         F.lit(batch_id),
        "_pipeline_version": F.lit(PIPELINE_VERSION),
        "_ingestion_date":   F.to_date(F.col("ingestion_timestamp")),
    })


# =============================================================================
# Write
# =============================================================================

def write_gcs(df, batch_number: int, partition_path: str) -> int:
    """Write Bronze Parquet to GCS. Returns the number of records written."""
    record_count = df.count()

    if record_count == 0:
        logger.info(f"Batch {batch_number}: 0 valid records after validation")
        return 0

    # coalesce(1) — each batch is small enough to fit in one Parquet file.
    # Avoids accumulating many small files per partition over time.
    df.coalesce(1).write.mode("overwrite").parquet(partition_path)
    logger.info(f"Batch {batch_number}: wrote {record_count} records to {partition_path}")
    return record_count




# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("MarineFlow-Bronze-Positions")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped", "true")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


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
    logger.info(f"GCS Output:   {BRONZE_OUTPUT_DIR}")
    logger.info(f"BQ Table:     {BQ_TABLE}")

    spark = create_spark_session()
    total_records = 0
    batch_number  = 0

    try:
        while True:
            batch_number += 1
            batch_id = str(uuid.uuid4())[:8]
            logger.info(f"Starting batch {batch_number} (id={batch_id})")

            messages = pull_messages_from_pubsub(BATCH_SIZE)

            if messages:
                df = build_dataframe(spark, messages)
                df = validate(df)
                df = add_partition_columns(df)

                sample = df.select("partition_date", "partition_hour").first()
                if sample:
                    partition_path = (
                        f"{BRONZE_OUTPUT_DIR}"
                        f"/partition_date={sample['partition_date']}"
                        f"/partition_hour={sample['partition_hour']}"
                    )
                    df = add_lineage(df, batch_id, partition_path)
                    records_written = write_gcs(df, batch_number, partition_path)
                    total_records += records_written

                    # BigQuery reads directly from GCS via external table —
                    # no explicit write needed here.
                else:
                    logger.info(f"Batch {batch_number}: 0 valid records after validation")

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