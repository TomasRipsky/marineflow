# =============================================================================
# MARINEFLOW — Spark Bronze Layer Job
# processing/spark_streaming/bronze_positions.py
#
# Reads vessel position messages from Pub/Sub using the Python client,
# processes them in micro-batches with Spark and writes to:
#   1. GCS Parquet (Bronze layer — raw data store)
#   2. BigQuery vessel_positions_raw (with full lineage metadata)
#
# Bronze philosophy:
# - Minimal validation — only filter physically impossible coordinates
# - No business transformations — data arrives exactly as from source
# - Full lineage — every record knows its origin, batch, file and version
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

GCP_PROJECT_ID   = os.getenv("GCP_PROJECT_ID", "")
GCS_BUCKET       = os.getenv("GCS_BUCKET", "")
SUBSCRIPTION_ID  = os.getenv("PUBSUB_SUB_POSITIONS", "vessel-positions-spark-sub")
SUBSCRIPTION_PATH = f"projects/{GCP_PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"

BRONZE_OUTPUT_DIR = f"gs://{GCS_BUCKET}/bronze/vessel_positions"
BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE", "marineflow_bronze")
BQ_TABLE          = f"{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.vessel_positions_raw"

BATCH_SIZE     = int(os.getenv("BRONZE_BATCH_SIZE", "500"))
BATCH_INTERVAL = int(os.getenv("BRONZE_BATCH_INTERVAL", "30"))
MAX_BATCHES    = int(os.getenv("BRONZE_MAX_BATCHES", "0"))

# Pipeline version — in production this would come from git describe
PIPELINE_VERSION = os.getenv("PIPELINE_VERSION", "dev")


# =============================================================================
# Pub/Sub Pull
# =============================================================================

def pull_messages_from_pubsub(max_messages: int) -> List[dict]:
    """
    Pull a batch of messages from Pub/Sub and acknowledge them.
    Returns a list of parsed JSON dicts with lineage metadata attached.
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
                # Attach Pub/Sub metadata for lineage
                data["_pubsub_message_id"] = received_message.message.message_id
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
# Schema
# =============================================================================

def get_bronze_schema():
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, DoubleType, IntegerType,
    )
    # Exact mirror of the aisstream.io WebSocket message structure.
    # Field names match the original JSON keys — no renaming, no transformation.
    # Enrichments (flag_country, nav status string, vessel_type) happen in Silver.
    from pyspark.sql.types import BooleanType
    return StructType([
        # Message.PositionReport — from vessel transponder
        StructField("Cog",                       DoubleType(),  True),
        StructField("CommunicationState",         IntegerType(), True),
        StructField("Latitude",                   DoubleType(),  True),
        StructField("Longitude",                  DoubleType(),  True),
        StructField("MessageID",                  IntegerType(), True),
        StructField("NavigationalStatus",         IntegerType(), True),  # raw int
        StructField("PositionAccuracy",           BooleanType(), True),
        StructField("Raim",                       BooleanType(), True),
        StructField("RateOfTurn",                 IntegerType(), True),
        StructField("RepeatIndicator",            IntegerType(), True),
        StructField("Sog",                        DoubleType(),  True),
        StructField("Spare",                      IntegerType(), True),
        StructField("SpecialManoeuvreIndicator",  IntegerType(), True),
        StructField("Timestamp",                  IntegerType(), True),
        StructField("TrueHeading",                IntegerType(), True),  # 511 = N/A
        StructField("UserID",                     IntegerType(), True),
        StructField("Valid",                      BooleanType(), True),
        # MetaData — added by aisstream.io
        StructField("MMSI",                       StringType(),  True),
        StructField("MMSI_String",                StringType(),  True),
        StructField("ShipName",                   StringType(),  True),  # raw, untrimmed
        StructField("time_utc",                   StringType(),  True),  # normalized ISO 8601
        # Pipeline metadata
        StructField("ingestion_timestamp",        StringType(),  True),
        StructField("source",                     StringType(),  True),
        StructField("raw_message",                StringType(),  True),
        StructField("_pubsub_message_id",         StringType(),  True),
    ])


# =============================================================================
# Bronze processing
# =============================================================================

def process_batch(
    spark,
    messages: List[dict],
    batch_number: int,
    batch_id: str,
) -> int:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, IntegerType

    if not messages:
        logger.info(f"Batch {batch_number}: no messages to process")
        return 0

    SCHEMA = get_bronze_schema()

    # Normalize types and ensure all schema fields exist
    for msg in messages:
        for field in ["latitude", "longitude", "speed_over_ground", "course_over_ground"]:
            if field in msg and msg[field] is not None:
                msg[field] = float(msg[field])
        if "heading" in msg and msg["heading"] is not None:
            msg["heading"] = int(msg["heading"])
        for sf in SCHEMA.fields:
            if sf.name not in msg:
                msg[sf.name] = None

    # Keep only schema fields
    messages = [{sf.name: msg.get(sf.name) for sf in SCHEMA.fields} for msg in messages]

    df = spark.createDataFrame(messages, schema=SCHEMA)

    # Bronze validation — filter only physically impossible records
    # Field names match raw message: MMSI, Latitude, Longitude, time_utc
    validated = df.filter(
        F.col("MMSI").isNotNull()
        & F.col("Latitude").isNotNull()
        & F.col("Longitude").isNotNull()
        & F.col("Latitude").between(-90, 90)
        & F.col("Longitude").between(-180, 180)
        & (F.col("Latitude") != 91.0)
        & (F.col("Longitude") != 181.0)
        & F.col("time_utc").isNotNull()
        & (F.col("time_utc") != "")
    )

    # Parse timestamps and derive partition columns
    enriched = validated.withColumns({
        "time_utc":            F.to_timestamp(F.col("time_utc")),
        "ingestion_timestamp": F.to_timestamp(F.col("ingestion_timestamp")),
        "partition_date":      F.to_date(F.col("time_utc")),
        "partition_hour":      F.hour(F.col("time_utc")),
    })

    # Filter records where timestamp parsing failed
    enriched = enriched.filter(
        F.col("partition_date").isNotNull()
        & F.col("partition_hour").isNotNull()
    )

    # Add lineage columns
    sample_date = enriched.select("partition_date", "partition_hour").first()
    partition_path = (
        f"{BRONZE_OUTPUT_DIR}"
        f"/partition_date={sample_date['partition_date']}"
        f"/partition_hour={sample_date['partition_hour']}"
    ) if sample_date else BRONZE_OUTPUT_DIR

    enriched = enriched.withColumns({
        "_source_system":    F.coalesce(F.col("source"), F.lit("unknown")),
        "_source_file":      F.lit(partition_path),
        "_batch_id":         F.lit(batch_id),
        "_pipeline_version": F.lit(PIPELINE_VERSION),
        "_ingestion_date":   F.to_date(F.col("ingestion_timestamp")),
    })

    record_count = enriched.count()

    if record_count == 0:
        logger.info(f"Batch {batch_number}: 0 valid records after validation")
        return 0

    # --- Write 1: GCS Parquet ---
    if sample_date:
        enriched.write \
            .mode("overwrite") \
            .parquet(partition_path)
    else:
        enriched.write \
            .mode("append") \
            .partitionBy("partition_date", "partition_hour") \
            .parquet(BRONZE_OUTPUT_DIR)
        
    logger.info(f"Batch {batch_number}: wrote {record_count} records to GCS")

    # --- Write 2: BigQuery with lineage ---
    write_bronze_bigquery(enriched, batch_number)

    return record_count


def write_bronze_bigquery(df, batch_number: int) -> None:
    """
    Write Bronze records to BigQuery vessel_positions_raw.
    Only includes fields present in PositionReport messages.
    ShipStaticData fields (draught, imo_number, callsign, eta, destination)
    are excluded — they are processed from the metadata topic separately.
    """
    # Select exactly the BQ schema columns — raw field names preserved
    bq_df = df.select(
        # Message.PositionReport
        "Cog", "CommunicationState", "Latitude", "Longitude",
        "MessageID", "NavigationalStatus", "PositionAccuracy", "Raim",
        "RateOfTurn", "RepeatIndicator", "Sog", "Spare",
        "SpecialManoeuvreIndicator", "Timestamp", "TrueHeading",
        "UserID", "Valid",
        # MetaData
        "MMSI", "MMSI_String", "ShipName", "time_utc",
        # Pipeline metadata
        "ingestion_timestamp", "raw_message",
        # Lineage
        "_source_system", "_source_file", "_pubsub_message_id",
        "_batch_id", "_pipeline_version", "_ingestion_date",
    )

    try:
        (
            bq_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", GCS_BUCKET)
            .option("writeMethod", "indirect")
            .option("partitionOverwriteMode", "STATIC")
            .option("createDisposition", "CREATE_NEVER")
            .option("writeDisposition", "WRITE_APPEND")
            .option("allowFieldRelaxation", "true")
            .mode("append")
            .save()
        )
        logger.info(f"Batch {batch_number}: wrote to BigQuery {BQ_TABLE}")
    except Exception as e:
        logger.error(f"BigQuery write failed (non-fatal): {e}")


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
    logger.info(f"Pipeline version: {PIPELINE_VERSION}")

    spark = create_spark_session()
    total_records = 0
    batch_number = 0

    try:
        while True:
            batch_number += 1
            batch_id = str(uuid.uuid4())[:8]  # short unique ID per batch
            logger.info(f"Starting batch {batch_number} (id={batch_id})")

            messages = pull_messages_from_pubsub(BATCH_SIZE)
            records_written = process_batch(spark, messages, batch_number, batch_id)
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