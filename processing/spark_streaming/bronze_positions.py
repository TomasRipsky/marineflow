# =============================================================================
# MARINEFLOW — Spark Bronze Layer Job
# processing/spark_streaming/bronze_positions.py
#
# Reads vessel position messages from GCS using Spark Structured Streaming.
# Messages arrive via the Pub/Sub Cloud Storage subscription which writes
# each batch of Pub/Sub messages as a JSON file to:
#   gs://{bucket}/pubsub-landing/vessel-positions/
#
# Each file contains one Pub/Sub envelope per line:
#   {
#     "subscription": "...",
#     "message": {
#       "data": "<base64 encoded raw AIS JSON>",
#       "messageId": "...",
#       "publishTime": "...",
#       "attributes": {"source": "aisstream_live", "message_type": "PositionReport"}
#     }
#   }
#
# Bronze responsibilities:
#   1. Decode base64 message.data → raw AIS JSON
#   2. Extract PositionReport fields with their original names
#   3. Filter physically impossible coordinates
#   4. Add partition columns and lineage fields
#   5. Write to GCS Bronze Parquet (coalesce 1 file per micro-batch)
#
# Bronze philosophy:
#   - Zero business transformations — field names match aisstream.io exactly
#   - Minimal validation — only reject physically impossible records
#   - Full lineage — every record knows its Pub/Sub messageId, batch and version
#
# Run:
#   spark-submit \
#     --jars jars/gcs-connector-hadoop3-latest.jar \
#     bronze_positions.py
# =============================================================================

import logging
import os
import sys
import uuid

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
PIPELINE_VERSION  = os.getenv("PIPELINE_VERSION", "dev")

LANDING_DIR       = f"gs://{GCS_BUCKET}/pubsub-landing/vessel-positions"
BRONZE_OUTPUT_DIR = f"gs://{GCS_BUCKET}/bronze/vessel_positions"
CHECKPOINT_DIR    = f"gs://{GCS_BUCKET}/checkpoints/bronze_positions"

MAX_FILES_PER_TRIGGER = int(os.getenv("BRONZE_MAX_FILES_PER_TRIGGER", "10"))


# =============================================================================
# Pub/Sub envelope schema
# =============================================================================

def get_landing_schema():
    """
    Schema of the JSON files written by the Pub/Sub Cloud Storage subscription.
    GCP writes the raw AIS message directly — no envelope wrapper.
    Each file contains one complete AIS JSON message.
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType,
        IntegerType, BooleanType, LongType,
    )
    position_report = StructType([
        StructField("Cog",                      DoubleType(),  True),
        StructField("CommunicationState",        IntegerType(), True),
        StructField("Latitude",                  DoubleType(),  True),
        StructField("Longitude",                 DoubleType(),  True),
        StructField("MessageID",                 IntegerType(), True),
        StructField("NavigationalStatus",        IntegerType(), True),
        StructField("PositionAccuracy",          BooleanType(), True),
        StructField("Raim",                      BooleanType(), True),
        StructField("RateOfTurn",                IntegerType(), True),
        StructField("RepeatIndicator",           IntegerType(), True),
        StructField("Sog",                       DoubleType(),  True),
        StructField("Spare",                     IntegerType(), True),
        StructField("SpecialManoeuvreIndicator", IntegerType(), True),
        StructField("Timestamp",                 IntegerType(), True),
        StructField("TrueHeading",               IntegerType(), True),
        StructField("UserID",                    IntegerType(), True),
        StructField("Valid",                     BooleanType(), True),
    ])
    return StructType([
        StructField("Message", StructType([
            StructField("PositionReport", position_report, True),
        ]), True),
        StructField("MessageType", StringType(), True),
        StructField("MetaData", StructType([
            StructField("MMSI",        LongType(),   True),
            StructField("MMSI_String", LongType(),   True),
            StructField("ShipName",    StringType(), True),
            StructField("time_utc",    StringType(), True),
            StructField("latitude",    DoubleType(), True),
            StructField("longitude",   DoubleType(), True),
        ]), True),
    ])


# =============================================================================
# Bronze schema
# =============================================================================

def get_bronze_schema():
    """
    Exact mirror of the aisstream.io PositionReport message structure.
    Field names match the original JSON keys — no renaming, no transformation.
    """
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, DoubleType, IntegerType, BooleanType,
    )
    return StructType([
        # Message.PositionReport — from vessel transponder
        StructField("Cog",                      DoubleType(),  True),
        StructField("CommunicationState",        IntegerType(), True),
        StructField("Latitude",                  DoubleType(),  True),
        StructField("Longitude",                 DoubleType(),  True),
        StructField("MessageID",                 IntegerType(), True),
        StructField("NavigationalStatus",        IntegerType(), True),
        StructField("PositionAccuracy",          BooleanType(), True),
        StructField("Raim",                      BooleanType(), True),
        StructField("RateOfTurn",                IntegerType(), True),
        StructField("RepeatIndicator",           IntegerType(), True),
        StructField("Sog",                       DoubleType(),  True),
        StructField("Spare",                     IntegerType(), True),
        StructField("SpecialManoeuvreIndicator", IntegerType(), True),
        StructField("Timestamp",                 IntegerType(), True),
        StructField("TrueHeading",               IntegerType(), True),
        StructField("UserID",                    IntegerType(), True),
        StructField("Valid",                     BooleanType(), True),
        # MetaData — added by aisstream.io
        StructField("MMSI",                      StringType(),  True),
        StructField("MMSI_String",               StringType(),  True),
        StructField("ShipName",                  StringType(),  True),
        StructField("time_utc",                  StringType(),  True),
    ])


# =============================================================================
# Extract AIS fields
# =============================================================================

def extract_fields(df):
    """
    Flatten the nested AIS JSON structure into top-level Bronze columns.
    GCS subscription writes raw AIS JSON directly — no base64 or envelope.
    Only PositionReport messages are extracted here.
    ShipStaticData is handled by silver_metadata.py from its own landing dir.
    """
    from pyspark.sql import functions as F

    return df.filter(
        F.col("MessageType") == "PositionReport"
    ).select(
        # PositionReport fields — raw names preserved exactly as from transponder
        F.col("Message.PositionReport.Cog").alias("Cog"),
        F.col("Message.PositionReport.CommunicationState").alias("CommunicationState"),
        F.col("Message.PositionReport.Latitude").alias("Latitude"),
        F.col("Message.PositionReport.Longitude").alias("Longitude"),
        F.col("Message.PositionReport.MessageID").alias("MessageID"),
        F.col("Message.PositionReport.NavigationalStatus").alias("NavigationalStatus"),
        F.col("Message.PositionReport.PositionAccuracy").alias("PositionAccuracy"),
        F.col("Message.PositionReport.Raim").alias("Raim"),
        F.col("Message.PositionReport.RateOfTurn").alias("RateOfTurn"),
        F.col("Message.PositionReport.RepeatIndicator").alias("RepeatIndicator"),
        F.col("Message.PositionReport.Sog").alias("Sog"),
        F.col("Message.PositionReport.Spare").alias("Spare"),
        F.col("Message.PositionReport.SpecialManoeuvreIndicator").alias("SpecialManoeuvreIndicator"),
        F.col("Message.PositionReport.Timestamp").alias("Timestamp"),
        F.col("Message.PositionReport.TrueHeading").alias("TrueHeading"),
        F.col("Message.PositionReport.UserID").alias("UserID"),
        F.col("Message.PositionReport.Valid").alias("Valid"),
        # MetaData fields — added by aisstream.io
        F.col("MetaData.MMSI").cast("string").alias("MMSI"),
        F.col("MetaData.MMSI_String").cast("string").alias("MMSI_String"),
        F.col("MetaData.ShipName").alias("ShipName"),
        F.col("MetaData.time_utc").alias("time_utc"),
        # Source system from MessageType presence
        F.lit("aisstream_live").alias("_source_system"),
    )


# =============================================================================
# Validation
# =============================================================================

def validate(df):
    """Filter physically impossible records — Bronze rejects only what cannot be real."""
    from pyspark.sql import functions as F

    return df.filter(
        F.col("MMSI").isNotNull()
        & F.col("Latitude").isNotNull()
        & F.col("Longitude").isNotNull()
        & F.col("Latitude").between(-90, 90)
        & F.col("Longitude").between(-180, 180)
        & (F.col("Latitude")  != 91.0)
        & (F.col("Longitude") != 181.0)
    )


# =============================================================================
# Partition columns and lineage
# =============================================================================

def add_partition_and_lineage(df, batch_id: str):
    """Add partition columns and lineage fields."""
    from pyspark.sql import functions as F

    df = df.withColumns({
        "time_utc":            F.to_timestamp(F.col("time_utc")),
        "ingestion_timestamp": F.current_timestamp(),
        "partition_date":      F.to_date(F.col("time_utc")),
        "partition_hour":      F.hour(F.col("time_utc")),
    })

    df = df.filter(
        F.col("partition_date").isNotNull()
        & F.col("partition_hour").isNotNull()
    )

    return df.withColumns({
        "_batch_id":         F.lit(batch_id),
        "_pipeline_version": F.lit(PIPELINE_VERSION),
        "_ingestion_date":   F.to_date(F.col("ingestion_timestamp")),
        "_source_file":      F.lit(BRONZE_OUTPUT_DIR),
    })


# =============================================================================
# Write micro-batch
# =============================================================================

def process_micro_batch(df, epoch_id: int):
    """
    Called by Structured Streaming for each micro-batch.
    Decodes, validates, enriches and writes to GCS Bronze.
    """
    from pyspark.sql import functions as F

    batch_id = str(uuid.uuid4())[:8]
    logger.info(f"Processing micro-batch epoch={epoch_id} batch_id={batch_id}")

    extracted = extract_fields(df)
    validated  = validate(extracted)
    enriched   = add_partition_and_lineage(validated, batch_id)

    record_count = enriched.count()
    if record_count == 0:
        logger.info(f"Micro-batch {epoch_id}: no valid PositionReport records")
        return

    # Write partitioned Parquet — one file per micro-batch per partition
    (
        enriched.coalesce(1)
        .write
        .mode("append")
        .partitionBy("partition_date", "partition_hour")
        .parquet(BRONZE_OUTPUT_DIR)
    )
    logger.info(f"Micro-batch {epoch_id}: wrote {record_count} records to {BRONZE_OUTPUT_DIR}")


# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("MarineFlow-Bronze-Positions")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
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
    logger.info(f"Landing dir:  {LANDING_DIR}")
    logger.info(f"Bronze output: {BRONZE_OUTPUT_DIR}")
    logger.info(f"Checkpoint:   {CHECKPOINT_DIR}")

    spark = create_spark_session()

    # Read new JSON files as they arrive in the landing directory.
    # GCS subscription writes raw AIS JSON directly — one message per file.
    ais_stream = (
        spark.readStream
        .format("json")
        .schema(get_landing_schema())
        .option("path", LANDING_DIR)
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .option("latestFirst", "false")
        .load()
    )

    query = (
        ais_stream.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="30 seconds")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Shutdown signal received")
        query.stop()
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()