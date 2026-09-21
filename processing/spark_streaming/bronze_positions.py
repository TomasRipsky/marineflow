# =============================================================================
# MARINEFLOW — Spark Bronze Layer Job
# processing/spark_streaming/bronze_positions.py
#
# Reads vessel position messages directly from the Kafka topic
# "vessel-positions" using Spark Structured Streaming's native Kafka source
# (spark-sql-kafka) — push-based, no GCS landing zone, no file-polling.
#
# The producer publishes the raw aisstream.io JSON as-is (see
# ingestion/ais_producer/parser.py), so the message schema below is
# unchanged from the old GCS-landing version — only the *source* changed.
#
# Bronze responsibilities:
#   1. Parse the Kafka message value (JSON) into structured columns
#   2. Extract PositionReport fields with their original names
#   3. Filter physically impossible coordinates
#   4. Add partition columns and lineage fields
#   5. Write to GCS Bronze Parquet
#
# Bronze philosophy:
#   - Zero business transformations — field names match aisstream.io exactly
#   - Minimal validation — only reject physically impossible records
#   - Full lineage — every record knows its Kafka offset, batch and version
#
# Run:
#   spark-submit \
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
#     --jars jars/gcs-connector-hadoop3-latest.jar \
#     bronze_positions.py
# =============================================================================

import os
import sys
import time
import uuid

import structlog
from dotenv import load_dotenv

load_dotenv()

# py4j's own client logger is very chatty at INFO ("Received command c on
# object id p0" for every JVM<->Python call) and drowns out the logs that
# actually matter. Silence it independently of Spark's own log level.
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
logger = structlog.get_logger("bronze_positions")

# =============================================================================
# Configuration
# =============================================================================

GCP_PROJECT_ID    = os.getenv("GCP_PROJECT_ID", "")
GCS_BUCKET        = os.getenv("GCS_BUCKET", "")
PIPELINE_VERSION  = os.getenv("PIPELINE_VERSION", "dev")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC_POSITIONS   = os.getenv("KAFKA_TOPIC_POSITIONS", "vessel-positions")
KAFKA_TOPIC_POSITIONS_BRONZE = os.getenv("KAFKA_TOPIC_POSITIONS_BRONZE", "vessel-positions-bronze")
# Only applies the first time this job runs (no checkpoint yet) — once a
# checkpoint exists, Spark always resumes from the committed offsets and
# ignores this setting. "earliest" is convenient for local dev/validation
# since it replays whatever the producer already wrote to the topic.
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

BRONZE_OUTPUT_DIR = f"gs://{GCS_BUCKET}/bronze/vessel_positions"
# New checkpoint path — the old file-source checkpoint (checkpoints/bronze_positions)
# is NOT compatible with a Kafka source (different offset tracking format) and
# would crash the job if reused. Keep this path, don't point back at the old one.
CHECKPOINT_DIR    = f"gs://{GCS_BUCKET}/checkpoints/bronze_positions_kafka"

MAX_OFFSETS_PER_TRIGGER = int(os.getenv("BRONZE_MAX_OFFSETS_PER_TRIGGER", "1000"))


# =============================================================================
# Pub/Sub envelope schema
# =============================================================================

def get_landing_schema():
    """
    Schema of the raw aisstream.io JSON published to the Kafka topic by the
    producer (ingestion/ais_producer/parser.py) — published as-is, no envelope.
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
# Extract AIS fields
# =============================================================================

def extract_fields(df):
    """
    Flatten the nested AIS JSON structure into top-level Bronze columns.
    Only PositionReport messages are extracted here — the "positions" topic
    should only ever carry PositionReport (see parser.py routing), but the
    filter stays as a defensive check.
    ShipStaticData is handled by silver_metadata.py from its own Kafka topic.
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
    t0 = time.monotonic()

    if df.isEmpty():
        logger.info(
            "⏳ idle — no new messages this cycle, waiting for next trigger",
            epoch=epoch_id,
        )
        return

    raw_count = df.count()
    logger.info(
        "▶ micro-batch started",
        epoch=epoch_id, batch_id=batch_id, rows_read=raw_count,
    )

    extracted = extract_fields(df)
    validated  = validate(extracted)
    enriched   = add_partition_and_lineage(validated, batch_id)

    # Row counts are extra Spark actions (real cost) — worth it here for
    # visibility into what's silently dropped; drop them first if this
    # ever needs to be squeezed for max throughput at higher volume.
    valid_count = enriched.count()
    rejected_count = raw_count - valid_count

    # Write partitioned Parquet — one file per micro-batch per partition
    # Sink 1: GCS Parquet — permanent historical archive
    (
        enriched.coalesce(1)
        .write
        .mode("append")
        .partitionBy("partition_date", "partition_hour")
        .parquet(BRONZE_OUTPUT_DIR)
    )

    # Sink 2: Kafka topic — Silver reads this directly (push-based), no GCS polling
    (
        enriched
        .select(
            F.col("MMSI").cast("string").alias("key"),
            F.to_json(F.struct(*enriched.columns)).alias("value"),
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", KAFKA_TOPIC_POSITIONS_BRONZE)
        .save()
    )

    duration_ms = int((time.monotonic() - t0) * 1000)
    logger.info(
        "✅ micro-batch complete",
        epoch=epoch_id, batch_id=batch_id,
        rows_written=valid_count, rows_rejected=rejected_count,
        duration_ms=duration_ms,
    )


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
        .config("spark.jars", "/opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar")
        .config("spark.jars.ivy", "/opt/spark/.ivy2")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.type", "APPLICATION_DEFAULT")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =============================================================================
# Entry point
# =============================================================================

def validate_config() -> None:
    missing = [v for v in ["GCS_BUCKET"] if not GCS_BUCKET]
    if missing:
        logger.error("✗ missing required environment variables", missing=missing)
        sys.exit(1)


def main() -> None:
    validate_config()

    logger.info(
        "🚀 starting MarineFlow Bronze Positions job",
        kafka_topic=KAFKA_TOPIC_POSITIONS,
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        bronze_output=BRONZE_OUTPUT_DIR,
        checkpoint=CHECKPOINT_DIR,
        trigger_interval="30 seconds",
        max_offsets_per_trigger=MAX_OFFSETS_PER_TRIGGER,
    )

    spark = create_spark_session()

    from pyspark.sql import functions as F

    # Native Kafka source — push-based, no file polling. Kafka gives us
    # key/value as binary plus topic/partition/offset/timestamp metadata;
    # we only need value (the raw AIS JSON), parsed against the same schema
    # the old file-source version used.
    kafka_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_POSITIONS)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")  # tolerate offset gaps in local dev (topic recreated, retention, etc.)
        .load()
    )

    ais_stream = (
        kafka_raw
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(F.from_json(F.col("json_value"), get_landing_schema()).alias("data"))
        .select("data.*")
    )

    query = (
        ais_stream.writeStream
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("👂 listening on Kafka — one cycle every 30s")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("🛑 shutdown signal received, stopping stream gracefully")
        query.stop()
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()