# =============================================================================
# MARINEFLOW — Spark Bronze Metadata Job
# processing/spark_streaming/bronze_metadata.py
#
# Reads ShipStaticData from Kafka topic "vessel-metadata", flattens to raw
# field names (zero business transformation, same philosophy as
# bronze_positions.py), and writes to two sinks:
#   1. GCS Parquet (bronze/vessel_metadata) — permanent raw archive
#   2. Kafka topic "vessel-metadata-bronze" — Silver reads this directly
#
# Normally started with scripts/jobs.sh (see scripts/README.md), which also restarts it
# after a kill and passes every flag below. Delivery is at-least-once: a job killed
# between a batch's write and its checkpoint replays that batch
# (docs/plans/at-least-once-duplicates.md).
#
# Run by hand:
#   spark-submit \
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
#     --jars jars/gcs-connector-hadoop3-latest.jar \
#     bronze_metadata.py
# =============================================================================

import os
import sys
import time
import uuid

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
logger = structlog.get_logger("bronze_metadata")

# =============================================================================
# Configuration
# =============================================================================

GCS_BUCKET       = os.getenv("GCS_BUCKET", "")
PIPELINE_VERSION = os.getenv("PIPELINE_VERSION", "dev")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC_METADATA        = os.getenv("KAFKA_TOPIC_METADATA", "vessel-metadata")
KAFKA_TOPIC_METADATA_BRONZE = os.getenv("KAFKA_TOPIC_METADATA_BRONZE", "vessel-metadata-bronze")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

BRONZE_OUTPUT_DIR = f"gs://{GCS_BUCKET}/bronze/vessel_metadata"
CHECKPOINT_DIR    = f"gs://{GCS_BUCKET}/checkpoints/bronze_metadata"

MAX_OFFSETS_PER_TRIGGER = int(os.getenv("BRONZE_METADATA_MAX_OFFSETS_PER_TRIGGER", "4000"))


def get_landing_schema():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
    ship_static = StructType([
        StructField("UserID",               IntegerType(), True),
        StructField("Type",                 IntegerType(), True),
        StructField("ImoNumber",            IntegerType(), True),
        StructField("Callsign",             StringType(),  True),
        StructField("Name",                 StringType(),  True),
        StructField("Destination",          StringType(),  True),
        StructField("MaximumStaticDraught", FloatType(),   True),
    ])
    return StructType([
        StructField("Message", StructType([StructField("ShipStaticData", ship_static, True)]), True),
        StructField("MessageType", StringType(), True),
        StructField("MetaData", StructType([
            StructField("MMSI",     LongType(),   True),
            StructField("ShipName", StringType(), True),
        ]), True),
    ])


def extract_fields(df):
    """Flatten to raw field names — no semantic transformation (Bronze philosophy)."""
    from pyspark.sql import functions as F
    df = df.filter(F.col("MessageType") == "ShipStaticData")
    return df.select(
        F.col("MetaData.MMSI").cast("string").alias("MMSI"),
        F.col("MetaData.ShipName").alias("ShipName"),
        F.col("Message.ShipStaticData.Type").alias("Type"),
        F.col("Message.ShipStaticData.ImoNumber").alias("ImoNumber"),
        F.col("Message.ShipStaticData.Callsign").alias("Callsign"),
        F.col("Message.ShipStaticData.Name").alias("Name"),
        F.col("Message.ShipStaticData.Destination").alias("Destination"),
        F.col("Message.ShipStaticData.MaximumStaticDraught").alias("MaximumStaticDraught"),
    ).filter(F.col("MMSI").isNotNull())


def add_lineage(df, batch_id: str):
    from pyspark.sql import functions as F
    return df.withColumns({
        "ingestion_timestamp": F.current_timestamp(),
        "_source_system":      F.lit("aisstream_live"),
        "_batch_id":           F.lit(batch_id),
        "_pipeline_version":   F.lit(PIPELINE_VERSION),
        "partition_date":      F.current_date(),
    })


def process_micro_batch(df, epoch_id: int):
    from pyspark.sql import functions as F

    batch_id = str(uuid.uuid4())[:8]
    t0 = time.monotonic()

    if df.isEmpty():
        logger.info("⏳ idle — no new messages this cycle, waiting for next trigger", epoch=epoch_id)
        return

    raw_count = df.count()
    logger.info("▶ micro-batch started", epoch=epoch_id, batch_id=batch_id, rows_read=raw_count)
    enriched = add_lineage(extract_fields(df), batch_id)
    valid_count = enriched.count()

    # Sink 1: GCS Parquet — permanent raw archive
    (
        enriched.coalesce(1)
        .write.mode("append")
        .partitionBy("partition_date")
        .parquet(BRONZE_OUTPUT_DIR)
    )

    # Sink 2: Kafka — Silver Metadata reads this directly
    (
        enriched
        .select(
            F.col("MMSI").alias("key"),
            F.to_json(F.struct(*enriched.columns)).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", KAFKA_TOPIC_METADATA_BRONZE)
        .save()
    )

    logger.info(
        "✅ micro-batch complete",
        epoch=epoch_id, batch_id=batch_id,
        rows_read=raw_count, rows_written=valid_count,
        duration_ms=int((time.monotonic() - t0) * 1000),
    )


def create_spark_session():
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("MarineFlow-Bronze-Metadata")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.jars", "/opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar")
        .config("spark.jars.ivy", "/opt/spark/.ivy2")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def validate_config() -> None:
    if not GCS_BUCKET:
        logger.error("✗ missing required environment variable", missing=["GCS_BUCKET"])
        sys.exit(1)


def main() -> None:
    validate_config()
    logger.info(
        "🚀 starting MarineFlow Bronze Metadata job",
        kafka_topic=KAFKA_TOPIC_METADATA,
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        bronze_output=BRONZE_OUTPUT_DIR,
        bronze_topic=KAFKA_TOPIC_METADATA_BRONZE,
        checkpoint=CHECKPOINT_DIR,
        trigger_interval="30 seconds",
    )

    spark = create_spark_session()
    from pyspark.sql import functions as F

    kafka_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_METADATA)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
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
        .queryName("bronze_metadata")
        .foreachBatch(process_micro_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
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
