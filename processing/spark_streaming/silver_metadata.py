# =============================================================================
# MARINEFLOW — Spark Silver Metadata Job
# processing/spark_streaming/silver_metadata.py
#
# Reads ShipStaticData messages directly from the Kafka topic
# "vessel-metadata" using Spark Structured Streaming's native Kafka source,
# and transforms them into the Silver vessel_metadata table.
#
# The producer publishes the raw aisstream.io JSON as-is.
#
# Fields populated here:
#   vessel_type_normalized  — AIS integer → semantic category
#   destination_clean       — normalized free text
#   imo_number, callsign, draught, flag_country
#
# Run:
#   spark-submit \
#     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
#     --jars jars/gcs-connector-hadoop3-latest.jar \
#     silver_metadata.py
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
logger = structlog.get_logger("silver_metadata")

# =============================================================================
# Configuration
# =============================================================================

GCP_PROJECT_ID    = os.getenv("GCP_PROJECT_ID", "")
GCS_BUCKET        = os.getenv("GCS_BUCKET", "")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC_METADATA_BRONZE = os.getenv("KAFKA_TOPIC_METADATA_BRONZE", "vessel-metadata-bronze")
KAFKA_STARTING_OFFSETS  = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

METADATA_OUTPUT_DIR = f"gs://{GCS_BUCKET}/silver/vessel_metadata"
# New checkpoint path — incompatible with the old file-source checkpoint.
CHECKPOINT_DIR    = f"gs://{GCS_BUCKET}/checkpoints/silver_metadata_kafka"

MAX_OFFSETS_PER_TRIGGER = int(os.getenv("METADATA_MAX_OFFSETS_PER_TRIGGER", "1000"))

# =============================================================================
# Reference data
# =============================================================================

# AIS vessel type codes → normalized category (ITU-R M.1371-5, Table 20)
VESSEL_TYPE_MAP = {
    **dict.fromkeys(range(70, 80), "cargo"),
    **dict.fromkeys(range(80, 90), "tanker"),
    **dict.fromkeys(range(60, 70), "passenger"),
    **dict.fromkeys(range(30, 36), "fishing"),
    **dict.fromkeys(range(50, 60), "special_craft"),
    52: "tug",  # AIS type 52 = Tug; must come after the range above to override it
    **dict.fromkeys(range(36, 40), "sailing_or_pleasure"),
    **dict.fromkeys(range(20, 30), "wing_in_ground"),
    **dict.fromkeys(range(90, 100), "other"),
}

DESTINATION_JUNK = {
    "", "NULL", "NONE", "N/A", "NA", "NIL", "UNKNOWN",
    "?", ".", "0", "00", "000", "TBD", "TBA",
}

MID_MAP = {
    "211": "DE", "219": "DK", "224": "ES", "225": "ES",
    "226": "FR", "228": "FR", "232": "GB", "233": "GB",
    "244": "NL", "245": "NL", "247": "IT", "248": "MT",
    "255": "PT", "257": "NO", "265": "SE", "266": "SE",
    "269": "CH", "271": "TR", "273": "RU", "276": "EE",
    "277": "LV", "278": "LT", "303": "US", "338": "US",
    "366": "US", "367": "US", "368": "US", "369": "US",
    "412": "CN", "413": "CN", "414": "CN", "416": "TW",
    "431": "JP", "432": "JP", "440": "KR", "441": "KR",
    "477": "HK", "518": "NZ", "503": "AU", "636": "LR",
    "657": "TZ", "667": "GN", "710": "BR", "720": "AR",
}


# =============================================================================
# Envelope schema
# =============================================================================

def get_landing_schema():
    """Flat schema matching bronze_metadata.py's output (published to Kafka)."""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
    return StructType([
        StructField("MMSI",                 StringType(),    True),
        StructField("ShipName",              StringType(),    True),
        StructField("Type",                  IntegerType(),   True),
        StructField("ImoNumber",             IntegerType(),   True),
        StructField("Callsign",              StringType(),    True),
        StructField("Name",                  StringType(),    True),
        StructField("Destination",           StringType(),    True),
        StructField("MaximumStaticDraught",  FloatType(),     True),
        StructField("ingestion_timestamp",   TimestampType(), True),
    ])





# =============================================================================
# Transform
# =============================================================================

def decode_and_transform(df):
    """Apply Silver transforms to the flat Bronze metadata records."""
    from pyspark.sql import functions as F

    # Build vessel_type expression
    type_expr = F.lit(None).cast("string")
    for code, label in VESSEL_TYPE_MAP.items():
        type_expr = F.when(F.col("Type") == code, label).otherwise(type_expr)
    type_expr = F.when(
        F.col("Type").isNotNull() & type_expr.isNull(), F.lit("other")
    ).otherwise(type_expr)

    # Build flag_country from MMSI MID
    flag_expr = F.lit(None).cast("string")
    for mid, country in MID_MAP.items():
        flag_expr = F.when(F.col("MMSI").startswith(mid), country).otherwise(flag_expr)

    return df.select(
        F.col("MMSI").alias("mmsi"),
        F.coalesce(F.trim(F.col("ShipName")), F.trim(F.col("Name"))).alias("vessel_name"),
        type_expr.alias("vessel_type_normalized"),
        F.col("ImoNumber").cast("string").alias("imo_number"),
        F.trim(F.col("Callsign")).alias("callsign"),
        F.when(
            F.upper(F.trim(F.col("Destination"))).isin(list(DESTINATION_JUNK)),
            F.lit(None).cast("string")
        ).otherwise(F.upper(F.trim(F.col("Destination")))).alias("destination_clean"),
        F.col("MaximumStaticDraught").alias("draught"),
        flag_expr.alias("flag_country"),
        F.lit("aisstream_live").alias("source_system"),
        F.current_timestamp().alias("processing_timestamp"),
    ).filter(F.col("mmsi").isNotNull())


# =============================================================================
# Write micro-batch
# =============================================================================

def process_micro_batch(df, epoch_id: int):
    """Called by Structured Streaming for each micro-batch."""
    from pyspark.sql import functions as F

    batch_id = str(uuid.uuid4())[:8]
    t0 = time.monotonic()
    raw_count = df.count()
    logger.info("▶ micro-batch started", epoch=epoch_id, batch_id=batch_id, rows_read=raw_count)

    transformed = decode_and_transform(df)

    bq_df = transformed.withColumns({
        "_silver_batch_id":   F.lit(batch_id),
        "partition_date":     F.to_date(F.col("processing_timestamp")),
    })

    record_count = bq_df.count()
    if record_count == 0:
        logger.info("⏳ idle — no valid ShipStaticData records", epoch=epoch_id, rows_read=raw_count)
        return

    (
        bq_df.coalesce(1)
        .write
        .mode("append")
        .partitionBy("partition_date")
        .parquet(METADATA_OUTPUT_DIR)
    )
    logger.info(
        "✅ micro-batch complete",
        epoch=epoch_id, batch_id=batch_id,
        rows_read=raw_count, rows_written=record_count,
        duration_ms=int((time.monotonic() - t0) * 1000),
    )


# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("MarineFlow-Silver-Metadata")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.jars.ivy", "/opt/spark/.ivy2")
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
        "🚀 starting MarineFlow Silver Metadata job",
        kafka_topic=KAFKA_TOPIC_METADATA_BRONZE,
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        output=METADATA_OUTPUT_DIR,
        checkpoint=CHECKPOINT_DIR,
        trigger_interval="30 seconds",
    )

    spark = create_spark_session()

    from pyspark.sql import functions as F

    kafka_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_METADATA_BRONZE)
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
        .queryName("silver_metadata")
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
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()