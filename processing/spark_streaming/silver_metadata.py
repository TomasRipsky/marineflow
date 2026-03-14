# =============================================================================
# MARINEFLOW — Spark Silver Metadata Job
# processing/spark_streaming/silver_metadata.py
#
# Reads ShipStaticData messages from the GCS landing directory
# (written by the vessel-metadata Pub/Sub Cloud Storage subscription)
# and transforms them into the Silver vessel_metadata table.
#
# Envelope format (same as bronze_positions.py):
#   message.data = base64(raw AIS ShipStaticData JSON)
#
# Fields populated here:
#   vessel_type_normalized  — AIS integer → semantic category
#   destination_clean       — normalized free text
#   imo_number, callsign, draught, flag_country
#
# Run:
#   spark-submit \
#     --jars jars/gcs-connector-hadoop3-latest.jar \
#     silver_metadata.py
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

LANDING_DIR       = f"gs://{GCS_BUCKET}/pubsub-landing/vessel-metadata"
METADATA_OUTPUT_DIR = f"gs://{GCS_BUCKET}/silver/vessel_metadata"
CHECKPOINT_DIR    = f"gs://{GCS_BUCKET}/checkpoints/silver_metadata"

MAX_FILES_PER_TRIGGER = int(os.getenv("METADATA_MAX_FILES_PER_TRIGGER", "10"))

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
    """
    Schema of the JSON files written by the Pub/Sub Cloud Storage subscription.
    GCP writes the raw AIS ShipStaticData message directly — no envelope.
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType,
        IntegerType, FloatType, LongType,
    )
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
        StructField("Message", StructType([
            StructField("ShipStaticData", ship_static, True),
        ]), True),
        StructField("MessageType", StringType(), True),
        StructField("MetaData", StructType([
            StructField("MMSI",     LongType(),   True),
            StructField("ShipName", StringType(), True),
        ]), True),
    ])





# =============================================================================
# Transform
# =============================================================================

def decode_and_transform(df):
    """
    Extract ShipStaticData fields and apply Silver transforms.
    GCS subscription writes raw AIS JSON directly — no envelope to decode.
    """
    from pyspark.sql import functions as F

    # Filter only ShipStaticData messages
    df = df.filter(F.col("MessageType") == "ShipStaticData")

    # Build vessel_type expression
    type_expr = F.lit(None).cast("string")
    for code, label in VESSEL_TYPE_MAP.items():
        type_expr = F.when(
            F.col("Message.ShipStaticData.Type") == code, label
        ).otherwise(type_expr)
    type_expr = F.when(
        F.col("Message.ShipStaticData.Type").isNotNull() & type_expr.isNull(),
        F.lit("other")
    ).otherwise(type_expr)

    # Build flag_country from MMSI MID
    mmsi_str = F.col("MetaData.MMSI").cast("string")
    flag_expr = F.lit(None).cast("string")
    for mid, country in MID_MAP.items():
        flag_expr = F.when(mmsi_str.startswith(mid), country).otherwise(flag_expr)

    return df.select(
        F.col("MetaData.MMSI").cast("string").alias("mmsi"),
        F.coalesce(
            F.trim(F.col("MetaData.ShipName")),
            F.trim(F.col("Message.ShipStaticData.Name"))
        ).alias("vessel_name"),
        type_expr.alias("vessel_type_normalized"),
        F.col("Message.ShipStaticData.ImoNumber").cast("string").alias("imo_number"),
        F.trim(F.col("Message.ShipStaticData.Callsign")).alias("callsign"),
        F.when(
            F.upper(F.trim(F.col("Message.ShipStaticData.Destination"))).isin(
                list(DESTINATION_JUNK)
            ),
            F.lit(None).cast("string")
        ).otherwise(
            F.upper(F.trim(F.col("Message.ShipStaticData.Destination")))
        ).alias("destination_clean"),
        F.col("Message.ShipStaticData.MaximumStaticDraught").alias("draught"),
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
    logger.info(f"Processing metadata micro-batch epoch={epoch_id} batch_id={batch_id}")

    transformed = decode_and_transform(df)

    bq_df = transformed.withColumns({
        "_silver_batch_id":   F.lit(batch_id),
        "partition_date":     F.to_date(F.col("processing_timestamp")),
    })

    record_count = bq_df.count()
    if record_count == 0:
        logger.info(f"Micro-batch {epoch_id}: no valid ShipStaticData records")
        return

    (
        bq_df.coalesce(1)
        .write
        .mode("append")
        .partitionBy("partition_date")
        .parquet(METADATA_OUTPUT_DIR)
    )
    logger.info(f"Micro-batch {epoch_id}: wrote {record_count} metadata records to {METADATA_OUTPUT_DIR}")


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

    logger.info("Starting MarineFlow Silver Metadata job")
    logger.info(f"Landing dir: {LANDING_DIR}")
    logger.info(f"Output:      {METADATA_OUTPUT_DIR}")
    logger.info(f"Checkpoint:  {CHECKPOINT_DIR}")

    spark = create_spark_session()

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
        .trigger(processingTime="60 seconds")
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