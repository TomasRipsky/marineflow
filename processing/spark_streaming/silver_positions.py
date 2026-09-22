# =============================================================================
# MARINEFLOW — Spark Silver Layer Job
# processing/spark_streaming/silver_positions.py
#
# Reads Bronze messages directly from the Kafka topic "vessel-positions-bronze" and applies
# all business transformations.
#
# Field renaming (Bronze raw names → Silver semantic names):
#   MMSI              → mmsi
#   ShipName          → vessel_name (trimmed)
#   Latitude          → latitude
#   Longitude         → longitude
#   Sog               → speed_over_ground
#   Cog               → course_over_ground
#   TrueHeading       → heading (511 → null)
#   NavigationalStatus (int) → navigational_status (string)
#   time_utc          → event_timestamp
#
# Enrichments derived here for the first time:
#   flag_country         — from MMSI MID prefix
#   ocean_region         — bounding box from lat/lon
#   nearest_port         — proximity to major ports
#   eez_country          — EEZ from port proximity
#   is_in_port_zone      — within port radius
#   distance_to_port_km  — Haversine distance to nearest port
#   speed_change_rate    — delta vs previous message per vessel
#   heading_change_degrees — delta vs previous message per vessel
#
# Run:
#   spark-submit \
#     --jars jars/gcs-connector-hadoop3-latest.jar,jars/spark-bigquery-0.40.0.jar \
#     --driver-class-path jars/gcs-connector-hadoop3-latest.jar:jars/spark-bigquery-0.40.0.jar \
#     silver_positions.py
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
logger = structlog.get_logger("silver_positions")

# =============================================================================
# Configuration
# =============================================================================

GCP_PROJECT_ID    = os.getenv("GCP_PROJECT_ID", "")
GCS_BUCKET        = os.getenv("GCS_BUCKET", "")
BQ_DATASET_SILVER = os.getenv("BQ_DATASET_SILVER", "marineflow_silver")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC_POSITIONS_BRONZE = os.getenv("KAFKA_TOPIC_POSITIONS_BRONZE", "vessel-positions-bronze")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")

SILVER_OUTPUT_DIR = f"gs://{GCS_BUCKET}/silver/vessel_positions"
CHECKPOINT_DIR    = f"gs://{GCS_BUCKET}/checkpoints/silver_positions_kafka"

MAX_OFFSETS_PER_TRIGGER = int(os.getenv("SILVER_MAX_OFFSETS_PER_TRIGGER", "1000"))

# =============================================================================
# Reference data
# =============================================================================

NAV_STATUS_MAP = {
    0:  "under_way_engine",
    1:  "at_anchor",
    2:  "not_under_command",
    3:  "restricted_manoeuvrability",
    4:  "constrained_by_draught",
    5:  "moored",
    6:  "aground",
    7:  "engaged_in_fishing",
    8:  "under_way_sailing",
    15: "undefined",
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

OCEAN_REGIONS = [
    # Format: (lat_min, lat_max, lon_min, lon_max, "region_name")
    # 1. Mediterranean Sea (Narrow bounding box tightly wrapping the sea)
    (30, 46, -6, 42, "mediterranean"),
    # 2. Arctic Ocean (From the polar circle up to the North Pole, full longitude wrap)
    (65, 90, -180, 180, "arctic_ocean"),
    # 3. Southern Ocean / Antarctic (Official IHO boundary south of 60°S)
    (-90, -60, -180, 180, "southern_ocean"),
    # 4. Indian Ocean (From East Africa to Western Australia, bounded north by Asia)
    (-60, 30, 20, 147, "indian_ocean"),
    # 5. Atlantic Ocean (Vertical corridor spanning both North and South Atlantic)
    (-60, 65, -70, 20, "atlantic_ocean"),
    # 6. Eastern Pacific Ocean (From the American coastlines to the Antimeridian)
    (-60, 65, -180, -70, "pacific_east"),
    # 7. Western Pacific Ocean (From Eastern Australia/Asia to the Antimeridian)
    (-60, 65, 147, 180, "pacific_west"),
]

MAJOR_PORTS = [
    (51.9,    4.1,   0.5, "Rotterdam",   "NL"),
    (53.5,    9.9,   0.5, "Hamburg",     "DE"),
    (31.2,  121.5,   0.5, "Shanghai",    "CN"),
    (22.3,  114.2,   0.3, "Hong Kong",   "HK"),
    ( 1.26, 103.8,   0.5, "Singapore",   "SG"),
    (35.4,  139.7,   0.5, "Tokyo",       "JP"),
    (33.7, -118.2,   0.5, "Los Angeles", "US"),
    (40.7,  -74.0,   0.5, "New York",    "US"),
    (-23.9, -46.3,   0.5, "Santos",      "BR"),
    (18.9,   72.8,   0.5, "Mumbai",      "IN"),
    (25.2,   55.3,   0.3, "Dubai",       "AE"),
    (30.1,   32.3,   0.3, "Port Said",   "EG"),
    (37.9,   23.7,   0.3, "Piraeus",     "GR"),
    (41.3,    2.1,   0.3, "Barcelona",   "ES"),
    (43.3,    5.4,   0.3, "Marseille",   "FR"),
]


# =============================================================================
# Bronze schema — must match exactly what bronze_positions.py writes
# =============================================================================

def get_bronze_schema():
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, DoubleType, IntegerType, BooleanType, TimestampType, DateType,
    )
    return StructType([
        StructField("Cog",                      DoubleType(),    True),
        StructField("CommunicationState",        IntegerType(),   True),
        StructField("Latitude",                  DoubleType(),    True),
        StructField("Longitude",                 DoubleType(),    True),
        StructField("MessageID",                 IntegerType(),   True),
        StructField("NavigationalStatus",        IntegerType(),   True),
        StructField("PositionAccuracy",          BooleanType(),   True),
        StructField("Raim",                      BooleanType(),   True),
        StructField("RateOfTurn",                IntegerType(),   True),
        StructField("RepeatIndicator",           IntegerType(),   True),
        StructField("Sog",                       DoubleType(),    True),
        StructField("Spare",                     IntegerType(),   True),
        StructField("SpecialManoeuvreIndicator", IntegerType(),   True),
        StructField("Timestamp",                 IntegerType(),   True),
        StructField("TrueHeading",               IntegerType(),   True),
        StructField("UserID",                    IntegerType(),   True),
        StructField("Valid",                     BooleanType(),   True),
        StructField("MMSI",                      StringType(),    True),
        StructField("MMSI_String",               StringType(),    True),
        StructField("ShipName",                  StringType(),    True),
        StructField("time_utc",                  TimestampType(), True),
        StructField("ingestion_timestamp",       TimestampType(), True),
        StructField("_pubsub_message_id",        StringType(),    True),
        StructField("_pubsub_publish_time",      StringType(),    True),
        StructField("_source_system",            StringType(),    True),
        StructField("_batch_id",                 StringType(),    True),
        StructField("_pipeline_version",         StringType(),    True),
        StructField("_ingestion_date",           DateType(),      True),
        StructField("_source_file",              StringType(),    True),
    ])


# =============================================================================
# Transformations
# =============================================================================

def rename_bronze_fields(df):
    """Rename raw Bronze field names to Silver semantic names."""
    from pyspark.sql import functions as F

    nav_expr = F.lit(None).cast("string")
    for code, label in NAV_STATUS_MAP.items():
        nav_expr = F.when(F.col("NavigationalStatus") == code, label).otherwise(nav_expr)
    nav_expr = F.when(
        F.col("NavigationalStatus").isNotNull() & nav_expr.isNull(),
        F.concat(F.lit("unknown_"), F.col("NavigationalStatus").cast("string"))
    ).otherwise(nav_expr)

    flag_expr = F.lit(None).cast("string")
    for mid, country in MID_MAP.items():
        flag_expr = F.when(F.col("MMSI").startswith(mid), country).otherwise(flag_expr)

    return df.withColumns({
        "mmsi":                F.col("MMSI"),
        "vessel_name":         F.trim(F.col("ShipName")),
        "latitude":            F.col("Latitude"),
        "longitude":           F.col("Longitude"),
        "speed_over_ground":   F.col("Sog"),
        "course_over_ground":  F.col("Cog"),
        "heading":             F.when(F.col("TrueHeading") != 511,
                                      F.col("TrueHeading")).otherwise(None),
        "navigational_status": nav_expr,
        "flag_country":        flag_expr,
        "event_timestamp":     F.col("time_utc"),
    })


def deduplicate(df):
    """Remove duplicate (mmsi, event_timestamp) — keep most recently ingested."""
    from pyspark.sql import functions as F, Window

    window = Window.partitionBy("mmsi", "event_timestamp").orderBy(
        F.col("ingestion_timestamp").desc()
    )
    return (
        df.withColumn("_row_num", F.row_number().over(window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )


def enrich_geospatial(df):
    """Add ocean region, port proximity and Haversine distance."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    ocean_expr = F.lit("open_ocean")
    for min_lat, max_lat, min_lon, max_lon, region in OCEAN_REGIONS:
        ocean_expr = F.when(
            F.col("latitude").between(min_lat, max_lat)
            & F.col("longitude").between(min_lon, max_lon),
            region
        ).otherwise(ocean_expr)
    df = df.withColumn("ocean_region", ocean_expr)

    port_expr     = F.lit(None).cast(StringType())
    country_expr  = F.lit(None).cast(StringType())
    in_port_expr  = F.lit(False)
    port_lat_expr = F.lit(None).cast("double")
    port_lon_expr = F.lit(None).cast("double")

    for p_lat, p_lon, radius, port_name, country in MAJOR_PORTS:
        near = (
            (F.abs(F.col("latitude")  - p_lat) <= radius)
            & (F.abs(F.col("longitude") - p_lon) <= radius)
        )
        port_expr     = F.when(near, port_name).otherwise(port_expr)
        country_expr  = F.when(near, country).otherwise(country_expr)
        in_port_expr  = in_port_expr | near
        port_lat_expr = F.when(near, F.lit(p_lat)).otherwise(port_lat_expr)
        port_lon_expr = F.when(near, F.lit(p_lon)).otherwise(port_lon_expr)

    df = df.withColumns({
        "nearest_port":    port_expr,
        "eez_country":     country_expr,
        "is_in_port_zone": in_port_expr,
        "_port_lat":       port_lat_expr,
        "_port_lon":       port_lon_expr,
    })

    return df.withColumn(
        "distance_to_port_km",
        F.when(
            F.col("_port_lat").isNotNull(),
            F.sqrt(
                F.pow((F.col("latitude")  - F.col("_port_lat")) * 111.0, 2) +
                F.pow((F.col("longitude") - F.col("_port_lon")) * 111.0, 2)
            )
        ).otherwise(None)
    ).drop("_port_lat", "_port_lon")


def calculate_movement_deltas(df):
    """Speed and heading change vs previous message per vessel."""
    from pyspark.sql import functions as F, Window
    from pyspark.sql.types import DoubleType

    window = Window.partitionBy("mmsi").orderBy("event_timestamp")

    df = df.withColumns({
        "prev_speed":   F.lag("speed_over_ground", 1).over(window),
        "prev_heading": F.lag("heading", 1).over(window),
    })

    return df.withColumns({
        "speed_change_rate":      F.abs(
            F.col("speed_over_ground") - F.col("prev_speed")
        ).cast(DoubleType()),
        "heading_change_degrees": F.abs(
            F.col("heading").cast(DoubleType()) - F.col("prev_heading").cast(DoubleType())
        ),
    }).drop("prev_speed", "prev_heading")


def transform_to_silver(df):
    """Apply all Silver transformations and select final schema."""
    from pyspark.sql import functions as F

    df = rename_bronze_fields(df)
    df = deduplicate(df)
    df = enrich_geospatial(df)
    df = calculate_movement_deltas(df)
    df = df.withColumn("processing_timestamp", F.current_timestamp())

    cols = df.columns

    return df.select(
        # Identity
        "mmsi",
        "vessel_name",
        "flag_country",
        # Position
        "latitude",
        "longitude",
        "ocean_region",
        "nearest_port",
        "eez_country",
        "is_in_port_zone",
        "distance_to_port_km",
        # Movement
        "speed_over_ground",
        "course_over_ground",
        "heading",
        "navigational_status",
        "speed_change_rate",
        "heading_change_degrees",
        # Timestamps
        "event_timestamp",
        "processing_timestamp",
        # Lineage propagated from Bronze
        F.col("_source_system")    if "_source_system"    in cols else F.lit(None).cast("string").alias("_source_system"),
        F.col("_source_file")      if "_source_file"      in cols else F.lit(None).cast("string").alias("_source_file"),
        F.col("_batch_id").alias("_bronze_batch_id") if "_batch_id" in cols else F.lit(None).cast("string").alias("_bronze_batch_id"),
        F.col("_pipeline_version") if "_pipeline_version" in cols else F.lit(None).cast("string").alias("_pipeline_version"),
    )


# =============================================================================
# Write micro-batch
# =============================================================================

def process_micro_batch(df, epoch_id: int):
    """
    Called by Structured Streaming for each micro-batch.
    Spark guarantees each Bronze file is processed exactly once via checkpointing.
    No manual hour filtering needed — the checkpoint handles it.
    """
    from pyspark.sql import functions as F

    silver_batch_id = str(uuid.uuid4())[:8]
    t0 = time.monotonic()
    raw_count = df.count()
    logger.info("▶ micro-batch started", epoch=epoch_id, batch_id=silver_batch_id, rows_read=raw_count)

    silver_df = transform_to_silver(df)
    silver_df = silver_df.withColumns({
        "_silver_batch_id": F.lit(silver_batch_id),
        "partition_date":   F.to_date(F.col("event_timestamp")),
    })

    record_count = silver_df.count()
    if record_count == 0:
        logger.info("⏳ idle — no records after transformation", epoch=epoch_id, rows_read=raw_count)
        return

    (
        silver_df.coalesce(2)
        .write
        .mode("append")
        .partitionBy("partition_date")
        .parquet(SILVER_OUTPUT_DIR)
    )
    logger.info(
        "✅ micro-batch complete",
        epoch=epoch_id, batch_id=silver_batch_id,
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
        .appName("MarineFlow-Silver-Positions")
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
    missing = [v for v in ["GCP_PROJECT_ID", "GCS_BUCKET"] if not os.getenv(v)]
    if missing:
        logger.error("✗ missing required environment variables", missing=missing)
        sys.exit(1)


def main() -> None:
    validate_config()

    logger.info(
        "🚀 starting MarineFlow Silver Positions job",
        kafka_topic=KAFKA_TOPIC_POSITIONS_BRONZE,
        silver_output=SILVER_OUTPUT_DIR,
        checkpoint=CHECKPOINT_DIR,
        trigger_interval="30 seconds",
    )

    spark = create_spark_session()

    from pyspark.sql import functions as F

    kafka_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_POSITIONS_BRONZE)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze_stream = (
        kafka_raw
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(F.from_json(F.col("json_value"), get_bronze_schema()).alias("data"))
        .select("data.*")
    )

    query = (
        bronze_stream.writeStream
        .queryName("silver_positions")
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