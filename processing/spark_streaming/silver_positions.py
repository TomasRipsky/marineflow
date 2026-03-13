# =============================================================================
# MARINEFLOW — Spark Silver Layer Job
# processing/spark_streaming/silver_positions.py
#
# Reads Bronze Parquet from GCS and applies all business transformations:
#
# Field renaming (Bronze raw names → Silver semantic names):
#   MMSI         → mmsi
#   ShipName     → vessel_name (trimmed)
#   Latitude     → latitude
#   Longitude    → longitude
#   Sog          → speed_over_ground
#   Cog          → course_over_ground
#   TrueHeading  → heading (511 filtered as null)
#   NavigationalStatus (int) → navigational_status (string)
#   time_utc     → event_timestamp
#
# Enrichments (not in Bronze — derived here for the first time):
#   flag_country         — derived from MMSI MID prefix
#   vessel_type_normalized — from ShipStaticData join (placeholder: unknown)
#   ocean_region         — bounding box from lat/lon
#   nearest_port         — proximity to major ports
#   eez_country          — EEZ from port proximity
#   is_in_port_zone      — within port radius
#   distance_to_port_km  — Haversine distance to nearest port
#   destination_clean    — normalized free text
#   speed_change_rate    — delta vs previous message
#   heading_change_degrees — delta vs previous message
#
# Run:
#   spark-submit \
#     --jars jars/gcs-connector-hadoop3-latest.jar,jars/spark-bigquery-0.40.0.jar \
#     --driver-class-path jars/gcs-connector-hadoop3-latest.jar:jars/spark-bigquery-0.40.0.jar \
#     silver_positions.py
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

BRONZE_INPUT_DIR  = f"gs://{GCS_BUCKET}/bronze/vessel_positions"
SILVER_OUTPUT_DIR = f"gs://{GCS_BUCKET}/silver/vessel_positions"
BQ_TABLE          = f"{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.vessel_positions_clean"

BATCH_INTERVAL = int(os.getenv("SILVER_BATCH_INTERVAL", "60"))
MAX_BATCHES    = int(os.getenv("SILVER_MAX_BATCHES", "0"))

# =============================================================================
# Reference data
# =============================================================================

# AIS navigational status — ITU-R M.1371-5 Table 20
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

# Flag country from MMSI MID prefix
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

# Ocean regions — (min_lat, max_lat, min_lon, max_lon, name)
OCEAN_REGIONS = [
    (20,   45,  -10,  42,  "mediterranean"),
    (35,   90,  -30,  60,  "north_sea_arctic"),
    (-90, -30, -180, 180,  "southern_ocean"),
    (-90,  90,   20,  80,  "indian_ocean"),
    (-90,  90, -180, -30,  "atlantic_west"),
    (-90,  90,  -30,  20,  "atlantic_east"),
    (-90,  90,   80, 180,  "pacific"),
]

# Major ports — (lat, lon, radius_deg, name, country)
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
# Step 1 — Rename Bronze fields to Silver semantic names
# =============================================================================

def rename_bronze_fields(df):
    """
    Rename raw Bronze field names to Silver semantic names.
    This is the first transformation — nothing else should reference Bronze names.
    """
    from pyspark.sql import functions as F

    # Build nav status mapping expression
    nav_expr = F.lit(None).cast("string")
    for code, label in NAV_STATUS_MAP.items():
        nav_expr = F.when(F.col("NavigationalStatus") == code, label).otherwise(nav_expr)
    # Unknown codes get a string representation
    nav_expr = F.when(
        F.col("NavigationalStatus").isNotNull() & nav_expr.isNull(),
        F.concat(F.lit("unknown_"), F.col("NavigationalStatus").cast("string"))
    ).otherwise(nav_expr)

    # Flag country from MMSI MID prefix
    flag_expr = F.lit(None).cast("string")
    for mid, country in MID_MAP.items():
        flag_expr = F.when(
            F.col("MMSI").startswith(mid), country
        ).otherwise(flag_expr)

    return df.withColumns({
        "mmsi":                F.col("MMSI"),
        "vessel_name":         F.trim(F.col("ShipName")),
        "latitude":            F.col("Latitude"),
        "longitude":           F.col("Longitude"),
        "speed_over_ground":   F.col("Sog"),
        "course_over_ground":  F.col("Cog"),
        # TrueHeading 511 = unavailable per AIS spec — set to null
        "heading":             F.when(F.col("TrueHeading") != 511,
                                      F.col("TrueHeading")).otherwise(None),
        "navigational_status": nav_expr,
        "flag_country":        flag_expr,
        "event_timestamp":     F.col("time_utc"),
    })


# =============================================================================
# Step 2 — Deduplication
# =============================================================================

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


# =============================================================================
# Step 3 — Geospatial enrichment
# =============================================================================

def enrich_geospatial(df):
    """Add ocean region, port proximity and distance."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType

    # Ocean region
    ocean_expr = F.lit("open_ocean")
    for min_lat, max_lat, min_lon, max_lon, region in OCEAN_REGIONS:
        ocean_expr = F.when(
            F.col("latitude").between(min_lat, max_lat)
            & F.col("longitude").between(min_lon, max_lon),
            region
        ).otherwise(ocean_expr)
    df = df.withColumn("ocean_region", ocean_expr)

    # Port proximity
    port_expr    = F.lit(None).cast(StringType())
    country_expr = F.lit(None).cast(StringType())
    in_port_expr = F.lit(False)
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

    # Haversine approximation — 1 degree ≈ 111 km
    df = df.withColumn(
        "distance_to_port_km",
        F.when(
            F.col("_port_lat").isNotNull(),
            F.sqrt(
                F.pow((F.col("latitude")  - F.col("_port_lat")) * 111.0, 2) +
                F.pow((F.col("longitude") - F.col("_port_lon")) * 111.0, 2)
            )
        ).otherwise(None)
    ).drop("_port_lat", "_port_lon")

    return df


# =============================================================================
# Step 4 — Movement deltas
# =============================================================================

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


# =============================================================================
# Step 5 — Destination normalization
# =============================================================================

def normalize_destination(df):
    """Normalize destination free text — uppercase and trim."""
    from pyspark.sql import functions as F

    # destination comes from ShipStaticData — will be null in PositionReport records
    # We keep the column for when we join with the metadata topic in a future phase
    if "destination" in df.columns:
        return df.withColumn("destination_clean", F.upper(F.trim(F.col("destination"))))
    return df.withColumn("destination_clean", F.lit(None).cast("string"))


# =============================================================================
# Master transform
# =============================================================================

def transform_to_silver(df):
    """Apply all Silver transformations in sequence."""
    from pyspark.sql import functions as F

    df = rename_bronze_fields(df)
    df = deduplicate(df)
    df = enrich_geospatial(df)
    df = calculate_movement_deltas(df)
    df = normalize_destination(df)
    df = df.withColumn("processing_timestamp", F.current_timestamp())

    cols = df.columns

    return df.select(
        # Semantic fields
        "mmsi",
        "vessel_name",
        F.lit(None).cast("string").alias("vessel_type_normalized"),  # from metadata join — Phase 3
        "latitude",
        "longitude",
        "speed_over_ground",
        "course_over_ground",
        "heading",
        "navigational_status",
        "destination_clean",
        "flag_country",
        "ocean_region",
        "eez_country",
        "is_in_port_zone",
        "nearest_port",
        "distance_to_port_km",
        "speed_change_rate",
        "heading_change_degrees",
        "event_timestamp",
        "processing_timestamp",
        # Lineage propagated from Bronze
        F.col("_source_system")    if "_source_system"    in cols else F.lit(None).cast("string").alias("_source_system"),
        F.col("_source_file")      if "_source_file"      in cols else F.lit(None).cast("string").alias("_source_file"),
        F.col("_batch_id").alias("_bronze_batch_id") if "_batch_id" in cols else F.lit(None).cast("string").alias("_bronze_batch_id"),
        F.col("_pipeline_version") if "_pipeline_version" in cols else F.lit(None).cast("string").alias("_pipeline_version"),
    )


# =============================================================================
# Read Bronze
# =============================================================================

def read_new_bronze_files(spark, last_processed_date: str = None):
    from pyspark.sql import functions as F

    df = spark.read.parquet(BRONZE_INPUT_DIR)

    if os.getenv("SILVER_DEBUG"):
        logger.info("DEBUG mode: limited to 10 rows")
        return df.limit(10)

    if last_processed_date:
        df = df.filter(F.col("partition_date") >= last_processed_date)

    return df


# =============================================================================
# Write Silver
# =============================================================================

def write_silver_gcs(df, batch_number: int) -> int:
    from pyspark.sql import functions as F

    enriched = df.withColumn("partition_date", F.to_date(F.col("event_timestamp")))
    record_count = enriched.count()

    if record_count == 0:
        logger.info(f"Batch {batch_number}: no records to write to GCS")
        return 0

    enriched.write.mode("append").partitionBy("partition_date").parquet(SILVER_OUTPUT_DIR)
    logger.info(f"Batch {batch_number}: wrote {record_count} records to {SILVER_OUTPUT_DIR}")
    return record_count


def write_silver_bigquery(df, batch_number: int) -> None:
    import uuid
    from pyspark.sql import functions as F

    silver_batch_id = str(uuid.uuid4())[:8]
    bq_df = df.withColumn("_silver_batch_id", F.lit(silver_batch_id))

    try:
        (
            bq_df.write
            .format("bigquery")
            .option("table", BQ_TABLE)
            .option("temporaryGcsBucket", GCS_BUCKET)
            .option("writeMethod", "indirect")
            .option("createDisposition", "CREATE_NEVER")
            .option("writeDisposition", "WRITE_APPEND")
            .option("allowFieldRelaxation", "true")
            .mode("append")
            .save()
        )
        logger.info(f"Batch {batch_number} (silver_batch={silver_batch_id}): wrote to {BQ_TABLE}")
    except Exception as e:
        logger.error(f"BigQuery write failed (non-fatal): {e}")


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

    logger.info("Starting MarineFlow Silver Positions job")
    logger.info(f"Input:    {BRONZE_INPUT_DIR}")
    logger.info(f"Output:   {SILVER_OUTPUT_DIR}")
    logger.info(f"BigQuery: {BQ_TABLE}")

    spark = create_spark_session()
    total_records = 0
    batch_number  = 0

    try:
        while True:
            batch_number += 1
            logger.info(f"Starting Silver batch {batch_number}")

            try:
                bronze_df = read_new_bronze_files(spark)
                silver_df = transform_to_silver(bronze_df)
                records   = write_silver_gcs(silver_df, batch_number)
                total_records += records

                if records > 0:
                    write_silver_bigquery(silver_df, batch_number)

            except Exception as e:
                logger.error(f"Batch {batch_number} failed: {e}", exc_info=True)

            logger.info(f"Total Silver records written: {total_records}")

            if MAX_BATCHES > 0 and batch_number >= MAX_BATCHES:
                logger.info(f"Reached max batches ({MAX_BATCHES}), stopping")
                break

            logger.info(f"Waiting {BATCH_INTERVAL}s until next batch...")
            time.sleep(BATCH_INTERVAL)

    except KeyboardInterrupt:
        logger.info(f"Shutdown signal: total records written: {total_records}")
    except Exception as e:
        logger.error(f"Silver job failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()