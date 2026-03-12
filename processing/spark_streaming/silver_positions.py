# =============================================================================
# MARINEFLOW — Spark Silver Layer Job
# processing/spark_streaming/silver_positions.py
#
# Reads Bronze Parquet files from GCS, applies business transformations
# and writes clean data to GCS Silver layer + BigQuery.
#
# Silver transformations:
# - Deduplication by (mmsi, event_timestamp)
# - Vessel type normalization (AIS codes → human readable)
# - Geospatial enrichment (ocean region, port proximity)
# - Movement deltas (speed change rate, heading change)
# - Destination normalization (free text cleanup)
#
# Run:
#   spark-submit \
#     --jars jars/gcs-connector-hadoop3-latest.jar,jars/spark-3.5-bigquery-0.36.1.jar \
#     silver_positions.py
# =============================================================================

import logging
import os
import sys
import time
from datetime import datetime, timezone

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
BQ_DATASET_SILVER = os.getenv("BQ_DATASET_SILVER", "marineflow_silver")

BRONZE_INPUT_DIR = f"gs://{GCS_BUCKET}/bronze/vessel_positions"
SILVER_OUTPUT_DIR = f"gs://{GCS_BUCKET}/silver/vessel_positions"
BQ_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.vessel_positions_clean"

BATCH_INTERVAL = int(os.getenv("SILVER_BATCH_INTERVAL", "60"))
MAX_BATCHES = int(os.getenv("SILVER_MAX_BATCHES", "0"))

# =============================================================================
# Reference data — geospatial enrichment
# =============================================================================

# (min_lat, max_lat, min_lon, max_lon, region_name)
OCEAN_REGIONS = [
    (20,  45,  -10,  42,  "mediterranean"),
    (35,  90,  -30,  60,  "north_sea_arctic"),
    (-30, -90, -180, 180, "southern_ocean"),
    (-90,  90,  20,   80, "indian_ocean"),
    (-90,  90, -180, -30, "atlantic_west"),
    (-90,  90,  -30,  20, "atlantic_east"),
    (-90,  90,   80, 180, "pacific"),
]

# (lat, lon, radius_deg, port_name, country)
MAJOR_PORTS = [
    (51.9,    4.1,   0.5, "Rotterdam",     "NL"),
    (53.5,    9.9,   0.5, "Hamburg",       "DE"),
    (31.2,  121.5,   0.5, "Shanghai",      "CN"),
    (22.3,  114.2,   0.3, "Hong Kong",     "HK"),
    (1.26,  103.8,   0.5, "Singapore",     "SG"),
    (35.4,  139.7,   0.5, "Tokyo",         "JP"),
    (33.7, -118.2,   0.5, "Los Angeles",   "US"),
    (40.7,  -74.0,   0.5, "New York",      "US"),
    (-23.9, -46.3,   0.5, "Santos",        "BR"),
    (18.9,   72.8,   0.5, "Mumbai",        "IN"),
    (25.2,   55.3,   0.3, "Dubai",         "AE"),
    (30.1,   32.3,   0.3, "Port Said",     "EG"),
    (37.9,   23.7,   0.3, "Piraeus",       "GR"),
    (41.3,    2.1,   0.3, "Barcelona",     "ES"),
    (43.3,    5.4,   0.3, "Marseille",     "FR"),
]


# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("MarineFlow-Silver-Positions")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g"))
        # GCS connector
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
        # BigQuery connector
        .config("parentProject", GCP_PROJECT_ID)
        .config("temporaryGcsBucket", GCS_BUCKET)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# =============================================================================
# Transformations
# =============================================================================

def normalize_vessel_type(df):
    """
    Convert raw AIS vessel type codes to human-readable categories.
    AIS type codes are integers encoded as strings in our schema.
    """
    from pyspark.sql import functions as F

    return df.withColumn(
        "vessel_type_normalized",
        F.when(F.col("vessel_type").between("70", "79"), "cargo")
         .when(F.col("vessel_type").between("80", "89"), "tanker")
         .when(F.col("vessel_type").between("60", "69"), "passenger")
         .when(F.col("vessel_type").between("30", "35"), "fishing")
         .when(F.col("vessel_type").isin("52", "53"),    "tug")
         .when(F.col("vessel_type").between("50", "59"), "special_craft")
         .otherwise("other")
    )


def enrich_geospatial(df):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, DoubleType

    # Ocean region
    ocean_expr = F.lit("open_ocean")
    for min_lat, max_lat, min_lon, max_lon, region in OCEAN_REGIONS:
        ocean_expr = F.when(
            F.col("latitude").between(min_lat, max_lat)
            & F.col("longitude").between(min_lon, max_lon),
            F.lit(region),
        ).otherwise(ocean_expr)
    df = df.withColumn("ocean_region", ocean_expr)

    # Port proximity with coordinates for distance calculation
    port_expr    = F.lit(None).cast(StringType())
    country_expr = F.lit(None).cast(StringType())
    port_lat_expr = F.lit(None).cast(DoubleType())
    port_lon_expr = F.lit(None).cast(DoubleType())
    in_port_expr = F.lit(False)

    for p_lat, p_lon, radius, port_name, country in MAJOR_PORTS:
        near = (
            (F.abs(F.col("latitude") - p_lat) <= radius)
            & (F.abs(F.col("longitude") - p_lon) <= radius)
        )
        port_expr     = F.when(near, F.lit(port_name)).otherwise(port_expr)
        country_expr  = F.when(near, F.lit(country)).otherwise(country_expr)
        port_lat_expr = F.when(near, F.lit(float(p_lat))).otherwise(port_lat_expr)
        port_lon_expr = F.when(near, F.lit(float(p_lon))).otherwise(port_lon_expr)
        in_port_expr  = in_port_expr | near

    df = df.withColumns({
        "nearest_port":    port_expr,
        "eez_country":     country_expr,
        "is_in_port_zone": in_port_expr,
        "_port_lat":       port_lat_expr,
        "_port_lon":       port_lon_expr,
    })

    # Haversine approximation: distance in km
    df = df.withColumn(
        "distance_to_port_km",
        F.when(
            F.col("_port_lat").isNotNull(),
            F.sqrt(
                F.pow((F.col("latitude")  - F.col("_port_lat")) * F.lit(111.0), 2) +
                F.pow((F.col("longitude") - F.col("_port_lon")) * F.lit(111.0), 2)
            )
        ).otherwise(F.lit(None).cast(DoubleType()))
    ).drop("_port_lat", "_port_lon")

    return df


def calculate_movement_deltas(df):
    from pyspark.sql import functions as F, Window
    from pyspark.sql.types import DoubleType

    window = Window.partitionBy("mmsi").orderBy("event_timestamp")

    df = df.withColumns({
        "prev_speed":   F.lag("speed_over_ground", 1).over(window),
        "prev_heading": F.lag("heading", 1).over(window),
    })

    df = df.withColumns({
        "speed_change_rate": F.abs(
            F.col("speed_over_ground") - F.col("prev_speed")
        ).cast(DoubleType()),
        "heading_change_degrees": F.abs(
            F.col("heading") - F.col("prev_heading")
        ).cast(DoubleType()),
    }).drop("prev_speed", "prev_heading")

    return df


def normalize_destination(df):
    """
    Standardize the destination field.
    AIS destinations are free text entered by the captain — very inconsistent.
    """
    from pyspark.sql import functions as F

    return df.withColumn(
        "destination_clean",
        F.upper(F.trim(F.col("destination")))
    )


def deduplicate(df):
    """
    Remove duplicate records — same vessel at the same timestamp.
    Keeps the most recently ingested record per (mmsi, event_timestamp).
    """
    from pyspark.sql import functions as F, Window

    window = Window.partitionBy("mmsi", "event_timestamp").orderBy(
        F.col("ingestion_timestamp").desc()
    )
    return (
        df.withColumn("_row_num", F.row_number().over(window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )


def transform_to_silver(df):
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, IntegerType

    df = deduplicate(df)
    df = normalize_vessel_type(df)
    df = enrich_geospatial(df)
    df = calculate_movement_deltas(df)
    df = normalize_destination(df)
    df = df.withColumn("processing_timestamp", F.current_timestamp())

    # Only cast fields where type mismatch would cause real errors
    df = df.withColumns({
        "event_timestamp":      F.to_timestamp(F.col("event_timestamp")),
        "heading":              F.col("heading").cast(IntegerType()),
    })

    return df.select(
        "mmsi",
        "vessel_name",
        "vessel_type_normalized",
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
    )


# =============================================================================
# Read Bronze
# =============================================================================

def read_new_bronze_files(spark, last_processed_date: str = None):
    """
    Read Bronze Parquet files from GCS.
    If last_processed_date is set, only read files from that partition onwards.
    """
    from pyspark.sql import functions as F

    df = spark.read.parquet(BRONZE_INPUT_DIR)

    # DEBUG: limit to 10 rows to test BQ write quickly
    if os.getenv("SILVER_DEBUG"):
        logger.info("DEBUG mode: limited to 10 rows")
        return df.limit(10)
    elif last_processed_date:
        df = df.filter(F.col("partition_date") >= last_processed_date)

    return df


# =============================================================================
# Write Silver
# =============================================================================

def write_silver_gcs(df, batch_number: int) -> int:
    """Write Silver data to GCS as Parquet partitioned by date."""
    from pyspark.sql import functions as F

    enriched = df.withColumn(
        "partition_date", F.to_date(F.col("event_timestamp"))
    )

    record_count = enriched.count()

    if record_count == 0:
        logger.info(f"Batch {batch_number}: no records to write to GCS")
        return 0

    enriched.write.mode("append").partitionBy("partition_date").parquet(
        SILVER_OUTPUT_DIR
    )

    logger.info(f"Batch {batch_number}: wrote {record_count} records to {SILVER_OUTPUT_DIR}")
    return record_count


def write_silver_bigquery(df, batch_number: int) -> None:
    try:
        # Debug: print exact schema Spark is sending
        logger.info(f"Schema being sent to BigQuery:")
        for field in df.schema.fields:
            logger.info(f"  {field.name}: {field.dataType} nullable={field.nullable}")
        (
            df.write
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

    logger.info("Starting MarineFlow Silver Positions job")
    logger.info(f"Input:  {BRONZE_INPUT_DIR}")
    logger.info(f"Output: {SILVER_OUTPUT_DIR}")
    logger.info(f"BigQuery: {BQ_TABLE}")

    spark = create_spark_session()
    total_records = 0
    batch_number = 0

    try:
        while True:
            batch_number += 1
            logger.info(f"Starting Silver batch {batch_number}")

            try:
                bronze_df = read_new_bronze_files(spark)
                silver_df = transform_to_silver(bronze_df)
                records = write_silver_gcs(silver_df, batch_number)
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