# =============================================================================
# MARINEFLOW — Spark Silver Layer Job
# processing/spark_streaming/silver_positions.py
#
# Reads from Bronze (GCS Parquet), applies business transformations:
# - Deduplication by MMSI + event_timestamp
# - Vessel type normalization
# - Geospatial enrichment (ocean region, nearest port zone)
# - Speed and heading delta calculations
# - Destination normalization
#
# Writes clean data to GCS Silver layer and BigQuery silver dataset.
#
# Run:
#   spark-submit \
#     --packages com.google.cloud.spark:spark-3.5-bigquery:0.36.1 \
#     silver_positions.py
# =============================================================================

import logging
import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

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
GOOGLE_CREDENTIALS = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "/opt/spark/gcp-credentials.json",
)

BRONZE_INPUT_DIR = f"gs://{GCS_BUCKET}/bronze/vessel_positions"
SILVER_OUTPUT_DIR = f"gs://{GCS_BUCKET}/silver/vessel_positions"
CHECKPOINT_DIR = f"gs://{GCS_BUCKET}/checkpoints/silver_positions"
BQ_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET_SILVER}.vessel_positions_clean"

TRIGGER_INTERVAL = os.getenv("SPARK_TRIGGER_INTERVAL", "60 seconds")

# =============================================================================
# Reference data — geospatial enrichment
# These are broadcast variables loaded once and reused across all partitions
# =============================================================================

# Major ocean regions defined by approximate bounding boxes
# (min_lat, max_lat, min_lon, max_lon, region_name)
OCEAN_REGIONS = [
    (-90,  90, -180, -30,  "atlantic_west"),
    (-90,  90,  -30,  20,  "atlantic_east"),
    (-90,  90,   20,  80,  "indian_ocean"),
    (-90,  90,   80, 180,  "pacific"),
    ( 35,  90,  -30,  60,  "arctic_north_sea"),
    (-30, -90, -180, 180,  "southern_ocean"),
    ( 20,  45,  -10,  42,  "mediterranean"),
]

# Major port zones (lat, lon, radius_deg, port_name, country)
MAJOR_PORTS = [
    (51.9,    4.1,   0.5, "Rotterdam",  "NL"),
    (53.5,    9.9,   0.5, "Hamburg",    "DE"),
    (31.2,  121.5,   0.5, "Shanghai",   "CN"),
    (22.3,  114.2,   0.3, "Hong Kong",  "HK"),
    (1.26,  103.8,   0.5, "Singapore",  "SG"),
    (35.4,  139.7,   0.5, "Tokyo",      "JP"),
    (33.7, -118.2,   0.5, "Los Angeles","US"),
    (40.7,  -74.0,   0.5, "New York",   "US"),
    (37.8, -122.3,   0.3, "San Francisco","US"),
    (-23.9, -46.3,   0.5, "Santos",     "BR"),
    (18.9,   72.8,   0.5, "Mumbai",     "IN"),
    (25.2,   55.3,   0.3, "Dubai",      "AE"),
    (30.1,   32.3,   0.3, "Port Said",  "EG"),
    (37.9,   23.7,   0.3, "Piraeus",    "GR"),
    (41.3,    2.1,   0.3, "Barcelona",  "ES"),
]


# =============================================================================
# Spark Session
# =============================================================================

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("MarineFlow-Silver-Positions")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "3g"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "2g"))
        # GCS
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            GOOGLE_CREDENTIALS,
        )
        # BigQuery connector
        .config("spark.sql.shuffle.partitions", "8")
        .config("parentProject", GCP_PROJECT_ID)
        .config("temporaryGcsBucket", GCS_BUCKET)
        .config("streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =============================================================================
# Transformation functions
# =============================================================================

def normalize_vessel_type(df):
    """
    Normalize raw AIS vessel type codes to human-readable categories.
    AIS type codes are integers — we bucket them into standard categories.
    """
    return df.withColumn(
        "vessel_type_normalized",
        F.when(F.col("vessel_type").between("70", "79"), "cargo")
         .when(F.col("vessel_type").between("80", "89"), "tanker")
         .when(F.col("vessel_type").between("60", "69"), "passenger")
         .when(F.col("vessel_type").between("30", "35"), "fishing")
         .when(F.col("vessel_type").isin("52", "53"), "tug")
         .when(F.col("vessel_type").between("50", "59"), "special_craft")
         .when(F.col("vessel_type").between("20", "29"), "wing_in_ground")
         .otherwise("other")
    )


def enrich_geospatial(df):
    """
    Add ocean region and port proximity based on lat/lon coordinates.
    Uses approximate bounding box checks — sufficient for portfolio purposes.
    """
    # Ocean region classification
    ocean_expr = F.lit("open_ocean")
    for min_lat, max_lat, min_lon, max_lon, region in OCEAN_REGIONS:
        ocean_expr = F.when(
            F.col("latitude").between(min_lat, max_lat)
            & F.col("longitude").between(min_lon, max_lon),
            F.lit(region),
        ).otherwise(ocean_expr)

    df = df.withColumn("ocean_region", ocean_expr)

    # Port proximity — find if vessel is within any major port zone
    port_name_expr = F.lit(None).cast(StringType())
    port_country_expr = F.lit(None).cast(StringType())
    in_port_expr = F.lit(False)

    for p_lat, p_lon, radius, port_name, country in MAJOR_PORTS:
        near_port = (
            (F.abs(F.col("latitude") - p_lat) <= radius)
            & (F.abs(F.col("longitude") - p_lon) <= radius)
        )
        port_name_expr = F.when(near_port, F.lit(port_name)).otherwise(port_name_expr)
        port_country_expr = F.when(near_port, F.lit(country)).otherwise(port_country_expr)
        in_port_expr = in_port_expr | near_port

    df = df.withColumns({
        "nearest_port": port_name_expr,
        "eez_country": port_country_expr,
        "is_in_port_zone": in_port_expr,
    })

    return df


def calculate_movement_deltas(df):
    """
    Calculate speed change rate and heading change per vessel.
    Uses a window function ordered by event_timestamp per MMSI.

    These deltas are key features for the anomaly detection model.
    """
    window = Window.partitionBy("mmsi").orderBy("event_timestamp")

    df = df.withColumns({
        "prev_speed": F.lag("speed_over_ground", 1).over(window),
        "prev_heading": F.lag("heading", 1).over(window),
    })

    df = df.withColumns({
        "speed_change_rate": F.abs(
            F.col("speed_over_ground") - F.col("prev_speed")
        ),
        "heading_change_degrees": F.abs(
            F.col("heading") - F.col("prev_heading")
        ),
    }).drop("prev_speed", "prev_heading")

    return df


def normalize_destination(df):
    """
    Clean up the destination field — AIS destinations are free text
    entered by captains and can be very inconsistent.
    """
    return df.withColumn(
        "destination_clean",
        F.upper(F.trim(F.col("destination")))
    )


def deduplicate(df):
    """
    Remove duplicate records — same vessel at the same timestamp.
    Keeps the first occurrence per (mmsi, event_timestamp) pair.
    """
    window = Window.partitionBy("mmsi", "event_timestamp").orderBy(
        F.col("ingestion_timestamp").desc()
    )
    return (
        df.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )


# =============================================================================
# Main transformation pipeline
# =============================================================================

def transform_to_silver(df):
    """Apply all Silver transformations in order."""
    df = deduplicate(df)
    df = normalize_vessel_type(df)
    df = enrich_geospatial(df)
    df = calculate_movement_deltas(df)
    df = normalize_destination(df)
    df = df.withColumn("processing_timestamp", F.current_timestamp())

    # Select only the columns defined in the Silver BigQuery schema
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
        "speed_change_rate",
        "heading_change_degrees",
        "event_timestamp",
        "processing_timestamp",
    )


# =============================================================================
# Write to Silver
# =============================================================================

def write_silver_gcs(df, output_path: str, checkpoint_dir: str):
    """Write Silver data to GCS as Parquet, partitioned by date."""
    enriched = df.withColumn(
        "partition_date", F.to_date(F.col("event_timestamp"))
    )
    return (
        enriched.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_dir)
        .partitionBy("partition_date")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


def write_silver_bigquery(df, bq_table: str, checkpoint_dir: str):
    """Write Silver data to BigQuery for analytical queries."""
    return (
        df.writeStream
        .format("bigquery")
        .outputMode("append")
        .option("table", bq_table)
        .option("checkpointLocation", f"{checkpoint_dir}_bq")
        .option("temporaryGcsBucket", GCS_BUCKET)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


# =============================================================================
# Entry point
# =============================================================================

def main() -> None:
    missing = [v for v in ["GCP_PROJECT_ID", "GCS_BUCKET"] if not os.getenv(v)]
    if missing:
        logger.error(f"Missing environment variables: {missing}")
        sys.exit(1)

    logger.info("Starting MarineFlow Silver Positions job")

    spark = create_spark_session()

    try:
        # Read from Bronze (streaming read of Parquet files)
        bronze_df = (
            spark.readStream
            .schema(
                spark.read
                .parquet(BRONZE_INPUT_DIR)
                .schema
            )
            .parquet(BRONZE_INPUT_DIR)
        )

        silver_df = transform_to_silver(bronze_df)

        # Write to both GCS and BigQuery simultaneously
        gcs_query = write_silver_gcs(silver_df, SILVER_OUTPUT_DIR, CHECKPOINT_DIR)
        bq_query = write_silver_bigquery(silver_df, BQ_TABLE, CHECKPOINT_DIR)

        logger.info("Silver streaming queries started")

        # Wait for both queries
        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        logger.info("Shutdown signal received")
    except Exception as e:
        logger.error(f"Silver job failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
