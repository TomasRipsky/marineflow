"""Quick data-quality check of the Bronze positions Parquet.

Prints the row count, the null count of the key columns and a sample of rows, using the column
names Bronze really writes (raw aisstream.io names plus the lineage columns).

Run it inside the Spark image with the GCS flags from the README, for example:

    spark-submit --jars jars/gcs-connector-hadoop3-latest.jar \
      --driver-class-path jars/gcs-connector-hadoop3-latest.jar diagnose_bronze.py

It reads gs://$GCS_BUCKET/bronze/vessel_positions; set BRONZE_INPUT_DIR to point it elsewhere
(a local path works too).
"""
import os
import sys

from dotenv import load_dotenv

load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GCS_BUCKET = os.getenv("GCS_BUCKET", "")
BRONZE_INPUT_DIR = os.getenv("BRONZE_INPUT_DIR") or (f"gs://{GCS_BUCKET}/bronze/vessel_positions" if GCS_BUCKET else "")
KEY_COLUMNS = ["MMSI", "Latitude", "Longitude", "Sog", "ShipName", "time_utc"]

if not BRONZE_INPUT_DIR:
    print("Set GCS_BUCKET or BRONZE_INPUT_DIR")
    sys.exit(2)

spark = (
    SparkSession.builder
    .appName("DiagnoseBronze")
    .master("local[*]")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .config("spark.hadoop.google.cloud.auth.type", "APPLICATION_DEFAULT")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(BRONZE_INPUT_DIR)
total = df.count()
print(f"\nBronze records in {BRONZE_INPUT_DIR}: {total}")

if total:
    print("\nNull counts per column:")
    for col in KEY_COLUMNS:
        null_count = df.filter(F.col(col).isNull()).count()
        print(f"  {col}: {null_count} nulls ({round(null_count / total * 100, 1)}%)")

    print("\nSample of records:")
    df.select("MMSI", "time_utc", "ingestion_timestamp", "_source_system", "_batch_id").show(10, truncate=False)

spark.stop()
