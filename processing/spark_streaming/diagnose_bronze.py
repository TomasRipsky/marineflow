import os
from dotenv import load_dotenv
load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GCS_BUCKET = os.getenv("GCS_BUCKET", "")
BRONZE_INPUT_DIR = f"gs://{GCS_BUCKET}/bronze/vessel_positions"

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
print(f"\nTotal Bronze records: {total}")
print(f"\nNull counts per column:")

for col in ["mmsi", "event_timestamp", "latitude", "longitude", 
            "speed_over_ground", "vessel_name", "destination"]:
    null_count = df.filter(F.col(col).isNull()).count()
    pct = round(null_count / total * 100, 1)
    print(f"  {col}: {null_count} nulls ({pct}%)")

print(f"\nSample of records where event_timestamp is null:")
df.filter(F.col("event_timestamp").isNull()).show(5, truncate=False)

print(f"\nSample event_timestamp values (raw string):")
df.select("event_timestamp", "ingestion_timestamp", "source").show(10, truncate=False)

spark.stop()