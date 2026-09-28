"""End-to-end smoke test of the hot path on real Spark, inside the project's Spark image.

It checks what the unit tests cannot: that the pinned pandas/pyarrow work with
Spark 3.5 on the image's Python 3.8, and that applyInPandasWithState runs the
real process_vessel. No Kafka or GCP is needed: a synthetic `rate` stream moves
four vessels ~12 nm per message (an impossible speed) and every fifth message
carries SOG 102.3 ("not available"). Expected: IMPOSSIBLE_SPEED alerts only.
The pre-fix code raised GPS_SPOOFING / SUDDEN_ACCELERATION here.

Run from the repo root (needs Docker and network for pip):

    docker run --rm --entrypoint /bin/sh \
      -e PYTHONPATH=/opt/spark/processing/spark_streaming \
      -v "$PWD/processing:/opt/spark/processing:ro" \
      -v "$PWD/tests/smoke:/smoke:ro" \
      marineflow-spark:3.5.0 \
      -c "pip install -q -r /opt/spark/processing/spark_streaming/requirements.txt && python3 /smoke/hot_alerts_smoke.py"
"""
import sys

sys.path.insert(0, "/opt/spark/processing/spark_streaming")

import hot_alerts as h  # noqa: E402
import pandas  # noqa: E402
import pyarrow  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.streaming.state import GroupStateTimeout  # noqa: E402

RUN_SECONDS = 30

print(f"python {sys.version.split()[0]} | pandas {pandas.__version__} | pyarrow {pyarrow.__version__}")

spark = (
    SparkSession.builder.master("local[2]").appName("hot-alerts-smoke")
    .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false").getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

positions = (
    spark.readStream.format("rate").option("rowsPerSecond", 40).load()
    .select(
        (F.col("value") % 4).cast("string").alias("MMSI"),
        (F.lit(10.0) + F.col("value") * 0.05).alias("Latitude"),
        F.lit(10.0).alias("Longitude"),
        F.when(F.col("value") % 5 == 0, F.lit(102.3)).otherwise(F.lit(10.0)).alias("Sog"),
        F.date_format(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("time_utc"),
    )
)

alerts = positions.groupBy("MMSI").applyInPandasWithState(
    h.process_vessel,
    outputStructType=h.get_output_schema(),
    stateStructType=h.get_state_schema(),
    outputMode="append",
    timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
)

query = (
    alerts.writeStream.format("memory").queryName("alerts").outputMode("append")
    .trigger(processingTime="3 seconds").start()
)
query.awaitTermination(RUN_SECONDS)
error = query.exception()
query.stop()

if error is not None:
    print(f"FAIL: the streaming query crashed: {error}")
    sys.exit(1)

counts = {row["alert_type"]: row["n"] for row in
          spark.sql("SELECT alert_type, COUNT(*) AS n FROM alerts GROUP BY alert_type").collect()}
print(f"alerts by type: {counts}")

if not counts:
    print("FAIL: no alerts were produced")
    sys.exit(1)
if set(counts) != {"IMPOSSIBLE_SPEED"}:
    print(f"FAIL: expected only IMPOSSIBLE_SPEED, got {sorted(counts)}")
    sys.exit(1)
print("OK: applyInPandasWithState runs on the pinned stack and SOG 102.3 is ignored")
