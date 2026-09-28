"""Smoke test of the Silver compaction on real Spark, inside the project's Spark image.

Uses the local filesystem through the same Hadoop FileSystem API that gs:// uses, so no GCP is
touched. It checks what the in-memory unit tests cannot: real Parquet files, the hive-partitioned
read BigQuery relies on, and the scenarios that destroyed data before (overwriting a path that is
being read) or could lose it (late files, a crash half way through the commit).

Run from the repo root (needs Docker):

    docker run --rm --entrypoint /bin/sh \
      -e PYTHONPATH=/opt/spark/processing/spark_streaming \
      -v "$PWD/processing:/opt/spark/processing:ro" \
      -v "$PWD/tests/smoke:/smoke:ro" \
      marineflow-spark:3.5.0 \
      -c "python3 /smoke/compaction_smoke.py"
"""
import glob
import os
import shutil
import sys

sys.path.insert(0, "/opt/spark/processing/spark_streaming")

import compact_silver as cs  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

ROOT = "/tmp/compaction_smoke"
BASE = f"{ROOT}/silver"
DATASET, DAY = "vessel_positions", "2020-01-01"
PART = cs.partition_dir(BASE, DATASET, DAY)

spark = SparkSession.builder.master("local[2]").appName("compaction-smoke").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
storage = cs.Storage(spark)

failures = []


def check(condition, message):
    print(("PASS " if condition else "FAIL ") + message)
    if not condition:
        failures.append(message)


def write_batch(first_id, rows):
    """One micro-batch: one small file in the day's partition, like silver_positions.py."""
    (spark.range(first_id, first_id + rows)
     .selectExpr("cast(id as string) as mmsi", "id * 1.5 as speed", f"cast('{DAY}' as date) as partition_date")
     .coalesce(1).write.mode("append").partitionBy("partition_date").parquet(f"{BASE}/{DATASET}"))


def fresh(batches=6, rows=100):
    shutil.rmtree(ROOT, ignore_errors=True)
    for i in range(batches):
        write_batch(i * rows, rows)
    return batches * rows


def data_files():
    return sorted(glob.glob(PART + "/*.parquet"))


def table_ids():
    """What BigQuery sees: the hive-partitioned table read, without the staging folder."""
    return sorted(int(r["mmsi"]) for r in spark.read.parquet(f"{BASE}/{DATASET}").select("mmsi").collect())


def staging_files():
    return [f for f in glob.glob(f"{ROOT}/silver/{cs.STAGING_DIR}/**/*", recursive=True) if os.path.isfile(f)]


def run(**kwargs):
    return cs.compact(spark, storage, BASE, DATASET, DAY, **kwargs)


# --- A: the normal case (this is what wiped the partition before) ------------------------------
total = fresh()
ids_before = table_ids()
check(len(data_files()) == 6 and len(ids_before) == total, f"A setup: 6 small files, {total} rows")
result = run()
check(result["status"] == "compacted" and result["files_before"] == 6, f"A compaction ran: {result}")
check(len(data_files()) == 1, f"A partition now has 1 file: {[os.path.basename(f) for f in data_files()]}")
check(table_ids() == ids_before, "A every row is still there, once (no loss, no duplicates)")
check(staging_files() == [], "A staging area is empty")
check(spark.read.parquet(data_files()[0]).columns == ["mmsi", "speed"], "A compacted file keeps the original schema (no partition column)")

# --- B: a late micro-batch lands after the snapshot ----------------------------------------------
total = fresh()
ids_before = table_ids()
real_apply = cs.apply_manifest


def apply_with_late_file(storage_, manifest, run_dir):
    write_batch(9000, 10)          # the streaming job writes into the partition while we compact
    return real_apply(storage_, manifest, run_dir)


cs.apply_manifest = apply_with_late_file
run()
cs.apply_manifest = real_apply
check(table_ids() == sorted(ids_before + list(range(9000, 9010))), "B the late file survived and nothing was duplicated")
check(len(data_files()) == 2, "B partition = compacted file + the late file")

# --- C: the process dies while deleting the originals --------------------------------------------
total = fresh()
ids_before = table_ids()
real_delete = cs.Storage.delete
crashed = []


def crashing_delete(self, p, recursive=False):
    if not crashed and os.path.basename(p).startswith("part-") and p.endswith(".parquet"):
        crashed.append(p)
        raise RuntimeError("simulated crash while deleting the originals")
    return real_delete(self, p, recursive)


cs.Storage.delete = crashing_delete
try:
    run()
    check(False, "C the simulated crash should have raised")
except RuntimeError:
    check(True, "C the run failed loudly instead of swallowing the error")
cs.Storage.delete = real_delete
check(set(ids_before) <= set(table_ids()), "C after the crash no row is missing")
run()   # the next run recovers first
check(table_ids() == ids_before, "C the next run finished the commit: exactly the original rows, no duplicates")
check(len(data_files()) == 1 and staging_files() == [], "C 1 compacted file and no staging left")

# --- D: dry run -------------------------------------------------------------------------------------
total = fresh()
before = data_files()
result = run(dry_run=True)
check(result["status"] == "dry-run" and data_files() == before and staging_files() == [], "D dry run verifies and changes nothing")

# --- E: several output files when the target size is small --------------------------------------
total = fresh()
size = sum(os.path.getsize(f) for f in data_files())
result = run(target_bytes=size // 2 + 1)
check(result["files_after"] == 2 and len(data_files()) == 2, f"E size-based split: {result['files_after']} output files")
check(table_ids() == list(range(total)), "E rows preserved across the split")

# --- F: Spark's in-progress output is left alone ------------------------------------------------------
total = fresh()
tmp = f"{PART}/_temporary/0"
os.makedirs(tmp)
shutil.copy(data_files()[0], f"{tmp}/task-in-progress.parquet")
run()
check(os.path.exists(f"{tmp}/task-in-progress.parquet"), "F files under _temporary/ are not read, moved or deleted")
check(table_ids() == list(range(total)), "F rows unaffected")

# --- G: guards ---------------------------------------------------------------------------------------
try:
    cs.compact(spark, storage, BASE, DATASET, cs.utc_today())
    check(False, "G today must be refused")
except ValueError:
    check(True, "G today's partition is refused")
fresh(batches=1)
check(run()["status"] == "skipped", "G a single-file partition is skipped")

print("\nALL CHECKS PASSED" if not failures else f"\n{len(failures)} CHECK(S) FAILED")
spark.stop()
sys.exit(1 if failures else 0)
