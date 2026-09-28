# =============================================================================
# MARINEFLOW — Silver partition compaction
# processing/spark_streaming/compact_silver.py
#
# The Silver streaming jobs write one small Parquet file per micro-batch. This
# job merges the small files of one closed day into a few large ones, so the
# BigQuery external tables scan fewer objects.
#
# Safety model. Object stores have no atomic rename or overwrite, and reading a
# path while overwriting it destroys the data (the previous in-DAG version did
# exactly that), so:
#   1. Snapshot the data files of the partition. Files that arrive later (late
#      data from the streaming job) are never read, moved or deleted.
#   2. Read exactly those files and write the compacted output to a staging
#      directory OUTSIDE the table prefix.
#   3. Verify schema and row count of the staged output against the snapshot.
#      Any mismatch aborts before the partition is touched.
#   4. Write a manifest, move the staged files into the partition, delete the
#      snapshot files. A run that dies after step 3 is finished by the next run
#      (roll forward, never lose data).
#
# Run one compaction at a time. Errors are raised, never swallowed.
#
# Usage (inside the Spark container, same flags as the other GCS jobs):
#   spark-submit --master local[2] --driver-memory 1g \
#     --conf spark.hadoop.fs.gs.impl=... --jars .../gcs-connector-hadoop3-latest.jar \
#     compact_silver.py [--date YYYY-MM-DD] [--dataset all] [--dry-run]
# =============================================================================

import argparse
import json
import logging
import math
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s compact_silver %(message)s")
logging.getLogger("py4j").setLevel(logging.WARNING)
log = logging.getLogger("compact_silver")

DATASETS = ("vessel_positions", "vessel_metadata")
STAGING_DIR = "_compaction_staging"     # sibling of the dataset folders, so outside every table prefix
DEFAULT_TARGET_MB = 256


# =============================================================================
# Pure helpers
# =============================================================================

def is_data_file(path: str) -> bool:
    """Judges the file name only. It is applied to the direct children of a directory (Storage.list_files
    is not recursive), so in-progress output under _temporary/ is never listed in the first place."""
    name = path.rsplit("/", 1)[-1]
    return name.endswith(".parquet") and not name.startswith(("_", "."))


def partition_dir(base: str, dataset: str, day: str) -> str:
    return f"{base}/{dataset}/partition_date={day}"


def staging_root(base: str, dataset: str, day: str) -> str:
    return f"{base}/{STAGING_DIR}/{dataset}/partition_date={day}"


def output_file_count(total_bytes: int, target_bytes: int) -> int:
    return max(1, math.ceil(total_bytes / target_bytes))


def utc_today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def utc_yesterday() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


# =============================================================================
# Storage (Hadoop FileSystem, so it works for gs:// and for local paths in tests)
# =============================================================================

class Storage:
    def __init__(self, spark):
        self._spark = spark
        self._jvm = spark._jvm
        self._conf = spark._jsc.hadoopConfiguration()

    def _path(self, p):
        return self._jvm.org.apache.hadoop.fs.Path(p)

    def _fs(self, p):
        return self._path(p).getFileSystem(self._conf)

    def exists(self, p):
        return self._fs(p).exists(self._path(p))

    def list_files(self, d):
        """Direct child files of a directory as (path, size)."""
        if not self.exists(d):
            return []
        return [(s.getPath().toString(), s.getLen()) for s in self._fs(d).listStatus(self._path(d)) if s.isFile()]

    def list_dirs(self, d):
        if not self.exists(d):
            return []
        return [s.getPath().toString() for s in self._fs(d).listStatus(self._path(d)) if s.isDirectory()]

    def rename(self, src, dst):
        if not self._fs(src).rename(self._path(src), self._path(dst)):
            raise RuntimeError(f"rename failed: {src} -> {dst}")

    def delete(self, p, recursive=False):
        self._fs(p).delete(self._path(p), recursive)

    def write_text(self, p, text):
        out = self._fs(p).create(self._path(p), True)
        out.write(bytearray(text.encode("utf-8")))
        out.close()

    def read_text(self, p):
        return self._spark.sparkContext.wholeTextFiles(p).collect()[0][1]


# =============================================================================
# Commit / recovery (storage-only, no Spark: unit-tested with an in-memory fake)
# =============================================================================

def apply_manifest(storage, manifest: dict, run_dir: str) -> None:
    """Roll a commit forward: move staged files in, delete the originals, drop staging. Idempotent."""
    for src, dst in manifest["moves"]:
        if storage.exists(dst):
            continue
        if not storage.exists(src):
            raise RuntimeError(f"neither {src} nor {dst} exist: manual repair needed, originals were not touched")
        storage.rename(src, dst)
    for original in manifest["originals"]:
        if storage.exists(original):
            storage.delete(original)
    storage.delete(run_dir, recursive=True)


def recover(storage, root: str) -> None:
    """Finish what an interrupted run left behind. Staging without a manifest never reached the
    commit, so the partition is untouched and the staging is simply discarded."""
    for run_dir in storage.list_dirs(root):
        manifest_path = f"{run_dir}/manifest.json"
        if storage.exists(manifest_path):
            log.warning("finishing an interrupted compaction: %s", run_dir)
            apply_manifest(storage, json.loads(storage.read_text(manifest_path)), run_dir)
        else:
            log.warning("discarding unfinished staging: %s", run_dir)
            storage.delete(run_dir, recursive=True)


# =============================================================================
# Compaction
# =============================================================================

def _schema(df):
    return [(f.name, f.dataType.simpleString()) for f in df.schema.fields]


def compact(spark, storage, base, dataset, day, target_bytes=DEFAULT_TARGET_MB * 1024 * 1024,
            min_files=2, dry_run=False) -> dict:
    if day >= utc_today():
        raise ValueError(f"{day} is not a closed day (today is {utc_today()} UTC): refusing to compact it")

    part = partition_dir(base, dataset, day)
    root = staging_root(base, dataset, day)

    recover(storage, root)

    files = [(p, s) for p, s in storage.list_files(part) if is_data_file(p)]
    if len(files) < min_files:
        log.info("%s %s: %d data file(s), nothing to compact", dataset, day, len(files))
        return {"dataset": dataset, "day": day, "status": "skipped", "files_before": len(files)}

    paths = [p for p, _ in files]
    total_bytes = sum(s for _, s in files)
    run_id = uuid.uuid4().hex[:8]
    run_dir = f"{root}/{run_id}"
    stage = f"{run_dir}/data"

    df = spark.read.option("mergeSchema", "true").parquet(*paths)
    expected = df.count()
    n_out = output_file_count(total_bytes, target_bytes)
    log.info("%s %s: compacting %d files (%d rows, %.1f MB) into %d", dataset, day, len(files), expected,
             total_bytes / 1048576, n_out)

    df.coalesce(n_out).write.mode("errorifexists").parquet(stage)

    staged = spark.read.parquet(stage)
    if _schema(staged) != _schema(df) or staged.count() != expected:
        storage.delete(run_dir, recursive=True)
        raise RuntimeError(f"verification failed for {dataset} {day}: the compacted output does not match "
                           f"the {len(files)} source files; the partition was not touched")

    staged_files = sorted(p for p, _ in storage.list_files(stage) if is_data_file(p))
    if dry_run:
        storage.delete(run_dir, recursive=True)
        log.info("%s %s: dry run OK (%d -> %d files), nothing changed", dataset, day, len(files), len(staged_files))
        return {"dataset": dataset, "day": day, "status": "dry-run", "files_before": len(files),
                "files_after": len(staged_files), "rows": expected}

    moves = [[src, f"{part}/compacted-{run_id}-{i:04d}.snappy.parquet"] for i, src in enumerate(staged_files)]
    manifest = {"run_id": run_id, "dataset": dataset, "day": day, "originals": paths, "moves": moves}
    storage.write_text(f"{run_dir}/manifest.json", json.dumps(manifest))
    apply_manifest(storage, manifest, run_dir)

    log.info("%s %s: done, %d files -> %d", dataset, day, len(files), len(moves))
    return {"dataset": dataset, "day": day, "status": "compacted", "files_before": len(files),
            "files_after": len(moves), "rows": expected}


# =============================================================================
# Entry point
# =============================================================================

def create_spark_session():
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .appName("MarineFlow-Compact-Silver")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.type", "APPLICATION_DEFAULT")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Compact the small Parquet files of a closed Silver partition.")
    parser.add_argument("--date", default=None, help="partition_date to compact, YYYY-MM-DD (default: yesterday, UTC)")
    parser.add_argument("--dataset", default="all", choices=("all",) + DATASETS)
    parser.add_argument("--base", default=None, help="Silver root (default: gs://$GCS_BUCKET/silver)")
    parser.add_argument("--target-mb", type=int, default=DEFAULT_TARGET_MB, help="approximate size of each output file")
    parser.add_argument("--min-files", type=int, default=2, help="skip partitions with fewer data files")
    parser.add_argument("--dry-run", action="store_true", help="write and verify the compacted output, then discard it")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    base = args.base or (f"gs://{os.environ['GCS_BUCKET']}/silver" if os.getenv("GCS_BUCKET") else None)
    if not base:
        log.error("set GCS_BUCKET or pass --base")
        return 2
    day = args.date or utc_yesterday()
    datasets = DATASETS if args.dataset == "all" else (args.dataset,)

    spark = create_spark_session()
    try:
        storage = Storage(spark)
        for dataset in datasets:
            result = compact(spark, storage, base, dataset, day, target_bytes=args.target_mb * 1024 * 1024,
                             min_files=args.min_files, dry_run=args.dry_run)
            log.info("result: %s", result)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
