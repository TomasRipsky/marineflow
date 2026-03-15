# =============================================================================
# MARINEFLOW — Airflow Pipeline DAG
# orchestration/dags/marineflow_pipeline.py
#
# Orchestrates the hourly Silver → Gold pipeline.
#
# Schedule: every hour at minute 5 (gives Bronze 5 min to finish the hour)
#
# Task graph:
#
#   check_bronze_ready
#         │
#   run_silver_positions ──► run_silver_metadata
#         │                        │
#         └──────────┬─────────────┘
#                    │
#              dbt_run_gold
#                    │
#              dbt_test_gold
#                    │
#         [daily @ 02:05 UTC only]
#              compact_gcs          ← merge small Parquet files
#
# Design notes:
#   - Silver positions reads Bronze files for the PREVIOUS hour (closed partition)
#   - Silver metadata runs in parallel — not hour-dependent
#   - dbt runs after both Silver jobs complete
#   - GCS compaction runs daily to merge small files accumulated during the day
#   - All Spark jobs use DockerOperator targeting dedicated containers
# =============================================================================

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from docker.types import Mount


# =============================================================================
# Constants
# =============================================================================

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "marineflow-489815")
GCS_BUCKET     = os.getenv("GCS_BUCKET", "marineflow-lake-marineflow-489815")
ADC_PATH       = "/tmp/adc.json"  # mounted into scheduler container

SPARK_SUBMIT_BASE = "/opt/spark/bin/spark-submit"
SPARK_CONF = " ".join([
    "--master local[2]",
    "--driver-memory 2g",
    "--conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    "--conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    "--conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT",
    "--conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2",
    "--conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true",
    "--jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar",
    "--driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar",
])

# Shared environment for all Spark tasks
SPARK_ENV = {
    "GCP_PROJECT_ID":              GCP_PROJECT_ID,
    "GCS_BUCKET":                  GCS_BUCKET,
    "GOOGLE_CLOUD_PROJECT":        GCP_PROJECT_ID,
    "GOOGLE_APPLICATION_CREDENTIALS": ADC_PATH,
    "BQ_DATASET_SILVER":           "marineflow_silver",
    "PIPELINE_VERSION":            "0.1.0",
}

# Mount processing code and ADC credentials into Spark containers
SPARK_MOUNTS = [
    Mount(target="/opt/spark/processing", source="marineflow_processing", type="volume"),
    Mount(target="/tmp/adc.json",         source=ADC_PATH,                type="bind", read_only=True),
]

DBT_DIR = "/opt/airflow/dags/../../../transformation/dbt"


# =============================================================================
# Default args
# =============================================================================

default_args = {
    "owner":            "marineflow",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
    "email_on_retry":   False,
}


# =============================================================================
# Helper functions
# =============================================================================

def check_bronze_partition_exists(**context) -> bool:
    """
    Verify that the Bronze partition for the previous hour exists in GCS
    before triggering Silver. Returns False to short-circuit if not ready.
    """
    from google.cloud import storage

    # Airflow execution time — we process the previous hour
    execution_dt = context["data_interval_start"]
    prev_hour    = execution_dt - timedelta(hours=1)
    partition    = f"bronze/vessel_positions/partition_date={prev_hour.strftime('%Y-%m-%d')}/partition_hour={prev_hour.hour}"

    client = storage.Client(project=GCP_PROJECT_ID)
    blobs  = list(client.list_blobs(GCS_BUCKET, prefix=partition, max_results=1))

    if blobs:
        context["task_instance"].log.info(f"Bronze partition ready: {partition}")
        return True
    else:
        context["task_instance"].log.warning(f"Bronze partition not found: {partition} — skipping this run")
        return False


def is_daily_run(**context) -> bool:
    """
    Returns True only when the execution hour is 2 UTC (02:05 schedule).
    Used to gate the daily GCS compaction task.
    """
    return context["data_interval_start"].hour == 2


def compact_gcs_partition(**context) -> None:
    """
    Compact small Parquet files in the previous day's Silver partition.
    Reads all small files, coalesces to 1, overwrites the partition.
    Runs daily after dbt to keep the Silver layer clean.
    """
    from pyspark.sql import SparkSession
    from google.cloud import storage

    execution_dt = context["data_interval_start"]
    prev_day     = (execution_dt - timedelta(days=1)).strftime("%Y-%m-%d")
    silver_path  = f"gs://{GCS_BUCKET}/silver/vessel_positions/partition_date={prev_day}"

    context["task_instance"].log.info(f"Compacting Silver partition: {silver_path}")

    spark = (
        SparkSession.builder
        .appName("MarineFlow-Compaction")
        .master("local[2]")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(silver_path)
        count = df.count()
        if count == 0:
            context["task_instance"].log.info("No data to compact")
            return

        df.coalesce(1).write.mode("overwrite").parquet(silver_path)
        context["task_instance"].log.info(f"Compacted {count} records into single file")
    finally:
        spark.stop()


# =============================================================================
# DAG definition
# =============================================================================

with DAG(
    dag_id="marineflow_pipeline",
    description="Hourly Silver → Gold pipeline with daily GCS compaction",
    schedule_interval="5 * * * *",   # every hour at :05
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["marineflow", "silver", "gold", "dbt"],
    doc_md="""
## MarineFlow Hourly Pipeline

Runs every hour at :05 to process the previous hour's Bronze data.

**Flow:**
1. `check_bronze_ready` — verify Bronze partition exists for previous hour
2. `silver_positions` — transform Bronze → Silver (runs in spark-silver-positions container)
3. `silver_metadata` — normalize ShipStaticData (runs in spark-silver-metadata container)
4. `dbt_run` — materialize Gold models (vessel_activity_summary, port_traffic, anomaly_candidates)
5. `dbt_test` — run data quality tests
6. `compact_gcs` — merge small Silver Parquet files (daily only, at 02:05 UTC)
    """,
) as dag:

    # ── 1. Check Bronze partition ──────────────────────────────────────────
    check_bronze_ready = ShortCircuitOperator(
        task_id="check_bronze_ready",
        python_callable=check_bronze_partition_exists,
        provide_context=True,
        doc_md="Verify Bronze GCS partition for the previous hour exists before proceeding.",
    )

    # ── 2. Silver positions ────────────────────────────────────────────────
    # Runs in the dedicated spark-silver-positions container
    silver_positions = DockerOperator(
        task_id="silver_positions",
        image="apache/spark:3.5.0",
        container_name="marineflow-spark-silver-positions",
        command=f"{SPARK_SUBMIT_BASE} {SPARK_CONF} /opt/spark/processing/spark_streaming/silver_positions.py",
        environment={
            **SPARK_ENV,
            "SILVER_MAX_FILES_PER_TRIGGER": "20",
        },
        mounts=SPARK_MOUNTS,
        network_mode="marineflow_marineflow-net",
        auto_remove=False,      # container is long-lived, don't remove after task
        docker_url="unix://var/run/docker.sock",
        doc_md="Transform Bronze Parquet → Silver vessel_positions_clean.",
    )

    # ── 3. Silver metadata ─────────────────────────────────────────────────
    silver_metadata = DockerOperator(
        task_id="silver_metadata",
        image="apache/spark:3.5.0",
        container_name="marineflow-spark-silver-metadata",
        command=f"{SPARK_SUBMIT_BASE} {SPARK_CONF} /opt/spark/processing/spark_streaming/silver_metadata.py",
        environment={
            **SPARK_ENV,
            "METADATA_MAX_FILES_PER_TRIGGER": "20",
        },
        mounts=SPARK_MOUNTS,
        network_mode="marineflow_marineflow-net",
        auto_remove=False,
        docker_url="unix://var/run/docker.sock",
        doc_md="Transform ShipStaticData landing files → Silver vessel_metadata.",
    )

    # ── 4. dbt run ─────────────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir . --target prod",
        doc_md="Materialize Gold models: vessel_activity_summary, port_traffic, anomaly_candidates.",
    )

    # ── 5. dbt test ────────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir . --target prod",
        doc_md="Run data quality tests on all Gold models.",
    )

    # ── 6. Compact GCS (daily only) ────────────────────────────────────────
    is_daily = ShortCircuitOperator(
        task_id="is_daily_run",
        python_callable=is_daily_run,
        provide_context=True,
        doc_md="Gate: only proceed to compaction if this is the 02:05 UTC run.",
    )

    compact_gcs = PythonOperator(
        task_id="compact_gcs",
        python_callable=compact_gcs_partition,
        provide_context=True,
        doc_md="Merge small Silver Parquet files from the previous day into a single file per partition.",
    )

    # ── Task dependencies ──────────────────────────────────────────────────
    check_bronze_ready >> [silver_positions, silver_metadata]
    [silver_positions, silver_metadata] >> dbt_run
    dbt_run >> dbt_test
    dbt_test >> is_daily >> compact_gcs