# =============================================================================
# MARINEFLOW — Airflow Pipeline DAG
# orchestration/dags/marineflow_pipeline.py
#
# Orchestrates the hourly Gold materialization pipeline.
#
# What this DAG does NOT do:
#   - Launch Bronze or Silver Spark jobs — these run as continuous Structured
#     Streaming processes (Docker locally, Dataproc in production). They are
#     not batch jobs with a start/end and should not be managed by Airflow.
#
# What this DAG DOES:
#   - Verify new Silver data has arrived since the last run
#   - Run dbt to materialize Gold models (vessel_activity_summary, port_traffic,
#     anomaly_candidates)
#   - Run dbt data quality tests
#   - Daily: compact small Silver Parquet files into fewer larger files
#
# Schedule: every hour at :05 (gives Silver ~5 min to process the latest data)
#
# Production note:
#   In production the BashOperator dbt tasks would be replaced with
#   a DbtCloudRunJobOperator or KubernetesPodOperator running dbt in a
#   dedicated container. For local development BashOperator is sufficient.
#
# Task graph:
#
#   check_silver_data_arrived
#             │
#           dbt_run
#             │
#           dbt_test
#             │
#     [daily @ 02:05 UTC only]
#           compact_gcs
# =============================================================================

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


# =============================================================================
# Configuration
# =============================================================================

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "marineflow-489815")
GCS_BUCKET     = os.getenv("GCS_BUCKET", "marineflow-lake-marineflow-489815")

# Path to dbt project inside the Airflow container
# Mounted via docker-compose volume: ./transformation/dbt → /opt/airflow/dbt
DBT_DIR        = "/opt/airflow/dbt"
DBT_PROFILES   = "/opt/airflow/dbt"


# =============================================================================
# Default args
# =============================================================================

default_args = {
    "owner":            "marineflow",
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
    "email_on_retry":   False,
}


# =============================================================================
# Task functions
# =============================================================================

def check_silver_data_arrived(**context) -> bool:
    """
    Verify that Silver has written new data since the last DAG run.
    Checks for Parquet files in silver/vessel_positions/ written in the
    last 2 hours — if none found, short-circuits the DAG run.

    This prevents dbt from running when Silver has no new data to process
    (e.g. if the Spark job is temporarily stopped).
    """
    from google.cloud import storage
    from datetime import timezone

    client       = storage.Client(project=GCP_PROJECT_ID)
    bucket       = client.bucket(GCS_BUCKET)
    prefix       = "silver/vessel_positions/"
    cutoff       = datetime.now(timezone.utc) - timedelta(hours=2)

    blobs = client.list_blobs(GCS_BUCKET, prefix=prefix)
    recent = [b for b in blobs if b.updated and b.updated > cutoff]

    if recent:
        context["task_instance"].log.info(
            f"Found {len(recent)} Silver files updated since {cutoff.isoformat()}"
        )
        return True
    else:
        context["task_instance"].log.warning(
            f"No Silver files updated since {cutoff.isoformat()} — skipping Gold run"
        )
        return False


def is_daily_run(**context) -> bool:
    """
    Gate for the daily compaction task.
    Returns True only at the 02:05 UTC run.
    """
    return context["data_interval_start"].hour == 2


def compact_silver_partition(**context) -> None:
    """
    Compact small Silver Parquet files from the previous day.

    Bronze and Silver Structured Streaming jobs write one small Parquet file
    per micro-batch trigger (every 30-60 seconds). Over a day this creates
    ~1440 small files per partition. Compaction merges them into 1 file,
    improving query performance on the BigQuery external table.

    In production this would be replaced by Delta Lake ACID compaction
    or a Dataproc job. For local development PySpark runs inline.
    """
    from pyspark.sql import SparkSession

    execution_dt = context["data_interval_start"]
    prev_day     = (execution_dt - timedelta(days=1)).strftime("%Y-%m-%d")
    silver_path  = f"gs://{GCS_BUCKET}/silver/vessel_positions/partition_date={prev_day}"

    context["task_instance"].log.info(f"Compacting: {silver_path}")

    spark = (
        SparkSession.builder
        .appName("MarineFlow-Compaction")
        .master("local[2]")
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.type", "APPLICATION_DEFAULT")
        .getOrCreate()
    )

    try:
        df    = spark.read.parquet(silver_path)
        count = df.count()

        if count == 0:
            context["task_instance"].log.info("No data to compact")
            return

        df.coalesce(1).write.mode("overwrite").parquet(silver_path)
        context["task_instance"].log.info(
            f"Compacted {count} records into single Parquet file"
        )
    except Exception as e:
        context["task_instance"].log.warning(
            f"Compaction skipped — partition may not exist yet: {e}"
        )
    finally:
        spark.stop()


# =============================================================================
# DAG
# =============================================================================

with DAG(
    dag_id="marineflow_pipeline",
    description="Hourly Gold materialization + daily Silver compaction",
    schedule_interval="5 * * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["marineflow", "gold", "dbt"],
    doc_md="""
## MarineFlow Hourly Pipeline

Runs every hour at :05 to materialize Gold models from the latest Silver data.

Bronze and Silver Spark jobs run as **continuous Structured Streaming processes**
and are not managed by this DAG. In production they would run on Dataproc;
locally they run in dedicated Docker containers.

### Task flow
1. **check_silver_data_arrived** — verify Silver has new data (last 2h)
2. **dbt_run** — materialize `vessel_activity_summary`, `port_traffic`, `anomaly_candidates`
3. **dbt_test** — run data quality tests (not_null, unique, accepted_values)
4. **compact_gcs** — merge small Silver Parquet files *(daily at 02:05 UTC only)*

### Production upgrade path
Replace `BashOperator` dbt tasks with `KubernetesPodOperator` or
`DbtCloudRunJobOperator` for containerized, isolated dbt execution.
Replace `compact_silver_partition` PythonOperator with a Dataproc job
or Delta Lake automatic compaction.
    """,
) as dag:

    # ── 1. Check Silver data ───────────────────────────────────────────────
    check_silver = ShortCircuitOperator(
        task_id="check_silver_data_arrived",
        python_callable=check_silver_data_arrived,
        provide_context=True,
        doc_md="Short-circuit if Silver has no new data — prevents unnecessary dbt runs.",
    )

    # ── 2. dbt run ─────────────────────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROFILES} --target prod"
        ),
        doc_md="Materialize Gold models from Silver data.",
    )

    # ── 3. dbt test ────────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROFILES} --target prod"
        ),
        doc_md="Run data quality tests — fails the DAG run if any test fails.",
    )

    # ── 4. Gate: daily only ────────────────────────────────────────────────
    daily_gate = ShortCircuitOperator(
        task_id="is_daily_run",
        python_callable=is_daily_run,
        provide_context=True,
        doc_md="Only proceed to compaction at the 02:05 UTC run.",
    )

    # ── 5. Compact Silver GCS ──────────────────────────────────────────────
    compact_gcs = PythonOperator(
        task_id="compact_gcs",
        python_callable=compact_silver_partition,
        provide_context=True,
        doc_md="Merge ~1440 daily small Parquet files into 1 per partition.",
    )

    # ── Dependencies ───────────────────────────────────────────────────────
    check_silver >> dbt_run >> dbt_test >> daily_gate >> compact_gcs