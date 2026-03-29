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

    DBT = "/home/airflow/.local/bin/dbt"
    DBT_CMD = f"cd {DBT_DIR} && {DBT} {{}} --profiles-dir {DBT_PROFILES} --target prod"

    # ── 2. dbt — batch models (no dependencies between them) ───────────────
    dbt_batch = BashOperator(
        task_id="dbt_batch_models",
        bash_command=DBT_CMD.format(
            "run --select vessel_activity_summary port_traffic anomaly_candidates"
        ),
        doc_md="Materialize batch Gold models — no inter-model dependencies.",
    )

    # ── 3. dbt — incremental detection models ─────────────────────────────
    # Run in parallel — all read from stg_vessel_positions independently
    dbt_dark = BashOperator(
        task_id="dbt_dark_events",
        bash_command=DBT_CMD.format("run --select vessel_dark_events"),
        doc_md="Detect AIS blackout events (gap > 120 min).",
    )

    dbt_speed = BashOperator(
        task_id="dbt_speed_anomalies",
        bash_command=DBT_CMD.format("run --select vessel_speed_anomalies"),
        doc_md="Detect GPS spoofing, impossible speeds and sudden accelerations.",
    )

    dbt_loitering = BashOperator(
        task_id="dbt_loitering",
        bash_command=DBT_CMD.format("run --select vessel_loitering"),
        doc_md="Detect loitering sessions and potential STS transfers.",
    )

    # ── 4. dbt — risk score (depends on all three detection models) ────────
    dbt_risk = BashOperator(
        task_id="dbt_risk_score",
        bash_command=DBT_CMD.format("run --select vessel_risk_score"),
        doc_md="Aggregate 30-day risk score from dark events, speed anomalies and loitering.",
    )

    # ── 5. dbt test — all models ───────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_CMD.format("test"),
        doc_md="Run all data quality tests across Gold models.",
    )

    # ── 6. Gate: daily only ────────────────────────────────────────────────
    daily_gate = ShortCircuitOperator(
        task_id="is_daily_run",
        python_callable=is_daily_run,
        provide_context=True,
        doc_md="Only proceed to compaction at the 02:05 UTC run.",
    )

    # ── 7. Compact Silver GCS ──────────────────────────────────────────────
    compact_gcs = PythonOperator(
        task_id="compact_gcs",
        python_callable=compact_silver_partition,
        provide_context=True,
        doc_md="Merge ~1440 daily small Parquet files into 1 per partition.",
    )

    # ── Dependencies ───────────────────────────────────────────────────────
    #
    #   check_silver
    #        │
    #   dbt_batch   ──┐
    #   dbt_dark    ──┤
    #   dbt_speed   ──┤  (parallel)
    #   dbt_loitering─┤
    #                 │
    #            dbt_risk_score
    #                 │
    #            dbt_test
    #                 │
    #            daily_gate ── compact_gcs
    #
    check_silver >> [dbt_batch, dbt_dark, dbt_speed, dbt_loitering]
    [dbt_dark, dbt_speed, dbt_loitering] >> dbt_risk
    [dbt_batch, dbt_risk] >> dbt_test
    dbt_test >> daily_gate >> compact_gcs