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
#   - Compact the small Silver Parquet files. That is a Spark job
#     (processing/spark_streaming/compact_silver.py) run by hand; the Airflow
#     image has no Spark, and the compaction that used to live here deleted the
#     partition it was compacting.
#
# What this DAG DOES:
#   - Verify new Silver data has arrived since the last run
#   - Run dbt to materialize Gold models (vessel_activity_summary, port_traffic,etc)
#   - Run dbt data quality tests
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
#   ┌─────────┼──────────────┬──────────────┐
#   │         │              │              │
# dbt_batch  dbt_dark   dbt_speed   dbt_loitering      (parallel)
#   │         └──────────────┼──────────────┘
#   │                        │
# dbt_erratic          dbt_risk_score
#   │  (needs vessel_activity_summary from dbt_batch)
#   └────────────┬───────────┘
#                │
#             dbt_test
# =============================================================================

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
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


# =============================================================================
# DAG
# =============================================================================

with DAG(
    dag_id="marineflow_pipeline",
    description="Hourly Gold materialization (dbt run + test)",
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
2. **dbt_batch_models** — `vessel_activity_summary`, `port_traffic`
3. **dbt_dark_events / dbt_speed_anomalies / dbt_loitering** — incremental detection models, in parallel
4. **dbt_erratic_course** — `vessel_erratic_course` (reads `vessel_activity_summary`, so it waits for step 2)
5. **dbt_risk_score** — aggregates the three detection models
6. **dbt_test** — run data quality tests (not_null, unique, accepted_values)

### Silver compaction
Not part of this DAG. `processing/spark_streaming/compact_silver.py` merges the
small Parquet files of a closed day; run it by hand in the Spark image (see the
README). The Airflow image has no Spark, and the compaction that used to be here
deleted the partition it was compacting.

### Production upgrade path
Replace `BashOperator` dbt tasks with `KubernetesPodOperator` or
`DbtCloudRunJobOperator` for containerized, isolated dbt execution.
Run the Silver compaction as a scheduled Dataproc job, or use Delta Lake
automatic compaction.
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
            "run --select vessel_activity_summary port_traffic"
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
        doc_md="Detect loitering sessions: slow movement outside port zones for over 3 hours.",
    )

    # ── 3b. dbt — erratic course (reads vessel_activity_summary) ───────────
    dbt_erratic = BashOperator(
        task_id="dbt_erratic_course",
        bash_command=DBT_CMD.format("run --select vessel_erratic_course"),
        doc_md="Flag vessels with repeated sharp turns (>10/day). Needs vessel_activity_summary.",
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

    # ── Dependencies ───────────────────────────────────────────────────────
    #
    #   check_silver
    #        │
    #   dbt_batch   ──┬── dbt_erratic ─────┐
    #   dbt_dark    ──┤                    │
    #   dbt_speed   ──┼── dbt_risk_score ──┤  (detection models run in parallel)
    #   dbt_loitering─┘                    │
    #                                 dbt_test
    #
    check_silver >> [dbt_batch, dbt_dark, dbt_speed, dbt_loitering]
    dbt_batch >> dbt_erratic
    [dbt_dark, dbt_speed, dbt_loitering] >> dbt_risk
    [dbt_erratic, dbt_risk] >> dbt_test