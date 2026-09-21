# =============================================================================
# MARINEFLOW — Airflow Scheduler image
# orchestration/Dockerfile
#
# Bakes dbt-bigquery into the image so `dbt run`/`dbt test` (triggered by the
# DAG's BashOperator, executed inside the scheduler under LocalExecutor) work
# right after `docker compose up`, without a manual `docker exec pip install`.
# =============================================================================
FROM apache/airflow:2.8.1

RUN pip install --no-cache-dir \
    dbt-bigquery==1.8.2 \
    google-cloud-bigquery==3.13.0
