#!/usr/bin/env bash
# Run the five Spark jobs detached and supervised, instead of five terminals.
#
#   scripts/jobs.sh start [job...]    start the jobs (all five by default), one every STAGGER seconds
#   scripts/jobs.sh stop  [job...]    stop the supervisors and send the jobs SIGTERM (clean stop)
#   scripts/jobs.sh status            who is alive, memory, OOM kills, restarts, producer
#   scripts/jobs.sh logs <job>        follow one job's log
#
# Jobs: bronze bronze-metadata silver-positions silver-metadata hot-alerts
#
# Why: the jobs are plain spark-submit processes. When the Docker VM runs out of memory the kernel kills one,
# and nothing brought it back (the dashboards just lost a series). Each job now runs inside a small loop that
# restarts it after RESTART_DELAY seconds. That is safe because every job resumes from its checkpoint.
# The loop is not a fix for too little memory: raise the Docker VM memory first (see FRESH_START.md, step 0).
#
# Environment (all optional):
#   STAGGER=20          seconds between job starts (five JVMs starting together spike memory and CPU)
#   RESTART_DELAY=15    seconds before a dead job is restarted
#   DRIVER_MEMORY=1g    --driver-memory for every job
#   CONTAINER_PREFIX    container name prefix (default marineflow-spark; only the tests change it)
set -euo pipefail

STAGGER="${STAGGER:-20}"
RESTART_DELAY="${RESTART_DELAY:-15}"
DRIVER_MEMORY="${DRIVER_MEMORY:-1g}"
LOG=/tmp/job.log

ALL_JOBS=(bronze bronze-metadata silver-positions silver-metadata hot-alerts)

container_of() { echo "${CONTAINER_PREFIX:-marineflow-spark}-$1"; }

script_of() {
  case "$1" in
    bronze)            echo bronze_positions.py ;;
    bronze-metadata)   echo bronze_metadata.py ;;
    silver-positions)  echo silver_positions.py ;;
    silver-metadata)   echo silver_metadata.py ;;
    hot-alerts)        echo hot_alerts.py ;;
    *) echo "unknown job: $1 (jobs: ${ALL_JOBS[*]})" >&2; return 1 ;;
  esac
}

# The exact flags of the README / FRESH_START.md templates. hot-alerts is Kafka to Kafka: no GCS jars.
submit_cmd() {
  local script="$1" common
  common="/opt/spark/bin/spark-submit --master local[2] --driver-memory $DRIVER_MEMORY --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
  if [ "$script" = hot_alerts.py ]; then
    echo "$common /opt/spark/processing/spark_streaming/$script"
  else
    echo "$common --conf spark.ui.prometheus.enabled=true --conf spark.sql.streaming.metricsEnabled=true" \
         "--conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem" \
         "--conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS" \
         "--conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT" \
         "--conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2" \
         "--conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true" \
         "--jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar" \
         "--driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar" \
         "/opt/spark/processing/spark_streaming/$script"
  fi
}

# The marker comment makes the supervisor findable; the [m] keeps pkill/pgrep from matching their own command line.
marker() { echo "marineflow-loop:$1"; }

supervisor_alive() { docker exec "$(container_of "$1")" pgrep -f "[m]arineflow-loop:$1" >/dev/null 2>&1; }
# The JVM's own command line: the supervisor's text mentions the script too, but never this class name.
job_alive()        { docker exec "$(container_of "$1")" pgrep -f "[o]rg.apache.spark.deploy.SparkSubmit.*spark_streaming/$(script_of "$1")" >/dev/null 2>&1; }

start_job() {
  local job="$1" c script cmd
  c="$(container_of "$job")"; script="$(script_of "$job")"
  if ! docker ps --format '{{.Names}}' | grep -qx "$c"; then
    echo "  $job: container $c is not running (docker compose up -d first)" >&2; return 1
  fi
  if supervisor_alive "$job"; then echo "  $job: already supervised"; return 0; fi
  if job_alive "$job"; then
    echo "  $job: a spark-submit started by hand is running; stop it (Ctrl+C) before using this script" >&2; return 1
  fi
  cmd="$(submit_cmd "$script")"
  docker exec -d "$c" sh -c "# $(marker "$job")
echo \"--- supervisor started \$(date -u +%FT%TZ)\" >> $LOG
while true; do
  $cmd >> $LOG 2>&1
  echo \"--- $script exited with \$? at \$(date -u +%FT%TZ); restarting in ${RESTART_DELAY}s\" >> $LOG
  sleep $RESTART_DELAY
done"
  echo "  $job: started (log: scripts/jobs.sh logs $job)"
}

stop_job() {
  local job="$1" c script
  c="$(container_of "$job")"; script="$(script_of "$job")"
  docker exec "$c" pkill -f "[m]arineflow-loop:$job" 2>/dev/null || true      # the loop first, or it would restart the job
  docker exec "$c" pkill -TERM -f "[s]park_streaming/$script" 2>/dev/null || true
  echo "  $job: stopped"
}

jobs_or_all() { if [ "$#" -eq 0 ]; then echo "${ALL_JOBS[@]}"; else echo "$@"; fi; }

cmd_start() {
  local first=1 job
  for job in $(jobs_or_all "$@"); do
    script_of "$job" >/dev/null
    [ "$first" -eq 1 ] || sleep "$STAGGER"
    first=0
    start_job "$job" || true
  done
  echo "Check in a minute with: scripts/jobs.sh status"
}

cmd_stop() { local job; for job in $(jobs_or_all "$@"); do script_of "$job" >/dev/null; stop_job "$job"; done; }

cmd_status() {
  local vm job c state mem oom restarts
  vm="$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)"
  awk -v b="$vm" 'BEGIN { printf "Docker VM memory: %.1f GiB total\n", b / 1073741824 }'
  printf '%-18s %-10s %-10s %-9s %-9s %s\n' JOB SUPERVISOR SPARK MEMORY OOMKILLED RESTARTS
  for job in "${ALL_JOBS[@]}"; do
    c="$(container_of "$job")"
    if ! docker ps --format '{{.Names}}' | grep -qx "$c"; then printf '%-18s %s\n' "$job" "container not running"; continue; fi
    supervisor_alive "$job" && state=yes || state=no
    job_alive "$job" && alive=up || alive=DOWN
    mem="$(docker stats --no-stream --format '{{.MemUsage}}' "$c" | cut -d/ -f1)"
    oom="$(docker inspect "$c" --format '{{.State.OOMKilled}}')"
    restarts="$(docker exec "$c" sh -c "grep -c 'restarting in' $LOG 2>/dev/null || true" | head -1)"
    printf '%-18s %-10s %-10s %-9s %-9s %s\n' "$job" "$state" "$alive" "$mem" "$oom" "${restarts:-0}"
  done
  if pgrep -f "ais_producer|python.* main.py" >/dev/null 2>&1; then echo "Producer: running"; else echo "Producer: NOT running (cd ingestion/ais_producer && python main.py)"; fi
}

cmd_logs() { [ "$#" -eq 1 ] || { echo "usage: $0 logs <job>" >&2; exit 2; }; script_of "$1" >/dev/null; docker exec -it "$(container_of "$1")" tail -n 60 -f "$LOG"; }

case "${1:-}" in
  start)  shift; cmd_start "$@" ;;
  stop)   shift; cmd_stop "$@" ;;
  status) cmd_status ;;
  logs)   shift; cmd_logs "$@" ;;
  *) sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
