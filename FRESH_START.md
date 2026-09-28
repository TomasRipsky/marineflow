# Fresh start: wipe everything and run the pipeline from zero

Use this for an end-to-end test on empty data. It **deletes data** in Kafka, in the GCS bucket and in the BigQuery gold dataset, so read each block before you paste it. Nothing here is run for you.

Every command is meant to be run from the repository root, in this order. Steps 1 to 4 destroy; steps 5 to 9 rebuild and check.

| Step | What | Destroys data? |
|---|---|---|
| 0 | Variables | no |
| 1 | Stop the producer and the Spark jobs | no |
| 2 | Wipe Kafka and the Hot Alerts state | **yes** (local) |
| 3 | Wipe the bucket (`bronze/`, `silver/`, `checkpoints/`) | **yes** (GCS) |
| 4 | Drop the gold tables | **yes** (BigQuery) |
| 5 | Apply pending infrastructure changes | no (review first) |
| 6 | Rebuild and start the stack | no |
| 7 | Start the five jobs | no |
| 8 | Start the producer | no |
| 9 | Check every layer, then run the DAG | no |

---

## 0. Variables

```bash
export PROJECT=marineflow-489815
export BUCKET=marineflow-lake-marineflow-489815      # the full GCS_BUCKET name
```

## 1. Stop the producer and the jobs

Press Ctrl+C in the producer terminal and in each of the five `spark-submit` terminals. If a job was left running in the background:

```bash
docker exec marineflow-spark-bronze pkill -TERM -f bronze_positions.py
docker exec marineflow-spark-bronze-metadata pkill -TERM -f bronze_metadata.py
docker exec marineflow-spark-silver-positions pkill -TERM -f silver_positions.py
docker exec marineflow-spark-silver-metadata pkill -TERM -f silver_metadata.py
docker exec marineflow-spark-hot-alerts pkill -TERM -f hot_alerts.py
```

Pause the DAG so the scheduler does not run dbt against half-deleted data:

```bash
docker exec marineflow-airflow-scheduler airflow dags pause marineflow_pipeline
```

## 2. Wipe Kafka and the Hot Alerts state

The six topics live in the `kafka-data` volume. Deleting the topics and recreating them resets every offset to 0:

```bash
for t in vessel-positions vessel-positions-bronze vessel-metadata vessel-metadata-bronze vessel-alerts dead-letter-queue; do
  docker exec marineflow-kafka /opt/kafka/bin/kafka-topics.sh --delete --topic "$t" --bootstrap-server localhost:9092
done
docker compose up kafka-init-topics -d --build
docker exec marineflow-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

The last command must list all six topics again. The Hot Alerts state (per-vessel history) is a Docker volume, and a volume in use cannot be removed, so remove the container first (its data is in the volume, not in the container):

```bash
docker compose rm -sf spark-hot-alerts
docker volume ls | grep hot-alerts-checkpoint
docker volume rm marineflow_hot-alerts-checkpoint
```

Use the exact name printed by `docker volume ls` if your project folder gives it a different prefix.

## 3. Wipe the bucket

The checkpoints must go together with the topics, otherwise the jobs try to resume from offsets that no longer exist. The compaction staging area (`silver/_compaction_staging/`) goes with `silver/`.

```bash
gcloud storage ls gs://$BUCKET/          # look before you delete
gcloud storage rm --recursive gs://$BUCKET/bronze/ gs://$BUCKET/silver/ gs://$BUCKET/checkpoints/
gcloud storage ls gs://$BUCKET/          # should be empty
```

The BigQuery external tables (`marineflow_bronze.*_raw`, `marineflow_silver.*`) only read from these prefixes, so they are empty now and need no change.

## 4. Drop the gold tables

The gold models are incremental. If you leave the old tables, the first dbt run merges new rows into old ones. Dropping them makes dbt rebuild them from the empty Silver layer (the staging view `stg_vessel_positions` is kept; the DAG rebuilds it as its first dbt task).

```bash
for t in port_traffic vessel_activity_summary vessel_dark_events vessel_erratic_course vessel_loitering vessel_risk_score vessel_speed_anomalies; do
  bq rm -f -t "$PROJECT:marineflow_gold.$t"
done
bq ls "$PROJECT:marineflow_gold"           # should list no tables
```

Optionally clear the Airflow run history as well:

```bash
docker exec marineflow-airflow-scheduler airflow dags delete marineflow_pipeline --yes
```

## 5. Apply pending infrastructure changes

The Silver external tables were changed (`port_name` and `port_country` replaced the old port columns, and one always-null metadata column was removed). If you have not applied that yet, do it now, **before** the Silver jobs write the new files:

```bash
cd infra/terraform
terraform plan          # read it: only the two Silver external tables should change
terraform apply         # after reviewing the plan
cd ../..
```

## 6. Rebuild and start the stack

Rebuild so the containers pick up the latest code and requirements. Grafana is pinned to 13.2.2 and downloads its Kafka plugin on first start, so give it a minute.

```bash
docker compose up kafka kafka-init-topics -d --build
docker compose up spark-bronze spark-bronze-metadata spark-silver-positions spark-silver-metadata spark-hot-alerts -d --build
docker compose up postgres airflow-init airflow-webserver airflow-scheduler -d --build
docker compose up kafka-exporter prometheus grafana -d --build
docker compose ps
```

Every service should be `running` (`kafka-init-topics` and `airflow-init` exit after finishing, which is normal).

## 7. Start the five jobs (one terminal each)

```bash
docker exec -it marineflow-spark-bronze /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.ui.prometheus.enabled=true --conf spark.sql.streaming.metricsEnabled=true --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS --conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true --jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar --driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar /opt/spark/processing/spark_streaming/bronze_positions.py
```

```bash
docker exec -it marineflow-spark-bronze-metadata /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.ui.prometheus.enabled=true --conf spark.sql.streaming.metricsEnabled=true --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS --conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true --jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar --driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar /opt/spark/processing/spark_streaming/bronze_metadata.py
```

```bash
docker exec -it marineflow-spark-silver-positions /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.ui.prometheus.enabled=true --conf spark.sql.streaming.metricsEnabled=true --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS --conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true --jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar --driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar /opt/spark/processing/spark_streaming/silver_positions.py
```

```bash
docker exec -it marineflow-spark-silver-metadata /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.ui.prometheus.enabled=true --conf spark.sql.streaming.metricsEnabled=true --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS --conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true --jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar --driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar /opt/spark/processing/spark_streaming/silver_metadata.py
```

Hot Alerts is Kafka to Kafka: no GCS jars.

```bash
docker exec -it marineflow-spark-hot-alerts /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark/processing/spark_streaming/hot_alerts.py
```

Each job should print its startup log and then a micro-batch line every 30 seconds. An empty batch is normal until the producer runs.

## 8. Start the producer

In a new terminal, with the virtual environment active and `AIS_API_KEY` set in `.env`:

```bash
source venv/bin/activate
cd ingestion/ais_producer && python main.py
```

## 9. Check every layer

Give it two or three minutes, then walk the pipeline from the front to the back.

**Kafka** (the offsets must grow; the `-bronze` topics trail the raw ones a little; a sustained large gap usually means the Bronze job fell behind or crashed (check `docker inspect <container> --format '{{.State.OOMKilled}}'`), not that it is rejecting data — see the Overview dashboard's *Bronze gap* tile):

```bash
for t in vessel-positions vessel-positions-bronze vessel-metadata vessel-metadata-bronze vessel-alerts dead-letter-queue; do
  echo "$t"; docker exec marineflow-kafka /opt/kafka/bin/kafka-get-offsets.sh --broker-list localhost:9092 --topic "$t" --time -1
done
docker exec marineflow-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic vessel-positions-bronze --from-beginning --max-messages 3
```

`dead-letter-queue` should stay at or near 0. `vessel-alerts` may take a while: it needs a vessel to break a rule.

**GCS** (files appear after the first micro-batches; Silver positions are partitioned by `partition_date`):

```bash
gcloud storage ls gs://$BUCKET/bronze/ gs://$BUCKET/silver/ gs://$BUCKET/checkpoints/
gcloud storage ls "gs://$BUCKET/silver/vessel_positions/**" | head
```

**BigQuery** (the external tables read straight from those files):

```bash
bq query --use_legacy_sql=false "SELECT COUNT(*) AS rows_, COUNT(DISTINCT mmsi) AS vessels, MAX(port_name) AS a_port FROM \`$PROJECT.marineflow_silver.vessel_positions_clean\`"
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM \`$PROJECT.marineflow_bronze.vessel_positions_raw\`"
```

**Dashboards**: http://localhost:3000 (`admin` / `GRAFANA_ADMIN_PASSWORD`, default `marineflow_dev`) opens on *MarineFlow: Overview*. The Spark rate panels need the `metricsEnabled` flag used above. *Live traffic* draws the map from `vessel-positions-bronze`.

**Gold**: the DAG skips dbt unless Silver has a file updated in the last two hours, so start it once Silver has data. Unpause it and trigger a run by hand instead of waiting for the next `:05`:

```bash
docker exec marineflow-airflow-scheduler airflow dags unpause marineflow_pipeline
docker exec marineflow-airflow-scheduler airflow dags trigger marineflow_pipeline
```

Follow the run at http://localhost:4080 (`admin` / `admin`). All nine tasks should end green (`dbt_staging` rebuilds the staging view first), `dbt_test` last. Then:

```bash
bq ls "$PROJECT:marineflow_gold"
bq query --use_legacy_sql=false "SELECT COUNT(*) FROM \`$PROJECT.marineflow_gold.vessel_activity_summary\`"
```

Some gold tables (dark events, speed anomalies, loitering, risk score) legitimately stay empty or small in the first hours: they need a vessel that went silent, moved too fast or lingered. Empty is not a failure; a red task is.

## What to test on top

- **Hot path**: `vessel-alerts` fills over time; the *Latest alerts* table on *Live traffic* follows it.
- **Reset behaviour**: stop and restart one Silver job; it must resume from its checkpoint without reprocessing (offsets in the Kafka dashboard keep growing, no duplicate burst).
- **Compaction** (after a day of data, when small files have piled up): always with `--dry-run` first. The command is in the README under *Silver compaction*.

## If something goes wrong

| Symptom | Likely cause |
|---|---|
| A Spark job crashes at start with an offsets or checkpoint error | Topics and checkpoints were not wiped together. Repeat steps 2 and 3, then restart the job |
| `docker volume rm` says the volume is in use | The Hot Alerts container still exists. Run `docker compose rm -sf spark-hot-alerts` first |
| `bq query` on a Silver table fails on a column name | Step 5 was skipped: the external tables still have the old schema |
| The DAG run ends after the first task with everything skipped | Silver has no file from the last two hours. Check the Silver jobs and the producer |
| Grafana panels are empty | The stack was up less than a minute, or the job was not started with `spark.sql.streaming.metricsEnabled=true` |
| A job's driver process is just gone (no java process in `docker exec <container> ps aux`, "Spark jobs up" flickers below 4/4) | Likely OOM-killed: check `docker inspect <container> --format '{{.State.OOMKilled}}'`. Five 1 GB Spark drivers plus Kafka, Airflow, Grafana and Prometheus is heavy for Docker Desktop's default VM memory (often ~8 GB). Raise the VM's memory limit (Docker Desktop → Settings → Resources), or don't run all five jobs at once. Restart the affected job by hand; nothing supervises these processes |
| A job's very first batch after a restart is far slower than 30 s | Expected: it is catching up on everything the topic buffered while the job was down. Later batches return to normal once it clears the backlog |
| *Live traffic* map is empty | `vessel-positions-bronze` is empty: Bronze is not running or the producer is not connected |
