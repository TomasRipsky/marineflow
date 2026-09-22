# MarineFlow 🚢
### Real-Time Global Maritime Intelligence Platform

> *"Designed a system that takes the pulse of global shipping in real time."*

MarineFlow is an end-to-end data engineering portfolio project that processes AIS (Automatic Identification System) vessel tracking signals in real time. It ingests, transforms, enriches and serves naval positioning data through a modern lakehouse architecture on GCP.

---

## Table of Contents

1. [What is AIS and why is it interesting?](#what-is-ais-and-why-is-it-interesting)
2. [Architecture](#architecture)
3. [Tech Stack](#tech-stack)
4. [Project Structure](#project-structure)
5. [Pipeline Phases](#pipeline-phases)
   - [Phase 1 — AIS Ingestion](#phase-1--ais-ingestion)
   - [Phase 2 — Spark Bronze Layer](#phase-2--spark-bronze-layer)
   - [Phase 3 — Silver, Gold, dbt & Airflow](#phase-3--silver-gold-dbt--airflow)
   - [Phase 4 — API & Dashboard](#phase-4--api--dashboard) *(pending)*
5. [GCP Infrastructure (Terraform)](#gcp-infrastructure-terraform)
6. [Local Setup](#local-setup)
7. [Design Decisions](#design-decisions)
8. [Known Limitations](#known-limitations)
9. [How to Run](#how-to-run)

---

## What is AIS and why is it interesting?

AIS (Automatic Identification System) is a mandatory radio protocol for all commercial vessels over 300 tons. Every ship broadcasts its GPS position, speed, heading, destination and identity every 2–10 seconds.

This generates a continuous global stream of hundreds of thousands of messages per minute — exactly the kind of data a modern streaming pipeline is built to handle.

**Why is it relevant for data engineering?**

- **Real volume**: ~300 messages/second with global coverage
- **Variety**: positions, static metadata, alerts, navigational status
- **Velocity**: seconds of latency from vessel to system
- **Real use cases**: logistics (Amazon, Maersk), energy (tanker tracking), security (dark vessel detection)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                              INGESTION                                    │
│                                                                           │
│  aisstream.io WebSocket ──► AIS Producer (Python)  ──► Kafka topics     │
│  (live global AIS feed)      local process (venv)       vessel-positions │
│                                                           vessel-metadata │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
              Kafka (local, KRaft mode, Docker)
              spark-sql-kafka native connector — push-based, no
              file-polling, no GCS landing zone
                               │
┌──────────────────────────────▼───────────────────────────────────────────┐
│               PROCESSING (Spark Structured Streaming)                     │
│               Continuous processes — not managed by Airflow               │
│                                                                           │
│  spark-bronze      ──► readStream(landing) ──► validate ──► GCS Bronze  │
│  [own container]       JSON files               min rules    Parquet     │
│                                                                           │
│  spark-silver-pos  ──► readStream(Bronze)  ──► enrich  ──► GCS Silver   │
│  [own container]       Parquet files            geo+deltas   Parquet     │
│                                                                           │
│  spark-silver-meta ──► readStream(landing) ──► normalize ──► GCS Silver │
│  [own container]       metadata JSON            type+dest    Parquet     │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                  BigQuery External Tables
                  (always in sync with GCS — no write step)
                               │
┌──────────────────────────────▼───────────────────────────────────────────┐
│                   ORCHESTRATION (Airflow — hourly)                        │
│                                                                           │
│  check_silver_data_arrived                                                │
│          ↓                                                                │
│  dbt run  ──► vessel_activity_summary · port_traffic · anomaly_candidates│
│          ↓                                                                │
│  dbt test ──► not_null · unique · accepted_values                        │
│          ↓                                                                │
│  compact_gcs  [daily 02:05 UTC — merge small Silver Parquet files]       │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────────────┐
│                              SERVING                                      │
│                                                                           │
│  FastAPI ──► Cloud Run ──► Dashboard WebSocket          [Phase 4]        │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| **Producer** | Python, local process | Runs locally alongside Kafka/Spark (Compute Engine VM removed after the Kafka migration) |
| **Messaging** | Apache Kafka (KRaft, local Docker) | Native `spark-sql-kafka` connector — push-based streaming, no landing zone or file-polling latency |
| **Processing** | Apache Spark 3.5 — Structured Streaming | True streaming with checkpointing, exactly-once semantics |
| **Lakehouse** | GCS Parquet + BigQuery External Tables | GCS as single source of truth, BQ reads directly |
| **Transformations** | dbt 1.8 | Versioned SQL, automatic lineage, data quality tests |
| **Orchestration** | Apache Airflow 2.8 | Orchestrates dbt Gold materialization hourly |
| **IaC** | Terraform | Fully reproducible infrastructure |
| **Containers** | Docker Compose | One container per Spark job — isolated checkpoints |
| **Secret Management** | GCP Secret Manager | API keys stored securely, injected at runtime |

---

## Project Structure

```
marineflow/
├── ingestion/
│   ├── ais_producer/           # Live WebSocket producer → Kafka
│   │   ├── main.py             # Infinite retry, graceful shutdown
│   │   ├── producer.py         # KafkaPublisher (confluent-kafka), keyed by mmsi
│   │   ├── parser.py           # Minimal: validate, normalize timestamp, route
│   │   ├── config.py           # Env var configuration
│   │   ├── Dockerfile          # For local development
│   │   └── requirements.txt
│   └── simulator/              # Synthetic fleet generator (fallback)
│       ├── main.py             # 8 real routes, GPS noise, anomalies
│       ├── Dockerfile
│       └── requirements.txt
│
├── processing/
│   ├── Dockerfile               # Custom Spark image (deps baked in at build)
│   └── spark_streaming/
│       ├── bronze_positions.py  # Kafka → Bronze (GCS + Kafka bronze topic)
│       ├── silver_positions.py  # Kafka bronze topic → Silver
│       ├── bronze_metadata.py   # Kafka → Bronze (GCS + Kafka bronze topic)
│       ├── silver_metadata.py   # Kafka bronze topic → Silver
│       └── requirements.txt
│
├── transformation/
│   └── dbt/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── packages.yml
│       ├── models/
│       │   ├── staging/
│       │   │   ├── sources.yml
│       │   │   └── stg_vessel_positions.sql  # positions + metadata join
│       │   └── gold/
│       │       ├── schema.yml                # data quality tests
│       │       ├── vessel_activity_summary.sql
│       │       ├── port_traffic.sql
│       │       └── anomaly_candidates.sql
│       └── macros/
│           └── safe_divide.sql
│
├── orchestration/
│   └── dags/
│       └── marineflow_pipeline.py  # Hourly dbt + daily compaction
│
├── infra/
│   └── terraform/
│       ├── main.tf
│       ├── variables.tf
│       ├── outputs.tf
│       └── modules/
│           ├── gcs/        # Buckets + lifecycle policies
│           ├── bigquery/   # Datasets + external + Gold tables
│           └── iam/        # Service account and roles
│
├── monitoring/
│   └── prometheus/
│       └── prometheus.yml
│
├── docker-compose.yml      # Spark (3 containers) + Airflow + Postgres
├── .env
└── README.md
```

---

## Pipeline Phases

### Phase 1 — AIS Ingestion

**Status: ✅ Complete**

The AIS Producer runs as a **local process** (`python main.py` in a venv), publishing directly to a local Kafka broker. It previously ran as a systemd service on a Compute Engine e2-micro VM publishing to Pub/Sub — that infrastructure (`infra/terraform/modules/compute/`) was removed once the Kafka migration made it obsolete. It can be reintroduced later if the pipeline moves off a fully local setup.

- `parser.py`: validates coordinates, normalizes ISO 8601 timestamp, routes `PositionReport` → `vessel-positions` and `ShipStaticData` → `vessel-metadata`. Publishes raw AIS JSON — zero transformation.
- `producer.py`: `confluent-kafka` producer, messages keyed by `mmsi` (same-vessel messages land on the same partition, preserving per-vessel order downstream), DLQ topic for failed messages.
- `main.py`: **infinite retry** with exponential backoff (2s → 60s) — WebSocket disconnections are expected and handled transparently.

**Running the producer:**
```bash
cd ingestion/ais_producer && python main.py
```

#### Kafka Topics

| Topic | Content |
|-------|---------|
| `vessel-positions` | Raw PositionReport (producer output) |
| `vessel-positions-bronze` | Flattened Bronze rows (Bronze output → Silver input) |
| `vessel-metadata` | Raw ShipStaticData (producer output) |
| `vessel-metadata-bronze` | Flattened Bronze rows (Bronze output → Silver input) |
| `dead-letter-queue` | Failed messages |

Bronze reads the raw topic, writes to GCS (permanent archive) **and** to its `-bronze` topic; Silver reads the `-bronze` topic directly — Kafka carries the stream end-to-end between layers, GCS is Bronze's durable record, not Silver's input.

Local single-broker Kafka (KRaft mode, no Zookeeper) via Docker Compose — see `docker-compose.yml`. Spark reads these topics directly with the native `spark-sql-kafka` connector, no landing zone in between.

---

### Phase 2 — Spark Bronze Layer

**Status: ✅ Complete**

Bronze reads from the `vessel-positions` Kafka topic using Structured Streaming's native Kafka source — push-based, no file polling.

```
Kafka topic vessel-positions
  {"Message": {"PositionReport": {...}}, "MessageType": "...", "MetaData": {...}}
        │
  spark.readStream.format("kafka") — push-based, native connector
        │
  extract_fields()           — flatten nested JSON, PositionReport only
  validate()                 — reject impossible lat/lon, null MMSI
  add_partition_and_lineage() — timestamps, _batch_id, partition columns
        │
  coalesce(1).write.append
        │
bronze/vessel_positions/partition_date=YYYY-MM-DD/partition_hour=HH/
```

Runs in dedicated container `marineflow-spark-bronze` — own JVM, own checkpoint.

---

### Phase 3 — Silver, Gold, dbt & Airflow

**Status: ✅ Complete**

#### Silver Positions

Reads Bronze Parquet via Structured Streaming. Transformations:

| Step | What it does |
|------|-------------|
| `rename_bronze_fields()` | Raw Bronze keys → semantic Silver names |
| `deduplicate()` | Keep latest per `(mmsi, event_timestamp)` |
| `enrich_geospatial()` | ocean_region, nearest_port, distance_to_port_km |
| `calculate_movement_deltas()` | speed_change_rate, heading_change_degrees |

`vessel_type_normalized` and `destination_clean` are **intentionally absent** — they come from `ShipStaticData`, live in `vessel_metadata`, and are joined in dbt staging.

#### Silver Metadata

Reads the `vessel-metadata` Kafka topic directly via Structured Streaming. Normalizes `vessel_type` (AIS int → category), `destination_clean`, `flag_country`.

#### External Tables — GCS as single source of truth

```
GCS bronze/vessel_positions/  ←→  BQ External: marineflow_bronze.vessel_positions_raw
GCS silver/vessel_positions/  ←→  BQ External: marineflow_silver.vessel_positions_clean
GCS silver/vessel_metadata/   ←→  BQ External: marineflow_silver.vessel_metadata
                                         ↓ dbt joins and aggregates
                                  BQ Native: marineflow_gold.*
```

Truncating a BQ table has no effect on data. `terraform apply` recreates any dropped table pointing at the same GCS data.

#### dbt Gold Models

Seven models across two tiers — batch aggregations and incremental event detection:

**Batch models** (full refresh each run):

| Model | Grain | Description |
|-------|-------|-------------|
| `vessel_activity_summary` | (mmsi, date_day) | Daily activity — distance, speed, AIS gaps, sharp turns |
| `port_traffic` | (nearest_port, date_day) | Daily port traffic — vessel types, entries, flag diversity |
| `anomaly_candidates` | (mmsi, date_day, alert_type) | Rule-based flags: ais_gap, speed_anomaly, dark_vessel, erratic_course |

**Incremental event detection models** (merge strategy, partition by event_date):

| Model | Grain | Description |
|-------|-------|-------------|
| `vessel_dark_events` | (mmsi, signal_recovered_at) | AIS blackout events >120 min — detects transponder tampering, EEZ crossings during gap |
| `vessel_speed_anomalies` | (mmsi, event_timestamp) | GPS spoofing, impossible speeds and sudden accelerations by vessel type |
| `vessel_loitering` | (mmsi, window_start) | Loitering sessions (0.1–4 knots, outside port, >3h) — STS transfer detection |
| `vessel_risk_score` | (mmsi, score_date) | 30-day weighted risk score aggregating all detection signals |

**Risk score weighting:**
```
GPS spoofing signals  × 15
STS transfer signals  × 12
Dark events           × 10
EEZ crossing gaps     × 8
Speed anomalies       × 5
Loitering events      × 3
```

**Model dependency graph:**
```
stg_vessel_positions
        │
        ├── vessel_activity_summary    ─┐
        ├── port_traffic               ─┤  (batch, parallel)
        ├── anomaly_candidates         ─┤
        ├── vessel_dark_events         ─┤
        ├── vessel_speed_anomalies     ─┤  (incremental, parallel)
        ├── vessel_loitering           ─┘
        │                               │
        └───────────────────────────────▼
                               vessel_risk_score
                               (depends on dark + speed + loitering)
```

`stg_vessel_positions` joins positions + metadata — single join point for all Gold models.

#### Airflow DAG — `marineflow_pipeline`

Schedule: `5 * * * *` (every hour at :05)

```
check_silver_data_arrived
        │
        ├── dbt_batch_models      (vessel_activity_summary, port_traffic, anomaly_candidates)
        ├── dbt_dark_events       ─┐
        ├── dbt_speed_anomalies   ─┤  parallel
        └── dbt_loitering         ─┘
                                   │
                           dbt_risk_score    (waits for dark + speed + loitering)
                                   │
                             dbt_test        (all models)
                                   │
                       [daily 02:05 UTC only]
                             compact_gcs
```

**Important design note:** Bronze and Silver Spark jobs run as **continuous Structured Streaming processes** — they are not launched by Airflow. Airflow only orchestrates downstream Gold materialization. In production, Spark jobs would run on Dataproc; locally they run in dedicated Docker containers.

---

### Phase 4 — API & Dashboard

**Status: ⏳ Pending**

FastAPI + WebSockets, world map dashboard, Cloud Run Service, Prometheus + Grafana.

---

## GCP Infrastructure (Terraform)

**GCP Project**: `marineflow-489815` | **Region**: `us-central1`

### Resources

**GCS** (`modules/gcs/`):
- `marineflow-lake-{project}` with lifecycle:
  - All data → Nearline 30d, Coldline 90d

**Kafka**: local only (Docker Compose, KRaft mode) — not provisioned by Terraform. Replaced the `pubsub` Terraform module, which was removed.

**BigQuery** (`modules/bigquery/`):
- `marineflow_bronze` — external table `vessel_positions_raw`
- `marineflow_silver` — external tables `vessel_positions_clean`, `vessel_metadata`
- `marineflow_gold` — native tables managed by dbt

**IAM** (`modules/iam/`):
- `marineflow-sa` with minimum required roles

---

## Local Setup

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11 | Producer (local dev), simulator |
| Java | 17+ | Spark (JVM) |
| Docker Desktop | 29+ | Spark containers + Airflow |
| gcloud CLI | 372+ | Auth, GCP operations |
| Terraform | 1.5+ | Infrastructure |
| dbt-bigquery | 1.8.2 | Gold transformations |

### Environment variables (.env)

```bash
# GCP
GCP_PROJECT_ID=marineflow-489815
GCS_BUCKET=marineflow-lake-marineflow-489815
GOOGLE_CLOUD_PROJECT=marineflow-489815

# ADC — mounted into Spark containers by Docker Compose
ADC_PATH=/Users/<your_user>/.config/gcloud/application_default_credentials.json

# AIS
AIS_API_KEY=<your aisstream.io key>

# Kafka (local broker via Docker Compose)
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_POSITIONS=vessel-positions
KAFKA_TOPIC_METADATA=vessel-metadata
KAFKA_TOPIC_DLQ=dead-letter-queue

# Pipeline
PIPELINE_VERSION=0.1.0
BQ_DATASET_BRONZE=marineflow_bronze
BQ_DATASET_SILVER=marineflow_silver

# Postgres / Airflow
POSTGRES_USER=marineflow
POSTGRES_PASSWORD=marineflow_dev_password
POSTGRES_DB=marineflow
```

### Local ports

| Service | Port | URL |
|---------|------|-----|
| Airflow UI | 4080 | http://localhost:4080 (admin/admin) |
| PostgreSQL | 5434 | localhost:5434 |
| Grafana | 3000 | http://localhost:3000 |
| Prometheus | 9090 | http://localhost:9090 |

---

## Design Decisions

### Kafka instead of Pub/Sub + GCS Cloud Storage Subscriptions

The original design used Pub/Sub with a Cloud Storage Subscription writing JSON files to a GCS landing zone, which Spark then read via its file-source streaming connector (poll-based). In practice this meant real latency between a message being published and a file actually being visible to Spark, plus an extra hop and small-files overhead. Spark's native `spark-sql-kafka` connector is push-based streaming against Kafka directly — no landing zone, no polling. Kafka runs locally (KRaft mode, Docker) alongside Spark. The `infra/terraform/modules/pubsub/` module was removed; Bronze/Silver-metadata now read Kafka topics directly (see Phase 1/2 above).

### VM for the AIS Producer — historical, removed

The original design ran the producer as a systemd service on a Compute Engine e2-micro VM (rationale: Cloud Run/Functions kill idle WebSocket connections after ~10 min; a VM doesn't). Since the migration to a local-only Kafka broker, the producer runs as a local process instead — a remote VM can't reach a broker that only exists on localhost. `infra/terraform/modules/compute/` was removed; the same rationale would apply again if the pipeline ever moves off a fully local setup.

### Structured Streaming + Checkpointing

Each Spark job uses `spark.readStream` with a GCS checkpoint. Exactly-once semantics, automatic resume on restart, no manual hour filtering or sleep intervals.

### Airflow orchestrates dbt only — not Spark

Bronze and Silver are continuous streaming processes, not batch jobs. Airflow is designed for tasks with defined start/end. Launching Structured Streaming jobs from Airflow would be an antipattern. In production, Spark would run on Dataproc (always-on cluster) or GKE.

### One Docker container per Spark job

Structured Streaming checkpoint lock prevents two streams in the same JVM. Dedicated containers (`spark-bronze`, `spark-silver-positions`, `spark-silver-metadata`) give each job isolation — `local[2]` each, sharing the host machine fairly.

### External tables for Bronze and Silver

GCS is the single source of truth. No BQ write step in Spark. Truncating a BQ table has no effect on data. Full recoverability from GCS.

### vessel_type + destination in vessel_metadata only

These fields come from `ShipStaticData`, not `PositionReport`. Storing them as nulls in `vessel_positions_clean` was an antipattern. They live in `vessel_metadata` and are joined in the dbt staging model.

---

## Known Limitations

### ~~Python dependencies not persisted in Docker~~ — resolved

Spark (`processing/Dockerfile`) and the Airflow scheduler (`orchestration/Dockerfile`) now bake their Python deps in at build time. `docker compose up --build` is enough — no more manual `docker exec pip install` after recreating containers.

### Spark in local mode

Each container runs `local[2]`. In production: Dataproc or GKE with proper cluster sizing.

### Small files accumulation

`coalesce(1/2)` limits file count per batch but files accumulate over time. The daily Airflow compaction task merges previous day's files. Delta Lake would handle this automatically in production.

### aisstream.io BETA

Service without guaranteed SLA. Infinite retry with exponential backoff handles transient disconnections. Simulator available as offline fallback.

---

## How to Run

### 1. Initial setup

```bash
git clone <repo-url>
cd marineflow
python3.11 -m venv venv      # pydantic-core (2.6.4) has no prebuilt wheel beyond 3.11/3.12 — pin the version
source venv/bin/activate      # Windows: .\venv\Scripts\Activate.ps1
cp .env.example .env

gcloud config configurations activate marineflow
gcloud auth application-default login
gcloud auth application-default set-quota-project marineflow-489815
```

### 2. GCP Infrastructure

```bash
cd infra/terraform
terraform init
terraform plan
terraform apply -var="repo_url=https://github.com/YOUR_USER/marineflow.git"

# Add API key to Secret Manager
echo -n "YOUR_AIS_API_KEY" | gcloud secrets versions add ais-api-key --data-file=- --project=marineflow-489815
```

### 3. Start Docker stack

```bash
# Spark containers (builds processing/Dockerfile the first time, cached after)
docker compose up spark-bronze spark-bronze-metadata spark-silver-positions spark-silver-metadata -d --build

# Airflow (builds orchestration/Dockerfile for the scheduler, dbt included)
docker compose up postgres airflow-init airflow-webserver airflow-scheduler -d --build
```

### 4. Start the producer (local)

```bash
cd ingestion/ais_producer && python main.py
```

*(A Compute Engine VM previously ran this in the cloud — removed after the Kafka migration, since a remote VM can't reach a local-only broker. See Design Decisions.)*

### 5. Verify messages are flowing (~10s after the producer starts)

```bash
docker exec marineflow-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic vessel-positions \
  --from-beginning --max-messages 5
```

### 6. Launch Spark jobs (four terminals)

```bash
# Bronze positions — Kafka vessel-positions -> GCS + Kafka vessel-positions-bronze
docker exec marineflow-spark-bronze /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS --conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true --conf spark.sql.streaming.metricsEnabled=true --conf spark.metrics.namespace=bronze_positions --jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar --driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar /opt/spark/processing/spark_streaming/bronze_positions.py

# Bronze metadata (separate terminal) — Kafka vessel-metadata -> GCS + Kafka vessel-metadata-bronze
docker exec marineflow-spark-bronze-metadata /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS --conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true --conf spark.sql.streaming.metricsEnabled=true --conf spark.metrics.namespace=bronze_metadata --jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar --driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar /opt/spark/processing/spark_streaming/bronze_metadata.py

# Silver positions (separate terminal) — reads Kafka vessel-positions-bronze
docker exec marineflow-spark-silver-positions /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS --conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true --conf spark.sql.streaming.metricsEnabled=true --conf spark.metrics.namespace=silver_positions --jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar --driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar /opt/spark/processing/spark_streaming/silver_positions.py

# Silver metadata (separate terminal) — reads Kafka vessel-metadata-bronze
docker exec marineflow-spark-silver-metadata /opt/spark/bin/spark-submit --master local[2] --driver-memory 1g --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS --conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true --conf spark.sql.streaming.metricsEnabled=true --conf spark.metrics.namespace=silver_metadata --jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar --driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar /opt/spark/processing/spark_streaming/silver_metadata.py
```

### 7. dbt Gold models

```bash
cd transformation/dbt
dbt deps
dbt run
dbt test
```

### 8. Airflow

Access http://localhost:4080 (admin/admin) and enable the `marineflow_pipeline` DAG.

### 9. Verify end-to-end

```bash
gcloud storage ls gs://marineflow-lake-marineflow-489815/bronze/vessel_positions/
gcloud storage ls gs://marineflow-lake-marineflow-489815/silver/vessel_positions/

bq query --use_legacy_sql=false "SELECT COUNT(*) FROM marineflow-489815.marineflow_gold.vessel_activity_summary"
bq query --use_legacy_sql=false "SELECT alert_type, severity, COUNT(*) as n FROM marineflow-489815.marineflow_gold.anomaly_candidates GROUP BY 1,2 ORDER BY 3 DESC"
```

---

*MarineFlow — Portfolio project*
*Stack: Python · Apache Kafka · Apache Spark · GCS · BigQuery · dbt · Airflow · Terraform · Secret Manager*