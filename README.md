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
   - [Phase 4 — ML Models](#phase-4--ml-models) *(pending)*
   - [Phase 5 — API & Dashboard](#phase-5--api--dashboard) *(pending)*
6. [GCP Infrastructure (Terraform)](#gcp-infrastructure-terraform)
7. [Local Setup](#local-setup)
8. [Design Decisions](#design-decisions)
9. [Known Limitations](#known-limitations)
10. [How to Run](#how-to-run)

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
│  aisstream.io WebSocket ──► AIS Producer (Python)  ──► Pub/Sub topics   │
│  (live global AIS feed)      Compute Engine e2-micro   vessel-positions  │
│                              systemd service            vessel-metadata   │
│                              auto-restart on failure                      │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
              GCP Cloud Storage Subscriptions
              (GCP writes JSON files automatically — no code)
                               │
              pubsub-landing/vessel-positions/*.json
              pubsub-landing/vessel-metadata/*.json
              (auto-deleted after 2 days via GCS lifecycle)
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
│                           ML + SERVING                                    │
│                                                                           │
│  MLflow ──► Anomaly Detector (Isolation Forest)       [Phase 4]          │
│         ──► Activity Classifier (XGBoost)             [Phase 4]          │
│         ──► ETA Predictor (LSTM)                      [Phase 4]          │
│                                                                           │
│  FastAPI + Redis ──► Cloud Run ──► Dashboard WebSocket [Phase 5]         │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| **Producer** | Compute Engine e2-micro + systemd | WebSocket long-lived connections require no TCP idle timeouts — VM is the right tool vs Cloud Run/Functions |
| **Messaging** | Google Cloud Pub/Sub | Managed, native GCP integration |
| **Landing** | GCS Cloud Storage Subscriptions | GCP writes Pub/Sub messages to GCS automatically — zero ingestion code in Spark |
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
│   ├── ais_producer/           # Live WebSocket producer → Pub/Sub
│   │   ├── main.py             # Infinite retry, graceful shutdown
│   │   ├── producer.py         # PubSubPublisher with batching and DLQ
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
│   └── spark_streaming/
│       ├── bronze_positions.py  # GCS landing → Bronze (Structured Streaming)
│       ├── silver_positions.py  # Bronze → Silver (Structured Streaming)
│       ├── silver_metadata.py   # GCS landing → Silver metadata
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
│           ├── pubsub/     # Topics + Cloud Storage subscriptions
│           ├── bigquery/   # Datasets + external + Gold tables
│           ├── compute/    # e2-micro VM for AIS producer
│           └── iam/        # Service account and roles
│
├── monitoring/
│   └── prometheus/
│       └── prometheus.yml
│
├── docker-compose.yml      # Spark (3 containers) + Airflow + Postgres + Redis
├── .env
└── README.md
```

---

## Pipeline Phases

### Phase 1 — AIS Ingestion

**Status: ✅ Complete**

The AIS Producer runs as a **systemd service on a Compute Engine e2-micro VM** — not in Cloud Run or Cloud Functions. This is an intentional design decision: WebSocket connections require no TCP idle timeouts. Cloud Run kills idle TCP connections after ~10 minutes regardless of WebSocket ping configuration. A VM has no such constraint.

- `parser.py`: validates coordinates, normalizes ISO 8601 timestamp, routes `PositionReport` → `vessel-positions` and `ShipStaticData` → `vessel-metadata`. Publishes raw AIS JSON — zero transformation.
- `producer.py`: batching, DLQ for failed messages.
- `main.py`: **infinite retry** with exponential backoff (2s → 60s) — WebSocket disconnections are expected and handled transparently.

**Starting/stopping the VM:**
```powershell
# Stop (no VM charges while stopped, only disk ~$0.04/mo)
gcloud compute instances stop marineflow-ais-producer --zone=us-central1-a --project=marineflow-489815

# Start (systemd auto-starts the producer on boot)
gcloud compute instances start marineflow-ais-producer --zone=us-central1-a --project=marineflow-489815
```

**Viewing logs:**
```powershell
gcloud compute ssh marineflow-ais-producer --zone=us-central1-a --project=marineflow-489815 --tunnel-through-iap --command="sudo journalctl -u ais-producer -f"
```

#### Pub/Sub Topics + Cloud Storage Subscriptions

| Topic | Content | GCS Landing Path |
|-------|---------|-----------------|
| `vessel-positions` | PositionReport | `pubsub-landing/vessel-positions/` |
| `vessel-metadata` | ShipStaticData | `pubsub-landing/vessel-metadata/` |
| `dead-letter-queue` | Failed messages | *(monitoring)* |

GCS subscription writes each message as a JSON file automatically. Files auto-delete after 2 days via lifecycle policy.

---

### Phase 2 — Spark Bronze Layer

**Status: ✅ Complete**

Bronze reads from `pubsub-landing/` using Structured Streaming — detects new JSON files automatically via checkpoint.

```
pubsub-landing/vessel-positions/*.json
  {"Message": {"PositionReport": {...}}, "MessageType": "...", "MetaData": {...}}
        │
  spark.readStream (new files detected automatically)
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

Reads `pubsub-landing/vessel-metadata/` via Structured Streaming. Normalizes `vessel_type` (AIS int → category), `destination_clean`, `flag_country`.

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

### Phase 4 — ML Models

**Status: ⏳ Pending**

Three models tracked with MLflow:
- **Anomaly Detector (Isolation Forest)** — continuous anomaly score from `anomaly_candidates`
- **Activity Classifier (XGBoost)** — vessel state: in_transit, fishing, anchored, port_manoeuvre
- **ETA Predictor (LSTM)** — time of arrival prediction from position sequences

---

### Phase 5 — API & Dashboard

**Status: ⏳ Pending**

FastAPI + WebSockets, Redis cache, world map dashboard, Cloud Run Service, Prometheus + Grafana.

---

## GCP Infrastructure (Terraform)

**GCP Project**: `marineflow-489815` | **Region**: `us-central1`

### Resources

**GCS** (`modules/gcs/`):
- `marineflow-lake-{project}` with lifecycle:
  - `pubsub-landing/` → deleted after 2 days
  - All data → Nearline 30d, Coldline 90d

**Pub/Sub** (`modules/pubsub/`):
- 5 topics + pull subscriptions
- 2 Cloud Storage subscriptions (`vessel-positions-gcs-sub`, `vessel-metadata-gcs-sub`)
- IAM: Pub/Sub SA → `Storage Object Creator` + `Storage Legacy Bucket Reader`

**Compute** (`modules/compute/`):
- `marineflow-ais-producer` — e2-micro VM, Debian 12
- systemd service auto-starts producer on boot
- API key injected from Secret Manager at startup
- SSH access via IAP tunnel only (no direct internet exposure)
- Free tier eligible (1 e2-micro/month in us-* regions)

**BigQuery** (`modules/bigquery/`):
- `marineflow_bronze` — external table `vessel_positions_raw`
- `marineflow_silver` — external tables `vessel_positions_clean`, `vessel_metadata`
- `marineflow_gold` — native tables managed by dbt
- `marineflow_features` — ML Feature Store (Phase 4)

**IAM** (`modules/iam/`):
- `marineflow-sa` with minimum required roles

**Secret Manager:**
- `ais-api-key` — aisstream.io API key, injected into VM at boot

---

## Local Setup

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11 | Producer (local dev), simulator |
| Java | 17+ | Spark (JVM) |
| Docker Desktop | 29+ | Spark containers + Airflow |
| gcloud CLI | 372+ | Auth, GCP operations, VM SSH |
| Terraform | 1.5+ | Infrastructure |
| dbt-bigquery | 1.8.2 | Gold transformations |

### Environment variables (.env)

```bash
# GCP
GCP_PROJECT_ID=marineflow-489815
GCS_BUCKET=marineflow-lake-marineflow-489815
GOOGLE_CLOUD_PROJECT=marineflow-489815

# ADC — mounted into Spark containers by Docker Compose
ADC_PATH=C:\Users\usuario\AppData\Roaming\gcloud\application_default_credentials.json

# AIS (local producer only — VM uses Secret Manager)
AIS_API_KEY=<your aisstream.io key>

# Pub/Sub
PUBSUB_TOPIC_POSITIONS=vessel-positions
PUBSUB_TOPIC_METADATA=vessel-metadata
PUBSUB_TOPIC_DLQ=dead-letter-queue

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
| MLflow | 5001 | http://localhost:5001 |
| Grafana | 3000 | http://localhost:3000 |
| Prometheus | 9090 | http://localhost:9090 |

> **Windows / Hyper-V**: ports 8064–8763 are reserved. All service ports are assigned below 8064.

---

## Design Decisions

### VM for the AIS Producer, not Cloud Run

Cloud Run Jobs and Cloud Functions have TCP idle timeouts (~10 min) that kill WebSocket connections regardless of `ping_interval`. A Compute Engine e2-micro has no such constraint, costs ~$0/month on the free tier, and systemd provides robust process management with automatic restart on failure.

### Pub/Sub → GCS via Cloud Storage Subscriptions

GCP writes messages directly to GCS as JSON files — no pull loop, no manual ack, no race conditions in Spark. Eliminates the most fragile part of the original design.

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

### Python dependencies not persisted in Docker

`pip install` in Spark and Airflow containers is lost on container recreation. Phase 5 will resolve this with custom Dockerfiles baked into the images.

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
python -m venv venv
source venv/bin/activate      # Windows: .\venv\Scripts\Activate.ps1
cp .env.example .env

gcloud config configurations activate marineflow
gcloud auth application-default login
gcloud auth application-default set-quota-project marineflow-489815
```

### 2. GCP Infrastructure

```powershell
cd infra/terraform
terraform init
terraform plan
terraform apply -var="repo_url=https://github.com/YOUR_USER/marineflow.git"

# Add API key to Secret Manager
echo -n "YOUR_AIS_API_KEY" | gcloud secrets versions add ais-api-key --data-file=- --project=marineflow-489815
```

### 3. Start Docker stack

```powershell
# Spark containers
docker compose up spark-bronze spark-silver-positions spark-silver-metadata -d

# Airflow
docker compose up postgres airflow-webserver airflow-scheduler -d

# Install deps in Spark containers
docker exec -u root marineflow-spark-bronze pip install pyspark==3.5.0 google-cloud-storage==2.16.0 python-dotenv==1.0.1
docker exec -u root marineflow-spark-silver-positions pip install pyspark==3.5.0 google-cloud-storage==2.16.0 python-dotenv==1.0.1
docker exec -u root marineflow-spark-silver-metadata pip install pyspark==3.5.0 google-cloud-storage==2.16.0 python-dotenv==1.0.1

# Install dbt in Airflow scheduler
docker exec -u airflow marineflow-airflow-scheduler python -m pip install dbt-bigquery==1.8.2 google-cloud-bigquery==3.13.0
```

### 4. Start VM producer (or local for dev)

```powershell
# Start the GCP VM (producer auto-starts via systemd)
gcloud compute instances start marineflow-ais-producer --zone=us-central1-a --project=marineflow-489815

# Or run locally for development
cd ingestion/ais_producer && python main.py
```

### 5. Verify landing files (~60s after producer starts)

```powershell
gcloud storage ls gs://marineflow-lake-marineflow-489815/pubsub-landing/vessel-positions/
```

### 6. Launch Spark jobs (three terminals)

```powershell
# Bronze
docker exec marineflow-spark-bronze /opt/spark/bin/spark-submit `
  --master local[2] --driver-memory 2g `
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem `
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS `
  --conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT `
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 `
  --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true `
  --jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar `
  --driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar `
  /opt/spark/processing/spark_streaming/bronze_positions.py

# Silver positions (separate terminal)
docker exec marineflow-spark-silver-positions /opt/spark/bin/spark-submit `
  --master local[2] --driver-memory 2g `
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem `
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS `
  --conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT `
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 `
  --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true `
  --jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar `
  --driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar `
  /opt/spark/processing/spark_streaming/silver_positions.py

# Silver metadata (separate terminal)
docker exec marineflow-spark-silver-metadata /opt/spark/bin/spark-submit `
  --master local[2] --driver-memory 2g `
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem `
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS `
  --conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT `
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 `
  --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true `
  --jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar `
  --driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar `
  /opt/spark/processing/spark_streaming/silver_metadata.py
```

### 7. dbt Gold models

```powershell
cd transformation/dbt
dbt deps
dbt run
dbt test
```

### 8. Airflow

Access http://localhost:4080 (admin/admin) and enable the `marineflow_pipeline` DAG.

### 9. Verify end-to-end

```powershell
gcloud storage ls gs://marineflow-lake-marineflow-489815/bronze/vessel_positions/
gcloud storage ls gs://marineflow-lake-marineflow-489815/silver/vessel_positions/

bq query --use_legacy_sql=false "SELECT COUNT(*) FROM marineflow-489815.marineflow_gold.vessel_activity_summary"
bq query --use_legacy_sql=false "SELECT alert_type, severity, COUNT(*) as n FROM marineflow-489815.marineflow_gold.anomaly_candidates GROUP BY 1,2 ORDER BY 3 DESC"
```

---

*MarineFlow — Portfolio project*
*Stack: Python · Apache Spark · GCP Pub/Sub · GCS · BigQuery · dbt · Airflow · Terraform · Compute Engine · Secret Manager*