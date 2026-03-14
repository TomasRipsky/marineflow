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
   - [Phase 3 — Silver, Gold & dbt](#phase-3--silver-gold--dbt)
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
│  aisstream.io WebSocket ──► AIS Producer ──► Pub/Sub topics              │
│  (live global AIS feed)      (Python)         vessel-positions            │
│                                               vessel-metadata             │
│  Simulator (fallback)   ──► same topics                                  │
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
│                    PROCESSING (Spark Structured Streaming)                │
│                                                                           │
│  spark-bronze      ──► readStream(landing) ──► validate ──► GCS Bronze  │
│  [own container]       JSON files               min rules    Parquet     │
│                                                                           │
│  spark-silver-pos  ──► readStream(Bronze)  ──► enrich  ──► GCS Silver   │
│  [own container]       Parquet files            geo+deltas   Parquet     │
│                                                                           │
│  spark-silver-meta ──► readStream(landing) ──► normalize ──► GCS Silver │
│  [own container]       metadata JSON            type+dest    Parquet     │
│                                                                           │
│  Each job has its own Docker container and checkpoint — no conflicts.    │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                  BigQuery External Tables
                  (read directly from GCS — always in sync)
                               │
┌──────────────────────────────▼───────────────────────────────────────────┐
│                        GOLD + ANALYTICS (dbt)                             │
│                                                                           │
│  stg_vessel_positions    ──► joins positions + metadata on mmsi          │
│  vessel_activity_summary ──► daily activity per vessel                   │
│  port_traffic            ──► daily traffic per port                      │
│  anomaly_candidates      ──► rule-based flags (→ Isolation Forest Ph.4) │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────────────┐
│                           ML + SERVING                                    │
│                                                                           │
│  MLflow ──► Activity Classifier (XGBoost)             [Phase 4]          │
│         ──► Anomaly Detector (Isolation Forest)        [Phase 4]          │
│         ──► ETA Predictor (LSTM)                       [Phase 4]          │
│                                                                           │
│  FastAPI + Redis ──► Cloud Run ──► Dashboard WebSocket [Phase 5]         │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| **Messaging** | Google Cloud Pub/Sub | Managed, native GCP, Cloud Storage subscriptions eliminate pull code |
| **Landing** | GCS Cloud Storage Subscriptions | GCP writes Pub/Sub messages to GCS automatically — zero ingestion code in Spark |
| **Processing** | Apache Spark 3.5 — Structured Streaming | True streaming with checkpointing, exactly-once semantics, no manual loops |
| **Lakehouse** | GCS Parquet + BigQuery External Tables | GCS as single source of truth, BQ reads directly — no data duplication |
| **Transformations** | dbt 1.8 | Versioned SQL, automatic lineage, data quality tests |
| **Orchestration** | Apache Airflow | De facto standard for DE pipelines *(Phase 3 pending)* |
| **ML Tracking** | MLflow | Reproducible experiments, model registry *(Phase 4)* |
| **Serving** | FastAPI + Redis + Cloud Run | Modern API, cache, serverless *(Phase 5)* |
| **IaC** | Terraform | Reproducible and versioned infrastructure |
| **Containers** | Docker Compose | One container per Spark job — isolated checkpoints, no conflicts |

---

## Project Structure

```
marineflow/
├── ingestion/
│   ├── ais_producer/           # Live WebSocket producer → Pub/Sub
│   │   ├── main.py             # Entry point, retry, graceful shutdown
│   │   ├── producer.py         # PubSubPublisher with batching and DLQ
│   │   ├── parser.py           # Minimal: validate coords, normalize timestamp, route
│   │   ├── config.py           # Env var configuration
│   │   └── requirements.txt
│   └── simulator/              # Synthetic fleet generator (fallback)
│       ├── main.py             # 8 real shipping routes, GPS noise, anomalies
│       └── requirements.txt
│
├── processing/
│   └── spark_streaming/
│       ├── bronze_positions.py  # GCS landing → Bronze Parquet (Structured Streaming)
│       ├── silver_positions.py  # Bronze → Silver Parquet (Structured Streaming)
│       ├── silver_metadata.py   # GCS landing → Silver metadata Parquet
│       └── requirements.txt
│
├── transformation/
│   └── dbt/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── packages.yml
│       ├── models/
│       │   ├── staging/
│       │   │   ├── sources.yml              # Silver external tables as dbt sources
│       │   │   └── stg_vessel_positions.sql # positions + metadata join
│       │   └── gold/
│       │       ├── schema.yml               # data quality tests
│       │       ├── vessel_activity_summary.sql
│       │       ├── port_traffic.sql
│       │       └── anomaly_candidates.sql
│       └── macros/
│           └── safe_divide.sql
│
├── infra/
│   └── terraform/
│       ├── main.tf
│       ├── variables.tf
│       ├── outputs.tf
│       └── modules/
│           ├── gcs/        # Buckets + lifecycle policies
│           ├── pubsub/     # Topics + Cloud Storage subscriptions
│           ├── bigquery/   # Datasets + external tables + Gold native tables
│           └── iam/        # Service account and roles
│
├── monitoring/
│   └── prometheus/
│       └── prometheus.yml
│
├── docker-compose.yml      # Three dedicated Spark containers (bronze, silver-pos, silver-meta)
├── .env
└── README.md
```

---

## Pipeline Phases

### Phase 1 — AIS Ingestion

**Status: ✅ Complete**

**`ingestion/ais_producer/`** — Live producer

Connects via WebSocket to `wss://stream.aisstream.io/v0/stream`, validates and routes raw AIS messages to Pub/Sub.

- `parser.py`: minimal — validates coordinates, normalizes ISO 8601 timestamp, routes `PositionReport` → `vessel-positions` topic and `ShipStaticData` → `vessel-metadata` topic. Publishes the **raw AIS JSON as-is** — zero transformation. No Pydantic models, no field mapping.
- `producer.py`: `PubSubPublisher` with batching, publish callbacks, DLQ for failed messages.
- `main.py`: exponential retry with `tenacity`, SIGTERM/SIGINT graceful shutdown.

**`ingestion/simulator/`** — Synthetic fallback

Generates a realistic synthetic fleet for development and testing without depending on the external service. 8 real shipping routes, GPS noise, 5% anomalous vessels.

#### Pub/Sub Topics + Cloud Storage Subscriptions

| Topic | Content | GCS Landing Path |
|-------|---------|-----------------|
| `vessel-positions` | PositionReport (lat/lon/speed/heading) | `pubsub-landing/vessel-positions/` |
| `vessel-metadata` | ShipStaticData (name/type/flag/destination) | `pubsub-landing/vessel-metadata/` |
| `maritime-alerts` | Anomaly detector output | *(Phase 4)* |
| `dead-letter-queue` | Failed messages | *(monitoring)* |

GCP writes each Pub/Sub message as a JSON file to the landing path automatically — no code required. Files are auto-deleted after 2 days via GCS lifecycle policy.

---

### Phase 2 — Spark Bronze Layer

**Status: ✅ Complete**

Bronze is the **raw landing zone**. Data arrives exactly as from the source — minimal transformation, maximum fidelity.

#### Architecture

```
pubsub-landing/vessel-positions/*.json
  {"Message": {"PositionReport": {...}}, "MessageType": "...", "MetaData": {...}}
        │
        ▼ spark.readStream (detects new files automatically)
        │
  extract_fields()     — flatten nested JSON, filter PositionReport only
        │
  validate()           — reject lat > 90, lon > 180, null MMSI
        │
  add_partition_and_lineage()  — timestamps, partition_date/hour, _batch_id
        │
        ▼ coalesce(1).write.mode("append")
bronze/vessel_positions/partition_date=YYYY-MM-DD/partition_hour=HH/
  part-00000.snappy.parquet
```

**No envelope decoding.** The Cloud Storage subscription writes the raw AIS JSON directly — no base64, no wrapper. Spark reads it as plain JSON.

**Structured Streaming with checkpointing.** Spark tracks which files have been processed in `gs://bucket/checkpoints/bronze_positions/`. If Bronze restarts, it resumes from exactly where it left off — no duplicate processing, no lost data.

**One container per job.** Bronze runs in `marineflow-spark-bronze` — its own Docker container with its own JVM. This prevents checkpoint conflicts with Silver.

---

### Phase 3 — Silver, Gold & dbt

**Status: ✅ Complete**

#### Silver Positions (`silver_positions.py`)

Reads Bronze Parquet using Structured Streaming — no manual loops, no hour filtering. Spark checkpoint handles exactly-once semantics automatically.

Transformations in sequence:

| Step | Function | What it does |
|------|----------|-------------|
| 1 | `rename_bronze_fields()` | Raw Bronze keys → semantic Silver names (`MMSI`→`mmsi`, `Sog`→`speed_over_ground`, etc.) |
| 2 | `deduplicate()` | Removes duplicate `(mmsi, event_timestamp)` — keeps most recently ingested |
| 3 | `enrich_geospatial()` | Adds `ocean_region`, `nearest_port`, `is_in_port_zone`, `distance_to_port_km` |
| 4 | `calculate_movement_deltas()` | Computes `speed_change_rate` and `heading_change_degrees` vs previous message per vessel |

**Fields intentionally absent** from `vessel_positions_clean`: `vessel_type_normalized` and `destination_clean` — these come from `ShipStaticData`, not `PositionReport`. They live in `vessel_metadata` and are joined in dbt staging.

#### Silver Metadata (`silver_metadata.py`)

Reads `pubsub-landing/vessel-metadata/` using Structured Streaming. Applies:
- `vessel_type_normalized`: AIS integer → semantic category (`cargo`, `tanker`, `passenger`, etc.)
- `destination_clean`: strips junk AIS free text, normalizes to uppercase
- `flag_country`: derived from MMSI MID prefix

#### External Tables — GCS as single source of truth

All three Spark jobs write **only to GCS**. BigQuery reads directly via external tables:

```
GCS bronze/vessel_positions/  ←→  BQ External: marineflow_bronze.vessel_positions_raw
GCS silver/vessel_positions/  ←→  BQ External: marineflow_silver.vessel_positions_clean
GCS silver/vessel_metadata/   ←→  BQ External: marineflow_silver.vessel_metadata
                                         ↓ dbt reads and joins
                                  BQ Native: marineflow_gold.*
```

Benefits: always in sync, no BQ write step in Spark, truncating a BQ table has no effect on data, full recoverability from GCS.

#### dbt Gold Models

Three Gold models materialized as partitioned, clustered BigQuery native tables:

<details>
<summary><strong>vessel_activity_summary</strong> — daily activity per vessel</summary>

Grain: `(mmsi, date_day)`. Key metrics: `estimated_distance_km`, `avg_speed_knots`, `active_minutes`, `positions_in_port`, `ais_gaps_30min`, `ais_gaps_2hr`, `sudden_speed_changes`, `sharp_turns`. Partitioned by `date_day`, clustered by `mmsi`, `flag_country`.
</details>

<details>
<summary><strong>port_traffic</strong> — daily traffic per port</summary>

Grain: `(nearest_port, date_day)`. Key metrics: `unique_vessels`, `vessel_entries`, `cargo_vessels`, `tanker_vessels`, `distinct_flag_countries`, `sudden_manoeuvres`. Partitioned by `date_day`, clustered by `nearest_port`, `eez_country`.
</details>

<details>
<summary><strong>anomaly_candidates</strong> — rule-based anomaly flags</summary>

Grain: `(mmsi, date_day, alert_type)`. Four alert types: `ais_gap`, `speed_anomaly`, `dark_vessel`, `erratic_course`. Each with `low/medium/high` severity. Primary input for Isolation Forest in Phase 4. Alert ID is a deterministic surrogate key via `dbt_utils.generate_surrogate_key`.
</details>

#### dbt Staging

`stg_vessel_positions.sql` joins `vessel_positions_clean` with `vessel_metadata` on `mmsi` (most recent metadata per vessel). Single join point — all Gold models inherit `vessel_type_normalized` and `destination_clean` from here.

#### Data Quality Tests

| Model | Tests |
|-------|-------|
| `vessel_activity_summary` | `not_null`, `unique_combination_of_columns(mmsi, date_day)` |
| `port_traffic` | `not_null`, `unique_combination_of_columns(nearest_port, date_day)` |
| `anomaly_candidates` | `unique` + `not_null` on `alert_id`, `accepted_values` on `alert_type` and `severity` |

#### Small files mitigation

| Job | coalesce | Reason |
|-----|----------|--------|
| Bronze | `coalesce(1)` | One file per micro-batch — batches are small |
| Silver positions | `coalesce(2)` | Higher volume, multiple batches per day |
| Silver metadata | `coalesce(1)` | Metadata messages are sparse |

---

### Phase 4 — ML Models

**Status: ⏳ Pending**

Three models tracked with MLflow:
- **Activity Classifier (XGBoost)** — vessel activity state classification
- **Anomaly Detector (Isolation Forest)** — uses `anomaly_candidates` Gold table as labeled input
- **ETA Predictor (LSTM)** — time of arrival prediction

---

### Phase 5 — API & Dashboard

**Status: ⏳ Pending**

FastAPI + WebSockets, Redis cache, world map dashboard, Cloud Run deploy via Terraform, Prometheus + Grafana.

---

## GCP Infrastructure (Terraform)

All infrastructure defined as code in `infra/terraform/`. Fully reproducible with `terraform apply`.

**GCP Project**: `marineflow-489815` | **Region**: `us-central1`

### Resources

**GCS** (`modules/gcs/`):
- `marineflow-tfstate` — Terraform remote state
- `marineflow-lake-{project}` — data lake with lifecycle policies:
  - `pubsub-landing/` → deleted after 2 days
  - All data → Nearline after 30 days, Coldline after 90 days

**Pub/Sub** (`modules/pubsub/`):
- 5 topics with pull subscriptions
- 2 Cloud Storage subscriptions (`vessel-positions-gcs-sub`, `vessel-metadata-gcs-sub`) — write JSON to GCS landing automatically
- IAM: Pub/Sub service account granted `Storage Object Creator` + `Storage Legacy Bucket Reader` on the data lake bucket

**BigQuery** (`modules/bigquery/`):
- `marineflow_bronze` — external table `vessel_positions_raw`
- `marineflow_silver` — external tables `vessel_positions_clean`, `vessel_metadata`
- `marineflow_gold` — native tables managed by dbt
- `marineflow_features` — ML Feature Store (Phase 4)

**IAM** (`modules/iam/`):
- Service account `marineflow-sa` with minimum required roles

---

## Local Setup

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11 | Producer, simulator |
| Java | 17+ | Spark (JVM) |
| Docker Desktop | 29+ | Three dedicated Spark containers |
| gcloud CLI | 372+ | Auth and GCP operations |
| Terraform | 1.5+ | Infrastructure |
| dbt-bigquery | 1.8.2 | Gold transformations |

### Environment variables (.env)

```bash
# GCP
GCP_PROJECT_ID=marineflow-489815
GCS_BUCKET=marineflow-lake-marineflow-489815
GOOGLE_CLOUD_PROJECT=marineflow-489815

# ADC path — used by Docker Compose to mount credentials into Spark containers
ADC_PATH=C:\Users\usuario\AppData\Roaming\gcloud\application_default_credentials.json

# AIS (live producer only)
AIS_API_KEY=<your aisstream.io key>

# Pub/Sub
PUBSUB_TOPIC_POSITIONS=vessel-positions
PUBSUB_TOPIC_METADATA=vessel-metadata
PUBSUB_TOPIC_DLQ=dead-letter-queue

# Pipeline
PIPELINE_VERSION=0.1.0

# BigQuery
BQ_DATASET_BRONZE=marineflow_bronze
BQ_DATASET_SILVER=marineflow_silver

# MLflow / Postgres
POSTGRES_USER=marineflow
POSTGRES_PASSWORD=marineflow_dev_password
POSTGRES_DB=marineflow
```

### Local ports

| Service | Port | UI |
|---------|------|----|
| MLflow | 5001 | http://localhost:5001 |
| Grafana | 3000 | http://localhost:3000 |
| Prometheus | 9090 | http://localhost:9090 |

> **Note**: The Spark Master/Worker UI ports (4040/4041) are no longer exposed — each job runs in its own container in `local[2]` mode, accessible via `docker logs`.

---

## Design Decisions

### Parser: zero transformation, publish raw JSON

The parser's only job is validate coordinates, normalize the ISO 8601 timestamp, and route the message to the correct topic. It publishes the **raw aisstream.io JSON as-is** — no Pydantic models, no field mapping, no derived fields. All business logic starts in Silver.

### Pub/Sub → GCS via Cloud Storage Subscriptions

Instead of pulling Pub/Sub in Spark (the original approach), GCP Cloud Storage subscriptions write messages directly to GCS as JSON files. Spark reads those files with `readStream`. This eliminates:
- Pull loop in Spark (`while True` + `time.sleep`)
- Manual ack of messages
- Race conditions between Bronze write and Silver read
- The need for a BigQuery connector in Bronze

### Structured Streaming instead of micro-batch loops

All three Spark jobs use `spark.readStream` with checkpointing. Spark tracks processed files automatically — no manual hour filtering, no sleep intervals, no restart logic. Exactly-once semantics guaranteed by the checkpoint.

### One Docker container per Spark job

Spark Structured Streaming uses a checkpoint lock file to guarantee only one process reads a given stream at a time. Running two `spark-submit` commands in the same container caused `CONCURRENT_STREAM_LOG_UPDATE` errors. Solution: each job has its own container (`spark-bronze`, `spark-silver-positions`, `spark-silver-metadata`) with `local[2]` mode — 2 cores each, sharing the host machine fairly.

### External tables for Bronze and Silver

GCS is the single source of truth. BigQuery external tables read Parquet directly — no copy, always in sync, no BQ write step in Spark. If a BQ table is dropped, `terraform apply` recreates it pointing at the same GCS data.

### Bronze: zero business transformations

Bronze is the immutable raw clone of the source. Strict separation:

| Layer | Responsibility |
|-------|---------------|
| **Parser** | Validate coordinates, normalize timestamp, route to topic |
| **Bronze** | Flatten nested JSON, impossible coordinate filter, partition columns, lineage fields |
| **Silver** | All business logic: field renaming, nav status translation, flag country, geospatial enrichment, movement deltas |
| **Gold (dbt)** | Aggregations, metrics, anomaly flags |

### GCS lifecycle for landing dir

The `pubsub-landing/` prefix auto-deletes after 2 days via GCS lifecycle policy. No cleanup job needed. 2 days provides recovery margin if Bronze is temporarily down.

---

## Known Limitations

### aisstream.io BETA

Service in BETA without guaranteed SLA. Intermittent WebSocket issues encountered. Live producer verified working. Simulator available as offline fallback.

### Python dependencies not persisted in Docker

`pip install` in Spark containers is lost on container recreation. Will be resolved in Phase 5 with a custom `Dockerfile`.

```powershell
docker exec -u root marineflow-spark-bronze pip install `
  pyspark==3.5.0 google-cloud-storage==2.16.0 python-dotenv==1.0.1
```

### Spark in local mode

Each container runs `local[2]` — two cores per job, no real distribution. For production: Dataproc or Kubernetes with proper cluster sizing.

### Small files accumulation

`coalesce(1/2)` limits file count per batch but does not eliminate small files over time. A periodic compaction job or Delta Lake would handle this in production.

---

## How to Run

### 1. Initial setup

```bash
git clone <repo-url>
cd marineflow
python -m venv venv
source venv/bin/activate      # Windows: .\venv\Scripts\Activate.ps1
cp .env.example .env           # fill in your values

gcloud config configurations activate marineflow
gcloud auth application-default login
gcloud auth application-default set-quota-project marineflow-489815
```

### 2. GCP Infrastructure

```bash
cd infra/terraform
terraform init
terraform plan
terraform apply
```

### 3. Start Docker containers

```powershell
docker compose up spark-bronze spark-silver-positions spark-silver-metadata -d

# Install Python deps in each container
docker exec -u root marineflow-spark-bronze pip install `
  pyspark==3.5.0 google-cloud-storage==2.16.0 python-dotenv==1.0.1

docker exec -u root marineflow-spark-silver-positions pip install `
  pyspark==3.5.0 google-cloud-storage==2.16.0 python-dotenv==1.0.1

docker exec -u root marineflow-spark-silver-metadata pip install `
  pyspark==3.5.0 google-cloud-storage==2.16.0 python-dotenv==1.0.1
```

### 4. Start AIS producer

```powershell
cd ingestion/ais_producer
python main.py
# Or simulator: python ../simulator/main.py --vessels 20 --interval 3.0
```

### 5. Verify landing files arrive in GCS (~60s)

```powershell
gcloud storage ls gs://marineflow-lake-marineflow-489815/pubsub-landing/vessel-positions/
```

### 6. Launch Spark jobs (three separate terminals)

```powershell
# Terminal 1 — Bronze
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

# Terminal 2 — Silver positions
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

# Terminal 3 — Silver metadata
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

### 7. Run dbt Gold models

```powershell
cd transformation/dbt
dbt deps
dbt run
dbt test
```

### 8. Verify end-to-end

```bash
# GCS layers
gcloud storage ls gs://marineflow-lake-marineflow-489815/bronze/vessel_positions/
gcloud storage ls gs://marineflow-lake-marineflow-489815/silver/vessel_positions/

# BigQuery Gold
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) FROM marineflow-489815.marineflow_gold.vessel_activity_summary"
bq query --use_legacy_sql=false \
  "SELECT alert_type, COUNT(*) FROM marineflow-489815.marineflow_gold.anomaly_candidates GROUP BY 1"
```

---

*MarineFlow — Portfolio project*
*Stack: Python · Apache Spark · GCP Pub/Sub · GCS · BigQuery · dbt · Airflow · MLflow · FastAPI · Terraform*