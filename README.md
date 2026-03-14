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
┌──────────────────────────────────────────────────────────────────────┐
│                            INGESTION                                  │
│                                                                       │
│  aisstream.io WebSocket ──► AIS Producer (Python) ──► Pub/Sub        │
│  (live global AIS feed)      ingestion/ais_producer                  │
│                                                                       │
│  Simulator (fallback)   ──► 8 synthetic routes   ──► Pub/Sub        │
│  (dev / testing)              ingestion/simulator                    │
└──────────────────────────────┬────────────────────────────────────────┘
                               │
                     vessel-positions topic
                     vessel-metadata topic
                               │
┌──────────────────────────────▼────────────────────────────────────────┐
│                       PROCESSING (Spark)                               │
│                                                                        │
│  bronze_positions.py ──► Pull Pub/Sub ──► Validate ──► GCS Parquet   │
│  (micro-batch)            Python client    min rules    Bronze layer  │
│                                                                        │
│  silver_positions.py ──► Read Bronze ──► Rename + Enrich ──► GCS    │
│  (hourly, closed hour)                    geo + deltas    Silver layer│
│                                                                        │
│  silver_metadata.py  ──► Pull Pub/Sub ──► Normalize ──► GCS Parquet  │
│  (vessel-metadata topic)  ShipStaticData   type+dest    Silver layer  │
└──────────────────────────────┬────────────────────────────────────────┘
                               │
                    BigQuery External Tables
                    (read directly from GCS Parquet)
                               │
┌──────────────────────────────▼────────────────────────────────────────┐
│                      GOLD + ANALYTICS (dbt)                            │
│                                                                        │
│  stg_vessel_positions  ──► joins positions + metadata on mmsi         │
│  vessel_activity_summary ──► daily activity per vessel                 │
│  port_traffic          ──► daily traffic per port                      │
│  anomaly_candidates    ──► rule-based anomaly flags (→ ML Phase 4)    │
└──────────────────────────────┬────────────────────────────────────────┘
                               │
┌──────────────────────────────▼────────────────────────────────────────┐
│                          ML + SERVING                                  │
│                                                                        │
│  MLflow ──► Activity Classifier (XGBoost)                             │
│         ──► Anomaly Detector (Isolation Forest)                        │
│         ──► ETA Predictor (LSTM)                                       │
│                                                                        │
│  FastAPI + Redis ──► Cloud Run ──► Dashboard WebSocket                │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| **Messaging** | Google Cloud Pub/Sub | Managed, no infra to maintain, native GCP integration |
| **Processing** | Apache Spark 3.5 (Docker) | Industry standard for batch/streaming, essential in DE portfolios |
| **Lakehouse** | GCS Parquet + BigQuery External Tables | GCS as source of truth, BQ reads directly — no data duplication |
| **Transformations** | dbt 1.8 | Versioned SQL, automatic lineage, data quality tests |
| **Orchestration** | Apache Airflow | De facto standard for DE pipelines |
| **ML Tracking** | MLflow | Reproducible experiments, model registry |
| **Serving** | FastAPI + Redis + Cloud Run | Modern API, cache, serverless |
| **IaC** | Terraform | Reproducible and versioned infrastructure |
| **Monitoring** | Prometheus + Grafana | Pipeline operational metrics |
| **Containers** | Docker Compose | Reproducible local environment |

---

## Project Structure

```
marineflow/
├── ingestion/
│   ├── ais_producer/           # Live WebSocket producer → Pub/Sub
│   │   ├── main.py             # Entry point, exponential retry, graceful shutdown
│   │   ├── producer.py         # PubSubPublisher with batching and DLQ
│   │   ├── parser.py           # Minimal AIS parser — zero business logic
│   │   ├── config.py           # Env var configuration
│   │   └── requirements.txt
│   └── simulator/              # Synthetic fleet generator
│       ├── main.py             # 8 real shipping routes, GPS noise, anomalies
│       └── requirements.txt
│
├── processing/
│   └── spark_streaming/
│       ├── bronze_positions.py # Pub/Sub → GCS Bronze (raw clone)
│       ├── silver_positions.py # Bronze → GCS Silver (enriched, hourly)
│       ├── silver_metadata.py  # vessel-metadata topic → GCS Silver
│       ├── diagnose_bronze.py  # diagnostic utility
│       └── requirements.txt
│
├── transformation/
│   └── dbt/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── packages.yml
│       ├── models/
│       │   ├── staging/
│       │   │   ├── sources.yml              # Silver external tables as sources
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
│           ├── gcs/        # Bronze/Silver/Gold buckets
│           ├── pubsub/     # Topics and subscriptions
│           ├── bigquery/   # Datasets + external tables
│           └── iam/        # Service account and roles
│
├── monitoring/
│   └── prometheus/
│       └── prometheus.yml
│
├── docker-compose.yml
├── .env
└── README.md
```

---

## Pipeline Phases

### Phase 1 — AIS Ingestion

**Status: ✅ Complete**

**`ingestion/ais_producer/`** — Live producer

Connects via WebSocket to `wss://stream.aisstream.io/v0/stream`, parses AIS messages and publishes to Pub/Sub.

- `parser.py`: minimal ingestion layer — validates coordinates, normalizes ISO 8601 timestamps, preserves all raw field names unchanged. Zero business logic. Routes `PositionReport` → `vessel-positions` topic, `ShipStaticData` → `vessel-metadata` topic.
- `producer.py`: `PubSubPublisher` with batching (up to 100 messages/batch), publish callbacks and Dead Letter Queue for failed messages.
- `main.py`: exponential retry with `tenacity`, SIGTERM/SIGINT graceful shutdown.

**`ingestion/simulator/`** — Synthetic fallback

Generates a realistic synthetic fleet for development and testing without depending on the external service.

- 8 real shipping routes (Trans-Atlantic, Asia-Europe Suez, Trans-Pacific, etc.)
- Linear interpolation between waypoints with simulated GPS noise
- 5% anomalous vessels (impossible speeds, AIS gaps) for anomaly detection training

#### Pub/Sub Topics

| Topic | Content | Spark Subscription |
|-------|---------|-------------------|
| `vessel-positions` | PositionReport (lat/lon/speed/heading) | `vessel-positions-spark-sub` |
| `vessel-metadata` | ShipStaticData (name/type/flag/destination) | `vessel-metadata-spark-sub` |
| `maritime-alerts` | Anomaly detector output | `maritime-alerts-spark-sub` |
| `dead-letter-queue` | Failed messages for reprocessing | `dead-letter-queue-spark-sub` |

---

### Phase 2 — Spark Bronze Layer

**Status: ✅ Complete**

Bronze is the **raw landing zone**. Data arrives exactly as from the source — minimal transformation, maximum fidelity. If something fails in upper layers, you can always reprocess from Bronze.

#### Bronze philosophy

Bronze has four clearly separated responsibilities:

| Function | What it does |
|----------|-------------|
| `build_dataframe()` | Aligns message dicts to the Spark schema — fills missing fields with None, drops extra fields |
| `validate()` | Filters only physically impossible records (lat > 90, lon > 180, null MMSI) |
| `add_partition_columns()` | Casts ISO 8601 strings to TimestampType, derives `partition_date` and `partition_hour` |
| `add_lineage()` | Attaches `_batch_id`, `_source_file`, `_pipeline_version` to every record |

**No business transformations.** Field names match the original aisstream.io JSON keys exactly. All enrichments (flag country, nav status translation, vessel type) happen in Silver.

#### GCS partition layout

```
bronze/vessel_positions/
  partition_date=2026-03-13/
    partition_hour=14/
      part-00000.snappy.parquet   ← coalesce(1): one file per batch
    partition_hour=15/
      part-00000.snappy.parquet
```

#### Delay design

Bronze writes directly to each hour partition using `mode("overwrite")`. Silver **excludes the current hour** while Bronze is actively writing to it, avoiding `FileNotFoundError` race conditions. This is intentional — Silver processes fully closed partitions (max delay: 1 hour). Airflow (Phase 3) triggers Silver once per hour over the previous hour's partition.

---

### Phase 3 — Silver, Gold & dbt

**Status: ✅ Complete**

#### Silver Positions (`silver_positions.py`)

Reads Bronze Parquet from GCS and applies all business transformations in a fixed sequence:

| Step | Function | What it does |
|------|----------|-------------|
| 1 | `rename_bronze_fields()` | Renames raw Bronze keys to semantic Silver names (`MMSI` → `mmsi`, `Sog` → `speed_over_ground`, etc.) |
| 2 | `deduplicate()` | Removes duplicate `(mmsi, event_timestamp)` — keeps most recently ingested |
| 3 | `enrich_geospatial()` | Adds `ocean_region`, `nearest_port`, `is_in_port_zone`, `distance_to_port_km` |
| 4 | `calculate_movement_deltas()` | Computes `speed_change_rate` and `heading_change_degrees` vs previous message per vessel |

**Fields intentionally excluded** from `vessel_positions_clean`: `vessel_type_normalized` and `destination_clean` — these come from `ShipStaticData`, not `PositionReport`. They live in `vessel_metadata` and are joined in the dbt staging model.

#### Silver Metadata (`silver_metadata.py`)

Reads `vessel-metadata` Pub/Sub topic (ShipStaticData messages) and writes to `silver/vessel_metadata/` in GCS. Applies:
- `vessel_type_normalized`: raw AIS integer → semantic category (`cargo`, `tanker`, `passenger`, etc.)
- `destination_clean`: strips junk AIS text, normalizes to uppercase
- `flag_country`: derived from MMSI MID prefix

#### External Tables vs Native Tables

Bronze and Silver are exposed to BigQuery as **external tables** — BigQuery reads directly from GCS Parquet without copying data. This means:

- **GCS is the single source of truth** — truncating a BQ table has no effect on the data
- **Always in sync** — any new file written by Spark is immediately queryable in BQ
- **No write step** — Spark jobs only write to GCS; the BQ step has been eliminated from all three jobs
- **Recoverability** — if a BQ table is dropped, `terraform apply` recreates it pointing at the same GCS data

Gold tables (managed by dbt) remain **native BQ tables** because dbt needs DML (`INSERT OVERWRITE`) to materialize them.

```
GCS bronze/vessel_positions/  ←→  BQ External: marineflow_bronze.vessel_positions_raw
GCS silver/vessel_positions/  ←→  BQ External: marineflow_silver.vessel_positions_clean
GCS silver/vessel_metadata/   ←→  BQ External: marineflow_silver.vessel_metadata
                                          ↓ dbt reads
                                   BQ Native: marineflow_gold.*
```

#### dbt Gold Models

Three Gold models materialized as partitioned, clustered BigQuery tables:

<details>
<summary><strong>vessel_activity_summary</strong> — daily activity per vessel</summary>

Grain: `(mmsi, date_day)`. Answers: how far did vessel X travel today? How long was it in port vs at sea? Did it show anomalous behaviour?

Key metrics: `estimated_distance_km`, `avg_speed_knots`, `active_minutes`, `positions_in_port`, `ais_gaps_30min`, `ais_gaps_2hr`, `sudden_speed_changes`, `sharp_turns`.

Partitioned by `date_day`, clustered by `mmsi`, `flag_country`.
</details>

<details>
<summary><strong>port_traffic</strong> — daily traffic per port</summary>

Grain: `(nearest_port, date_day)`. Answers: how many vessels passed through Rotterdam today? What vessel types dominate Singapore?

Key metrics: `unique_vessels`, `vessel_entries`, `cargo_vessels`, `tanker_vessels`, `distinct_flag_countries`, `sudden_manoeuvres`.

Partitioned by `date_day`, clustered by `nearest_port`, `eez_country`.
</details>

<details>
<summary><strong>anomaly_candidates</strong> — rule-based anomaly flags</summary>

Grain: `(mmsi, date_day, alert_type)`. Primary input for the Isolation Forest anomaly detector in Phase 4.

Four alert types: `ais_gap` (>2hr silence), `speed_anomaly` (sudden extreme speed change), `dark_vessel` (moving + AIS gaps), `erratic_course` (repeated sharp turns). Each with `low/medium/high` severity.

Alert ID is a deterministic surrogate key via `dbt_utils.generate_surrogate_key`.
</details>

#### dbt Staging

`stg_vessel_positions.sql` joins `vessel_positions_clean` with `vessel_metadata` on `mmsi` (taking the most recent metadata per vessel). This is the only place the join logic lives — all Gold models inherit `vessel_type_normalized` and `destination_clean` from here.

#### Data Quality Tests

dbt tests run automatically after every `dbt run`:

| Model | Tests |
|-------|-------|
| `vessel_activity_summary` | `not_null` on key fields, `unique_combination_of_columns(mmsi, date_day)` |
| `port_traffic` | `not_null` on key fields, `unique_combination_of_columns(nearest_port, date_day)` |
| `anomaly_candidates` | `unique` + `not_null` on `alert_id`, `accepted_values` on `alert_type` and `severity` |

#### Small files mitigation

Each Spark batch generates Parquet files. Without mitigation, hundreds of small files accumulate per partition. Implemented `coalesce()` on all three jobs:

- Bronze: `coalesce(1)` — one file per batch (batches are small, ~500 messages)
- Silver positions: `coalesce(2)` — two files per daily partition (higher volume, multiple batches)
- Silver metadata: `coalesce(1)` — metadata messages are sparse

In production, a periodic compaction job (or Delta Lake) would handle this automatically.

---

### Phase 4 — ML Models

**Status: ⏳ Pending**

Three models planned, all tracked with MLflow:

**1. Activity Classifier (XGBoost)** — classifies vessel activity state: in transit, fishing, waiting, port manoeuvre. Features: speed, heading, vessel type, geographic zone.

**2. Anomaly Detector (Isolation Forest)** — detects anomalous behaviour using `anomaly_candidates` Gold table as input signal. Publishes alerts to `maritime-alerts` Pub/Sub topic.

**3. ETA Predictor (LSTM)** — predicts time of arrival to port using historical speed, typical routes. Weekly retraining via Airflow.

---

### Phase 5 — API & Dashboard

**Status: ⏳ Pending**

- FastAPI with WebSockets for real-time position streaming
- Redis as cache for last known positions
- World map dashboard of naval traffic
- Cloud Run deploy via Terraform
- Prometheus + Grafana for operational metrics

---

## GCP Infrastructure (Terraform)

All infrastructure defined as code in `infra/terraform/`. Fully reproducible with `terraform apply`.

**GCP Project**: `marineflow-489815` | **Region**: `us-central1`

### Resources

**GCS** (`modules/gcs/`):
- `marineflow-tfstate` — Terraform remote state
- `marineflow-lake-{project}` — data lake: `bronze/`, `silver/`, `gold/`, `checkpoints/`, `models/`, `schemas/`

**Pub/Sub** (`modules/pubsub/`):
- 5 topics with subscriptions: `vessel-positions`, `vessel-metadata`, `maritime-alerts`, `port-events`, `dead-letter-queue`

**BigQuery** (`modules/bigquery/`):
- `marineflow_bronze` — external table `vessel_positions_raw` → GCS Bronze
- `marineflow_silver` — external tables `vessel_positions_clean`, `vessel_metadata` → GCS Silver
- `marineflow_gold` — native tables managed by dbt: `vessel_activity_summary`, `port_traffic`, `anomaly_candidates`
- `marineflow_features` — ML Feature Store (Phase 4)

**IAM** (`modules/iam/`):
- Service account `marineflow-sa` with minimum required roles

### Authentication: ADC instead of Service Account Keys

The GCP org has `constraints/iam.disableServiceAccountKeyCreation` policy enabled. Instead of keys we use **Application Default Credentials (ADC)** — the Google-recommended approach for local development.

```bash
gcloud auth application-default login
gcloud auth application-default set-quota-project marineflow-489815
```

---

## Local Setup

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11 | Producer, simulator, Spark jobs |
| Java | 17+ | Spark (JVM) |
| Docker Desktop | 29+ | Spark container |
| gcloud CLI | 372+ | Auth and GCP operations |
| Terraform | 1.5+ | Infrastructure |
| dbt-bigquery | 1.8.2 | Gold transformations |

### Environment variables (.env)

```bash
# GCP
GCP_PROJECT_ID=marineflow-489815
GCS_BUCKET=marineflow-lake-marineflow-489815
GOOGLE_CLOUD_PROJECT=marineflow-489815

# AIS (live producer only)
AIS_API_KEY=<your aisstream.io key>

# Pub/Sub
PUBSUB_TOPIC_POSITIONS=vessel-positions
PUBSUB_TOPIC_METADATA=vessel-metadata
PUBSUB_TOPIC_DLQ=dead-letter-queue
PUBSUB_SUB_POSITIONS=vessel-positions-spark-sub
PUBSUB_SUB_METADATA=vessel-metadata-spark-sub

# Spark
SPARK_MASTER=local[*]
SPARK_DRIVER_MEMORY=3g

# Bronze
BRONZE_BATCH_SIZE=500
BRONZE_BATCH_INTERVAL=30
BRONZE_MAX_BATCHES=0
PIPELINE_VERSION=0.1.0

# Silver
SILVER_BATCH_INTERVAL=60
SILVER_MAX_BATCHES=0
METADATA_BATCH_INTERVAL=60
METADATA_MAX_BATCHES=0

# BigQuery
BQ_DATASET_BRONZE=marineflow_bronze
BQ_DATASET_SILVER=marineflow_silver
```

### Local ports

| Service | Port | UI |
|---------|------|----|
| Spark Master UI | 4040 | http://localhost:4040 |
| Spark Worker UI | 4041 | http://localhost:4041 |
| MLflow | 5001 | http://localhost:5001 |
| Grafana | 3000 | http://localhost:3000 |
| Prometheus | 9090 | http://localhost:9090 |

> **Windows / Hyper-V note**: Hyper-V reserves port ranges (typically 8064–8763). Use ports below 8064 or between 8764–49999. Ports 4040/4041 are Spark's native UI ports and work correctly.

---

## Design Decisions

### Bronze philosophy: zero transformations

Bronze is the immutable raw clone of the source. If you over-filter in Bronze and later discover the filter was wrong, you've permanently lost data. The separation is strict:

| Layer | Responsibility |
|-------|---------------|
| **Parser** | Validate coordinates, normalize ISO 8601 timestamp, route message to correct topic |
| **Bronze** | Schema enforcement, impossible coordinate filter, partition columns, lineage fields |
| **Silver** | All business logic: field renaming, nav status translation, flag country derivation, geospatial enrichment, movement deltas |
| **Gold (dbt)** | Aggregations, metrics, anomaly flags |

### External tables for Bronze and Silver

GCS is the single source of truth. BigQuery external tables read Parquet directly from GCS — no data copy, always in sync. If a BQ table is dropped, `terraform apply` recreates it pointing at the same GCS data. The BQ write step has been eliminated from all Spark jobs, simplifying the pipeline and removing the most error-prone step (indirect write via GCS temp bucket).

### 1-hour delay between Bronze and Silver

Bronze writes directly to each hour partition using `mode("overwrite")`. Reading a partition while it's being overwritten causes `SparkFileNotFoundException`. Silver avoids this by excluding the current hour — it only reads fully closed partitions. Maximum delay: 1 hour. Airflow (Phase 3) will trigger Silver once per hour on the previous hour's closed partition. In a production scenario requiring true real-time, this would be addressed with Delta Lake ACID transactions.

### Micro-batch instead of Spark Structured Streaming

No official Pub/Sub connector exists for Spark on Maven. Alternatives evaluated:
1. Add Kafka as intermediary (Pub/Sub → Kafka → Spark) — over-engineering
2. Use Dataflow (Apache Beam) instead of Spark — changes the entire processing stack
3. **Micro-batch with Python client** (chosen) — replicates the same semantics as Structured Streaming

### Schema defined explicitly, not inferred

Spark infers schema by sampling data. If a field is `None` in all records of a batch, inference fails with `CANNOT_DETERMINE_TYPE`. Explicit schema eliminates this fragility and also documents the system's data contract.

### ADC instead of Service Account Keys

Org policy `constraints/iam.disableServiceAccountKeyCreation` prevents JSON key creation. ADC is Google's recommended approach for local development — credentials are tied to the authenticated user and rotate automatically.

---

## Known Limitations

### aisstream.io BETA

The live AIS streaming service is in BETA without guaranteed SLA. Intermittent WebSocket connectivity issues were encountered during development. **The live producer is verified working** — data with `"source": "aisstream_live"` confirmed arriving at Pub/Sub. The simulator is available as offline fallback.

### Python dependencies not persisted in Docker

Python dependencies installed in the Spark container via `docker exec pip install` are lost when the container is recreated. This will be resolved in Phase 5 with a custom `Dockerfile`.

**Current workaround**:
```powershell
docker exec -u root marineflow-spark-master pip install `
  pyspark==3.5.0 google-cloud-pubsub==2.21.1 `
  google-cloud-storage==2.16.0 python-dotenv==1.0.1 structlog==24.1.0
```

### Spark in local mode

Spark runs in `local[*]` mode — a single process using all available cores. No real distribution between workers. For production: `spark://spark-master:7077` with multiple workers, or Dataproc.

### Small files

Each Spark batch generates one Parquet file per partition (`coalesce(1)`). Over time, many small files accumulate within a partition. Mitigated with `coalesce()` but not eliminated. A periodic compaction job (or Delta Lake) would handle this in production.

---

## How to Run

### 1. Initial setup

```bash
git clone <repo-url>
cd marineflow
python -m venv venv
source venv/bin/activate      # or .\venv\Scripts\Activate.ps1 on Windows
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

### 3. Start Docker stack

```powershell
docker compose up spark-master spark-worker -d

docker exec -u root marineflow-spark-master pip install `
  pyspark==3.5.0 google-cloud-pubsub==2.21.1 `
  google-cloud-storage==2.16.0 python-dotenv==1.0.1 structlog==24.1.0
```

### 4. Start producer or simulator

```powershell
# Live producer
cd ingestion/ais_producer
python main.py

# Or synthetic simulator
cd ingestion/simulator
python main.py --vessels 20 --interval 3.0
```

### 5. Run Bronze job

```powershell
docker exec `
  -e GCP_PROJECT_ID=marineflow-489815 `
  -e GCS_BUCKET=marineflow-lake-marineflow-489815 `
  -e PUBSUB_SUB_POSITIONS=vessel-positions-spark-sub `
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/adc.json `
  -e GOOGLE_CLOUD_PROJECT=marineflow-489815 `
  -e BQ_DATASET_BRONZE=marineflow_bronze `
  -e PIPELINE_VERSION=0.1.0 `
  -e BRONZE_BATCH_SIZE=500 `
  -e BRONZE_BATCH_INTERVAL=30 `
  marineflow-spark-master /opt/spark/bin/spark-submit `
  --master local[*] --driver-memory 3g `
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem `
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS `
  --conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT `
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 `
  --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true `
  --conf parentProject=marineflow-489815 `
  --conf temporaryGcsBucket=marineflow-lake-marineflow-489815 `
  --jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar,/opt/spark/processing/jars/spark-bigquery-with-dependencies_2.12-0.40.0.jar `
  --driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar:/opt/spark/processing/jars/spark-bigquery-with-dependencies_2.12-0.40.0.jar `
  /opt/spark/processing/spark_streaming/bronze_positions.py
```

### 6. Run Silver jobs (after Bronze has closed the previous hour)

```powershell
# Silver positions
docker exec `
  -e GCP_PROJECT_ID=marineflow-489815 `
  -e GCS_BUCKET=marineflow-lake-marineflow-489815 `
  -e BQ_DATASET_SILVER=marineflow_silver `
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/adc.json `
  -e GOOGLE_CLOUD_PROJECT=marineflow-489815 `
  marineflow-spark-master /opt/spark/bin/spark-submit `
  --master local[*] --driver-memory 3g `
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem `
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS `
  --conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT `
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 `
  --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true `
  --conf parentProject=marineflow-489815 `
  --conf temporaryGcsBucket=marineflow-lake-marineflow-489815 `
  --jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar,/opt/spark/processing/jars/spark-bigquery-with-dependencies_2.12-0.40.0.jar `
  --driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar:/opt/spark/processing/jars/spark-bigquery-with-dependencies_2.12-0.40.0.jar `
  /opt/spark/processing/spark_streaming/silver_positions.py

# Silver metadata
docker exec `
  -e GCP_PROJECT_ID=marineflow-489815 `
  -e GCS_BUCKET=marineflow-lake-marineflow-489815 `
  -e BQ_DATASET_SILVER=marineflow_silver `
  -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/adc.json `
  -e GOOGLE_CLOUD_PROJECT=marineflow-489815 `
  -e PUBSUB_SUB_METADATA=vessel-metadata-spark-sub `
  marineflow-spark-master /opt/spark/bin/spark-submit `
  --master local[*] --driver-memory 3g `
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem `
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS `
  --conf spark.hadoop.google.cloud.auth.type=APPLICATION_DEFAULT `
  --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 `
  --conf spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped=true `
  --conf parentProject=marineflow-489815 `
  --conf temporaryGcsBucket=marineflow-lake-marineflow-489815 `
  --jars /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar,/opt/spark/processing/jars/spark-bigquery-with-dependencies_2.12-0.40.0.jar `
  --driver-class-path /opt/spark/processing/jars/gcs-connector-hadoop3-latest.jar:/opt/spark/processing/jars/spark-bigquery-with-dependencies_2.12-0.40.0.jar `
  /opt/spark/processing/spark_streaming/silver_metadata.py
```

### 7. Run dbt Gold models

```powershell
cd transformation/dbt
dbt deps
dbt run
dbt test
```

### 8. Verify data

```bash
# GCS
gcloud storage ls gs://marineflow-lake-marineflow-489815/bronze/vessel_positions/
gcloud storage ls gs://marineflow-lake-marineflow-489815/silver/vessel_positions/
gcloud storage ls gs://marineflow-lake-marineflow-489815/silver/vessel_metadata/

# BigQuery
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) FROM marineflow-489815.marineflow_bronze.vessel_positions_raw"
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) FROM marineflow-489815.marineflow_gold.vessel_activity_summary"
```

---

*MarineFlow — Portfolio project*
*Stack: Python · Apache Spark · GCP Pub/Sub · GCS · BigQuery · dbt · Airflow · MLflow · FastAPI · Terraform*