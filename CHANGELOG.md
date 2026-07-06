# Changelog

All notable changes to MarineFlow are documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [1.0.0] — 2026-03-29

### Summary
First stable release. Full end-to-end maritime intelligence pipeline:
real-time AIS ingestion → Bronze/Silver lakehouse → Gold analytics → ML anomaly detection.

---

### Phase 1 — Infrastructure & Ingestion

**Added**
- GCP project provisioned via Terraform: Pub/Sub, GCS, BigQuery, Compute Engine, IAM
- AIS WebSocket producer deployed on Compute Engine `e2-micro` VM (`marineflow-ais-producer`) with systemd for auto-restart
- Pub/Sub GCS subscriptions writing raw AIS JSON to `gs://.../pubsub-landing/` — no intermediary pull loop required
- Docker Compose local development stack (Spark × 3, Airflow, PostgreSQL, Redis, MLflow, Streamlit, Prometheus, Grafana)

**Architecture decisions**
- Cloud Run evaluated and rejected for WebSocket producer: TCP idle timeout kills persistent connections. VM + systemd is the correct pattern for long-lived streaming connections.
- Pub/Sub GCS subscriptions chosen over Spark pull loop: simpler, more reliable, no custom consumer code to maintain.

---

### Phase 2 — Bronze & Silver Layers (Spark Structured Streaming)

**Added**
- Bronze layer: Spark Structured Streaming job reads raw JSON from `pubsub-landing/`, applies schema, writes Parquet to `gs://.../bronze/`
- Silver positions job: Bronze → enriched vessel positions with computed fields (speed_change_rate, heading_change_degrees, is_in_port_zone, ocean_region, eez_country, distance_to_port_km)
- Silver metadata job: vessel static data (vessel_name, flag_country, vessel_type_normalized)
- External BigQuery tables over GCS Parquet for Bronze and Silver layers — avoids data duplication, cost-efficient at this scale
- GCS checkpoints per Spark job to guarantee exactly-once semantics
- `stg_vessel_positions` dbt staging model joining Silver positions + metadata

**Architecture decisions**
- One Docker container per Spark job: prevents GCS checkpoint conflicts between concurrent streaming jobs.
- External BQ tables (not native): Bronze and Silver data lives in GCS; BigQuery is a query engine, not a storage layer. Avoids paying for both GCS and BQ storage.

**Known limitation (Phase 5)**
- Python dependencies in Spark containers are lost on recreation and must be reinstalled manually. Fix: custom Dockerfiles with pre-installed deps.

---

### Phase 3 — Gold Layer (dbt) & Orchestration (Airflow)

**Added**
- 7 dbt Gold models:
  - `vessel_activity_summary` — daily activity summary per vessel (grain: mmsi × date_day)
  - `port_traffic` — daily port traffic analysis (grain: nearest_port × date_day)
  - `vessel_dark_events` — AIS blackout events > 120 min (incremental, merge)
  - `vessel_speed_anomalies` — GPS spoofing, impossible speeds, sudden accelerations (incremental, merge)
  - `vessel_loitering` — loitering sessions and STS transfer detection (incremental, merge)
  - `vessel_risk_score` — 30-day weighted risk score per vessel (incremental, merge)
  - `anomaly_candidates` — rule-based alert flags, primary input for Isolation Forest
- Comprehensive `schema.yml` with 40+ dbt tests across all Gold models
- Airflow DAG `marineflow_pipeline`: hourly Gold materialization + daily Silver compaction
- DAG task graph: `check_silver_arrived` → parallel detection models → `risk_score` → `dbt_test` → `compact_gcs` (daily gate)

**Architecture decisions**
- Gold tables not managed by Terraform — dbt owns `CREATE OR REPLACE TABLE`. Mixing IaC and dbt ownership of the same resource creates drift.
- Airflow orchestrates dbt only, not Spark. Structured Streaming is a continuous process, not a batch job with start/end — it does not belong in a DAG.
- Incremental models use `merge` strategy with lookback windows (2h–6h) to handle late-arriving data without full table scans.

**Bug fixes**
- `vessel_speed_anomalies`: `anomaly_type` CASE expression had no ELSE clause, producing NULL for `speed_change_rate > 10` records that passed the WHERE filter. Fixed with explicit `ELSE 'SUDDEN_ACCELERATION'`.
- `schema.yml`: `dark_vessel_severity_minimum_medium` test was untriggerable (model always assigns `high`). Replaced with `dark_vessel_severity_must_be_high` — a meaningful regression guard.
- `google-cloud-bigquery==3.13.0` pinned alongside `dbt-bigquery==1.7.0` to prevent `_CELLDATA_FROM_JSON` AttributeError.

---

### Phase 4 — ML Models

**Added**
- `ml/training/feature_engineering.py`:
  - `build_feature_matrix()` for Isolation Forest (no temporal separation needed — unsupervised)
  - `build_temporal_feature_matrix()` for XGBoost with strict temporal split: features [T-7..T] → target [T+1..T+7]
  - `MinMaxScaler` normalization of `risk_score` with rationale: risk_score distribution is heavily skewed; StandardScaler would assume normality; MinMaxScaler preserves distribution and is interpretable
- `ml/training/isolation_forest.py`:
  - `IsolationForest` with `contamination=0.05` (5% anomaly rate — conservative, consistent with dark vessel literature)
  - `IFArtifact` bundles model + MinMaxScaler — guarantees serving uses identical normalization ranges as training
  - Model persisted to `gs://.../ml/models/isolation_forest/`
- `ml/training/risk_classifier.py`:
  - `XGBClassifier` with early stopping, dynamic `scale_pos_weight`, AUC optimization
  - v1 had data leakage (ROC-AUC = 1.0, best_iteration = 0): model predicted `risk_score > 50` using `risk_score` as feature — a perfect tautology. Fixed in v2 with temporal separation and behavior-only features.
  - `XGBArtifact` bundles model + config + feature list for reproducible serving
  - Model persisted to `gs://.../ml/models/risk_classifier/`
- `ml/training/requirements.txt` and `ml/training/__init__.py`

**Architecture decisions**
- Normalization in `feature_engineering.py`, not in dbt: Gold `risk_score` is an auditable business metric. Altering it in dbt mixes transformation and ML preprocessing responsibilities.
- `risk_score_normalized` excluded from XGBoost features: even from the prior window, it aggregates 30-day signals and would contaminate causal learning from raw movement features.
- XGBoost chosen over alternatives: robust to skewed distributions, scale-invariant, handles sparse features, interpretable via feature importance — all relevant for AIS data characteristics.
- LSTM ETA predictor deferred: requires 6+ months of historical trajectory data. Architecturally planned, not yet implemented.

**Known limitation**
- With ~11 days of historical data the temporal dataset for XGBoost is small. Model is architecturally correct but needs 60-90 days of data to be statistically robust. `ValueError` raised with informative message when data is insufficient.

---

## Planned — [1.1.0]

- **Phase 5**: Custom Dockerfiles for Spark containers to persist Python dependencies across container recreation
- **Phase 5**: MLflow experiment tracking integration for Isolation Forest and XGBoost runs
- **Phase 6**: FastAPI serving layer (`api/fastapi/`) exposing `/predict/risk` and `/predict/anomaly` endpoints with Redis caching
- **Phase 6**: Airflow DAG task for scheduled ML model retraining (once 60+ days of data available)
- **Phase 7**: LSTM ETA predictor once sufficient trajectory history is available
- Prometheus metrics for model serving latency and prediction drift
- Grafana dashboard for operational monitoring