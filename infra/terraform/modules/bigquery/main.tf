# =============================================================================
# MARINEFLOW — BigQuery Module
# infra/terraform/modules/bigquery/main.tf
# =============================================================================

# -----------------------------------------------------------------------------
# Datasets: Bronze / Silver / Gold (Medallion architecture)
# -----------------------------------------------------------------------------
resource "google_bigquery_dataset" "bronze" {
  dataset_id                  = "marineflow_bronze"
  friendly_name               = "MarineFlow — Bronze Layer"
  description                 = "Raw data ingested from Pub/Sub with no transformations applied"
  location                    = var.bq_location
  project                     = var.project_id
  default_table_expiration_ms = null  # bronze data does not expire automatically

  labels = merge(var.labels, { layer = "bronze" })

  delete_contents_on_destroy = true  # useful in dev for clean teardown
}

resource "google_bigquery_dataset" "silver" {
  dataset_id                  = "marineflow_silver"
  friendly_name               = "MarineFlow — Silver Layer"
  description                 = "Cleaned, deduplicated and geospatially enriched vessel data"
  location                    = var.bq_location
  project                     = var.project_id
  default_table_expiration_ms = null

  labels = merge(var.labels, { layer = "silver" })

  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "gold" {
  dataset_id                  = "marineflow_gold"
  friendly_name               = "MarineFlow — Gold Layer"
  description                 = "Optimized analytical tables, dbt models and ML feature outputs"
  location                    = var.bq_location
  project                     = var.project_id
  default_table_expiration_ms = null

  labels = merge(var.labels, { layer = "gold" })

  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "ml_features" {
  dataset_id                  = "marineflow_features"
  friendly_name               = "MarineFlow — ML Feature Store"
  description                 = "Computed features for model training and real-time serving"
  location                    = var.bq_location
  project                     = var.project_id
  default_table_expiration_ms = null

  labels = merge(var.labels, { layer = "ml" })

  delete_contents_on_destroy = true
}

# -----------------------------------------------------------------------------
# Table Bronze: vessel_positions_raw
# Exact schema matching the raw AIS message structure
# -----------------------------------------------------------------------------
resource "google_bigquery_table" "vessel_positions_raw" {
  dataset_id          = google_bigquery_dataset.bronze.dataset_id
  table_id            = "vessel_positions_raw"
  project             = var.project_id
  deletion_protection = false

  description = "Raw AIS messages exactly as received from the stream"

  time_partitioning {
    type  = "DAY"
    field = "ingestion_timestamp"
  }

  clustering = ["mmsi"]

  schema = jsonencode([
    # Exact mirror of the aisstream.io WebSocket message.
    # Field names match the original JSON keys — no renaming, no transformation.
    # Two sections as in the raw message:
    #   Message.PositionReport: data from the vessel transponder (AIS standard)
    #   MetaData: fields added by aisstream.io (not from the transponder)
    # Enrichments (flag_country, nav status string, vessel_type) happen in Silver.
    # --- Message.PositionReport (vessel transponder — AIS ITU-R M.1371-5) ---
    { name = "Cog",                      type = "FLOAT64",   mode = "NULLABLE", description = "Course over ground in degrees" },
    { name = "CommunicationState",       type = "INTEGER",   mode = "NULLABLE", description = "AIS communication state" },
    { name = "Latitude",                 type = "FLOAT64",   mode = "REQUIRED", description = "Position latitude in decimal degrees" },
    { name = "Longitude",                type = "FLOAT64",   mode = "REQUIRED", description = "Position longitude in decimal degrees" },
    { name = "MessageID",                type = "INTEGER",   mode = "NULLABLE", description = "AIS message type ID (1, 2 or 3 for PositionReport)" },
    { name = "NavigationalStatus",       type = "INTEGER",   mode = "NULLABLE", description = "Raw navigational status integer per ITU-R M.1371-5" },
    { name = "PositionAccuracy",         type = "BOOLEAN",   mode = "NULLABLE", description = "Position accuracy flag" },
    { name = "Raim",                     type = "BOOLEAN",   mode = "NULLABLE", description = "Receiver autonomous integrity monitoring flag" },
    { name = "RateOfTurn",               type = "INTEGER",   mode = "NULLABLE", description = "Rate of turn — raw AIS value" },
    { name = "RepeatIndicator",          type = "INTEGER",   mode = "NULLABLE", description = "Message repeat indicator" },
    { name = "Sog",                      type = "FLOAT64",   mode = "NULLABLE", description = "Speed over ground in knots" },
    { name = "Spare",                    type = "INTEGER",   mode = "NULLABLE", description = "Spare bits" },
    { name = "SpecialManoeuvreIndicator",type = "INTEGER",   mode = "NULLABLE", description = "Special manoeuvre indicator" },
    { name = "Timestamp",                type = "INTEGER",   mode = "NULLABLE", description = "UTC second when report was generated" },
    { name = "TrueHeading",              type = "INTEGER",   mode = "NULLABLE", description = "True heading in degrees — 511 means unavailable" },
    { name = "UserID",                   type = "INTEGER",   mode = "REQUIRED", description = "MMSI from transponder (integer)" },
    { name = "Valid",                    type = "BOOLEAN",   mode = "NULLABLE", description = "Message validity flag" },
    # --- MetaData (added by aisstream.io, not from the transponder) ---
    { name = "MMSI",                     type = "STRING",    mode = "REQUIRED", description = "MMSI as string — from aisstream.io MetaData" },
    { name = "MMSI_String",              type = "STRING",    mode = "NULLABLE", description = "MMSI string duplicate from aisstream.io MetaData" },
    { name = "ShipName",                 type = "STRING",    mode = "NULLABLE", description = "Vessel name — raw untrimmed from aisstream.io MetaData" },
    { name = "time_utc",                 type = "TIMESTAMP", mode = "REQUIRED", description = "Event timestamp from aisstream.io — normalized to ISO 8601" },
    # --- Pipeline metadata (added by our ingestion layer) ---
    { name = "ingestion_timestamp",      type = "TIMESTAMP", mode = "REQUIRED", description = "Timestamp when message was received by our producer" },
    { name = "raw_message",              type = "STRING",    mode = "NULLABLE", description = "Full original JSON message — complete audit record" },
    # --- Lineage fields ---
    { name = "_source_system",           type = "STRING",    mode = "REQUIRED", description = "Origin system: aisstream_live or simulator" },
    { name = "_source_file",             type = "STRING",    mode = "NULLABLE", description = "GCS partition path of the Parquet file containing this record" },
    { name = "_pubsub_message_id",       type = "STRING",    mode = "NULLABLE", description = "Pub/Sub message ID — enables tracing back to original message" },
    { name = "_batch_id",                type = "STRING",    mode = "REQUIRED", description = "Spark batch ID that processed this record" },
    { name = "_pipeline_version",        type = "STRING",    mode = "REQUIRED", description = "Pipeline version tag that produced this record" },
    { name = "_ingestion_date",          type = "DATE",      mode = "REQUIRED", description = "Partition date derived from ingestion_timestamp" }
  ])
}

# -----------------------------------------------------------------------------
# Table Silver: vessel_positions_clean
# -----------------------------------------------------------------------------
resource "google_bigquery_table" "vessel_positions_clean" {
  dataset_id          = google_bigquery_dataset.silver.dataset_id
  table_id            = "vessel_positions_clean"
  project             = var.project_id
  deletion_protection = false

  description = "Cleaned, deduplicated positions enriched with geospatial context"

  time_partitioning {
    type  = "DAY"
    field = "event_timestamp"
  }

  clustering = ["mmsi", "ocean_region", "vessel_type_normalized"]

  labels = merge(var.labels, { layer = "silver", entity = "vessel_positions" })

  schema = jsonencode([
    { name = "mmsi",                    type = "STRING",    mode = "REQUIRED" },
    { name = "vessel_name",             type = "STRING",    mode = "NULLABLE" },
    { name = "vessel_type_normalized",  type = "STRING",    mode = "NULLABLE", description = "Vessel type normalized to standard categories" },
    { name = "latitude",                type = "FLOAT64",   mode = "REQUIRED" },
    { name = "longitude",               type = "FLOAT64",   mode = "REQUIRED" },
    { name = "speed_over_ground",       type = "FLOAT64",   mode = "NULLABLE" },
    { name = "course_over_ground",      type = "FLOAT64",   mode = "NULLABLE" },
    { name = "heading",                 type = "INTEGER",   mode = "NULLABLE" },
    { name = "navigational_status",     type = "STRING",    mode = "NULLABLE" },
    { name = "destination_clean",       type = "STRING",    mode = "NULLABLE", description = "Destination normalized against port reference database" },
    { name = "flag_country",            type = "STRING",    mode = "NULLABLE" },
    { name = "ocean_region",            type = "STRING",    mode = "NULLABLE", description = "Ocean region derived from coordinates" },
    { name = "eez_country",             type = "STRING",    mode = "NULLABLE", description = "Exclusive Economic Zone country code" },
    { name = "is_in_port_zone",         type = "BOOLEAN",   mode = "NULLABLE", description = "Whether the vessel is within a port boundary" },
    { name = "nearest_port",            type = "STRING",    mode = "NULLABLE" },
    { name = "distance_to_port_km",     type = "FLOAT64",   mode = "NULLABLE" },
    { name = "speed_change_rate",       type = "FLOAT64",   mode = "NULLABLE", description = "Speed delta compared to previous position" },
    { name = "heading_change_degrees",  type = "FLOAT64",   mode = "NULLABLE", description = "Heading delta compared to previous position" },
    { name = "event_timestamp",         type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "processing_timestamp",    type = "TIMESTAMP", mode = "REQUIRED" },
    # --- Lineage fields propagated from Bronze ---
    { name = "_source_system",          type = "STRING",    mode = "NULLABLE", description = "Origin system propagated from Bronze: aisstream_live or simulator" },
    { name = "_source_file",            type = "STRING",    mode = "NULLABLE", description = "GCS Bronze Parquet file this record was read from" },
    { name = "_bronze_batch_id",        type = "STRING",    mode = "NULLABLE", description = "Bronze batch ID that originally ingested this record" },
    { name = "_silver_batch_id",        type = "STRING",    mode = "REQUIRED", description = "Silver batch ID that transformed this record" },
    { name = "_pipeline_version",       type = "STRING",    mode = "NULLABLE", description = "Pipeline version that produced the Bronze record" }
  ])
}

# -----------------------------------------------------------------------------
# Table Gold: maritime_alerts
# -----------------------------------------------------------------------------
resource "google_bigquery_table" "maritime_alerts" {
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "maritime_alerts"
  project             = var.project_id
  deletion_protection = false

  description = "Alerts generated by the anomaly detection models"

  time_partitioning {
    type  = "DAY"
    field = "alert_timestamp"
  }

  clustering = ["alert_type", "severity"]

  labels = merge(var.labels, { layer = "gold", entity = "alerts" })

  schema = jsonencode([
    { name = "alert_id",          type = "STRING",    mode = "REQUIRED" },
    { name = "mmsi",              type = "STRING",    mode = "REQUIRED" },
    { name = "vessel_name",       type = "STRING",    mode = "NULLABLE" },
    { name = "alert_type",        type = "STRING",    mode = "REQUIRED", description = "One of: ais_gap, speed_anomaly, route_deviation, dark_vessel" },
    { name = "severity",          type = "STRING",    mode = "REQUIRED", description = "One of: low, medium, high, critical" },
    { name = "anomaly_score",     type = "FLOAT64",   mode = "REQUIRED", description = "Isolation Forest anomaly score (0 to 1)" },
    { name = "latitude",          type = "FLOAT64",   mode = "NULLABLE" },
    { name = "longitude",         type = "FLOAT64",   mode = "NULLABLE" },
    { name = "description",       type = "STRING",    mode = "NULLABLE" },
    { name = "alert_timestamp",   type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "model_version",     type = "STRING",    mode = "NULLABLE" }
  ])
}
