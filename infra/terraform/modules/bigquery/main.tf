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

  clustering = ["mmsi", "vessel_type"]

  labels = merge(var.labels, { layer = "bronze", entity = "vessel_positions" })

  schema = jsonencode([
    { name = "mmsi",                type = "STRING",    mode = "REQUIRED", description = "Maritime Mobile Service Identity — unique vessel identifier" },
    { name = "vessel_name",         type = "STRING",    mode = "NULLABLE", description = "Vessel name as broadcast" },
    { name = "vessel_type",         type = "STRING",    mode = "NULLABLE", description = "Vessel type (cargo, tanker, passenger, etc.)" },
    { name = "latitude",            type = "FLOAT64",   mode = "REQUIRED", description = "Position latitude in decimal degrees" },
    { name = "longitude",           type = "FLOAT64",   mode = "REQUIRED", description = "Position longitude in decimal degrees" },
    { name = "speed_over_ground",   type = "FLOAT64",   mode = "NULLABLE", description = "Speed over ground in knots" },
    { name = "course_over_ground",  type = "FLOAT64",   mode = "NULLABLE", description = "Course over ground in degrees" },
    { name = "heading",             type = "INTEGER",   mode = "NULLABLE", description = "True heading of the vessel in degrees" },
    { name = "navigational_status", type = "STRING",    mode = "NULLABLE", description = "Navigational status (under way, anchored, moored, etc.)" },
    { name = "destination",         type = "STRING",    mode = "NULLABLE", description = "Destination as declared by the vessel" },
    { name = "eta",                 type = "STRING",    mode = "NULLABLE", description = "ETA as declared by the vessel (MMDDHHmm format)" },
    { name = "draught",             type = "FLOAT64",   mode = "NULLABLE", description = "Vessel draught in metres" },
    { name = "flag_country",        type = "STRING",    mode = "NULLABLE", description = "Flag state country code (ISO 3166-1 alpha-2)" },
    { name = "imo_number",          type = "STRING",    mode = "NULLABLE", description = "IMO vessel identification number" },
    { name = "callsign",            type = "STRING",    mode = "NULLABLE", description = "Radio call sign" },
    { name = "event_timestamp",     type = "TIMESTAMP", mode = "REQUIRED", description = "Original AIS event timestamp" },
    { name = "ingestion_timestamp", type = "TIMESTAMP", mode = "REQUIRED", description = "Timestamp when the message was ingested into the pipeline" },
    { name = "source",              type = "STRING",    mode = "NULLABLE", description = "Message source: aisstream_live or simulator" },
    { name = "raw_message",         type = "STRING",    mode = "NULLABLE", description = "Original AIS message in JSON format for audit purposes" }
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
    { name = "processing_timestamp",    type = "TIMESTAMP", mode = "REQUIRED" }
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
