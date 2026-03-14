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
# External Table Bronze: vessel_positions_raw
#
# Reads directly from GCS Parquet — no data copy into BigQuery.
# This means Bronze is always in sync with GCS automatically.
# Truncating this table has no effect on the data — it lives in GCS.
# Silver scripts only write to GCS; BigQuery reads from there via this table.
# -----------------------------------------------------------------------------
resource "google_bigquery_table" "vessel_positions_raw" {
  dataset_id          = google_bigquery_dataset.bronze.dataset_id
  table_id            = "vessel_positions_raw"
  project             = var.project_id
  deletion_protection = false

  description = "External table — reads raw AIS Parquet directly from GCS. Source of truth is GCS, not BigQuery."

  labels = merge(var.labels, { layer = "bronze", entity = "vessel_positions", type = "external" })

  external_data_configuration {
    source_format    = "PARQUET"
    autodetect       = false
    source_uris      = ["gs://${var.gcs_bucket}-${var.project_id}/bronze/vessel_positions/*"]

    hive_partitioning_options {
      mode                     = "AUTO"
      source_uri_prefix        = "gs://${var.gcs_bucket}-${var.project_id}/bronze/vessel_positions/"
      require_partition_filter = false
    }

    parquet_options {
      enable_list_inference = true
    }

    schema = jsonencode([
      # --- Message.PositionReport (vessel transponder — AIS ITU-R M.1371-5) ---
      { name = "Cog",                       type = "FLOAT64",   mode = "NULLABLE", description = "Course over ground in degrees" },
      { name = "CommunicationState",        type = "INTEGER",   mode = "NULLABLE" },
      { name = "Latitude",                  type = "FLOAT64",   mode = "NULLABLE", description = "Position latitude in decimal degrees" },
      { name = "Longitude",                 type = "FLOAT64",   mode = "NULLABLE", description = "Position longitude in decimal degrees" },
      { name = "MessageID",                 type = "INTEGER",   mode = "NULLABLE", description = "AIS message type ID" },
      { name = "NavigationalStatus",        type = "INTEGER",   mode = "NULLABLE", description = "Raw navigational status integer — translated in Silver" },
      { name = "PositionAccuracy",          type = "BOOLEAN",   mode = "NULLABLE" },
      { name = "Raim",                      type = "BOOLEAN",   mode = "NULLABLE" },
      { name = "RateOfTurn",                type = "INTEGER",   mode = "NULLABLE" },
      { name = "RepeatIndicator",           type = "INTEGER",   mode = "NULLABLE" },
      { name = "Sog",                       type = "FLOAT64",   mode = "NULLABLE", description = "Speed over ground in knots" },
      { name = "Spare",                     type = "INTEGER",   mode = "NULLABLE" },
      { name = "SpecialManoeuvreIndicator", type = "INTEGER",   mode = "NULLABLE" },
      { name = "Timestamp",                 type = "INTEGER",   mode = "NULLABLE", description = "UTC second when report was generated" },
      { name = "TrueHeading",               type = "INTEGER",   mode = "NULLABLE", description = "True heading — 511 means unavailable" },
      { name = "UserID",                    type = "INTEGER",   mode = "NULLABLE", description = "MMSI from transponder (integer)" },
      { name = "Valid",                     type = "BOOLEAN",   mode = "NULLABLE" },
      # --- MetaData (added by aisstream.io) ---
      { name = "MMSI",                      type = "STRING",    mode = "NULLABLE", description = "MMSI as string" },
      { name = "MMSI_String",               type = "STRING",    mode = "NULLABLE" },
      { name = "ShipName",                  type = "STRING",    mode = "NULLABLE", description = "Raw untrimmed vessel name" },
      { name = "time_utc",                  type = "TIMESTAMP", mode = "NULLABLE", description = "Event timestamp normalized to ISO 8601" },
      # --- Pipeline metadata ---
      { name = "ingestion_timestamp",       type = "TIMESTAMP", mode = "NULLABLE", description = "When Bronze processed this record" },
      { name = "_source_system",            type = "STRING",    mode = "NULLABLE", description = "aisstream_live or simulator" },
      { name = "_batch_id",                 type = "STRING",    mode = "NULLABLE", description = "Spark micro-batch ID" },
      { name = "_pipeline_version",         type = "STRING",    mode = "NULLABLE" },
      { name = "_ingestion_date",           type = "DATE",      mode = "NULLABLE" },
      { name = "_source_file",              type = "STRING",    mode = "NULLABLE" }
    ])
  }
}

# -----------------------------------------------------------------------------
# External Table Silver: vessel_positions_clean
#
# Reads directly from GCS Parquet — Silver scripts only write to GCS.
# dbt Gold models read from this external table via the staging model.
# -----------------------------------------------------------------------------
resource "google_bigquery_table" "vessel_positions_clean" {
  dataset_id          = google_bigquery_dataset.silver.dataset_id
  table_id            = "vessel_positions_clean"
  project             = var.project_id
  deletion_protection = false

  description = "External table — reads cleaned Silver Parquet directly from GCS. Source of truth is GCS, not BigQuery."

  labels = merge(var.labels, { layer = "silver", entity = "vessel_positions", type = "external" })

  external_data_configuration {
    source_format    = "PARQUET"
    autodetect       = false
    source_uris      = ["gs://${var.gcs_bucket}-${var.project_id}/silver/vessel_positions/*"]

    hive_partitioning_options {
      mode                     = "AUTO"
      source_uri_prefix        = "gs://${var.gcs_bucket}-${var.project_id}/silver/vessel_positions/"
      require_partition_filter = false
    }

    parquet_options {
      enable_list_inference = true
    }

    schema = jsonencode([
      # --- Identity ---
      { name = "mmsi",                   type = "STRING",    mode = "NULLABLE" },
      { name = "vessel_name",            type = "STRING",    mode = "NULLABLE" },
      { name = "flag_country",           type = "STRING",    mode = "NULLABLE" },
      # --- Position ---
      { name = "latitude",               type = "FLOAT64",   mode = "NULLABLE" },
      { name = "longitude",              type = "FLOAT64",   mode = "NULLABLE" },
      { name = "ocean_region",           type = "STRING",    mode = "NULLABLE" },
      { name = "nearest_port",           type = "STRING",    mode = "NULLABLE" },
      { name = "eez_country",            type = "STRING",    mode = "NULLABLE" },
      { name = "is_in_port_zone",        type = "BOOLEAN",   mode = "NULLABLE" },
      { name = "distance_to_port_km",    type = "FLOAT64",   mode = "NULLABLE" },
      # --- Movement ---
      { name = "speed_over_ground",      type = "FLOAT64",   mode = "NULLABLE" },
      { name = "course_over_ground",     type = "FLOAT64",   mode = "NULLABLE" },
      { name = "heading",                type = "INTEGER",   mode = "NULLABLE" },
      { name = "navigational_status",    type = "STRING",    mode = "NULLABLE" },
      { name = "speed_change_rate",      type = "FLOAT64",   mode = "NULLABLE" },
      { name = "heading_change_degrees", type = "FLOAT64",   mode = "NULLABLE" },
      # --- Timestamps ---
      { name = "event_timestamp",        type = "TIMESTAMP", mode = "NULLABLE" },
      { name = "processing_timestamp",   type = "TIMESTAMP", mode = "NULLABLE" },
      # --- Lineage ---
      { name = "_source_system",         type = "STRING",    mode = "NULLABLE" },
      { name = "_source_file",           type = "STRING",    mode = "NULLABLE" },
      { name = "_bronze_batch_id",       type = "STRING",    mode = "NULLABLE" },
      { name = "_pipeline_version",      type = "STRING",    mode = "NULLABLE" },
      { name = "_silver_batch_id",       type = "STRING",    mode = "NULLABLE" }
    ])
  }
}
# -----------------------------------------------------------------------------
# External Table Silver: vessel_metadata
#
# Static vessel data from ShipStaticData messages — written to GCS by
# silver_metadata.py and read here by BigQuery as an external table.
# -----------------------------------------------------------------------------
resource "google_bigquery_table" "vessel_metadata" {
  dataset_id          = google_bigquery_dataset.silver.dataset_id
  table_id            = "vessel_metadata"
  project             = var.project_id
  deletion_protection = false

  description = "External table — vessel static metadata from ShipStaticData. Source of truth is GCS."

  labels = merge(var.labels, { layer = "silver", entity = "vessel_metadata", type = "external" })

  external_data_configuration {
    source_format    = "PARQUET"
    autodetect       = false
    source_uris      = ["gs://${var.gcs_bucket}-${var.project_id}/silver/vessel_metadata/*"]

    hive_partitioning_options {
      mode                     = "AUTO"
      source_uri_prefix        = "gs://${var.gcs_bucket}-${var.project_id}/silver/vessel_metadata/"
      require_partition_filter = false
    }

    parquet_options {
      enable_list_inference = true
    }

    schema = jsonencode([
      { name = "mmsi",                   type = "STRING",    mode = "NULLABLE" },
      { name = "vessel_name",            type = "STRING",    mode = "NULLABLE" },
      { name = "vessel_type_normalized", type = "STRING",    mode = "NULLABLE" },
      { name = "imo_number",             type = "STRING",    mode = "NULLABLE" },
      { name = "callsign",               type = "STRING",    mode = "NULLABLE" },
      { name = "destination_clean",      type = "STRING",    mode = "NULLABLE" },
      { name = "draught",                type = "FLOAT64",   mode = "NULLABLE" },
      { name = "flag_country",           type = "STRING",    mode = "NULLABLE" },
      { name = "source_system",          type = "STRING",    mode = "NULLABLE" },
      { name = "ingestion_timestamp",    type = "TIMESTAMP", mode = "NULLABLE" },
      { name = "processing_timestamp",   type = "TIMESTAMP", mode = "NULLABLE" },
      { name = "_silver_batch_id",       type = "STRING",    mode = "NULLABLE" }
    ])
  }
}

