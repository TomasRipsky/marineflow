# =============================================================================
# MARINEFLOW — GCS Module
# infra/terraform/modules/gcs/main.tf
# =============================================================================

# -----------------------------------------------------------------------------
# Bucket: Terraform State
# NOTE: This bucket must exist BEFORE running terraform init with the backend.
# Create it once manually with: gcloud storage buckets create gs://marineflow-tfstate
# -----------------------------------------------------------------------------
resource "google_storage_bucket" "tfstate" {
  name                        = "marineflow-tfstate"
  location                    = var.region
  project                     = var.project_id
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = false  # protect state in production

  versioning {
    enabled = true  # critical for recovering previous states
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      num_newer_versions = 5  # keep only the 5 most recent state versions
    }
  }

  labels = var.labels
}

# -----------------------------------------------------------------------------
# Bucket: Data Lake (Bronze / Silver / Gold)
# -----------------------------------------------------------------------------
resource "google_storage_bucket" "data_lake" {
  name                        = "${var.gcs_bucket_name}-${var.project_id}"
  location                    = var.region
  project                     = var.project_id
  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = false  # not needed for processed data
  }

  # Lifecycle: move older data to cheaper storage classes
  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
    condition {
      age = 30  # data older than 30 days → Nearline
    }
  }

  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
    condition {
      age = 90  # data older than 90 days → Coldline
    }
  }

  labels = var.labels
}

# -----------------------------------------------------------------------------
# Logical folders inside the bucket (empty placeholder objects)
# -----------------------------------------------------------------------------
resource "google_storage_bucket_object" "bronze_prefix" {
  name    = "bronze/.keep"
  content = "marineflow-placeholder"
  bucket  = google_storage_bucket.data_lake.name
}

resource "google_storage_bucket_object" "silver_prefix" {
  name    = "silver/.keep"
  content = "marineflow-placeholder"
  bucket  = google_storage_bucket.data_lake.name
}

resource "google_storage_bucket_object" "gold_prefix" {
  name    = "gold/.keep"
  content = "marineflow-placeholder"
  bucket  = google_storage_bucket.data_lake.name
}

resource "google_storage_bucket_object" "checkpoints_prefix" {
  name    = "checkpoints/.keep"
  content = "marineflow-placeholder"
  bucket  = google_storage_bucket.data_lake.name
}

resource "google_storage_bucket_object" "models_prefix" {
  name    = "models/.keep"
  content = "marineflow-placeholder"
  bucket  = google_storage_bucket.data_lake.name
}

resource "google_storage_bucket_object" "schemas_prefix" {
  name    = "schemas/.keep"
  content = "marineflow-placeholder"
  bucket  = google_storage_bucket.data_lake.name
}
