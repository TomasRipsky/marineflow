# =============================================================================
# MARINEFLOW — IAM Module
# infra/terraform/modules/iam/main.tf
# Note: The Service Account is created via gcloud (see README). This module
# manages the additional IAM bindings that Terraform needs to control.
# =============================================================================

# Binding: SA can read and write objects in GCS
resource "google_project_iam_member" "storage_object_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${var.service_account_email}"
}

# Binding: SA can write data to BigQuery tables
resource "google_project_iam_member" "bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${var.service_account_email}"
}

# Binding: SA can run BigQuery jobs
resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${var.service_account_email}"
}

# Binding: SA can write to Cloud Logging (not used by the current pipeline)
resource "google_project_iam_member" "log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${var.service_account_email}"
}

# Binding: SA can write custom metrics to Cloud Monitoring (not used by the current
# pipeline: metrics go to the local Prometheus)
resource "google_project_iam_member" "metric_writer" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${var.service_account_email}"
}
