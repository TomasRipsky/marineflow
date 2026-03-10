# =============================================================================
# MARINEFLOW — GCS Module: Outputs
# infra/terraform/modules/gcs/outputs.tf
# =============================================================================

output "lake_bucket_name" {
  description = "Name of the data lake bucket"
  value       = google_storage_bucket.data_lake.name
}

output "lake_bucket_url" {
  description = "gs:// URL of the data lake bucket"
  value       = "gs://${google_storage_bucket.data_lake.name}"
}

output "tfstate_bucket_name" {
  description = "Name of the Terraform state bucket"
  value       = google_storage_bucket.tfstate.name
}

output "tfstate_bucket_url" {
  description = "gs:// URL of the Terraform state bucket"
  value       = "gs://${google_storage_bucket.tfstate.name}"
}

output "bronze_path" {
  description = "Full GCS path for the Bronze layer"
  value       = "gs://${google_storage_bucket.data_lake.name}/bronze"
}

output "silver_path" {
  description = "Full GCS path for the Silver layer"
  value       = "gs://${google_storage_bucket.data_lake.name}/silver"
}

output "gold_path" {
  description = "Full GCS path for the Gold layer"
  value       = "gs://${google_storage_bucket.data_lake.name}/gold"
}

output "checkpoints_path" {
  description = "Full GCS path for Spark Structured Streaming checkpoints"
  value       = "gs://${google_storage_bucket.data_lake.name}/checkpoints"
}

output "models_path" {
  description = "Full GCS path for ML model artifacts"
  value       = "gs://${google_storage_bucket.data_lake.name}/models"
}
