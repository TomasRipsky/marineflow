# =============================================================================
# MARINEFLOW — Cloud Run Module: Outputs
# =============================================================================

output "producer_job_name" {
  description = "Cloud Run Job name for the AIS producer"
  value       = google_cloud_run_v2_job.ais_producer.name
}

output "producer_job_id" {
  description = "Full resource ID of the AIS producer Cloud Run Job"
  value       = google_cloud_run_v2_job.ais_producer.id
}

output "artifact_registry_url" {
  description = "Artifact Registry repository URL for pushing producer images"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.marineflow.repository_id}"
}

output "ais_api_key_secret_name" {
  description = "Secret Manager secret name for the aisstream.io API key"
  value       = google_secret_manager_secret.ais_api_key.secret_id
}
