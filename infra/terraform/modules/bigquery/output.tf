# =============================================================================
# MARINEFLOW — BigQuery Module: Outputs
# infra/terraform/modules/bigquery/outputs.tf
# =============================================================================

# --- Dataset IDs ---

output "dataset_ids" {
  description = "Map of all dataset layer names to their BigQuery dataset IDs"
  value = {
    bronze   = google_bigquery_dataset.bronze.dataset_id
    silver   = google_bigquery_dataset.silver.dataset_id
    gold     = google_bigquery_dataset.gold.dataset_id
    features = google_bigquery_dataset.ml_features.dataset_id
  }
}

output "bronze_dataset_id" {
  description = "BigQuery dataset ID for the Bronze layer"
  value       = google_bigquery_dataset.bronze.dataset_id
}

output "silver_dataset_id" {
  description = "BigQuery dataset ID for the Silver layer"
  value       = google_bigquery_dataset.silver.dataset_id
}

output "gold_dataset_id" {
  description = "BigQuery dataset ID for the Gold layer"
  value       = google_bigquery_dataset.gold.dataset_id
}

output "features_dataset_id" {
  description = "BigQuery dataset ID for the ML Feature Store"
  value       = google_bigquery_dataset.ml_features.dataset_id
}

# --- Table IDs (used by Spark writers and dbt) ---

output "vessel_positions_raw_table" {
  description = "Fully qualified table ID for vessel_positions_raw (project.dataset.table)"
  value       = "${var.project_id}.${google_bigquery_dataset.bronze.dataset_id}.${google_bigquery_table.vessel_positions_raw.table_id}"
}

output "vessel_positions_clean_table" {
  description = "Fully qualified table ID for vessel_positions_clean (project.dataset.table)"
  value       = "${var.project_id}.${google_bigquery_dataset.silver.dataset_id}.${google_bigquery_table.vessel_positions_clean.table_id}"
}

output "maritime_alerts_table" {
  description = "Fully qualified table ID for maritime_alerts (project.dataset.table)"
  value       = "${var.project_id}.${google_bigquery_dataset.gold.dataset_id}.${google_bigquery_table.maritime_alerts.table_id}"
}
