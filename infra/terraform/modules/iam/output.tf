# =============================================================================
# MARINEFLOW — IAM Module: Outputs
# infra/terraform/modules/iam/outputs.tf
# =============================================================================

output "service_account_email" {
  description = "Email of the Service Account managed by this module"
  value       = var.service_account_email
}

output "pubsub_publisher_binding" {
  description = "Resource ID of the Pub/Sub publisher IAM binding"
  value       = google_project_iam_member.pubsub_publisher.id
}

output "pubsub_subscriber_binding" {
  description = "Resource ID of the Pub/Sub subscriber IAM binding"
  value       = google_project_iam_member.pubsub_subscriber.id
}

output "storage_object_admin_binding" {
  description = "Resource ID of the Storage object admin IAM binding"
  value       = google_project_iam_member.storage_object_admin.id
}

output "bq_data_editor_binding" {
  description = "Resource ID of the BigQuery data editor IAM binding"
  value       = google_project_iam_member.bq_data_editor.id
}

output "bq_job_user_binding" {
  description = "Resource ID of the BigQuery job user IAM binding"
  value       = google_project_iam_member.bq_job_user.id
}

output "log_writer_binding" {
  description = "Resource ID of the log writer IAM binding"
  value       = google_project_iam_member.log_writer.id
}

output "metric_writer_binding" {
  description = "Resource ID of the metric writer IAM binding"
  value       = google_project_iam_member.metric_writer.id
}
