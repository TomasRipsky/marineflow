# =============================================================================
# MARINEFLOW — Pub/Sub Module: Variables
# infra/terraform/modules/pubsub/variables.tf
# =============================================================================

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "message_retention_days" {
  description = "Number of days to retain unacknowledged messages in topics"
  type        = number
  default     = 7
}

variable "ack_deadline_seconds" {
  description = "Acknowledgment deadline for Spark subscriptions in seconds"
  type        = number
  default     = 60
}

variable "labels" {
  description = "Labels to apply to all Pub/Sub resources"
  type        = map(string)
  default     = {}
}

variable "gcs_bucket" {
  description = "GCS bucket where Cloud Storage subscriptions will land Pub/Sub messages"
  type        = string
}

variable "pubsub_sa_email" {
  description = "Pub/Sub service account email — needs Storage Object Creator on the GCS bucket"
  type        = string
  default     = "service-174180607250@gcp-sa-pubsub.iam.gserviceaccount.com"
}
