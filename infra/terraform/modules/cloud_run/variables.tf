# =============================================================================
# MARINEFLOW — Cloud Run Module: Variables
# =============================================================================

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region for Cloud Run Jobs"
  type        = string
  default     = "us-central1"
}

variable "service_account_email" {
  description = "Service account email for Cloud Run Jobs"
  type        = string
}

variable "producer_image" {
  description = "Docker image URI for the AIS producer (in Artifact Registry)"
  type        = string
  default     = "us-central1-docker.pkg.dev/marineflow-489815/marineflow/ais-producer:latest"
}

variable "pubsub_topic_positions" {
  description = "Pub/Sub topic for vessel positions"
  type        = string
  default     = "vessel-positions"
}

variable "pubsub_topic_metadata" {
  description = "Pub/Sub topic for vessel metadata"
  type        = string
  default     = "vessel-metadata"
}

variable "pubsub_topic_dlq" {
  description = "Pub/Sub dead letter queue topic"
  type        = string
  default     = "dead-letter-queue"
}

variable "ais_api_key_secret" {
  description = "Secret Manager secret name containing the aisstream.io API key"
  type        = string
  default     = "ais-api-key"
}

variable "labels" {
  description = "Labels to apply to all Cloud Run resources"
  type        = map(string)
  default     = {}
}
