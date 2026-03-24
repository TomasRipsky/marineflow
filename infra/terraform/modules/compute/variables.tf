# =============================================================================
# MARINEFLOW — Compute Module: Variables
# =============================================================================

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone for the VM"
  type        = string
  default     = "us-central1-a"
}

variable "service_account_email" {
  description = "Service account email for the VM — uses ADC automatically"
  type        = string
}

variable "ais_api_key_secret" {
  description = "Secret Manager secret name containing the aisstream.io API key"
  type        = string
  default     = "ais-api-key"
}

variable "pubsub_topic_positions" {
  type    = string
  default = "vessel-positions"
}

variable "pubsub_topic_metadata" {
  type    = string
  default = "vessel-metadata"
}

variable "pubsub_topic_dlq" {
  type    = string
  default = "dead-letter-queue"
}

variable "repo_url" {
  description = "Git repository URL to clone the producer code"
  type        = string
  default     = ""
}

variable "labels" {
  type    = map(string)
  default = {}
}
