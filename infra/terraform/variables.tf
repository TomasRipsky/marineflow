# =============================================================================
# MARINEFLOW — Variables
# infra/terraform/variables.tf
# =============================================================================

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "Primary GCP region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "service_account_email" {
  description = "Email of the Service Account created via gcloud"
  type        = string
}

# GCS
variable "gcs_bucket_name" {
  description = "Base name for the data lake bucket"
  type        = string
  default     = "marineflow-lake"
}

variable "gcs_storage_class" {
  description = "Storage class for GCS buckets"
  type        = string
  default     = "STANDARD"
}

# BigQuery
variable "bq_location" {
  description = "Location for BigQuery datasets"
  type        = string
  default     = "US"
}

# Pub/Sub
variable "pubsub_message_retention_days" {
  description = "Number of days to retain messages in Pub/Sub topics"
  type        = number
  default     = 7
}

variable "pubsub_ack_deadline_seconds" {
  description = "Acknowledgment deadline for Pub/Sub subscriptions in seconds"
  type        = number
  default     = 60
}

variable "repo_url" {
  description = "Git repository URL"
  type        = string
}