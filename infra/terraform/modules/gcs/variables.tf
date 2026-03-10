# =============================================================================
# MARINEFLOW — GCS Module: Variables
# infra/terraform/modules/gcs/variables.tf
# =============================================================================

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCS bucket location"
  type        = string
  default     = "us-central1"
}

variable "gcs_bucket_name" {
  description = "Base name for the data lake bucket"
  type        = string
  default     = "marineflow-lake"
}

variable "storage_class" {
  description = "Default storage class for the data lake bucket"
  type        = string
  default     = "STANDARD"
}

variable "labels" {
  description = "Labels to apply to all GCS resources"
  type        = map(string)
  default     = {}
}
