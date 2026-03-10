# =============================================================================
# MARINEFLOW — BigQuery Module: Variables
# infra/terraform/modules/bigquery/variables.tf
# =============================================================================

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "Primary GCP region (used for dataset location fallback)"
  type        = string
  default     = "us-central1"
}

variable "bq_location" {
  description = "BigQuery dataset location — US for multi-region, us-central1 for single region"
  type        = string
  default     = "US"
}

variable "labels" {
  description = "Labels to apply to all BigQuery resources"
  type        = map(string)
  default     = {}
}
